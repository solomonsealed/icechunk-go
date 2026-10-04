// Package s3req builds (and SigV4-signs) the HTTP requests needed to read
// from S3-compatible object stores, independent of any HTTP client. It is
// shared by storage/httpstore (net/http) and cfworker (JS fetch).
package s3req

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/solomonsealed/icechunk-go/storage"
)

// Config is the bucket description shared with the public API.
type Config = storage.S3Config

// Request is an HTTP request ready to send.
type Request struct {
	Method string
	URL    string
	Header map[string]string
}

// httpTimeFormat is net/http.TimeFormat, copied to avoid linking net/http.
const httpTimeFormat = "Mon, 02 Jan 2006 15:04:05 GMT"

const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func region(c *Config) string {
	if c.Region == "" {
		return "us-east-1"
	}
	return c.Region
}

// EscapePath percent-encodes every byte except RFC 3986 unreserved
// characters and '/', as SigV4 canonical URIs require. It is also a safe
// encoding of object keys in plain HTTP URLs.
func EscapePath(p string) string {
	var b strings.Builder
	for i := 0; i < len(p); i++ {
		ch := p[i]
		if ch == '/' || ch == '-' || ch == '_' || ch == '.' || ch == '~' ||
			('A' <= ch && ch <= 'Z') || ('a' <= ch && ch <= 'z') || ('0' <= ch && ch <= '9') {
			b.WriteByte(ch)
		} else {
			fmt.Fprintf(&b, "%%%02X", ch)
		}
	}
	return b.String()
}

func escapeQuery(s string) string {
	return strings.ReplaceAll(EscapePath(s), "/", "%2F")
}

// target returns scheme://host and the escaped request path for key.
func target(c *Config, key string) (base, host, path string, err error) {
	objKey := key
	if c.Endpoint != "" {
		u, err := url.Parse(strings.TrimRight(c.Endpoint, "/"))
		if err != nil {
			return "", "", "", fmt.Errorf("s3: invalid endpoint %q: %w", c.Endpoint, err)
		}
		basePath := strings.TrimRight(u.Path, "/")
		// Custom endpoints (R2, MinIO, GCS interop, ...) are addressed
		// path-style unless the bucket is already part of the host name.
		if !strings.HasPrefix(u.Host, c.Bucket+".") {
			basePath += "/" + c.Bucket
		}
		return u.Scheme + "://" + u.Host, u.Host, EscapePath(basePath + "/" + objKey), nil
	}
	if c.ForcePathStyle {
		host = "s3." + region(c) + ".amazonaws.com"
		return "https://" + host, host, EscapePath("/" + c.Bucket + "/" + objKey), nil
	}
	host = c.Bucket + ".s3." + region(c) + ".amazonaws.com"
	return "https://" + host, host, EscapePath("/" + objKey), nil
}

func fullKey(c *Config, key string) string {
	if c.Prefix == "/" {
		return "/" + key // legacy root layout, see storage.S3Config.Prefix
	}
	p := strings.Trim(c.Prefix, "/")
	if p == "" {
		return key
	}
	return p + "/" + key
}

// Get builds a GetObject request for key (relative to Prefix).
func Get(c *Config, key string, opts *storage.GetOptions, now time.Time) (Request, error) {
	base, host, path, err := target(c, fullKey(c, key))
	if err != nil {
		return Request{}, err
	}
	r := Request{Method: "GET", URL: base + path, Header: GetHeaders(opts)}
	sign(c, &r, host, path, "", now)
	return r, nil
}

// List builds a ListObjectsV2 request for keys under prefix (relative to
// Prefix), continuing from token when non-empty.
func List(c *Config, prefix, token string, now time.Time) (Request, error) {
	base, host, path, err := target(c, "")
	if err != nil {
		return Request{}, err
	}
	q := map[string]string{"list-type": "2", "prefix": fullKey(c, prefix)}
	if token != "" {
		q["continuation-token"] = token
	}
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = escapeQuery(k) + "=" + escapeQuery(q[k])
	}
	query := strings.Join(parts, "&")
	// The bucket root path must end with "/" for path-style listing.
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	r := Request{Method: "GET", URL: base + path + "?" + query, Header: map[string]string{}}
	sign(c, &r, host, path, query, now)
	return r, nil
}

// StripPrefix converts a listed bucket key into a key relative to Prefix.
func StripPrefix(c *Config, key string) string {
	if c.Prefix == "/" {
		return strings.TrimPrefix(key, "/")
	}
	p := strings.Trim(c.Prefix, "/")
	if p == "" {
		return key
	}
	return strings.TrimPrefix(key, p+"/")
}

func sign(c *Config, r *Request, host, path, query string, now time.Time) {
	if c.Anonymous || c.AccessKeyID == "" {
		return
	}
	now = now.UTC()
	amzDate := now.Format("20060102T150405Z")
	date := amzDate[:8]
	r.Header["x-amz-date"] = amzDate
	r.Header["x-amz-content-sha256"] = emptySHA256
	signed := map[string]string{
		"host":                 host,
		"x-amz-date":           amzDate,
		"x-amz-content-sha256": emptySHA256,
	}
	if c.SessionToken != "" {
		r.Header["x-amz-security-token"] = c.SessionToken
		signed["x-amz-security-token"] = c.SessionToken
	}
	names := make([]string, 0, len(signed))
	for k := range signed {
		names = append(names, k)
	}
	sort.Strings(names)
	var canonHeaders strings.Builder
	for _, k := range names {
		canonHeaders.WriteString(k)
		canonHeaders.WriteString(":")
		canonHeaders.WriteString(strings.TrimSpace(signed[k]))
		canonHeaders.WriteString("\n")
	}
	signedHeaders := strings.Join(names, ";")
	canonical := strings.Join([]string{r.Method, path, query, canonHeaders.String(), signedHeaders, emptySHA256}, "\n")
	scope := date + "/" + region(c) + "/s3/aws4_request"
	sum := sha256.Sum256([]byte(canonical))
	toSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(sum[:])
	key := hmacSHA256([]byte("AWS4"+c.SecretAccessKey), date)
	key = hmacSHA256(key, region(c))
	key = hmacSHA256(key, "s3")
	key = hmacSHA256(key, "aws4_request")
	sig := hex.EncodeToString(hmacSHA256(key, toSign))
	r.Header["authorization"] = "AWS4-HMAC-SHA256 Credential=" + c.AccessKeyID + "/" + scope +
		", SignedHeaders=" + signedHeaders + ", Signature=" + sig
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

// GetHeaders returns the Range and conditional headers for a read.
func GetHeaders(opts *storage.GetOptions) map[string]string {
	h := map[string]string{}
	if opts == nil {
		return h
	}
	if r := opts.Range; r != nil {
		h["range"] = "bytes=" + strconv.FormatInt(r.Offset, 10) + "-" + strconv.FormatInt(r.Offset+r.Length-1, 10)
	}
	if opts.IfMatch != "" {
		etag := opts.IfMatch
		if !strings.HasPrefix(etag, "\"") && !strings.HasPrefix(etag, "W/") {
			etag = "\"" + etag + "\""
		}
		h["if-match"] = etag
	}
	if !opts.IfUnmodifiedSince.IsZero() {
		h["if-unmodified-since"] = opts.IfUnmodifiedSince.UTC().Format(httpTimeFormat)
	}
	return h
}

// ParseList decodes a ListObjectsV2 response.
func ParseList(body []byte) (keys []string, next string, truncated bool, err error) {
	var res struct {
		Contents []struct {
			Key string `xml:"Key"`
		} `xml:"Contents"`
		IsTruncated           bool   `xml:"IsTruncated"`
		NextContinuationToken string `xml:"NextContinuationToken"`
	}
	if err := xml.Unmarshal(body, &res); err != nil {
		return nil, "", false, fmt.Errorf("s3: decoding list response: %w", err)
	}
	for _, c := range res.Contents {
		keys = append(keys, c.Key)
	}
	return keys, res.NextContinuationToken, res.IsTruncated, nil
}

// SliceRange extracts the requested range from a full-object body, for
// servers that ignore the Range header and answer 200 with everything.
func SliceRange(body []byte, opts *storage.GetOptions) ([]byte, error) {
	if opts == nil || opts.Range == nil {
		return body, nil
	}
	r := opts.Range
	if r.Offset < 0 || r.Offset+r.Length > int64(len(body)) {
		return nil, fmt.Errorf("range [%d, %d) outside object of %d bytes", r.Offset, r.Offset+r.Length, len(body))
	}
	return body[r.Offset : r.Offset+r.Length], nil
}

// CheckStatus maps an HTTP response to the object bytes or a storage error.
// notFound lists extra statuses meaning "missing" (S3 answers 403 for
// missing keys when the caller may not list the bucket).
func CheckStatus(key string, status int, body []byte, opts *storage.GetOptions, notFound []int) ([]byte, error) {
	isNotFound := status == 404
	for _, s := range notFound {
		isNotFound = isNotFound || status == s
	}
	switch {
	case isNotFound:
		return nil, fmt.Errorf("%w: %s", storage.ErrNotFound, key)
	case status == 412:
		return nil, fmt.Errorf("%w: %s", storage.ErrPreconditionFailed, key)
	case status == 206:
		return body, nil
	case status == 200:
		return SliceRange(body, opts)
	}
	msg := strings.TrimSpace(string(body))
	if len(msg) > 300 {
		msg = msg[:300] + "..."
	}
	return nil, fmt.Errorf("GET %s: status %d: %s", key, status, msg)
}

// CheckConditions verifies a response against the conditional read options,
// as upstream does for virtual chunks: servers may ignore If-Match and
// If-Unmodified-Since, so a required ETag or Last-Modified header that is
// missing or does not match fails the read.
func CheckConditions(key string, opts *storage.GetOptions, etag, lastModified string) error {
	if opts == nil {
		return nil
	}
	if opts.IfMatch != "" && (etag == "" || stripETag(etag) != stripETag(opts.IfMatch)) {
		return fmt.Errorf("%w: %s (etag %q, want %q)", storage.ErrPreconditionFailed, key, etag, opts.IfMatch)
	}
	if !opts.IfUnmodifiedSince.IsZero() {
		t, err := time.Parse(httpTimeFormat, lastModified)
		if err != nil || t.After(opts.IfUnmodifiedSince) {
			return fmt.Errorf("%w: %s (last modified %q)", storage.ErrPreconditionFailed, key, lastModified)
		}
	}
	return nil
}

func stripETag(s string) string {
	return strings.Trim(strings.TrimPrefix(s, "W/"), `"`)
}
