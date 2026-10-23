// Package s3req builds (and SigV4-signs) the HTTP requests needed to read
// from S3-compatible object stores, independent of any HTTP client. It is
// shared by storage/httpstore (net/http) and cfworker (JS fetch).
package s3req

import (
	"bytes"
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
	signPayload(c, r, host, path, query, emptySHA256, now)
}

// signPayload signs a request whose body has the given SHA-256 (hex).
func signPayload(c *Config, r *Request, host, path, query, payloadHash string, now time.Time) {
	if c.Anonymous || c.AccessKeyID == "" {
		return
	}
	now = now.UTC()
	amzDate := now.Format("20060102T150405Z")
	date := amzDate[:8]
	r.Header["x-amz-date"] = amzDate
	r.Header["x-amz-content-sha256"] = payloadHash
	if c.SessionToken != "" {
		r.Header["x-amz-security-token"] = c.SessionToken
	}
	// S3 refuses requests with x-amz-* headers that are not signed (such as
	// x-amz-storage-class and x-amz-meta-*), so all of them are.
	signed := map[string]string{"host": host}
	for k, v := range r.Header {
		if strings.HasPrefix(k, "x-amz-") {
			signed[k] = v
		}
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
		canonHeaders.WriteString(strings.Join(strings.Fields(signed[k]), " "))
		canonHeaders.WriteString("\n")
	}
	signedHeaders := strings.Join(names, ";")
	canonical := strings.Join([]string{r.Method, path, query, canonHeaders.String(), signedHeaders, payloadHash}, "\n")
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

// Put builds a PutObject request for key; the caller sends body with it.
// Conditional writes use If-Match / If-None-Match: *, supported by AWS S3,
// Cloudflare R2, MinIO and most S3-compatible stores. The storage class
// and user metadata go in x-amz-storage-class and x-amz-meta-* headers.
func Put(c *Config, key string, body []byte, opts *storage.PutOptions, now time.Time) (Request, error) {
	base, host, path, err := target(c, fullKey(c, key))
	if err != nil {
		return Request{}, err
	}
	r := Request{Method: "PUT", URL: base + path, Header: map[string]string{"content-type": "application/octet-stream"}}
	if opts != nil {
		if opts.IfMatch != "" {
			r.Header["if-match"] = `"` + stripETag(opts.IfMatch) + `"`
		}
		if opts.IfNotExists {
			r.Header["if-none-match"] = "*"
		}
		if opts.StorageClass != "" {
			r.Header["x-amz-storage-class"] = opts.StorageClass
		}
		for k, v := range opts.Metadata {
			r.Header[metadataPrefix+strings.ToLower(k)] = v
		}
	}
	sum := sha256.Sum256(body)
	if !c.Anonymous && c.AccessKeyID != "" {
		signPayload(c, &r, host, path, "", hex.EncodeToString(sum[:]), now)
	}
	return r, nil
}

// Copy builds a CopyObject request copying src to dst, keeping src's user
// metadata (S3's default metadata directive), with opts's storage class
// and, if opts.IfMatch is set, only if src's ETag is IfMatch.
func Copy(c *Config, src, dst string, opts *storage.PutOptions, now time.Time) (Request, error) {
	base, host, path, err := target(c, fullKey(c, dst))
	if err != nil {
		return Request{}, err
	}
	r := Request{Method: "PUT", URL: base + path, Header: map[string]string{
		"x-amz-copy-source": "/" + c.Bucket + "/" + EscapePath(fullKey(c, src)),
	}}
	if opts != nil {
		if opts.IfMatch != "" {
			r.Header["x-amz-copy-source-if-match"] = `"` + stripETag(opts.IfMatch) + `"`
		}
		if opts.StorageClass != "" {
			r.Header["x-amz-storage-class"] = opts.StorageClass
		}
	}
	sign(c, &r, host, path, "", now)
	return r, nil
}

// CheckCopy maps a CopyObject response to the copy's version (the ETag in
// its CopyObjectResult) or an error: a failed x-amz-copy-source-if-match is
// 412, and S3 may report a failed copy in a 200 response's body.
func CheckCopy(src, dst string, status int, body []byte) (string, error) {
	switch {
	case status == 412:
		return "", fmt.Errorf("%w: %s", storage.ErrPreconditionFailed, src)
	case status == 404:
		return "", fmt.Errorf("%w: %s", storage.ErrNotFound, src)
	case status == 200 && !bytes.Contains(body, []byte("<Error>")):
		var res struct {
			ETag string `xml:"ETag"`
		}
		if xml.Unmarshal(body, &res) == nil && res.ETag != "" {
			return stripETag(res.ETag), nil
		}
		return "", fmt.Errorf("copy %s to %s: no ETag in the response", src, dst)
	}
	msg := strings.TrimSpace(string(body))
	if len(msg) > 300 {
		msg = msg[:300] + "..."
	}
	return "", fmt.Errorf("copy %s to %s: status %d: %s", src, dst, status, msg)
}

// Delete builds a DeleteObject request for key.
func Delete(c *Config, key string, now time.Time) (Request, error) {
	base, host, path, err := target(c, fullKey(c, key))
	if err != nil {
		return Request{}, err
	}
	r := Request{Method: "DELETE", URL: base + path, Header: map[string]string{}}
	sign(c, &r, host, path, "", now)
	return r, nil
}

// CheckPut maps a PutObject response to the new version (ETag) or an
// error. Failed conditions are 412 (or 409 when S3 sees a concurrent
// conditional write); both mean "retry with fresh data".
func CheckPut(key string, status int, body []byte, etag string) (string, error) {
	switch {
	case status == 200 || status == 201 || status == 204:
		return stripETag(etag), nil
	case status == 412 || status == 409:
		return "", fmt.Errorf("%w: %s", storage.ErrPreconditionFailed, key)
	}
	msg := strings.TrimSpace(string(body))
	if len(msg) > 300 {
		msg = msg[:300] + "..."
	}
	return "", fmt.Errorf("PUT %s: status %d: %s", key, status, msg)
}

// metadataPrefix starts the headers that carry an object's user metadata.
const metadataPrefix = "x-amz-meta-"

// WriteIDHeader is the response header holding an object's write id
// (storage.WriteIDMetadataKey).
const WriteIDHeader = metadataPrefix + storage.WriteIDMetadataKey

// ConditionalWriteID returns the write id a conditional write stamps on the
// object (storage.WriteIDMetadataKey in its metadata), or "" for
// unconditional writes and those without one.
func ConditionalWriteID(opts *storage.PutOptions) string {
	if opts == nil || opts.IfMatch == "" && !opts.IfNotExists {
		return ""
	}
	return opts.Metadata[storage.WriteIDMetadataKey]
}

// ReadBack classifies the HeadObject response read after a conditional
// write stamped with writeID failed with a failed condition or a transport
// error, whose response may have been lost, as upstream does
// (icechunk-storage readback.rs): ours reports that the stored object is
// that write, whose version is its ETag. A missing object, or one with
// another write id or none, is not ours. err reports a failed read-back
// (any other status), or our object without an ETag; callers return it
// rather than guess the write's outcome.
func ReadBack(key, writeID string, status int, storedID, etag string) (version string, ours bool, err error) {
	switch {
	case status == 404:
		return "", false, nil
	case status != 200:
		return "", false, fmt.Errorf("HEAD %s: status %d, reading back a conditional write", key, status)
	case storedID != writeID:
		return "", false, nil
	case etag == "":
		return "", false, fmt.Errorf("HEAD %s: the conditional write landed, but the object has no ETag", key)
	}
	return stripETag(etag), true, nil
}

// Head builds a HeadObject request for key.
func Head(c *Config, key string, now time.Time) (Request, error) {
	base, host, path, err := target(c, fullKey(c, key))
	if err != nil {
		return Request{}, err
	}
	r := Request{Method: "HEAD", URL: base + path, Header: map[string]string{}}
	sign(c, &r, host, path, "", now)
	return r, nil
}

// ParseLastModified parses an HTTP Last-Modified header.
func ParseLastModified(key string, status int, header string) (time.Time, error) {
	switch {
	case status == 404:
		return time.Time{}, fmt.Errorf("%w: %s", storage.ErrNotFound, key)
	case status != 200:
		return time.Time{}, fmt.Errorf("HEAD %s: status %d", key, status)
	}
	t, err := time.Parse(httpTimeFormat, header)
	if err != nil {
		return time.Time{}, fmt.Errorf("HEAD %s: no usable Last-Modified header %q", key, header)
	}
	return t, nil
}
