// Package httpstore implements storage.Storage over net/http: plain HTTP(S)
// servers (public buckets, CDNs, R2 public buckets) and S3-compatible APIs
// with SigV4 request signing.
//
// It is not meant for Cloudflare Workers: linking net/http roughly triples
// the size of a WebAssembly build. Use the cfworker package there.
package httpstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/solomonsealed/icechunk-go/internal/s3req"
	"github.com/solomonsealed/icechunk-go/storage"
)

// Options configures HTTP access.
type Options struct {
	// Client defaults to http.DefaultClient.
	Client *http.Client
	// Header is added to every request (e.g. an Authorization token).
	Header http.Header
	// NotFoundStatus lists extra status codes meaning "object not found".
	// S3 answers 403 for missing keys when the caller may not list the
	// bucket, so anonymous S3 access usually wants []int{403}.
	NotFoundStatus []int
}

func (o *Options) client() *http.Client {
	if o != nil && o.Client != nil {
		return o.Client
	}
	return http.DefaultClient
}

type response struct {
	status             int
	body               []byte
	etag, lastModified string
	header             http.Header
}

func do(ctx context.Context, o *Options, r s3req.Request) (response, error) {
	return doBody(ctx, o, r, nil)
}

func doBody(ctx context.Context, o *Options, r s3req.Request, body []byte) (response, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, r.Method, r.URL, rd)
	if err != nil {
		return response{}, err
	}
	if body != nil {
		req.ContentLength = int64(len(body))
	}
	for k, v := range r.Header {
		req.Header.Set(k, v)
	}
	if o != nil {
		for k, vs := range o.Header {
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
	}
	if ua := storage.UserAgent(ctx); ua != "" {
		if own := req.Header.Get("User-Agent"); own != "" {
			ua = own + " " + ua
		}
		req.Header.Set("User-Agent", ua)
	}
	resp, err := o.client().Do(req)
	if err != nil {
		return response{}, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	return response{resp.StatusCode, respBody, resp.Header.Get("ETag"), resp.Header.Get("Last-Modified"), resp.Header}, err
}

func checkStatus(o *Options, key string, resp response, opts *storage.GetOptions) ([]byte, error) {
	var notFound []int
	if o != nil {
		notFound = o.NotFoundStatus
	}
	data, err := s3req.CheckStatus(key, resp.status, resp.body, opts, notFound)
	if err == nil {
		err = s3req.CheckConditions(key, opts, resp.etag, resp.lastModified)
	}
	if err != nil && !errors.Is(err, storage.ErrNotFound) && !errors.Is(err, storage.ErrPreconditionFailed) {
		return nil, fmt.Errorf("httpstore: %w", err)
	}
	return data, err
}

// HTTP reads objects at baseURL/key with plain GET requests.
type HTTP struct {
	base string
	opts *Options
}

// New returns a storage reading baseURL/<key>.
func New(baseURL string, opts *Options) *HTTP {
	return &HTTP{base: strings.TrimRight(baseURL, "/"), opts: opts}
}

// Get implements storage.Storage.
func (h *HTTP) Get(ctx context.Context, key string, opts *storage.GetOptions) ([]byte, error) {
	r := s3req.Request{Method: "GET", URL: h.base + "/" + s3req.EscapePath(key), Header: s3req.GetHeaders(opts)}
	resp, err := do(ctx, h.opts, r)
	if err != nil {
		return nil, err
	}
	return checkStatus(h.opts, key, resp, opts)
}

// S3 reads from an S3-compatible bucket, signing requests with SigV4.
type S3 struct {
	cfg  storage.S3Config
	opts *Options
}

// NewS3 returns a storage for an S3-compatible bucket prefix.
func NewS3(cfg storage.S3Config, opts *Options) *S3 { return &S3{cfg: cfg, opts: opts} }

// Get implements storage.Storage.
func (s *S3) Get(ctx context.Context, key string, opts *storage.GetOptions) ([]byte, error) {
	r, err := s3req.Get(&s.cfg, key, opts, time.Now())
	if err != nil {
		return nil, err
	}
	resp, err := do(ctx, s.opts, r)
	if err != nil {
		return nil, err
	}
	return checkStatus(s.opts, key, resp, opts)
}

// List implements storage.Lister using ListObjectsV2.
func (s *S3) List(ctx context.Context, prefix string) ([]string, error) {
	var out []string
	token := ""
	for {
		r, err := s3req.List(&s.cfg, prefix, token, time.Now())
		if err != nil {
			return nil, err
		}
		resp, err := do(ctx, s.opts, r)
		if err != nil {
			return nil, err
		}
		if resp.status != http.StatusOK {
			return nil, fmt.Errorf("httpstore: listing %q: status %d: %s", prefix, resp.status, strings.TrimSpace(string(resp.body)))
		}
		keys, next, truncated, err := s3req.ParseList(resp.body)
		if err != nil {
			return nil, err
		}
		for _, k := range keys {
			out = append(out, s3req.StripPrefix(&s.cfg, k))
		}
		if !truncated || next == "" {
			return out, nil
		}
		token = next
	}
}

// GetVersion implements storage.Writer: the version is the object's ETag.
func (s *S3) GetVersion(ctx context.Context, key string) ([]byte, string, error) {
	r, err := s3req.Get(&s.cfg, key, nil, time.Now())
	if err != nil {
		return nil, "", err
	}
	resp, err := do(ctx, s.opts, r)
	if err != nil {
		return nil, "", err
	}
	data, err := checkStatus(s.opts, key, resp, nil)
	if err != nil {
		return nil, "", err
	}
	if resp.etag == "" {
		return nil, "", fmt.Errorf("httpstore: %s: response has no ETag; conditional writes are impossible", key)
	}
	return data, strings.Trim(strings.TrimPrefix(resp.etag, "W/"), `"`), nil
}

// Put implements storage.Writer with PutObject, using If-Match and
// If-None-Match for conditional writes, x-amz-storage-class for the storage
// class and x-amz-meta-* headers for user metadata. A conditional write
// stamped with a write id (storage.WriteIDMetadataKey) that fails with a
// failed condition or a transport error reads the object back: if it holds
// that write id, the write landed and its response was lost.
func (s *S3) Put(ctx context.Context, key string, data []byte, opts *storage.PutOptions) (string, error) {
	r, err := s3req.Put(&s.cfg, key, data, opts, time.Now())
	if err != nil {
		return "", err
	}
	writeID := s3req.ConditionalWriteID(opts)
	resp, err := doBody(ctx, s.opts, r, data)
	if err != nil {
		if writeID != "" {
			if v, ours, rerr := s.readBack(ctx, key, writeID); rerr != nil || ours {
				return v, rerr
			}
		}
		return "", err
	}
	v, err := s3req.CheckPut(key, resp.status, resp.body, resp.etag)
	if errors.Is(err, storage.ErrPreconditionFailed) && writeID != "" {
		if v, ours, rerr := s.readBack(ctx, key, writeID); rerr != nil || ours {
			return v, rerr
		}
	}
	if err != nil && !errors.Is(err, storage.ErrPreconditionFailed) {
		return "", fmt.Errorf("httpstore: %w", err)
	}
	return v, err
}

// readBack reads key's write id after an ambiguous conditional write (see
// s3req.ReadBack).
func (s *S3) readBack(ctx context.Context, key, writeID string) (string, bool, error) {
	r, err := s3req.Head(&s.cfg, key, time.Now())
	if err != nil {
		return "", false, err
	}
	resp, err := do(ctx, s.opts, r)
	if err != nil {
		return "", false, fmt.Errorf("httpstore: reading back %s: %w", key, err)
	}
	v, ours, err := s3req.ReadBack(key, writeID, resp.status, resp.header.Get(s3req.WriteIDHeader), resp.etag)
	if err != nil {
		return "", false, fmt.Errorf("httpstore: %w", err)
	}
	return v, ours, nil
}

// Copy implements storage.Copier with CopyObject.
func (s *S3) Copy(ctx context.Context, src, dst string, opts *storage.PutOptions) (string, error) {
	r, err := s3req.Copy(&s.cfg, src, dst, opts, time.Now())
	if err != nil {
		return "", err
	}
	resp, err := do(ctx, s.opts, r)
	if err != nil {
		return "", err
	}
	v, err := s3req.CheckCopy(src, dst, resp.status, resp.body)
	if err != nil && !errors.Is(err, storage.ErrPreconditionFailed) && !errors.Is(err, storage.ErrNotFound) {
		return "", fmt.Errorf("httpstore: %w", err)
	}
	return v, err
}

// Delete implements storage.Writer.
func (s *S3) Delete(ctx context.Context, key string) error {
	r, err := s3req.Delete(&s.cfg, key, time.Now())
	if err != nil {
		return err
	}
	resp, err := do(ctx, s.opts, r)
	if err != nil {
		return err
	}
	if resp.status != 200 && resp.status != 204 && resp.status != 404 {
		return fmt.Errorf("httpstore: DELETE %s: status %d", key, resp.status)
	}
	return nil
}

// LastModified implements storage.ModTimer with HeadObject.
func (s *S3) LastModified(ctx context.Context, key string) (time.Time, error) {
	r, err := s3req.Head(&s.cfg, key, time.Now())
	if err != nil {
		return time.Time{}, err
	}
	resp, err := do(ctx, s.opts, r)
	if err != nil {
		return time.Time{}, err
	}
	return s3req.ParseLastModified(key, resp.status, resp.lastModified)
}
