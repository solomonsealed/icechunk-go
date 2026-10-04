// Package httpstore implements storage.Storage over net/http: plain HTTP(S)
// servers (public buckets, CDNs, R2 public buckets) and S3-compatible APIs
// with SigV4 request signing.
//
// It is not meant for Cloudflare Workers: linking net/http roughly triples
// the size of a WebAssembly build. Use the cfworker package there.
package httpstore

import (
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
}

func do(ctx context.Context, o *Options, r s3req.Request) (response, error) {
	req, err := http.NewRequestWithContext(ctx, r.Method, r.URL, nil)
	if err != nil {
		return response{}, err
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
	resp, err := o.client().Do(req)
	if err != nil {
		return response{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return response{resp.StatusCode, body, resp.Header.Get("ETag"), resp.Header.Get("Last-Modified")}, err
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
