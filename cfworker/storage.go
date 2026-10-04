//go:build js && wasm

package cfworker

import (
	"context"
	"fmt"
	"strings"
	"syscall/js"
	"time"

	"github.com/solomonsealed/icechunk-go/internal/s3req"
	"github.com/solomonsealed/icechunk-go/storage"
)

// R2 reads objects from an R2 bucket binding (env.MY_BUCKET).
type R2 struct {
	bucket js.Value
	prefix string
}

// NewR2 returns a storage for the repository stored under prefix in the R2
// bucket bound as binding. As in storage.S3Config, the prefix "/" reads
// legacy bucket-root repositories whose keys start with a slash.
func NewR2(binding js.Value, prefix string) *R2 {
	if prefix != "/" {
		prefix = strings.Trim(prefix, "/")
		if prefix != "" {
			prefix += "/"
		}
	}
	return &R2{bucket: binding, prefix: prefix}
}

func (r *R2) check() error {
	if r.bucket.Type() != js.TypeObject || r.bucket.Get("get").Type() != js.TypeFunction {
		return fmt.Errorf("r2: the bucket binding is missing or is not an R2 bucket (check wrangler.toml)")
	}
	return nil
}

// Get implements storage.Storage using R2Bucket.get with range and onlyIf.
func (r *R2) Get(ctx context.Context, key string, opts *storage.GetOptions) ([]byte, error) {
	if err := r.check(); err != nil {
		return nil, err
	}
	o := jsObject.New()
	conditional := false
	if opts != nil {
		if rg := opts.Range; rg != nil {
			jr := jsObject.New()
			jr.Set("offset", rg.Offset)
			jr.Set("length", rg.Length)
			o.Set("range", jr)
		}
		if opts.IfMatch != "" || !opts.IfUnmodifiedSince.IsZero() {
			conditional = true
			c := jsObject.New()
			if opts.IfMatch != "" {
				c.Set("etagMatches", strings.Trim(opts.IfMatch, `"`))
			}
			if t := opts.IfUnmodifiedSince; !t.IsZero() {
				// Second-resolution "not modified after t", as R2 compares
				// millisecond upload times strictly.
				c.Set("uploadedBefore", jsDate.New(float64(t.Truncate(time.Second).Add(time.Second).UnixMilli())))
			}
			o.Set("onlyIf", c)
		}
	}
	obj, err := Await(ctx, r.bucket.Call("get", r.prefix+key, o))
	if err != nil {
		return nil, fmt.Errorf("r2: get %s: %w", key, err)
	}
	if obj.IsNull() || obj.IsUndefined() {
		return nil, fmt.Errorf("%w: %s", storage.ErrNotFound, key)
	}
	// A failed onlyIf returns the object's metadata without a body.
	if obj.Get("arrayBuffer").IsUndefined() {
		if conditional {
			return nil, fmt.Errorf("%w: %s", storage.ErrPreconditionFailed, key)
		}
		return nil, fmt.Errorf("r2: get %s: response has no body", key)
	}
	buf, err := Await(ctx, obj.Call("arrayBuffer"))
	if err != nil {
		return nil, fmt.Errorf("r2: reading %s: %w", key, err)
	}
	data := bytesOf(buf)
	if opts != nil && opts.Range != nil && int64(len(data)) != opts.Range.Length {
		return nil, fmt.Errorf("r2: %s: asked for %d bytes, got %d", key, opts.Range.Length, len(data))
	}
	return data, nil
}

// List implements storage.Lister using R2Bucket.list.
func (r *R2) List(ctx context.Context, prefix string) ([]string, error) {
	if err := r.check(); err != nil {
		return nil, err
	}
	var out []string
	cursor := ""
	for {
		o := jsObject.New()
		o.Set("prefix", r.prefix+prefix)
		if cursor != "" {
			o.Set("cursor", cursor)
		}
		res, err := Await(ctx, r.bucket.Call("list", o))
		if err != nil {
			return nil, fmt.Errorf("r2: list %s: %w", prefix, err)
		}
		objs := res.Get("objects")
		for i := 0; i < objs.Length(); i++ {
			out = append(out, strings.TrimPrefix(objs.Index(i).Get("key").String(), r.prefix))
		}
		if !res.Get("truncated").Truthy() {
			return out, nil
		}
		cursor = res.Get("cursor").String()
	}
}

type response struct {
	status             int
	body               []byte
	etag, lastModified string
}

// fetch performs a request with the Workers fetch API.
func fetch(ctx context.Context, r s3req.Request, extra map[string]string) (response, error) {
	init := jsObject.New()
	init.Set("method", r.Method)
	h := jsObject.New()
	for k, v := range extra {
		h.Set(k, v)
	}
	for k, v := range r.Header {
		h.Set(k, v)
	}
	init.Set("headers", h)
	resp, err := Await(ctx, js.Global().Call("fetch", r.URL, init))
	if err != nil {
		return response{}, err
	}
	buf, err := Await(ctx, resp.Call("arrayBuffer"))
	if err != nil {
		return response{}, err
	}
	header := func(name string) string {
		v := resp.Get("headers").Call("get", name)
		if v.Type() != js.TypeString {
			return ""
		}
		return v.String()
	}
	return response{resp.Get("status").Int(), bytesOf(buf), header("etag"), header("last-modified")}, nil
}

func checkResponse(key string, resp response, opts *storage.GetOptions, notFound []int) ([]byte, error) {
	data, err := s3req.CheckStatus(key, resp.status, resp.body, opts, notFound)
	if err == nil {
		err = s3req.CheckConditions(key, opts, resp.etag, resp.lastModified)
	}
	return data, err
}

// HTTP reads objects at baseURL/<key> with fetch (public buckets, R2
// public/custom domains, any static file server supporting Range).
type HTTP struct {
	base     string
	header   map[string]string
	notFound []int
}

// NewHTTP returns a fetch-based storage. header is sent with every request
// (e.g. {"authorization": "Bearer ..."}); it may be nil.
func NewHTTP(baseURL string, header map[string]string) *HTTP {
	return &HTTP{base: strings.TrimRight(baseURL, "/"), header: header}
}

// Get implements storage.Storage.
func (h *HTTP) Get(ctx context.Context, key string, opts *storage.GetOptions) ([]byte, error) {
	r := s3req.Request{Method: "GET", URL: h.base + "/" + s3req.EscapePath(key), Header: s3req.GetHeaders(opts)}
	resp, err := fetch(ctx, r, h.header)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", key, err)
	}
	return checkResponse(key, resp, opts, h.notFound)
}

// S3 reads from an S3-compatible bucket with SigV4-signed fetch requests.
type S3 struct {
	cfg      storage.S3Config
	notFound []int
}

// NewS3 returns a fetch-based storage for an S3-compatible bucket prefix.
// Anonymous S3 access answers 403 for missing keys, so it is treated as
// "not found" when cfg.Anonymous is set.
func NewS3(cfg storage.S3Config) *S3 {
	s := &S3{cfg: cfg}
	if cfg.Anonymous {
		s.notFound = []int{403}
	}
	return s
}

// Get implements storage.Storage.
func (s *S3) Get(ctx context.Context, key string, opts *storage.GetOptions) ([]byte, error) {
	r, err := s3req.Get(&s.cfg, key, opts, time.Now())
	if err != nil {
		return nil, err
	}
	resp, err := fetch(ctx, r, nil)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", key, err)
	}
	return checkResponse(key, resp, opts, s.notFound)
}

// List implements storage.Lister with ListObjectsV2.
func (s *S3) List(ctx context.Context, prefix string) ([]string, error) {
	var out []string
	token := ""
	for {
		r, err := s3req.List(&s.cfg, prefix, token, time.Now())
		if err != nil {
			return nil, err
		}
		resp, err := fetch(ctx, r, nil)
		if err != nil {
			return nil, err
		}
		if resp.status != 200 {
			return nil, fmt.Errorf("s3: listing %q: status %d: %s", prefix, resp.status, resp.body)
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
