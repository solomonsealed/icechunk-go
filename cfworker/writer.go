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

// GetVersion implements storage.Writer: the version is the object's ETag.
func (r *R2) GetVersion(ctx context.Context, key string) ([]byte, string, error) {
	if err := r.check(); err != nil {
		return nil, "", err
	}
	obj, err := Await(ctx, r.bucket.Call("get", r.prefix+key))
	if err != nil {
		return nil, "", fmt.Errorf("r2: get %s: %w", key, err)
	}
	if obj.IsNull() || obj.IsUndefined() {
		return nil, "", fmt.Errorf("%w: %s", storage.ErrNotFound, key)
	}
	buf, err := Await(ctx, obj.Call("arrayBuffer"))
	if err != nil {
		return nil, "", fmt.Errorf("r2: reading %s: %w", key, err)
	}
	return bytesOf(buf), obj.Get("etag").String(), nil
}

// Put implements storage.Writer with R2Bucket.put; conditions are passed as
// onlyIf headers, and R2 answers null when they fail.
func (r *R2) Put(ctx context.Context, key string, data []byte, opts *storage.PutOptions) (string, error) {
	if err := r.check(); err != nil {
		return "", err
	}
	o := jsObject.New()
	if opts != nil && (opts.IfMatch != "" || opts.IfNotExists) {
		h := js.Global().Get("Headers").New()
		if opts.IfMatch != "" {
			h.Call("set", "if-match", `"`+strings.Trim(opts.IfMatch, `"`)+`"`)
		}
		if opts.IfNotExists {
			h.Call("set", "if-none-match", "*")
		}
		o.Set("onlyIf", h)
	}
	obj, err := Await(ctx, r.bucket.Call("put", r.prefix+key, toUint8Array(data), o))
	if err != nil {
		return "", fmt.Errorf("r2: put %s: %w", key, err)
	}
	if obj.IsNull() || obj.IsUndefined() {
		return "", fmt.Errorf("%w: %s", storage.ErrPreconditionFailed, key)
	}
	return obj.Get("etag").String(), nil
}

// Delete implements storage.Writer.
func (r *R2) Delete(ctx context.Context, key string) error {
	if err := r.check(); err != nil {
		return err
	}
	_, err := Await(ctx, r.bucket.Call("delete", r.prefix+key))
	return err
}

// fetchBody performs a request with a body.
func fetchBody(ctx context.Context, r s3req.Request, body []byte) (response, error) {
	init := jsObject.New()
	init.Set("method", r.Method)
	h := jsObject.New()
	for k, v := range r.Header {
		h.Set(k, v)
	}
	init.Set("headers", h)
	if body != nil {
		init.Set("body", toUint8Array(body))
	}
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
	return response{status: resp.Get("status").Int(), body: bytesOf(buf), etag: header("etag"), lastModified: header("last-modified")}, nil
}

// GetVersion implements storage.Writer: the version is the object's ETag.
func (s *S3) GetVersion(ctx context.Context, key string) ([]byte, string, error) {
	r, err := s3req.Get(&s.cfg, key, nil, time.Now())
	if err != nil {
		return nil, "", err
	}
	resp, err := fetch(ctx, r, nil)
	if err != nil {
		return nil, "", fmt.Errorf("fetch %s: %w", key, err)
	}
	data, err := checkResponse(key, resp, nil, s.notFound)
	if err != nil {
		return nil, "", err
	}
	if resp.etag == "" {
		return nil, "", fmt.Errorf("s3: %s: response has no ETag; conditional writes are impossible", key)
	}
	return data, strings.Trim(strings.TrimPrefix(resp.etag, "W/"), `"`), nil
}

// Put implements storage.Writer with a SigV4-signed PutObject.
func (s *S3) Put(ctx context.Context, key string, data []byte, opts *storage.PutOptions) (string, error) {
	r, err := s3req.Put(&s.cfg, key, data, opts, time.Now())
	if err != nil {
		return "", err
	}
	resp, err := fetchBody(ctx, r, data)
	if err != nil {
		return "", fmt.Errorf("fetch PUT %s: %w", key, err)
	}
	return s3req.CheckPut(key, resp.status, resp.body, resp.etag)
}

// Delete implements storage.Writer.
func (s *S3) Delete(ctx context.Context, key string) error {
	r, err := s3req.Delete(&s.cfg, key, time.Now())
	if err != nil {
		return err
	}
	resp, err := fetchBody(ctx, r, nil)
	if err != nil {
		return err
	}
	if resp.status != 200 && resp.status != 204 && resp.status != 404 {
		return fmt.Errorf("s3: DELETE %s: status %d", key, resp.status)
	}
	return nil
}

// LastModified implements storage.ModTimer with R2Bucket.head.
func (r *R2) LastModified(ctx context.Context, key string) (time.Time, error) {
	if err := r.check(); err != nil {
		return time.Time{}, err
	}
	obj, err := Await(ctx, r.bucket.Call("head", r.prefix+key))
	if err != nil {
		return time.Time{}, fmt.Errorf("r2: head %s: %w", key, err)
	}
	if obj.IsNull() || obj.IsUndefined() {
		return time.Time{}, fmt.Errorf("%w: %s", storage.ErrNotFound, key)
	}
	return time.UnixMilli(int64(obj.Get("uploaded").Call("getTime").Float())), nil
}

// LastModified implements storage.ModTimer with HeadObject.
func (s *S3) LastModified(ctx context.Context, key string) (time.Time, error) {
	r, err := s3req.Head(&s.cfg, key, time.Now())
	if err != nil {
		return time.Time{}, err
	}
	resp, err := fetchBody(ctx, r, nil)
	if err != nil {
		return time.Time{}, err
	}
	return s3req.ParseLastModified(key, resp.status, resp.lastModified)
}
