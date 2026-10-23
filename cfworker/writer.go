//go:build js && wasm

package cfworker

import (
	"context"
	"errors"
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
// onlyIf headers, and R2 answers null when they fail. The storage class and
// user metadata go in its storageClass and customMetadata options. A
// conditional write stamped with a write id (storage.WriteIDMetadataKey)
// that fails reads the object back: if it holds that write id, the write
// landed and its response was lost.
func (r *R2) Put(ctx context.Context, key string, data []byte, opts *storage.PutOptions) (string, error) {
	if err := r.check(); err != nil {
		return "", err
	}
	o := jsObject.New()
	if opts != nil {
		if opts.IfMatch != "" || opts.IfNotExists {
			h := js.Global().Get("Headers").New()
			if opts.IfMatch != "" {
				h.Call("set", "if-match", `"`+strings.Trim(opts.IfMatch, `"`)+`"`)
			}
			if opts.IfNotExists {
				h.Call("set", "if-none-match", "*")
			}
			o.Set("onlyIf", h)
		}
		if opts.StorageClass != "" {
			o.Set("storageClass", opts.StorageClass)
		}
		if len(opts.Metadata) > 0 {
			m := jsObject.New()
			for k, v := range opts.Metadata {
				m.Set(k, v)
			}
			o.Set("customMetadata", m)
		}
	}
	writeID := s3req.ConditionalWriteID(opts)
	obj, err := Await(ctx, r.bucket.Call("put", r.prefix+key, toUint8Array(data), o))
	if err != nil {
		if writeID != "" {
			if v, ours, rerr := r.readBack(ctx, key, writeID); rerr != nil || ours {
				return v, rerr
			}
		}
		return "", fmt.Errorf("r2: put %s: %w", key, err)
	}
	if obj.IsNull() || obj.IsUndefined() {
		if writeID != "" {
			if v, ours, rerr := r.readBack(ctx, key, writeID); rerr != nil || ours {
				return v, rerr
			}
		}
		return "", fmt.Errorf("%w: %s", storage.ErrPreconditionFailed, key)
	}
	return obj.Get("etag").String(), nil
}

// readBack reads key's write id after an ambiguous conditional write: ours
// reports that the object holds writeID, with version its ETag.
func (r *R2) readBack(ctx context.Context, key, writeID string) (version string, ours bool, err error) {
	obj, err := Await(ctx, r.bucket.Call("head", r.prefix+key))
	if err != nil {
		return "", false, fmt.Errorf("r2: reading back %s: %w", key, err)
	}
	if obj.IsNull() || obj.IsUndefined() {
		return "", false, nil
	}
	stored := obj.Get("customMetadata")
	if stored.Type() != js.TypeObject {
		return "", false, nil
	}
	if id := stored.Get(storage.WriteIDMetadataKey); id.Type() != js.TypeString || id.String() != writeID {
		return "", false, nil
	}
	return obj.Get("etag").String(), true, nil
}

// Delete implements storage.Writer.
func (r *R2) Delete(ctx context.Context, key string) error {
	if err := r.check(); err != nil {
		return err
	}
	_, err := Await(ctx, r.bucket.Call("delete", r.prefix+key))
	return err
}

// fetchBody performs a request with a body (nil for none).
func fetchBody(ctx context.Context, r s3req.Request, body []byte) (response, error) {
	return fetchWith(ctx, r, nil, body)
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

// Put implements storage.Writer with a SigV4-signed PutObject. As
// httpstore.S3.Put, a conditional write stamped with a write id reads the
// object back when it fails with a failed condition or a transport error.
func (s *S3) Put(ctx context.Context, key string, data []byte, opts *storage.PutOptions) (string, error) {
	r, err := s3req.Put(&s.cfg, key, data, opts, time.Now())
	if err != nil {
		return "", err
	}
	writeID := s3req.ConditionalWriteID(opts)
	resp, err := fetchBody(ctx, r, data)
	if err != nil {
		if writeID != "" {
			if v, ours, rerr := s.readBack(ctx, key, writeID); rerr != nil || ours {
				return v, rerr
			}
		}
		return "", fmt.Errorf("fetch PUT %s: %w", key, err)
	}
	v, err := s3req.CheckPut(key, resp.status, resp.body, resp.etag)
	if errors.Is(err, storage.ErrPreconditionFailed) && writeID != "" {
		if v, ours, rerr := s.readBack(ctx, key, writeID); rerr != nil || ours {
			return v, rerr
		}
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
	resp, err := fetchBody(ctx, r, nil)
	if err != nil {
		return "", false, fmt.Errorf("s3: reading back %s: %w", key, err)
	}
	return s3req.ReadBack(key, writeID, resp.status, resp.header(s3req.WriteIDHeader), resp.etag)
}

// Copy implements storage.Copier with CopyObject.
func (s *S3) Copy(ctx context.Context, src, dst string, opts *storage.PutOptions) (string, error) {
	r, err := s3req.Copy(&s.cfg, src, dst, opts, time.Now())
	if err != nil {
		return "", err
	}
	resp, err := fetchBody(ctx, r, nil)
	if err != nil {
		return "", fmt.Errorf("fetch copy %s: %w", dst, err)
	}
	return s3req.CheckCopy(src, dst, resp.status, resp.body)
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
