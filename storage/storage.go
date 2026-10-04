// Package storage defines the object-store abstraction the Icechunk reader
// runs on, plus dependency-free implementations (local filesystem, in-memory,
// prefixing).
//
// Network-backed implementations live in sibling packages so that programs
// only link what they use: storage/httpstore (net/http: plain HTTP(S) and
// S3-compatible APIs with SigV4) and cfworker (Cloudflare Workers: R2
// bindings and the JS fetch API, without net/http).
package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// ErrNotFound is returned (possibly wrapped) when an object does not exist.
var ErrNotFound = errors.New("storage: object not found")

// ErrPreconditionFailed is returned (possibly wrapped) when a conditional
// read's IfMatch / IfUnmodifiedSince check fails, i.e. the object changed.
var ErrPreconditionFailed = errors.New("storage: object was modified (precondition failed)")

// ByteRange selects Length bytes starting at Offset.
type ByteRange struct {
	Offset int64
	Length int64
}

// GetOptions refines a read. A nil *GetOptions reads the whole object.
type GetOptions struct {
	// Range limits the read to a byte range. Nil reads the whole object.
	Range *ByteRange
	// IfMatch, when non-empty, requires the object's ETag to match.
	IfMatch string
	// IfUnmodifiedSince, when non-zero, requires the object to not have been
	// modified after this instant.
	IfUnmodifiedSince time.Time
}

// RangeOf is shorthand for a ranged read.
func RangeOf(offset, length int64) *GetOptions {
	return &GetOptions{Range: &ByteRange{Offset: offset, Length: length}}
}

// Storage is a read-only view of an object store rooted at some prefix.
// Keys are slash-separated paths relative to that root, e.g.
// "snapshots/1CECHNKREP0F1RSTCMT0". Implementations must be safe for
// concurrent use.
type Storage interface {
	Get(ctx context.Context, key string, opts *GetOptions) ([]byte, error)
}

// Lister is implemented by storages that can enumerate keys. It is only
// needed to list branches and tags of spec-v1 repositories.
type Lister interface {
	// List returns all keys under prefix (relative to the storage root).
	List(ctx context.Context, prefix string) ([]string, error)
}

// ---------------------------------------------------------------------------
// Local filesystem

// Local reads objects from a directory on the local filesystem.
type Local struct {
	root string
}

// NewLocal returns a Storage reading from the directory root.
func NewLocal(root string) *Local { return &Local{root: root} }

func (l *Local) path(key string) (string, error) {
	rel := filepath.FromSlash(key)
	if !filepath.IsLocal(rel) {
		return "", fmt.Errorf("storage: key %q escapes the storage root", key)
	}
	return filepath.Join(l.root, rel), nil
}

// Get implements Storage. IfUnmodifiedSince is checked against the file's
// modification time. Local files have no ETag, so reads with IfMatch fail
// with ErrPreconditionFailed (upstream Icechunk also rejects ETag-checksummed
// virtual chunks in file:// containers).
func (l *Local) Get(ctx context.Context, key string, opts *GetOptions) ([]byte, error) {
	path, err := l.path(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
		}
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.IsDir() {
		return nil, fmt.Errorf("%w: %s is a directory", ErrNotFound, key)
	}
	if opts != nil && !opts.IfUnmodifiedSince.IsZero() && st.ModTime().Truncate(time.Second).After(opts.IfUnmodifiedSince) {
		return nil, fmt.Errorf("%w: %s", ErrPreconditionFailed, key)
	}
	if opts != nil && opts.IfMatch != "" {
		return nil, fmt.Errorf("%w: %s: local files have no ETag to match %q", ErrPreconditionFailed, key, opts.IfMatch)
	}
	if opts == nil || opts.Range == nil {
		buf := make([]byte, st.Size())
		_, err := io.ReadFull(f, buf)
		return buf, err
	}
	r := opts.Range
	if r.Offset < 0 || r.Length < 0 || r.Offset+r.Length > st.Size() {
		return nil, fmt.Errorf("storage: range [%d, %d) out of bounds for %s (size %d)", r.Offset, r.Offset+r.Length, key, st.Size())
	}
	buf := make([]byte, r.Length)
	if _, err := f.ReadAt(buf, r.Offset); err != nil {
		return nil, err
	}
	return buf, nil
}

// List implements Lister.
func (l *Local) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	err := filepath.WalkDir(l.root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(l.root, p)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
		return nil
	})
	sort.Strings(keys)
	return keys, err
}

// ---------------------------------------------------------------------------
// In-memory

// Memory is an in-memory Storage, handy for tests and for repositories
// bundled into a binary.
type Memory struct {
	mu       sync.RWMutex
	objects  map[string][]byte
	versions map[string]uint64
	modTimes map[string]time.Time
}

// NewMemory returns an in-memory storage holding a copy of objects.
func NewMemory(objects map[string][]byte) *Memory {
	m := &Memory{objects: make(map[string][]byte, len(objects))}
	for k, v := range objects {
		m.objects[k] = v
	}
	return m
}

// Get implements Storage.
func (m *Memory) Get(ctx context.Context, key string, opts *GetOptions) ([]byte, error) {
	m.mu.RLock()
	data, ok := m.objects[key]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
	}
	if opts == nil || opts.Range == nil {
		return append([]byte(nil), data...), nil
	}
	r := opts.Range
	if r.Offset < 0 || r.Length < 0 || r.Offset+r.Length > int64(len(data)) {
		return nil, fmt.Errorf("storage: range [%d, %d) out of bounds for %s (size %d)", r.Offset, r.Offset+r.Length, key, len(data))
	}
	return append([]byte(nil), data[r.Offset:r.Offset+r.Length]...), nil
}

// List implements Lister.
func (m *Memory) List(ctx context.Context, prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var keys []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

// ---------------------------------------------------------------------------
// Prefixing

type prefixed struct {
	inner  Storage
	prefix string
}

// WithPrefix returns a Storage whose keys are resolved under prefix in inner.
// A trailing slash is added to a non-empty prefix if missing.
func WithPrefix(inner Storage, prefix string) Storage {
	prefix = strings.TrimLeft(prefix, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	if prefix == "" {
		return inner
	}
	return &prefixed{inner: inner, prefix: prefix}
}

func (p *prefixed) Get(ctx context.Context, key string, opts *GetOptions) ([]byte, error) {
	return p.inner.Get(ctx, p.prefix+key, opts)
}

func (p *prefixed) List(ctx context.Context, prefix string) ([]string, error) {
	l, ok := p.inner.(Lister)
	if !ok {
		return nil, fmt.Errorf("storage: %T does not support listing", p.inner)
	}
	keys, err := l.List(ctx, p.prefix+prefix)
	for i, k := range keys {
		keys[i] = strings.TrimPrefix(k, p.prefix)
	}
	return keys, err
}
