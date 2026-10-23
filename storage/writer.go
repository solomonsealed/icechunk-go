package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// PutOptions makes a write conditional, and sets the stored object's
// storage class and user metadata.
type PutOptions struct {
	// IfMatch, when non-empty, only writes if the object exists and its
	// current version equals IfMatch (an optimistic-concurrency update).
	IfMatch string
	// IfNotExists only writes if no object exists under the key.
	IfNotExists bool
	// StorageClass, when non-empty, is the storage class of the object,
	// passed to the store as is (S3's x-amz-storage-class, R2's
	// storageClass). Storages without storage classes ignore it.
	StorageClass string
	// Metadata is user metadata stored with the object (S3's x-amz-meta-*
	// headers, R2's custom metadata). Storages that keep no metadata
	// ignore it. When a conditional write's metadata holds a
	// WriteIDMetadataKey, storages that keep metadata recover from a lost
	// response as upstream does: if the write fails with a failed condition
	// or a transport error, they read the object back, and the write
	// succeeded if the object holds that write id.
	Metadata map[string]string
}

// WriteIDMetadataKey is the user metadata key of the unique id a
// conditional write stamps on the object, to recognise its own write when
// its response is lost (upstream's icechunkwriteid).
const WriteIDMetadataKey = "icechunkwriteid"

// Writer is implemented by storages that Icechunk can write to. Commits
// rely on GetVersion and conditional Put to update the repository's single
// mutable object (the repo info file) atomically.
type Writer interface {
	Storage
	// GetVersion reads a whole object together with an opaque version
	// token (an ETag or equivalent) usable as PutOptions.IfMatch.
	GetVersion(ctx context.Context, key string) (data []byte, version string, err error)
	// Put stores an object and returns its new version. A failed condition
	// returns an error wrapping ErrPreconditionFailed.
	Put(ctx context.Context, key string, data []byte, opts *PutOptions) (version string, err error)
	// Delete removes an object; deleting a missing object is not an error.
	Delete(ctx context.Context, key string) error
}

// Copier is implemented by storages that copy objects server side.
// Repositories back up their repo info file with it, as upstream does,
// and write the backup with Put otherwise.
type Copier interface {
	// Copy copies the object at src to dst, keeping its user metadata, and
	// returns the copy's version. opts may set the copy's StorageClass and,
	// with IfMatch, require src's version to be IfMatch (failing with an
	// error wrapping ErrPreconditionFailed otherwise); its other fields are
	// ignored.
	Copy(ctx context.Context, src, dst string, opts *PutOptions) (version string, err error)
}

// ModTimer is implemented by storages that can report when an object was
// last written, according to the store's own clock. Writers use it to
// refuse to commit when the local clock is far off (timestamps in the
// future would make other clients reject later updates).
type ModTimer interface {
	LastModified(ctx context.Context, key string) (time.Time, error)
}

// ---------------------------------------------------------------------------
// Memory

func (m *Memory) version(key string) string {
	return strconv.FormatUint(m.versions[key], 10)
}

// GetVersion implements Writer. Versions are per-key write counters.
func (m *Memory) GetVersion(ctx context.Context, key string) ([]byte, string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, "", fmt.Errorf("%w: %s", ErrNotFound, key)
	}
	return append([]byte(nil), data...), m.version(key), nil
}

// Put implements Writer.
func (m *Memory) Put(ctx context.Context, key string, data []byte, opts *PutOptions) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.objects[key]
	if opts != nil {
		if opts.IfNotExists && exists {
			return "", fmt.Errorf("%w: %s already exists", ErrPreconditionFailed, key)
		}
		if opts.IfMatch != "" && (!exists || m.version(key) != opts.IfMatch) {
			return "", fmt.Errorf("%w: %s changed", ErrPreconditionFailed, key)
		}
	}
	if m.versions == nil {
		m.versions = map[string]uint64{}
	}
	m.versions[key]++
	m.objects[key] = append([]byte(nil), data...)
	if m.modTimes == nil {
		m.modTimes = map[string]time.Time{}
	}
	m.modTimes[key] = time.Now()
	return m.version(key), nil
}

// LastModified implements ModTimer.
func (m *Memory) LastModified(ctx context.Context, key string) (time.Time, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.modTimes[key]
	if !ok {
		return time.Time{}, fmt.Errorf("%w: %s", ErrNotFound, key)
	}
	return t, nil
}

// LastModified implements ModTimer with the file's modification time.
func (l *Local) LastModified(ctx context.Context, key string) (time.Time, error) {
	path, err := l.path(key)
	if err != nil {
		return time.Time{}, err
	}
	st, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return time.Time{}, fmt.Errorf("%w: %s", ErrNotFound, key)
		}
		return time.Time{}, err
	}
	return st.ModTime(), nil
}

// Delete implements Writer.
func (m *Memory) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

// ---------------------------------------------------------------------------
// Local

// localLocks serializes conditional writes within this process; across
// processes, conditional writes take an exclusive lock file (see lockFile).
var localLocks sync.Map // root -> *sync.Mutex

func contentVersion(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:16])
}

// GetVersion implements Writer. The version is a hash of the content.
func (l *Local) GetVersion(ctx context.Context, key string) ([]byte, string, error) {
	data, err := l.Get(ctx, key, nil)
	if err != nil {
		return nil, "", err
	}
	return data, contentVersion(data), nil
}

// Put implements Writer. Objects are written to a temporary file and
// renamed into place, so readers never see partial objects. Conditional
// writes are atomic with respect to other writers using this package,
// including other processes on the same machine; as with upstream
// Icechunk's local storage, network filesystems are not supported for
// concurrent writers.
func (l *Local) Put(ctx context.Context, key string, data []byte, opts *PutOptions) (string, error) {
	path, err := l.path(key)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	conditional := opts != nil && (opts.IfMatch != "" || opts.IfNotExists)
	if conditional {
		mu, _ := localLocks.LoadOrStore(l.root, &sync.Mutex{})
		mu.(*sync.Mutex).Lock()
		defer mu.(*sync.Mutex).Unlock()
		unlock, err := lockFile(filepath.Join(l.root, ".icechunk-go.lock"))
		if err != nil {
			return "", err
		}
		defer unlock()
		current, err := os.ReadFile(path)
		exists := err == nil
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
		if opts.IfNotExists && exists {
			return "", fmt.Errorf("%w: %s already exists", ErrPreconditionFailed, key)
		}
		if opts.IfMatch != "" && (!exists || contentVersion(current) != opts.IfMatch) {
			return "", fmt.Errorf("%w: %s changed", ErrPreconditionFailed, key)
		}
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return "", err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return "", err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		os.Remove(tmp.Name())
		return "", err
	}
	return contentVersion(data), nil
}

// Delete implements Writer.
func (l *Local) Delete(ctx context.Context, key string) error {
	path, err := l.path(key)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}
