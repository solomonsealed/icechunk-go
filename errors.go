package icechunk

import (
	"errors"

	"github.com/solomonsealed/icechunk-go/internal/format"
	"github.com/solomonsealed/icechunk-go/storage"
)

var (
	// ErrRepositoryNotFound means no Icechunk repository exists at the storage root.
	ErrRepositoryNotFound = errors.New("icechunk: repository not found")
	// ErrRefNotFound means a branch or tag does not exist (or the tag was deleted).
	ErrRefNotFound = errors.New("icechunk: ref not found")
	// ErrSnapshotNotFound means a snapshot id is not part of the repository.
	ErrSnapshotNotFound = errors.New("icechunk: snapshot not found")
	// ErrNodeNotFound means no array or group exists at a path.
	ErrNodeNotFound = errors.New("icechunk: node not found")
	// ErrNotAnArray means an array operation was attempted on a group.
	ErrNotAnArray = errors.New("icechunk: node is not an array")
	// ErrKeyNotFound is returned by Store for Zarr keys that do not exist.
	ErrKeyNotFound = errors.New("icechunk: key not found")
	// ErrNoVirtualContainer means a virtual chunk's location matches none of
	// the configured virtual chunk containers.
	ErrNoVirtualContainer = errors.New("icechunk: no virtual chunk container configured for location")
	// ErrChunkModified means a virtual chunk's target object changed after
	// the reference was written (its ETag or modification time differs).
	ErrChunkModified = errors.New("icechunk: virtual chunk object was modified")
	// ErrFormat wraps errors for malformed or unsupported on-disk data.
	ErrFormat = format.ErrFormat
)

func isNotFound(err error) bool { return errors.Is(err, storage.ErrNotFound) }
