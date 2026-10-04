package icechunk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/solomonsealed/icechunk-go/internal/cache"
	"github.com/solomonsealed/icechunk-go/storage"
)

// Storage paths, relative to the repository root.
const (
	repoInfoPath    = "repo"
	snapshotsPrefix = "snapshots/"
	manifestsPrefix = "manifests/"
	chunksPrefix    = "chunks/"
	v1RefsPrefix    = "refs/"
	// overwrittenPrefix holds the replaced repo info files (spec v2).
	overwrittenPrefix = "overwritten/"
	defaultCacheSize  = 256 << 20
)

// Options tunes how a repository is read. The zero value is usable.
type Options struct {
	// VirtualChunkContainers maps URL prefixes (e.g. "s3://my-bucket/",
	// "https://example.com/data/" or, for upstream's named containers,
	// "vcc://name/") to the Storage that serves them. A virtual chunk whose
	// location starts with a prefix is read from that storage at the rest of
	// the location, percent-decoded and without any ?query or #fragment; the
	// longest matching prefix wins. Locations matching no prefix fail with
	// ErrNoVirtualContainer, and locations with "." or ".." path segments are
	// rejected, so only explicitly configured locations are ever accessed.
	VirtualChunkContainers map[string]storage.Storage

	// CacheBytes bounds the memory used to cache decoded snapshots and
	// manifests. Zero means 256 MiB; negative disables caching.
	CacheBytes int64

	// RepoInfoTTL is how long a fetched repo info file (branches, tags and
	// the snapshot graph of spec v2 repos) is reused before refetching. Zero
	// refetches on every ref lookup, which always observes the latest commits.
	RepoInfoTTL time.Duration

	// Concurrency bounds parallel chunk fetches in multi-chunk reads made
	// through Session.OpenArray. Zero means 16.
	Concurrency int
}

type virtualContainer struct {
	prefix string
	store  storage.Storage
}

// Repository is a read-only handle to an Icechunk repository (spec v1 or v2).
// It is safe for concurrent use, and is meant to be long-lived: it caches
// immutable snapshots and manifests across sessions.
type Repository struct {
	storage    storage.Storage
	opts       Options
	spec       int
	cache      *cache.Cache[cacheKey]
	containers []virtualContainer

	mu     sync.Mutex
	info   *RepoInfo
	infoAt time.Time
}

// cacheKey names a snapshot or manifest in Repository.cache.
type cacheKey struct {
	kind byte // 's' snapshot, 'm' manifest
	id   ObjectID12
}

// Open opens the repository rooted at st, detecting its spec version.
func Open(ctx context.Context, st storage.Storage, opts *Options) (*Repository, error) {
	r := &Repository{storage: st}
	if opts != nil {
		r.opts = *opts
	}
	budget := r.opts.CacheBytes
	switch {
	case budget == 0:
		budget = defaultCacheSize
	case budget < 0:
		budget = 0
	}
	r.cache = cache.New[cacheKey](budget)
	for prefix, s := range r.opts.VirtualChunkContainers {
		r.containers = append(r.containers, virtualContainer{prefix: prefix, store: s})
	}
	sort.Slice(r.containers, func(i, j int) bool {
		return len(r.containers[i].prefix) > len(r.containers[j].prefix)
	})

	info, err := r.fetchRepoInfo(ctx)
	switch {
	case err == nil:
		r.spec = info.SpecVersion
		r.info, r.infoAt = info, time.Now()
		return r, nil
	case !errors.Is(err, ErrRepositoryNotFound):
		return nil, err
	}
	// No repo info file: a spec v1 repository is identified by its main branch.
	if _, err := r.storage.Get(ctx, v1BranchKey("main"), nil); err != nil {
		if isNotFound(err) {
			return nil, ErrRepositoryNotFound
		}
		return nil, err
	}
	r.spec = 1
	return r, nil
}

// SpecVersion returns the repository's on-disk format version (1 or 2).
func (r *Repository) SpecVersion() int { return r.spec }

// Storage returns the storage the repository reads from.
func (r *Repository) Storage() storage.Storage { return r.storage }

func (r *Repository) fetchRepoInfo(ctx context.Context) (*RepoInfo, error) {
	raw, err := r.storage.Get(ctx, repoInfoPath, nil)
	if err != nil {
		if isNotFound(err) {
			return nil, ErrRepositoryNotFound
		}
		return nil, fmt.Errorf("icechunk: reading repo info: %w", err)
	}
	info, err := parseRepoInfo(raw)
	if err != nil {
		return nil, fmt.Errorf("icechunk: parsing repo info: %w", err)
	}
	return info, nil
}

// RepoInfo returns the decoded repo info file of a spec v2 repository,
// refetching it when older than Options.RepoInfoTTL.
func (r *Repository) RepoInfo(ctx context.Context) (*RepoInfo, error) {
	if r.spec < 2 {
		return nil, fmt.Errorf("icechunk: spec v%d repositories have no repo info file", r.spec)
	}
	r.mu.Lock()
	if r.info != nil && r.opts.RepoInfoTTL > 0 && time.Since(r.infoAt) < r.opts.RepoInfoTTL {
		info := r.info
		r.mu.Unlock()
		return info, nil
	}
	r.mu.Unlock()
	info, err := r.fetchRepoInfo(ctx)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.info, r.infoAt = info, time.Now()
	r.mu.Unlock()
	return info, nil
}

// Refresh forces the next ref lookup to refetch the repo info file.
func (r *Repository) Refresh() {
	r.mu.Lock()
	r.infoAt = time.Time{}
	r.mu.Unlock()
}

// Config returns the stored repository configuration (spec v2 only), e.g.
// inline chunk threshold, manifest splitting and virtual chunk containers.
func (r *Repository) Config(ctx context.Context) (any, error) {
	info, err := r.RepoInfo(ctx)
	if err != nil {
		return nil, err
	}
	return info.Config, nil
}

// OpsLog yields the operations log of a spec v2 repository, newest first,
// as upstream's Repository.ops_log does: the latest repo info file holds
// only its most recent updates (RepoInfo.Updates), and points at the
// previous file, kept in overwritten/, for older ones.
func (r *Repository) OpsLog(ctx context.Context) iter.Seq2[Update, error] {
	return func(yield func(Update, error) bool) {
		info, err := r.RepoInfo(ctx)
		seen := map[string]bool{}
		file := "" // the older repo info file being read
		for {
			if err != nil {
				yield(Update{}, err)
				return
			}
			for i, u := range info.Updates {
				// As upstream reports it, the newest update of an older file
				// was backed up as that file.
				if i == 0 && u.BackupPath == "" && len(seen) > 0 {
					u.BackupPath = file
				}
				if !yield(u, nil) {
					return
				}
			}
			prev := info.RepoBeforeUpdates
			if prev == "" {
				return
			}
			if seen[prev] {
				yield(Update{}, fmt.Errorf("%w: repo info files form a cycle at %s", ErrFormat, prev))
				return
			}
			seen[prev] = true
			file = prev
			raw, gerr := r.storage.Get(ctx, overwrittenPrefix+prev, nil)
			if gerr != nil {
				err = fmt.Errorf("icechunk: reading older repo info %s: %w", prev, gerr)
				continue
			}
			if info, err = parseRepoInfo(raw); err != nil {
				err = fmt.Errorf("icechunk: parsing older repo info %s: %w", prev, err)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Refs

// Spec v1 stores each ref under its own key. Upstream builds those keys as
// object_store paths, which percent-encode the name ("refs/branch.%C3%BC"
// for branch "ü"); listing refs returns the names in that encoded form, as
// upstream lists them.
func v1BranchKey(name string) string {
	return v1RefsPrefix + objectStorePart("branch."+name) + "/ref.json"
}
func v1TagKey(name string) string { return v1RefsPrefix + objectStorePart("tag."+name) + "/ref.json" }

// objectStoreEncoded are the ASCII characters object_store percent-encodes
// in a path segment, besides control characters (its INVALID set).
const objectStoreEncoded = "/\\{^}%`]\"<>[~#|*?"

// objectStorePart encodes one path segment as object_store's PathPart does:
// non-ASCII bytes, control characters and objectStoreEncoded become %XX,
// and the segments "." and ".." are escaped.
func objectStorePart(s string) string {
	switch s {
	case ".":
		return "%2E"
	case "..":
		return "%2E%2E"
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x20 || c >= 0x7f || strings.IndexByte(objectStoreEncoded, c) >= 0 {
			fmt.Fprintf(&b, "%%%02X", c)
		} else {
			b.WriteByte(c)
		}
	}
	return b.String()
}

// validRefName rejects names that cannot exist. Only spec v1 stores refs as
// storage keys, so only v1 forbids "/" in names. Like upstream, any other
// name is accepted, the empty one included.
func (r *Repository) validRefName(name string) error {
	if r.spec == 1 && strings.Contains(name, "/") {
		return fmt.Errorf("%w: invalid ref name %q", ErrRefNotFound, name)
	}
	return nil
}

func (r *Repository) readV1Ref(ctx context.Context, key, name string) (SnapshotID, error) {
	raw, err := r.storage.Get(ctx, key, nil)
	if err != nil {
		if isNotFound(err) {
			return SnapshotID{}, fmt.Errorf("%w: %s", ErrRefNotFound, name)
		}
		return SnapshotID{}, err
	}
	var data struct {
		Snapshot string `json:"snapshot"`
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return SnapshotID{}, fmt.Errorf("%w: ref %s: %v", ErrFormat, name, err)
	}
	return ParseSnapshotID(data.Snapshot)
}

// LookupBranch returns the snapshot a branch currently points to.
func (r *Repository) LookupBranch(ctx context.Context, name string) (SnapshotID, error) {
	if err := r.validRefName(name); err != nil {
		return SnapshotID{}, err
	}
	if r.spec == 1 {
		return r.readV1Ref(ctx, v1BranchKey(name), name)
	}
	info, err := r.RepoInfo(ctx)
	if err != nil {
		return SnapshotID{}, err
	}
	id, ok := info.Branches[name]
	if !ok {
		return SnapshotID{}, fmt.Errorf("%w: branch %s", ErrRefNotFound, name)
	}
	return id, nil
}

// LookupTag returns the snapshot a tag points to. Deleted tags are not found.
func (r *Repository) LookupTag(ctx context.Context, name string) (SnapshotID, error) {
	if err := r.validRefName(name); err != nil {
		return SnapshotID{}, err
	}
	if r.spec == 1 {
		_, err := r.storage.Get(ctx, v1TagKey(name)+".deleted", nil)
		switch {
		case err == nil:
			return SnapshotID{}, fmt.Errorf("%w: tag %s was deleted", ErrRefNotFound, name)
		case !isNotFound(err):
			return SnapshotID{}, err
		}
		return r.readV1Ref(ctx, v1TagKey(name), name)
	}
	info, err := r.RepoInfo(ctx)
	if err != nil {
		return SnapshotID{}, err
	}
	id, ok := info.Tags[name]
	if !ok {
		return SnapshotID{}, fmt.Errorf("%w: tag %s", ErrRefNotFound, name)
	}
	return id, nil
}

// ListBranches returns all branch names, sorted. Spec v1 repositories
// require a storage that implements storage.Lister.
func (r *Repository) ListBranches(ctx context.Context) ([]string, error) {
	if r.spec == 1 {
		return r.listV1Refs(ctx, "branch.")
	}
	info, err := r.RepoInfo(ctx)
	if err != nil {
		return nil, err
	}
	return info.BranchNames(), nil
}

// ListTags returns all (non-deleted) tag names, sorted.
func (r *Repository) ListTags(ctx context.Context) ([]string, error) {
	if r.spec == 1 {
		return r.listV1Refs(ctx, "tag.")
	}
	info, err := r.RepoInfo(ctx)
	if err != nil {
		return nil, err
	}
	return info.TagNames(), nil
}

func (r *Repository) listV1Refs(ctx context.Context, kind string) ([]string, error) {
	l, ok := r.storage.(storage.Lister)
	if !ok {
		return nil, fmt.Errorf("icechunk: listing refs of a spec v1 repository needs a storage that supports listing (%T does not)", r.storage)
	}
	keys, err := l.List(ctx, v1RefsPrefix+kind)
	if err != nil {
		return nil, err
	}
	refs, deleted := map[string]bool{}, map[string]bool{}
	for _, k := range keys {
		rest := strings.TrimPrefix(k, v1RefsPrefix+kind)
		name, file, ok := strings.Cut(rest, "/")
		if !ok {
			continue
		}
		switch file {
		case "ref.json":
			refs[name] = true
		case "ref.json.deleted":
			deleted[name] = true
		}
	}
	var out []string
	for name := range refs {
		if !deleted[name] {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out, nil
}

// ---------------------------------------------------------------------------
// Versions

type versionKind uint8

const (
	versionBranch versionKind = iota + 1
	versionTag
	versionSnapshot
)

// Version selects a snapshot: a branch tip, a tag, or a snapshot id. The
// zero Version selects nothing.
type Version struct {
	kind versionKind
	name string
	id   SnapshotID
}

// AtBranch selects the current tip of a branch.
func AtBranch(name string) Version { return Version{kind: versionBranch, name: name} }

// AtTag selects the snapshot a tag points to.
func AtTag(name string) Version { return Version{kind: versionTag, name: name} }

// AtSnapshot selects a snapshot by id.
func AtSnapshot(id SnapshotID) Version { return Version{kind: versionSnapshot, id: id} }

func (v Version) String() string {
	switch v.kind {
	case versionSnapshot:
		return "snapshot " + v.id.String()
	case versionTag:
		return fmt.Sprintf("tag %q", v.name)
	case versionBranch:
		return fmt.Sprintf("branch %q", v.name)
	}
	return "no version"
}

// Resolve returns the snapshot id a Version refers to.
func (r *Repository) Resolve(ctx context.Context, v Version) (SnapshotID, error) {
	switch v.kind {
	case versionSnapshot:
		return v.id, nil
	case versionTag:
		return r.LookupTag(ctx, v.name)
	case versionBranch:
		return r.LookupBranch(ctx, v.name)
	}
	return SnapshotID{}, fmt.Errorf("icechunk: empty version")
}

// ---------------------------------------------------------------------------
// Snapshots and ancestry

// Snapshot fetches (or returns cached) snapshot id.
func (r *Repository) Snapshot(ctx context.Context, id SnapshotID) (*Snapshot, error) {
	v, err := r.cache.Get(ctx, cacheKey{kind: 's', id: id}, func() (any, int64, error) {
		raw, err := r.storage.Get(ctx, snapshotsPrefix+id.String(), nil)
		if err != nil {
			if isNotFound(err) {
				return nil, 0, fmt.Errorf("%w: %s", ErrSnapshotNotFound, id)
			}
			return nil, 0, err
		}
		s, err := parseSnapshot(raw)
		if err != nil {
			return nil, 0, fmt.Errorf("icechunk: snapshot %s: %w", id, err)
		}
		return s, int64(len(s.buf)), nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*Snapshot), nil
}

func (r *Repository) manifest(ctx context.Context, id ManifestID) (*Manifest, error) {
	v, err := r.cache.Get(ctx, cacheKey{kind: 'm', id: id}, func() (any, int64, error) {
		raw, err := r.storage.Get(ctx, manifestsPrefix+id.String(), nil)
		if err != nil {
			return nil, 0, fmt.Errorf("icechunk: reading manifest %s: %w", id, err)
		}
		m, err := parseManifest(raw)
		if err != nil {
			return nil, 0, fmt.Errorf("icechunk: manifest %s: %w", id, err)
		}
		return m, int64(len(m.buf)), nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*Manifest), nil
}

// Ancestry yields the snapshot selected by v and then each of its ancestors,
// newest first, ending with the repository's initial snapshot.
func (r *Repository) Ancestry(ctx context.Context, v Version) iter.Seq2[SnapshotInfo, error] {
	return func(yield func(SnapshotInfo, error) bool) {
		id, err := r.Resolve(ctx, v)
		if err != nil {
			yield(SnapshotInfo{}, err)
			return
		}
		if r.spec >= 2 {
			info, err := r.RepoInfo(ctx)
			if err != nil {
				yield(SnapshotInfo{}, err)
				return
			}
			for {
				si, ok := info.snapshot(id)
				if !ok {
					yield(SnapshotInfo{}, fmt.Errorf("%w: %s", ErrSnapshotNotFound, id))
					return
				}
				if !yield(*si, nil) || si.ParentID == nil {
					return
				}
				id = *si.ParentID
			}
		}
		for {
			snap, err := r.Snapshot(ctx, id)
			if err != nil {
				yield(SnapshotInfo{}, err)
				return
			}
			si, err := snap.info()
			if !yield(si, err) || err != nil || si.ParentID == nil {
				return
			}
			id = *si.ParentID
		}
	}
}

// ReadonlySession opens the snapshot selected by v for reading.
func (r *Repository) ReadonlySession(ctx context.Context, v Version) (*Session, error) {
	id, err := r.Resolve(ctx, v)
	if err != nil {
		return nil, err
	}
	snap, err := r.Snapshot(ctx, id)
	if err != nil {
		return nil, err
	}
	return &Session{repo: r, snap: snap, id: id}, nil
}

// ---------------------------------------------------------------------------
// Chunk bytes

// fetchChunk reads bytes [off, off+length) of the chunk described by ref.
// A negative length reads to the end of the chunk.
func (r *Repository) fetchChunk(ctx context.Context, ref *ChunkRef, off, length int64) ([]byte, error) {
	size := int64(ref.Size())
	if length < 0 {
		length = size - off
	}
	if off < 0 || length < 0 || off+length > size {
		return nil, fmt.Errorf("icechunk: byte range [%d, %d) outside chunk of %d bytes", off, off+length, size)
	}
	if length == 0 {
		return []byte{}, nil // nothing to fetch (and "bytes=n-(n-1)" is not a valid range)
	}
	switch ref.Kind {
	case InlineChunk:
		return append([]byte(nil), ref.Inline[off:off+length]...), nil
	case NativeChunk:
		data, err := r.storage.Get(ctx, chunksPrefix+ref.ID.String(), storage.RangeOf(int64(ref.Offset)+off, length))
		if err != nil {
			return nil, fmt.Errorf("icechunk: reading chunk %s: %w", ref.ID, err)
		}
		return checkLen(data, length, ref.ID.String())
	case VirtualChunk:
		c, key, err := r.resolveVirtual(ref.Location)
		if err != nil {
			return nil, err
		}
		opts := storage.RangeOf(int64(ref.Offset)+off, length)
		opts.IfMatch = ref.ETag
		opts.IfUnmodifiedSince = ref.LastModified
		data, err := c.store.Get(ctx, key, opts)
		if err != nil {
			if errors.Is(err, storage.ErrPreconditionFailed) {
				return nil, fmt.Errorf("%w: %s", ErrChunkModified, ref.Location)
			}
			return nil, fmt.Errorf("icechunk: reading virtual chunk %s: %w", ref.Location, err)
		}
		return checkLen(data, length, ref.Location)
	}
	return nil, fmt.Errorf("%w: unknown chunk ref kind %d", ErrFormat, ref.Kind)
}

func checkLen(data []byte, want int64, what string) ([]byte, error) {
	if int64(len(data)) != want {
		return nil, fmt.Errorf("icechunk: %s: expected %d bytes, got %d", what, want, len(data))
	}
	return data, nil
}

// resolveVirtual maps a virtual chunk location to its container and the
// object key inside it. As upstream does, the query and fragment are dropped
// and the path is percent-decoded. Keys with "." or ".." segments are
// rejected: they could otherwise escape the container's prefix (a local
// directory, or an HTTP base URL once fetch normalizes the path).
func (r *Repository) resolveVirtual(location string) (virtualContainer, string, error) {
	loc := location
	if _, rest, ok := strings.Cut(loc, "://"); ok {
		pathStart := len(loc) - len(rest)
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			pathStart += i
		} else {
			pathStart = len(loc)
		}
		if j := strings.IndexAny(loc[pathStart:], "?#"); j >= 0 {
			loc = loc[:pathStart+j]
		}
	}
	for _, c := range r.containers {
		if !strings.HasPrefix(loc, c.prefix) {
			continue
		}
		key, err := url.PathUnescape(loc[len(c.prefix):])
		if err != nil {
			return c, "", fmt.Errorf("%w: invalid virtual chunk location %q: %v", ErrFormat, location, err)
		}
		for _, seg := range strings.Split(key, "/") {
			if seg == "." || seg == ".." {
				return c, "", fmt.Errorf("%w: virtual chunk location %q has relative path segments", ErrFormat, location)
			}
		}
		return c, key, nil
	}
	return virtualContainer{}, "", fmt.Errorf("%w: %s", ErrNoVirtualContainer, location)
}
