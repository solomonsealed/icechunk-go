package icechunk

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/solomonsealed/icechunk-go/internal/fbs"
	"github.com/solomonsealed/icechunk-go/internal/format"
	"github.com/solomonsealed/icechunk-go/storage"
)

var (
	// ErrConflict is wrapped by *ConflictError.
	ErrConflict = errors.New("icechunk: conflict")
	// ErrNoChanges is returned when committing a session without changes.
	ErrNoChanges = errors.New("icechunk: no changes to commit")
	// ErrRepositoryNotOnline means the repository status forbids writes.
	ErrRepositoryNotOnline = errors.New("icechunk: repository is not online")
	// ErrTooManyRetries means the repo info file kept changing under us.
	ErrTooManyRetries = errors.New("icechunk: gave up updating the repository after repeated concurrent updates")
	// ErrFeatureDisabled means a repository feature flag forbids an operation.
	ErrFeatureDisabled = errors.New("icechunk: feature disabled for this repository")
	// ErrClockSkew means the local clock disagrees with the object store's
	// by more than maxClockSkew; writing would record misleading timestamps.
	ErrClockSkew = errors.New("icechunk: local clock differs too much from the object store's clock")
	// ErrCannotDeleteMain is returned by DeleteBranch("main").
	ErrCannotDeleteMain = errors.New("icechunk: main branch cannot be deleted")
)

// maxClockSkew mirrors upstream's limit on the difference between commit
// timestamps and the object store's clock.
const maxClockSkew = 10 * time.Minute

// ConflictError reports that a commit could not be applied: the branch moved
// since the session started (and rebasing was off or impossible).
type ConflictError struct {
	Branch           string
	Expected, Actual SnapshotID
	Reason           string
}

func (e *ConflictError) Error() string {
	msg := fmt.Sprintf("icechunk: conflict committing to branch %s: expected tip %s, found %s", e.Branch, e.Expected, e.Actual)
	if e.Reason != "" {
		msg += ": " + e.Reason
	}
	return msg
}

// Is makes errors.Is(err, ErrConflict) true.
func (e *ConflictError) Is(target error) bool { return target == ErrConflict }

// InitialSnapshotID is the well-known id of every repository's first snapshot.
var InitialSnapshotID = SnapshotID{0x0b, 0x1c, 0xc8, 0xd6, 0x78, 0x75, 0x80, 0xf0, 0xe3, 0x3a, 0x65, 0x34}

const (
	featureCreateTag  = 4
	featureDeleteTag  = 5
	maxRepoAttempts   = 10
	defaultMaxRebases = 100
)

func (r *Repository) writer() (storage.Writer, error) {
	if r.spec < 2 {
		return nil, fmt.Errorf("icechunk: writing spec v%d repositories is not supported; upgrade with icechunk-python (icechunk.upgrade_icechunk_repository)", r.spec)
	}
	w, ok := r.storage.(storage.Writer)
	if !ok {
		return nil, ErrReadOnlyStorage
	}
	return w, nil
}

// encodeMetadata stores commit metadata the way upstream does: each value
// as a FlexBuffer of a JSON value. Values are normalized through JSON first
// (structs become objects, []byte a base64 string), because upstream
// parses every snapshot's metadata as JSON and a non-JSON value (such as a
// FlexBuffer blob) would make the repository unusable for it.
func encodeMetadata(md map[string]any) ([]rawMetadataItem, error) {
	items := make([]rawMetadataItem, 0, len(md))
	for k, v := range md {
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("icechunk: metadata %q is not JSON-compatible: %w", k, err)
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		var norm any
		if err := dec.Decode(&norm); err != nil {
			return nil, err
		}
		b, err := format.EncodeFlexBuffer(norm)
		if err != nil {
			return nil, fmt.Errorf("icechunk: metadata %q: %w", k, err)
		}
		items = append(items, rawMetadataItem{name: k, value: b})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].name < items[j].name })
	return items, nil
}

// withIcechunkMetadata returns a copy of md with key set to v in its
// "__icechunk" object, replacing "__icechunk" if it is not an object (as
// upstream's inject_icechunk_metadata does).
func withIcechunkMetadata(md map[string]any, key string, v any) (map[string]any, error) {
	out := make(map[string]any, len(md)+1)
	for k, x := range md {
		out[k] = x
	}
	ic := map[string]any{}
	if prev, ok := md["__icechunk"]; ok {
		raw, err := json.Marshal(prev)
		if err != nil {
			return nil, fmt.Errorf("icechunk: metadata %q is not JSON-compatible: %w", "__icechunk", err)
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		var obj map[string]any
		if dec.Decode(&obj) == nil && obj != nil {
			ic = obj
		}
	}
	ic[key] = v
	out["__icechunk"] = ic
	return out, nil
}

// backupName names the copy of the repo info file kept in overwritten/:
// "repo.<ms until 3000-01-01>.<random id>", newest sorting first.
func backupName(now time.Time) string {
	const year3000 = 32503680000000
	return fmt.Sprintf("repo.%014d.%s", year3000-now.UnixMilli(), randomID12())
}

// updateRepo applies mutate to the latest repo info file and writes the
// result with a conditional put, retrying when another writer got there
// first. As upstream requires, the replaced file is first copied to
// overwritten/.
func (r *Repository) updateRepo(ctx context.Context, mutate func(doc *repoDoc, backup string, now time.Time) error) error {
	w, err := r.writer()
	if err != nil {
		return err
	}
	for attempt := 0; attempt < maxRepoAttempts; attempt++ {
		if attempt >= 5 {
			// Immediate retries first, then exponential backoff with jitter.
			d := time.Duration(100<<min(attempt-5, 6)) * time.Millisecond
			d += time.Duration(rand.Int64N(int64(d)))
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		raw, version, err := w.GetVersion(ctx, repoInfoPath)
		if err != nil {
			return fmt.Errorf("icechunk: reading repo info: %w", err)
		}
		doc, err := parseRepoDoc(raw)
		if err != nil {
			return err
		}
		if doc.status.availability != fbs.RepoAvailabilityOnline {
			return fmt.Errorf("%w (status %s)", ErrRepositoryNotOnline, availabilityName(doc.status.availability))
		}
		now := time.Now()
		backup := backupName(now)
		if err := mutate(doc, backup, now); err != nil {
			return err
		}
		enc, err := doc.encode()
		if err != nil {
			return err
		}
		if _, err := w.Put(ctx, overwrittenPrefix+backup, raw, nil); err != nil {
			return fmt.Errorf("icechunk: backing up repo info: %w", err)
		}
		// The backup was just written: its store timestamp is the store's
		// "now". Refuse to record our timestamps if our clock is far off.
		if mt, ok := w.(storage.ModTimer); ok {
			if stored, err := mt.LastModified(ctx, overwrittenPrefix+backup); err == nil {
				if d := now.Sub(stored); d > maxClockSkew || d < -maxClockSkew {
					return fmt.Errorf("%w: local %s, store %s", ErrClockSkew, now.UTC().Format(time.RFC3339), stored.UTC().Format(time.RFC3339))
				}
			}
		}
		if _, err := w.Put(ctx, repoInfoPath, enc, &storage.PutOptions{IfMatch: version}); err != nil {
			if errors.Is(err, storage.ErrPreconditionFailed) {
				continue
			}
			return fmt.Errorf("icechunk: writing repo info: %w", err)
		}
		if info, err := parseRepoInfo(enc); err == nil {
			r.mu.Lock()
			r.info, r.infoAt = info, time.Now()
			r.mu.Unlock()
		}
		return nil
	}
	return ErrTooManyRetries
}

// ---------------------------------------------------------------------------
// Creating repositories and sessions

// Create initializes a new spec v2 repository in st, which must implement
// storage.Writer and not already hold a repository.
func Create(ctx context.Context, st storage.Storage, opts *Options) (*Repository, error) {
	w, ok := st.(storage.Writer)
	if !ok {
		return nil, ErrReadOnlyStorage
	}
	for _, key := range []string{repoInfoPath, v1BranchKey("main")} {
		if _, err := st.Get(ctx, key, storage.RangeOf(0, 1)); err == nil {
			return nil, fmt.Errorf("%w: a repository already exists here", ErrAlreadyExists)
		} else if !isNotFound(err) {
			return nil, err
		}
	}
	now := uint64(time.Now().UnixMicro())
	md, err := encodeMetadata(map[string]any{"__icechunk": map[string]any{"is_root": true}})
	if err != nil {
		return nil, err
	}
	snap, err := encodeSnapshot(&snapshotSpec{
		id: InitialSnapshotID, flushedAt: now, message: "Repository initialized", metadata: md,
	})
	if err != nil {
		return nil, err
	}
	tx, err := encodeTxLog(&txLog{id: InitialSnapshotID})
	if err != nil {
		return nil, err
	}
	doc := &repoDoc{
		specVersion: writeSpecVersion,
		tags:        map[string]SnapshotID{},
		branches:    map[string]SnapshotID{"main": InitialSnapshotID},
		snapshots: []docSnapshot{{
			id: InitialSnapshotID, flushedAt: now, message: "Repository initialized", metadata: md,
		}},
		status:  docStatus{availability: fbs.RepoAvailabilityOnline, setAt: now},
		updates: []docUpdate{{typ: fbs.UpdateTypeRepoInitializedUpdate, updatedAt: now}},
	}
	repoFile, err := doc.encode()
	if err != nil {
		return nil, err
	}
	if _, err := w.Put(ctx, snapshotsPrefix+InitialSnapshotID.String(), snap, nil); err != nil {
		return nil, err
	}
	if _, err := w.Put(ctx, "transactions/"+InitialSnapshotID.String(), tx, nil); err != nil {
		return nil, err
	}
	// The repo info file goes last: it is what makes the repository exist.
	if _, err := w.Put(ctx, repoInfoPath, repoFile, &storage.PutOptions{IfNotExists: true}); err != nil {
		if errors.Is(err, storage.ErrPreconditionFailed) {
			return nil, fmt.Errorf("%w: a repository already exists here", ErrAlreadyExists)
		}
		return nil, err
	}
	return Open(ctx, st, opts)
}

// WritableSession starts a session on the current tip of branch. Its
// changes become visible to others only when committed.
func (r *Repository) WritableSession(ctx context.Context, branch string) (*Session, error) {
	if _, err := r.writer(); err != nil {
		return nil, err
	}
	r.Refresh() // always start from the latest tip
	id, err := r.LookupBranch(ctx, branch)
	if err != nil {
		return nil, err
	}
	snap, err := r.Snapshot(ctx, id)
	if err != nil {
		return nil, err
	}
	return &Session{repo: r, snap: snap, id: id, branch: branch, cs: newChangeSet()}, nil
}

// ---------------------------------------------------------------------------
// Commit

// CommitOptions tunes Session.Commit.
type CommitOptions struct {
	// Metadata is stored with the commit (JSON-compatible values).
	Metadata map[string]any
	// Rebase, when the branch moved since the session started, re-applies
	// this session's changes on the new tip if the intervening commits did
	// not touch the same chunks or nodes; otherwise the commit fails with a
	// *ConflictError.
	Rebase bool
	// MaxRebases bounds rebase attempts under contention (default 100).
	MaxRebases int
	// AllowEmpty permits committing a session without changes.
	AllowEmpty bool
}

type commitPlan struct {
	id        SnapshotID
	snapshot  []byte
	parsed    *Snapshot
	flushedAt uint64
	txLog     []byte
	manifests map[ManifestID][]byte
}

// Commit writes the session's changes as a new snapshot and moves the
// branch to it. Afterwards the session is a read-only view of that snapshot.
func (s *Session) Commit(ctx context.Context, message string, opts *CommitOptions) (SnapshotID, error) {
	w, err := s.writable()
	if err != nil {
		return SnapshotID{}, err
	}
	if opts == nil {
		opts = &CommitOptions{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cs == nil {
		return SnapshotID{}, ErrReadOnlySession
	}
	if s.cs.isEmpty() && !opts.AllowEmpty {
		return SnapshotID{}, ErrNoChanges
	}
	maxRebases := opts.MaxRebases
	if maxRebases <= 0 {
		maxRebases = defaultMaxRebases
	}
	base, baseID := s.snap, s.id
	for rebases := 0; ; rebases++ {
		md := opts.Metadata
		if opts.Rebase {
			// Like upstream, rebasing commits record how many rebases they took.
			if md, err = withIcechunkMetadata(md, "rebase_attempts", rebases); err != nil {
				return SnapshotID{}, err
			}
		}
		metadata, err := encodeMetadata(md)
		if err != nil {
			return SnapshotID{}, err
		}
		plan, err := s.buildCommit(ctx, base, message, metadata)
		if err != nil {
			return SnapshotID{}, err
		}
		for id, data := range plan.manifests {
			if _, err := w.Put(ctx, manifestsPrefix+id.String(), data, nil); err != nil {
				return SnapshotID{}, fmt.Errorf("icechunk: writing manifest: %w", err)
			}
		}
		if _, err := w.Put(ctx, "transactions/"+plan.id.String(), plan.txLog, nil); err != nil {
			return SnapshotID{}, fmt.Errorf("icechunk: writing transaction log: %w", err)
		}
		if _, err := w.Put(ctx, snapshotsPrefix+plan.id.String(), plan.snapshot, nil); err != nil {
			return SnapshotID{}, fmt.Errorf("icechunk: writing snapshot: %w", err)
		}
		err = s.repo.updateRepo(ctx, func(doc *repoDoc, backup string, now time.Time) error {
			tip, ok := doc.branches[s.branch]
			if !ok {
				return fmt.Errorf("%w: branch %s was deleted", ErrRefNotFound, s.branch)
			}
			if tip != baseID {
				return &ConflictError{Branch: s.branch, Expected: baseID, Actual: tip}
			}
			parent := baseID
			if err := doc.addSnapshot(docSnapshot{
				id: plan.id, parent: &parent, flushedAt: plan.flushedAt, message: message, metadata: metadata,
			}); err != nil {
				return err
			}
			doc.branches[s.branch] = plan.id
			doc.pushUpdate(docUpdate{typ: fbs.UpdateTypeNewCommitUpdate, name: s.branch, newSnap: plan.id,
				updatedAt: doc.nextUpdateTime(now)}, backup, doc.updatesLimit())
			return nil
		})
		var conflict *ConflictError
		if errors.As(err, &conflict) && opts.Rebase && rebases < maxRebases {
			if err := s.checkRebase(ctx, baseID, conflict.Actual); err != nil {
				return SnapshotID{}, err
			}
			if base, err = s.repo.Snapshot(ctx, conflict.Actual); err != nil {
				return SnapshotID{}, err
			}
			baseID = conflict.Actual
			continue
		}
		if err != nil {
			return SnapshotID{}, err
		}
		s.repo.cache.Get(ctx, cacheKey{kind: 's', id: plan.id}, func() (any, int64, error) {
			return plan.parsed, int64(len(plan.parsed.buf)), nil
		})
		s.snap, s.id, s.cs = plan.parsed, plan.id, nil
		return plan.id, nil
	}
}

// buildCommit computes the files of a commit applying the session's changes
// on top of base. It fails with a *ConflictError if base contradicts them
// (which only happens when rebasing).
func (s *Session) buildCommit(ctx context.Context, base *Snapshot, message string, metadata []rawMetadataItem) (*commitPlan, error) {
	cs := s.cs
	baseNodes, err := base.Nodes()
	if err != nil {
		return nil, err
	}
	byPath := make(map[string]*Node, len(baseNodes))
	byID := make(map[NodeID]*Node, len(baseNodes))
	for _, n := range baseNodes {
		byPath[n.Path], byID[n.ID] = n, n
	}
	conflict := func(reason string) error {
		return &ConflictError{Branch: s.branch, Expected: s.id, Actual: base.ID(), Reason: reason}
	}
	for id, n := range cs.deleted {
		if bn, ok := byID[id]; !ok || bn.Path != n.Path {
			return nil, conflict(fmt.Sprintf("deleted node %s no longer exists", n.Path))
		}
	}
	for id := range cs.updatedGroups {
		if bn, ok := byID[id]; !ok || bn.Type != GroupNode {
			return nil, conflict("an updated group no longer exists")
		}
	}
	for id := range cs.updatedArrays {
		if bn, ok := byID[id]; !ok || bn.Type != ArrayNode {
			return nil, conflict("an updated array no longer exists")
		}
	}
	isNew := map[NodeID]bool{}
	for path, n := range cs.newNodes {
		isNew[n.ID] = true
		if bn, ok := byPath[path]; ok {
			if _, deleted := cs.deleted[bn.ID]; !deleted {
				return nil, conflict(fmt.Sprintf("a node already exists at %s", path))
			}
		}
	}
	for id := range cs.chunks {
		if !isNew[id] {
			if _, ok := byID[id]; !ok {
				return nil, conflict("an array with chunk changes no longer exists")
			}
		}
	}

	// Final node list.
	var nodes []*Node
	for _, bn := range baseNodes {
		n, err := cs.overlay(bn)
		if errors.Is(err, ErrNodeNotFound) {
			continue
		}
		c := *n
		if n.Array != nil {
			a := *n.Array
			c.Array = &a
		}
		nodes = append(nodes, &c)
	}
	for _, n := range cs.newNodes {
		c := *n
		if n.Array != nil {
			a := *n.Array
			a.Manifests = nil
			c.Array = &a
		}
		nodes = append(nodes, &c)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Path < nodes[j].Path })

	// New nodes need valid parents: no ancestor may be an array, and a parent
	// group that existed when the session started must not have been deleted
	// by a concurrent commit (upstream: NewNodeInInvalidGroup).
	final := make(map[string]*Node, len(nodes))
	for _, n := range nodes {
		final[n.Path] = n
	}
	for path := range cs.newNodes {
		for _, anc := range ancestors(path) {
			if fn, ok := final[anc]; ok {
				if fn.Type != GroupNode {
					return nil, conflict(fmt.Sprintf("cannot create %s: %s is an array", path, anc))
				}
				continue
			}
			if on, err := s.snap.Node(anc); err == nil && on.Type == GroupNode {
				if _, deletedByUs := cs.deleted[on.ID]; !deletedByUs {
					return nil, conflict(fmt.Sprintf("cannot create %s: its parent group %s was deleted", path, anc))
				}
			}
		}
	}

	// Manifests: rewrite those of arrays with chunk changes (or whose stored
	// extents fall outside a smaller new shape); keep the rest.
	baseFiles, err := base.ManifestFiles()
	if err != nil {
		return nil, err
	}
	fileInfo := make(map[ManifestID]ManifestFileInfo, len(baseFiles))
	for _, f := range baseFiles {
		fileInfo[f.ID] = f
	}
	plan := &commitPlan{id: randomID12(), manifests: map[ManifestID][]byte{}}
	var files []ManifestFileInfo
	seenFile := map[ManifestID]bool{}
	for _, n := range nodes {
		if n.Type != ArrayNode {
			continue
		}
		a := n.Array
		edits := cs.chunks[n.ID]
		changed := len(edits) > 0
		for _, mr := range a.Manifests {
			for d, r := range mr.Extents {
				if d < len(a.Shape) && r.To > a.Shape[d].NumChunks {
					changed = true // shrunk: drop refs outside the new shape
				}
			}
		}
		if !changed {
			// Arrays without chunk changes keep their manifests, as upstream.
			for _, mr := range a.Manifests {
				fi, ok := fileInfo[mr.ID]
				if !ok {
					return nil, fmt.Errorf("%w: snapshot lacks info for manifest %s", ErrFormat, mr.ID)
				}
				if !seenFile[mr.ID] {
					seenFile[mr.ID] = true
					files = append(files, fi)
				}
			}
			continue
		}
		splits := newManifestSplits(a, s.repo.splitSizes(n))
		// Splits to write anew: those with chunk edits, and those overlapping
		// a manifest that cannot be kept as is (it spans several splits, or
		// reaches beyond a smaller new shape).
		dirty := map[string]bool{}
		for _, e := range edits {
			if a.validChunkCoord(e.coords) {
				dirty[splits.of(e.coords)] = true
			}
		}
		keepable := func(mr ManifestRef) bool {
			for d, r := range mr.Extents {
				if d < len(a.Shape) && r.To > a.Shape[d].NumChunks {
					return false
				}
			}
			return len(splits.overlapping(mr.Extents)) == 1
		}
		for _, mr := range a.Manifests {
			if !keepable(mr) {
				for _, k := range splits.overlapping(mr.Extents) {
					dirty[k] = true
				}
			}
		}
		var refs []ManifestRef
		bySplit := map[string][]chunkEntry{}
		for _, mr := range a.Manifests {
			if keepable(mr) && !dirty[splits.overlapping(mr.Extents)[0]] {
				fi, ok := fileInfo[mr.ID]
				if !ok {
					return nil, fmt.Errorf("%w: snapshot lacks info for manifest %s", ErrFormat, mr.ID)
				}
				if !seenFile[mr.ID] {
					seenFile[mr.ID] = true
					files = append(files, fi)
				}
				refs = append(refs, mr)
				continue
			}
			m, err := s.repo.manifest(ctx, mr.ID)
			if err != nil {
				return nil, err
			}
			err = m.forEach(n.ID, func(coords []uint32, ref *ChunkRef) error {
				if mr.contains(coords) && a.validChunkCoord(coords) {
					if _, edited := edits[coordsKey(coords)]; !edited {
						k := splits.of(coords)
						bySplit[k] = append(bySplit[k], chunkEntry{coords: coords, ref: ref})
					}
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
		}
		for _, e := range edits {
			if e.ref != nil && a.validChunkCoord(e.coords) {
				k := splits.of(e.coords)
				bySplit[k] = append(bySplit[k], e)
			}
		}
		// One manifest per split holding chunks, as upstream writes them;
		// its extents are the bounding box of those chunks.
		for _, k := range sortedSplitKeys(bySplit) {
			entries := bySplit[k]
			sort.Slice(entries, func(i, j int) bool { return compareCoords(entries[i].coords, entries[j].coords) < 0 })
			mid := randomID12()
			data, err := encodeManifest(mid, n.ID, entries)
			if err != nil {
				return nil, err
			}
			plan.manifests[mid] = data
			files = append(files, ManifestFileInfo{ID: mid, SizeBytes: uint64(len(data)), NumChunkRefs: uint32(len(entries))})
			refs = append(refs, ManifestRef{ID: mid, Extents: boundingBox(entries)})
		}
		sort.Slice(refs, func(i, j int) bool { return compareExtents(refs[i].Extents, refs[j].Extents) < 0 })
		a.Manifests = refs
	}

	// Snapshot timestamps must increase along the history.
	now := time.Now()
	plan.flushedAt = uint64(now.UnixMicro())
	if parent := uint64(base.FlushedAt().UnixMicro()); plan.flushedAt <= parent {
		// The parent claims to be from the future (a writer with a fast
		// clock): follow it only within the clock-skew allowance.
		if time.UnixMicro(int64(parent)).Sub(now) > maxClockSkew {
			return nil, fmt.Errorf("%w: the parent snapshot is dated %s", ErrClockSkew, base.FlushedAt().Format(time.RFC3339))
		}
		plan.flushedAt = parent + 1
	}
	if plan.snapshot, err = encodeSnapshot(&snapshotSpec{
		id: plan.id, flushedAt: plan.flushedAt, message: message, metadata: metadata,
		manifestFiles: files, nodes: nodes,
	}); err != nil {
		return nil, err
	}
	if plan.parsed, err = parseSnapshot(plan.snapshot); err != nil {
		return nil, fmt.Errorf("icechunk: new snapshot does not decode: %w", err)
	}

	tx := &txLog{id: plan.id, updatedChunks: map[NodeID][][]uint32{}}
	for _, n := range cs.newNodes {
		if n.Type == GroupNode {
			tx.newGroups = append(tx.newGroups, n.ID)
		} else {
			tx.newArrays = append(tx.newArrays, n.ID)
		}
	}
	for id, n := range cs.deleted {
		if n.Type == GroupNode {
			tx.deletedGroups = append(tx.deletedGroups, id)
		} else {
			tx.deletedArrays = append(tx.deletedArrays, id)
		}
	}
	for id := range cs.updatedGroups {
		tx.updatedGroups = append(tx.updatedGroups, id)
	}
	for id := range cs.updatedArrays {
		tx.updatedArrays = append(tx.updatedArrays, id)
	}
	for id, edits := range cs.chunks {
		for _, e := range edits {
			tx.updatedChunks[id] = append(tx.updatedChunks[id], e.coords)
		}
	}
	if plan.txLog, err = encodeTxLog(tx); err != nil {
		return nil, err
	}
	return plan, nil
}

// ---------------------------------------------------------------------------
// Manifest splitting

// manifestSplits divides an array's chunk grid into the regions upstream
// writes separate manifests for: along each dimension, runs of size[d]
// chunks (0: the dimension is not split).
type manifestSplits struct {
	size []uint32
	grid []uint32
}

func newManifestSplits(a *ArrayInfo, size []uint32) manifestSplits {
	grid := make([]uint32, len(a.Shape))
	for d, s := range a.Shape {
		grid[d] = s.NumChunks
	}
	return manifestSplits{size: size, grid: grid}
}

func (ms manifestSplits) index(d int, c uint32) uint32 {
	if d >= len(ms.size) || ms.size[d] == 0 {
		return 0
	}
	return c / ms.size[d]
}

// of returns the key of the split holding chunk coords.
func (ms manifestSplits) of(coords []uint32) string {
	parts := make([]string, len(coords))
	for d, c := range coords {
		parts[d] = fmt.Sprint(ms.index(d, c))
	}
	return strings.Join(parts, ",")
}

// overlapping returns the keys of the splits a manifest's extents touch
// (empty extents cover the whole grid).
func (ms manifestSplits) overlapping(extents []ChunkRange) []string {
	keys := []string{""}
	for d := range ms.grid {
		lo, hi := uint32(0), ms.grid[d]
		if d < len(extents) {
			lo, hi = extents[d].From, extents[d].To
		}
		if hi == 0 {
			hi = 1
		}
		var next []string
		for _, k := range keys {
			for i := ms.index(d, lo); i <= ms.index(d, hi-1); i++ {
				if d > 0 {
					next = append(next, k+","+fmt.Sprint(i))
				} else {
					next = append(next, fmt.Sprint(i))
				}
			}
		}
		keys = next
	}
	return keys
}

func sortedSplitKeys(m map[string][]chunkEntry) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// compareExtents orders manifest refs by their extents.
func compareExtents(a, b []ChunkRange) int {
	for i := 0; i < min(len(a), len(b)); i++ {
		if a[i].From != b[i].From {
			return cmp.Compare(a[i].From, b[i].From)
		}
		if a[i].To != b[i].To {
			return cmp.Compare(a[i].To, b[i].To)
		}
	}
	return cmp.Compare(len(a), len(b))
}

// splitSizes returns, per dimension, how many chunks each manifest of the
// array covers under the repository's manifest.splitting config (0: not
// split). As upstream, the first entry of split_sizes whose array condition
// matches applies, and within it the first condition matching each
// dimension; regular expressions match anywhere in the path or name.
func (r *Repository) splitSizes(n *Node) []uint32 {
	sizes := make([]uint32, len(n.Array.Shape))
	r.mu.Lock()
	info := r.info
	r.mu.Unlock()
	if info == nil {
		return sizes
	}
	cfg, _ := info.Config.(map[string]any)
	manifest, _ := cfg["manifest"].(map[string]any)
	splitting, _ := manifest["splitting"].(map[string]any)
	rules, _ := splitting["split_sizes"].([]any)
	for _, rule := range rules {
		pair, _ := rule.([]any)
		if len(pair) != 2 || !arrayConditionMatches(pair[0], n.Path) {
			continue
		}
		dims, _ := pair[1].([]any)
		for d := range sizes {
			name := ""
			if d < len(n.Array.DimensionNames) {
				name = n.Array.DimensionNames[d]
			}
			for _, dc := range dims {
				m, _ := dc.(map[string]any)
				if dimConditionMatches(m["condition"], d, name) {
					if k, ok := configInt(m["num_chunks"]); ok && k > 0 && k <= math.MaxUint32 {
						sizes[d] = uint32(k)
					}
					break
				}
			}
		}
		return sizes
	}
	return sizes
}

func configInt(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case uint64:
		return int64(min(x, math.MaxInt64)), true
	case float64:
		return int64(x), true
	}
	return 0, false
}

// arrayConditionMatches evaluates a stored ManifestSplitCondition:
// "any_array", {"path_matches": {"regex": ...}}, {"name_matches": ...},
// {"and": [...]} or {"or": [...]}.
func arrayConditionMatches(c any, path string) bool {
	if s, ok := c.(string); ok {
		return s == "any_array"
	}
	m, _ := c.(map[string]any)
	matches := func(v any, s string) bool {
		spec, _ := v.(map[string]any)
		pattern, _ := spec["regex"].(string)
		re, err := regexp.Compile(pattern)
		return err == nil && re.MatchString(s)
	}
	for k, v := range m {
		switch k {
		case "path_matches":
			return matches(v, path)
		case "name_matches":
			return matches(v, path[strings.LastIndexByte(path, '/')+1:])
		case "and", "or":
			list, _ := v.([]any)
			for _, sub := range list {
				if arrayConditionMatches(sub, path) == (k == "or") {
					return k == "or"
				}
			}
			return k == "and"
		}
	}
	return false
}

// dimConditionMatches evaluates a stored ManifestSplitDimCondition: "Any",
// {"Axis": i} or {"DimensionName": name}.
func dimConditionMatches(c any, axis int, name string) bool {
	if s, ok := c.(string); ok {
		return s == "Any"
	}
	m, _ := c.(map[string]any)
	if v, ok := m["Axis"]; ok {
		i, ok := configInt(v)
		return ok && i == int64(axis)
	}
	if v, ok := m["DimensionName"]; ok {
		s, _ := v.(string)
		return name != "" && s == name
	}
	return false
}

// ancestors returns the proper ancestors of an absolute path, root first.
func ancestors(path string) []string {
	if path == "/" {
		return nil
	}
	out := []string{"/"}
	for i := 1; i < len(path); i++ {
		if path[i] == '/' {
			out = append(out, path[:i])
		}
	}
	return out
}

// boundingBox returns the manifest extents covering entries.
func boundingBox(entries []chunkEntry) []ChunkRange {
	nd := len(entries[0].coords)
	ext := make([]ChunkRange, nd)
	for d := range ext {
		ext[d] = ChunkRange{From: ^uint32(0)}
	}
	for _, e := range entries {
		for d, c := range e.coords {
			ext[d].From = min(ext[d].From, c)
			ext[d].To = max(ext[d].To, c+1)
		}
	}
	return ext
}

// checkRebase verifies that the commits between base and the new tip did
// not touch what this session changed, using their transaction logs.
func (s *Session) checkRebase(ctx context.Context, base, tip SnapshotID) error {
	raw, err := s.repo.storage.Get(ctx, repoInfoPath, nil)
	if err != nil {
		return err
	}
	doc, err := parseRepoDoc(raw)
	if err != nil {
		return err
	}
	conflict := func(reason string) error {
		return &ConflictError{Branch: s.branch, Expected: base, Actual: tip, Reason: reason}
	}
	// Transaction logs to check: each commit between base and tip, preceded
	// by the logs of expired ancestors folded into it (spec 2.1:
	// pruned_ancestor_tx_logs is part of a snapshot's history).
	var between []SnapshotID
	for id := tip; id != base; {
		i, ok := doc.snapshotIndex(id)
		if !ok || doc.snapshots[i].parent == nil {
			return conflict("the session's base snapshot is not an ancestor of the branch tip")
		}
		between = append(between, id)
		between = append(between, doc.snapshots[i].pruned...)
		id = *doc.snapshots[i].parent
	}
	touched := map[NodeID]bool{}
	for id := range s.cs.deleted {
		touched[id] = true
	}
	for id := range s.cs.updatedGroups {
		touched[id] = true
	}
	for id := range s.cs.updatedArrays {
		touched[id] = true
	}
	for _, id := range between {
		raw, err := s.repo.storage.Get(ctx, "transactions/"+id.String(), nil)
		if err != nil {
			return conflict(fmt.Sprintf("cannot read transaction log of %s: %v", id, err))
		}
		tx, moves, err := parseTxLog(raw)
		if err != nil {
			return err
		}
		if moves > 0 {
			return conflict(fmt.Sprintf("commit %s moved nodes", id))
		}
		for _, list := range [][]NodeID{tx.updatedGroups, tx.updatedArrays, tx.deletedGroups, tx.deletedArrays} {
			for _, n := range list {
				if touched[n] {
					return conflict(fmt.Sprintf("commit %s changed or deleted a node this session changed", id))
				}
				if _, ok := s.cs.chunks[n]; ok {
					return conflict(fmt.Sprintf("commit %s changed or deleted an array this session wrote chunks to", id))
				}
			}
		}
		for n, coords := range tx.updatedChunks {
			if touched[n] {
				return conflict(fmt.Sprintf("commit %s wrote chunks of a node this session changed", id))
			}
			ours := s.cs.chunks[n]
			for _, c := range coords {
				if _, clash := ours[coordsKey(c)]; clash {
					return conflict(fmt.Sprintf("commit %s wrote chunk %v that this session also wrote", id, c))
				}
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Branches and tags

func (r *Repository) refUpdate(ctx context.Context, f func(doc *repoDoc) (docUpdate, error)) error {
	return r.updateRepo(ctx, func(doc *repoDoc, backup string, now time.Time) error {
		u, err := f(doc)
		if err != nil {
			return err
		}
		u.updatedAt = doc.nextUpdateTime(now)
		doc.pushUpdate(u, backup, doc.updatesLimit())
		return nil
	})
}

func checkSnapshot(doc *repoDoc, id SnapshotID) error {
	if _, ok := doc.snapshotIndex(id); !ok {
		return fmt.Errorf("%w: %s", ErrSnapshotNotFound, id)
	}
	return nil
}

// CreateBranch creates a branch pointing at snapshot id.
func (r *Repository) CreateBranch(ctx context.Context, name string, id SnapshotID) error {
	if err := r.validRefName(name); err != nil {
		return err
	}
	return r.refUpdate(ctx, func(doc *repoDoc) (docUpdate, error) {
		if _, exists := doc.branches[name]; exists {
			return docUpdate{}, fmt.Errorf("%w: branch %s", ErrAlreadyExists, name)
		}
		if err := checkSnapshot(doc, id); err != nil {
			return docUpdate{}, err
		}
		doc.branches[name] = id
		return docUpdate{typ: fbs.UpdateTypeBranchCreatedUpdate, name: name}, nil
	})
}

// ResetBranch points an existing branch at snapshot id.
func (r *Repository) ResetBranch(ctx context.Context, name string, id SnapshotID) error {
	return r.refUpdate(ctx, func(doc *repoDoc) (docUpdate, error) {
		prev, exists := doc.branches[name]
		if !exists {
			return docUpdate{}, fmt.Errorf("%w: branch %s", ErrRefNotFound, name)
		}
		if err := checkSnapshot(doc, id); err != nil {
			return docUpdate{}, err
		}
		doc.branches[name] = id
		return docUpdate{typ: fbs.UpdateTypeBranchResetUpdate, name: name, prevSnap: prev}, nil
	})
}

// DeleteBranch deletes a branch. The main branch cannot be deleted.
func (r *Repository) DeleteBranch(ctx context.Context, name string) error {
	if name == "main" {
		return ErrCannotDeleteMain
	}
	return r.refUpdate(ctx, func(doc *repoDoc) (docUpdate, error) {
		prev, exists := doc.branches[name]
		if !exists {
			return docUpdate{}, fmt.Errorf("%w: branch %s", ErrRefNotFound, name)
		}
		delete(doc.branches, name)
		return docUpdate{typ: fbs.UpdateTypeBranchDeletedUpdate, name: name, prevSnap: prev}, nil
	})
}

// CreateTag creates an immutable tag pointing at snapshot id. Names of
// deleted tags cannot be reused.
func (r *Repository) CreateTag(ctx context.Context, name string, id SnapshotID) error {
	if err := r.validRefName(name); err != nil {
		return err
	}
	return r.refUpdate(ctx, func(doc *repoDoc) (docUpdate, error) {
		if !doc.featureFlagEnabled(featureCreateTag) {
			return docUpdate{}, fmt.Errorf("%w: create_tag", ErrFeatureDisabled)
		}
		if _, exists := doc.tags[name]; exists {
			return docUpdate{}, fmt.Errorf("%w: tag %s", ErrAlreadyExists, name)
		}
		for _, d := range doc.deletedTags {
			if d == name {
				return docUpdate{}, fmt.Errorf("%w: tag %s was deleted and its name cannot be reused", ErrAlreadyExists, name)
			}
		}
		if err := checkSnapshot(doc, id); err != nil {
			return docUpdate{}, err
		}
		doc.tags[name] = id
		return docUpdate{typ: fbs.UpdateTypeTagCreatedUpdate, name: name}, nil
	})
}

// DeleteTag deletes a tag; its name can never be used again.
func (r *Repository) DeleteTag(ctx context.Context, name string) error {
	return r.refUpdate(ctx, func(doc *repoDoc) (docUpdate, error) {
		if !doc.featureFlagEnabled(featureDeleteTag) {
			return docUpdate{}, fmt.Errorf("%w: delete_tag", ErrFeatureDisabled)
		}
		prev, exists := doc.tags[name]
		if !exists {
			return docUpdate{}, fmt.Errorf("%w: tag %s", ErrRefNotFound, name)
		}
		delete(doc.tags, name)
		doc.deletedTags = append(doc.deletedTags, name)
		return docUpdate{typ: fbs.UpdateTypeTagDeletedUpdate, name: name, prevSnap: prev}, nil
	})
}
