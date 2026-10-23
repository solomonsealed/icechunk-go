package icechunk

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"math"
	"sort"
	"strings"
	"sync"

	zarr "github.com/solomonsealed/zarr-go"
)

// Session is a view of one snapshot. Read-only sessions come from
// Repository.ReadonlySession; writable ones from Repository.WritableSession,
// whose reads include the session's uncommitted changes. Sessions are safe
// for concurrent use.
type Session struct {
	repo *Repository
	snap *Snapshot
	id   SnapshotID

	// Writable sessions only.
	branch string
	mu     sync.RWMutex
	cs     *changeSet // nil when read-only (or after a successful commit)
}

// Writable reports whether the session accepts changes.
func (s *Session) Writable() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cs != nil
}

// Branch returns the branch a writable session commits to.
func (s *Session) Branch() string { return s.branch }

// SnapshotID returns the id of the snapshot being read (for a writable
// session: its base snapshot, or the new one after Commit).
func (s *Session) SnapshotID() SnapshotID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.id
}

// Snapshot returns the decoded snapshot (see SnapshotID).
func (s *Session) Snapshot() *Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snap
}

// Repository returns the repository the session reads from.
func (s *Session) Repository() *Repository { return s.repo }

// Node returns the array or group at path ("/", "/a/b" or "a/b").
func (s *Session) Node(path string) (*Node, error) { return s.node(path) }

// Nodes returns all arrays and groups, sorted by path.
func (s *Session) Nodes() ([]*Node, error) { return s.nodes() }

func (s *Session) arrayNode(path string) (*Node, error) {
	n, err := s.node(path)
	if err != nil {
		return nil, err
	}
	if n.Type != ArrayNode {
		return nil, fmt.Errorf("%w: %s", ErrNotAnArray, n.Path)
	}
	return n, nil
}

// ChunkRef returns the reference for the chunk at coords of the array at
// path, or nil if that chunk was never written (readers use the fill value).
func (s *Session) ChunkRef(ctx context.Context, path string, coords []uint32) (*ChunkRef, error) {
	n, err := s.arrayNode(path)
	if err != nil {
		return nil, err
	}
	return s.chunkRef(ctx, n, coords)
}

func (s *Session) chunkRef(ctx context.Context, n *Node, coords []uint32) (*ChunkRef, error) {
	if ref, edited := s.sessionChunk(n, coords); edited {
		return ref, nil
	}
	a := n.Array
	if len(a.Manifests) == 0 || !a.validChunkCoord(coords) {
		return nil, nil
	}
	for i := range a.Manifests {
		mr := &a.Manifests[i]
		if !mr.contains(coords) {
			continue
		}
		m, err := s.repo.manifest(s.repo.reqArray(ctx, n.Path), mr.ID)
		if err != nil {
			return nil, err
		}
		ref, found, err := m.lookup(n.ID, coords)
		if err != nil || found {
			return ref, err
		}
		// Extents of an array's manifests never overlap: not found means absent.
		return nil, nil
	}
	return nil, nil
}

// GetChunk returns the encoded bytes of a chunk. ok is false when the chunk
// was never written.
func (s *Session) GetChunk(ctx context.Context, path string, coords []uint32) (data []byte, ok bool, err error) {
	return s.GetChunkRange(ctx, path, coords, 0, -1)
}

// GetChunkRange returns length encoded bytes of a chunk starting at offset
// (length < 0 reads to the end). ok is false when the chunk was never written.
func (s *Session) GetChunkRange(ctx context.Context, path string, coords []uint32, offset, length int64) ([]byte, bool, error) {
	n, err := s.arrayNode(path)
	if err != nil {
		return nil, false, err
	}
	ref, err := s.chunkRef(ctx, n, coords)
	if err != nil || ref == nil {
		return nil, false, err
	}
	data, err := s.repo.fetchChunk(s.repo.reqChunk(ctx, n.Path, coords), ref, offset, length)
	return data, err == nil, err
}

// ChunkEntry pairs chunk coordinates with their reference.
type ChunkEntry struct {
	Coords []uint32
	Ref    *ChunkRef
}

// ChunkRefs yields every written chunk of the array at path. Each manifest's
// refs come out in coordinate order.
func (s *Session) ChunkRefs(ctx context.Context, path string) iter.Seq2[ChunkEntry, error] {
	return func(yield func(ChunkEntry, error) bool) {
		n, err := s.arrayNode(path)
		if err != nil {
			yield(ChunkEntry{}, err)
			return
		}
		// Session edits replace what the manifests hold for those chunks.
		var edits map[string]chunkEntry
		isNew := false
		s.mu.RLock()
		if s.cs != nil {
			edits = make(map[string]chunkEntry, len(s.cs.chunks[n.ID]))
			for k, e := range s.cs.chunks[n.ID] {
				edits[k] = e
			}
			if nn, ok := s.cs.newNodes[n.Path]; ok && nn.ID == n.ID {
				isNew = true
			}
		}
		s.mu.RUnlock()
		stop := fmt.Errorf("stop")
		if !isNew {
			for i := range n.Array.Manifests {
				mr := &n.Array.Manifests[i]
				m, err := s.repo.manifest(s.repo.reqArray(ctx, n.Path), mr.ID)
				if err != nil {
					yield(ChunkEntry{}, err)
					return
				}
				err = m.forEach(n.ID, func(coords []uint32, ref *ChunkRef) error {
					if !mr.contains(coords) || !n.Array.validChunkCoord(coords) {
						return nil
					}
					if _, edited := edits[coordsKey(coords)]; edited {
						return nil
					}
					if !yield(ChunkEntry{Coords: coords, Ref: ref}, nil) {
						return stop
					}
					return nil
				})
				if err == stop {
					return
				}
				if err != nil {
					yield(ChunkEntry{}, err)
					return
				}
			}
		}
		for _, e := range sortedEdits(edits) {
			if e.ref == nil || !n.Array.validChunkCoord(e.coords) {
				continue
			}
			if !yield(ChunkEntry{Coords: e.coords, Ref: e.ref}, nil) {
				return
			}
		}
	}
}

func sortedEdits(m map[string]chunkEntry) []chunkEntry {
	out := make([]chunkEntry, 0, len(m))
	for _, e := range m {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return compareCoords(out[i].coords, out[j].coords) < 0 })
	return out
}

// OpenArray opens the array at path for typed, region-based reads (and,
// in a writable session, writes).
func (s *Session) OpenArray(ctx context.Context, path string) (*zarr.Array, error) {
	n, err := s.arrayNode(path)
	if err != nil {
		return nil, err
	}
	meta, err := zarr.ParseMetadata(n.ZarrMetadata)
	if err != nil {
		return nil, err
	}
	return zarr.NewArrayFromMetadata(meta, &chunkSource{s: s, n: n, meta: meta}, &zarr.Options{Concurrency: s.repo.opts.Concurrency})
}

// Attributes returns the attributes of the array or group at path.
func (s *Session) Attributes(path string) (map[string]any, error) {
	n, err := s.node(path)
	if err != nil {
		return nil, err
	}
	return zarr.Attributes(n.ZarrMetadata)
}

// chunkSource adapts one array node to zarr.ChunkSource.
type chunkSource struct {
	s    *Session
	mu   sync.Mutex
	n    *Node          // guarded by mu
	meta *zarr.Metadata // as the array was opened
	// prepared is set once the first write has checked the array's
	// current zarr.json (see prepareWrite).
	prepared bool
}

func (c *chunkSource) node() *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// prepareWrite runs before the first chunk is written or deleted through
// the array. It reads the array's zarr.json again, since the session may
// have replaced it since the array was opened: if the new one lays out
// chunks differently the write fails. Then, as zarr-python 3.4.1 does
// through upstream's store, it stores the repaired document of a stored one
// whose repair moves chunks (a chunk size of 0 read as 1, or as the inner
// chunk size of a shard; see zarr.Metadata.RepairPending), so that readers
// of the stored document find the chunks written under the repaired one.
// Arrays whose chunk key encoding upstream's store cannot address are
// refused, as zarr-python's writes to them fail there.
func (c *chunkSource) prepareWrite(ctx context.Context) (*Node, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.prepared {
		return c.n, nil
	}
	cur, err := c.s.arrayNode(c.n.Path)
	if err != nil {
		return nil, err
	}
	if cur.ID != c.n.ID {
		return nil, fmt.Errorf("%w: %s was replaced in this session", ErrNodeNotFound, c.n.Path)
	}
	m := c.meta
	if !bytes.Equal(cur.ZarrMetadata, c.n.ZarrMetadata) {
		if m, err = zarr.ParseMetadata(cur.ZarrMetadata); err != nil {
			return nil, fmt.Errorf("icechunk: array %s: %w", cur.Path, err)
		}
		if chunkLayout(m) != chunkLayout(c.meta) {
			return nil, fmt.Errorf("icechunk: array %s: its zarr.json has changed since the array was opened; reopen the array to write to it", cur.Path)
		}
	}
	if err := checkChunkKeys(cur.Path, m); err != nil {
		return nil, err
	}
	if m.RepairPending() {
		doc, err := m.Document()
		if err != nil {
			return nil, err
		}
		if err := c.s.SetMetadata(ctx, cur.Path, doc); err != nil {
			return nil, err
		}
		if cur, err = c.s.arrayNode(cur.Path); err != nil {
			return nil, err
		}
	}
	c.n, c.prepared = cur, true
	return cur, nil
}

// checkChunkKeys refuses arrays whose chunks zarr clients would look for
// under keys other than "<path>/c/<i>/<j>": upstream's store addresses
// chunks only by those keys (the "default" chunk key encoding with "/"),
// and refuses any other.
func checkChunkKeys(path string, m *zarr.Metadata) error {
	if k := m.ChunkKeyEncoding; k.Name != "default" || k.Separator != "/" {
		return fmt.Errorf("%w: array %s: Icechunk stores chunks only under the \"default\" chunk key encoding with separator \"/\", not %q with %q", ErrInvalidMetadata, path, k.Name, cmp.Or(k.Template, k.Separator))
	}
	return nil
}

// chunkLayout describes where an array's chunks lie: its regular chunk
// shape, or the chunk edges along each axis of a rectilinear grid, and the
// inner chunk shape of a sharded array.
func chunkLayout(m *zarr.Metadata) string {
	var b strings.Builder
	if grid := m.GridShape(); len(m.ChunkShape) == len(grid) {
		fmt.Fprint(&b, m.ChunkShape)
	} else {
		for d := range grid {
			coords := make([]uint64, len(grid))
			b.WriteString("[")
			for i := range grid[d] {
				coords[d] = i
				_, shape := m.ChunkBounds(coords)
				fmt.Fprint(&b, " ", shape[d])
			}
			b.WriteString("]")
		}
	}
	for _, c := range m.Codecs {
		if c.Name == "sharding_indexed" {
			var cfg struct {
				ChunkShape []uint64 `json:"chunk_shape"`
			}
			json.Unmarshal(c.Configuration, &cfg)
			fmt.Fprint(&b, " ", cfg.ChunkShape)
			break
		}
	}
	return b.String()
}

// chunkIndices converts zarr's chunk coordinates to Icechunk's.
func chunkIndices(coords []uint64) ([]uint32, error) {
	out := make([]uint32, len(coords))
	for i, c := range coords {
		if c > math.MaxUint32 {
			return nil, fmt.Errorf("icechunk: chunk coordinate %d out of range", c)
		}
		out[i] = uint32(c)
	}
	return out, nil
}

func (c *chunkSource) GetChunk(ctx context.Context, coords []uint64, r zarr.ByteRange) ([]byte, bool, error) {
	idx, err := chunkIndices(coords)
	if err != nil {
		return nil, false, err
	}
	n := c.node()
	ref, err := c.s.chunkRef(ctx, n, idx)
	if err != nil || ref == nil {
		return nil, false, err
	}
	// Chunk sizes are known from the manifest, so suffix ranges (for
	// shard indexes) need no extra request.
	off, length := r.Offset, r.Length
	if off < 0 {
		length = -off
		off += int64(ref.Size()) // out of range if the suffix is longer than the chunk
	} else if length == 0 {
		length = -1
	}
	data, err := c.s.repo.fetchChunk(c.s.repo.reqChunk(ctx, n.Path, idx), ref, off, length)
	return data, err == nil, err
}

// SetChunk and DeleteChunk make chunkSource a zarr.ChunkWriter for
// writable sessions.
func (c *chunkSource) SetChunk(ctx context.Context, coords []uint64, data []byte) error {
	idx, err := chunkIndices(coords)
	if err != nil {
		return err
	}
	n, err := c.prepareWrite(ctx)
	if err != nil {
		return err
	}
	return c.s.SetChunk(ctx, n.Path, idx, data)
}

func (c *chunkSource) DeleteChunk(ctx context.Context, coords []uint64) error {
	idx, err := chunkIndices(coords)
	if err != nil {
		return err
	}
	n, err := c.prepareWrite(ctx)
	if err != nil {
		return err
	}
	return c.s.DeleteChunk(ctx, n.Path, idx)
}
