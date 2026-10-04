package icechunk

import (
	"context"
	"fmt"
	"iter"
	"sort"
	"sync"

	"github.com/solomonsealed/icechunk-go/zarr"
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
		m, err := s.repo.manifest(ctx, mr.ID)
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
	ref, err := s.ChunkRef(ctx, path, coords)
	if err != nil || ref == nil {
		return nil, false, err
	}
	data, err := s.repo.fetchChunk(ctx, ref, offset, length)
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
				m, err := s.repo.manifest(ctx, mr.ID)
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

// OpenArray opens the array at path for typed, region-based reads.
func (s *Session) OpenArray(ctx context.Context, path string) (*zarr.Array, error) {
	n, err := s.arrayNode(path)
	if err != nil {
		return nil, err
	}
	return zarr.OpenArray(n.ZarrMetadata, &chunkSource{s: s, n: n}, &zarr.Options{Concurrency: s.repo.opts.Concurrency})
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
	s *Session
	n *Node
}

func (c *chunkSource) GetChunk(ctx context.Context, coords []uint32, offset, length int64) ([]byte, bool, error) {
	ref, err := c.s.chunkRef(ctx, c.n, coords)
	if err != nil || ref == nil {
		return nil, false, err
	}
	data, err := c.s.repo.fetchChunk(ctx, ref, offset, length)
	return data, err == nil, err
}

// SetChunk and DeleteChunk make chunkSource a zarr.ChunkWriter for
// writable sessions.
func (c *chunkSource) SetChunk(ctx context.Context, coords []uint32, data []byte) error {
	return c.s.SetChunk(ctx, c.n.Path, coords, data)
}

func (c *chunkSource) DeleteChunk(ctx context.Context, coords []uint32) error {
	return c.s.DeleteChunk(ctx, c.n.Path, coords)
}

func (c *chunkSource) ChunkSize(ctx context.Context, coords []uint32) (int64, bool, error) {
	ref, err := c.s.chunkRef(ctx, c.n, coords)
	if err != nil || ref == nil {
		return 0, false, err
	}
	return int64(ref.Size()), true, nil
}
