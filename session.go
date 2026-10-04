package icechunk

import (
	"context"
	"fmt"
	"iter"

	"github.com/solomonsealed/icechunk-go/zarr"
)

// Session is a read-only view of one snapshot. It is safe for concurrent use.
type Session struct {
	repo *Repository
	snap *Snapshot
	id   SnapshotID
}

// SnapshotID returns the id of the snapshot being read.
func (s *Session) SnapshotID() SnapshotID { return s.id }

// Snapshot returns the decoded snapshot.
func (s *Session) Snapshot() *Snapshot { return s.snap }

// Repository returns the repository the session reads from.
func (s *Session) Repository() *Repository { return s.repo }

// Node returns the array or group at path ("/", "/a/b" or "a/b").
func (s *Session) Node(path string) (*Node, error) { return s.snap.Node(path) }

// Nodes returns all arrays and groups, sorted by path.
func (s *Session) Nodes() ([]*Node, error) { return s.snap.Nodes() }

func (s *Session) arrayNode(path string) (*Node, error) {
	n, err := s.snap.Node(path)
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
		stop := fmt.Errorf("stop")
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
	n, err := s.snap.Node(path)
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

func (c *chunkSource) ChunkSize(ctx context.Context, coords []uint32) (int64, bool, error) {
	ref, err := c.s.chunkRef(ctx, c.n, coords)
	if err != nil || ref == nil {
		return 0, false, err
	}
	return int64(ref.Size()), true, nil
}
