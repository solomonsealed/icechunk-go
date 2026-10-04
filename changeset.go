package icechunk

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/solomonsealed/icechunk-go/storage"
	"github.com/solomonsealed/icechunk-go/zarr"
)

var (
	// ErrReadOnlySession is returned when writing through a read-only
	// session (or one that has already committed).
	ErrReadOnlySession = errors.New("icechunk: session is read-only")
	// ErrReadOnlyStorage means the repository's storage cannot be written
	// (it does not implement storage.Writer).
	ErrReadOnlyStorage = errors.New("icechunk: storage does not support writes")
	// ErrAlreadyExists means a node or ref already exists.
	ErrAlreadyExists = errors.New("icechunk: already exists")
	// ErrInvalidChunkCoords means chunk coordinates are outside the array's chunk grid.
	ErrInvalidChunkCoords = errors.New("icechunk: invalid chunk coordinates")
)

// changeSet records a session's uncommitted edits, with upstream's
// semantics: nodes created in the session are tracked by path and never
// appear as updated or deleted; deleting them simply forgets them.
type changeSet struct {
	newNodes      map[string]*Node      // path -> node created in this session
	updatedGroups map[NodeID][]byte     // existing groups with new zarr.json
	updatedArrays map[NodeID]*arrayEdit // existing arrays with new zarr.json
	deleted       map[NodeID]*Node      // existing nodes deleted in this session
	chunks        map[NodeID]map[string]chunkEntry
}

type arrayEdit struct {
	userData []byte
	shape    []DimensionShape
	dims     []string
}

func newChangeSet() *changeSet {
	return &changeSet{
		newNodes:      map[string]*Node{},
		updatedGroups: map[NodeID][]byte{},
		updatedArrays: map[NodeID]*arrayEdit{},
		deleted:       map[NodeID]*Node{},
		chunks:        map[NodeID]map[string]chunkEntry{},
	}
}

func (cs *changeSet) isEmpty() bool {
	return len(cs.newNodes) == 0 && len(cs.updatedGroups) == 0 && len(cs.updatedArrays) == 0 &&
		len(cs.deleted) == 0 && len(cs.chunks) == 0
}

func coordsKey(coords []uint32) string {
	b := make([]byte, 4*len(coords))
	for i, c := range coords {
		binary.BigEndian.PutUint32(b[4*i:], c)
	}
	return string(b)
}

func randomNodeID() NodeID {
	var id NodeID
	rand.Read(id[:])
	return id
}

func randomID12() ObjectID12 {
	var id ObjectID12
	rand.Read(id[:])
	return id
}

// parseNodeMetadata reads what Icechunk itself needs from a zarr.json: the
// node type and, for arrays, the shape in chunks and the dimension names.
// Codecs and data types are not validated, as upstream does not either.
func parseNodeMetadata(doc []byte) (NodeType, *arrayEdit, error) {
	var m struct {
		NodeType  string   `json:"node_type"`
		Shape     []uint64 `json:"shape"`
		ChunkGrid struct {
			Name          string `json:"name"`
			Configuration struct {
				ChunkShape  []uint64          `json:"chunk_shape"`
				ChunkShapes []json.RawMessage `json:"chunk_shapes"`
			} `json:"configuration"`
		} `json:"chunk_grid"`
		DimensionNames []*string `json:"dimension_names"`
	}
	if err := json.Unmarshal(doc, &m); err != nil {
		return 0, nil, fmt.Errorf("icechunk: invalid zarr.json: %w", err)
	}
	switch m.NodeType {
	case "group":
		return GroupNode, nil, nil
	case "array":
	default:
		return 0, nil, fmt.Errorf("icechunk: zarr.json node_type must be \"array\" or \"group\", not %q", m.NodeType)
	}
	edit := &arrayEdit{userData: doc, shape: make([]DimensionShape, len(m.Shape))}
	cfg := m.ChunkGrid.Configuration
	for d, n := range m.Shape {
		var num uint64
		switch m.ChunkGrid.Name {
		case "regular":
			if len(cfg.ChunkShape) != len(m.Shape) {
				return 0, nil, fmt.Errorf("icechunk: chunk_shape has %d dimensions, shape has %d", len(cfg.ChunkShape), len(m.Shape))
			}
			if c := cfg.ChunkShape[d]; c > 0 {
				num = (n + c - 1) / c
			}
		case "rectilinear":
			if len(cfg.ChunkShapes) != len(m.Shape) {
				return 0, nil, fmt.Errorf("icechunk: chunk_shapes has %d dimensions, shape has %d", len(cfg.ChunkShapes), len(m.Shape))
			}
			var step uint64
			var entries []json.RawMessage
			switch {
			case json.Unmarshal(cfg.ChunkShapes[d], &step) == nil && step > 0:
				num = (n + step - 1) / step
			case json.Unmarshal(cfg.ChunkShapes[d], &entries) == nil:
				for _, e := range entries {
					var rle []uint64
					if json.Unmarshal(e, &rle) == nil && len(rle) == 2 {
						num += rle[1]
					} else {
						num++
					}
				}
			default:
				return 0, nil, fmt.Errorf("icechunk: invalid rectilinear chunk_shapes entry %s", cfg.ChunkShapes[d])
			}
		default:
			return 0, nil, fmt.Errorf("icechunk: unsupported chunk grid %q", m.ChunkGrid.Name)
		}
		if num > 1<<32-1 {
			return 0, nil, fmt.Errorf("icechunk: too many chunks along dimension %d", d)
		}
		edit.shape[d] = DimensionShape{ArrayLength: n, NumChunks: uint32(num)}
	}
	if m.DimensionNames != nil {
		edit.dims = make([]string, len(m.DimensionNames))
		for i, name := range m.DimensionNames {
			if name != nil {
				edit.dims[i] = *name
			}
		}
	}
	return ArrayNode, edit, nil
}

// ---------------------------------------------------------------------------
// Session reads that see uncommitted changes

// node returns the node at path as the session currently sees it.
func (s *Session) node(path string) (*Node, error) {
	path, err := normalizePath(path)
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.cs == nil {
		return s.snap.Node(path)
	}
	return s.nodeLocked(path)
}

func (s *Session) nodeLocked(path string) (*Node, error) {
	if n, ok := s.cs.newNodes[path]; ok {
		return n, nil
	}
	n, err := s.snap.Node(path)
	if err != nil {
		return nil, err
	}
	return s.cs.overlay(n)
}

// overlay applies session edits to a node from the base snapshot.
func (cs *changeSet) overlay(n *Node) (*Node, error) {
	if _, gone := cs.deleted[n.ID]; gone {
		return nil, fmt.Errorf("%w: %s", ErrNodeNotFound, n.Path)
	}
	if md, ok := cs.updatedGroups[n.ID]; ok {
		c := *n
		c.ZarrMetadata = md
		return &c, nil
	}
	if e, ok := cs.updatedArrays[n.ID]; ok {
		c := *n
		a := *n.Array
		c.ZarrMetadata, a.Shape, a.DimensionNames = e.userData, e.shape, e.dims
		c.Array = &a
		return &c, nil
	}
	return n, nil
}

// nodes returns every node as the session sees it, sorted by path.
func (s *Session) nodes() ([]*Node, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	base, err := s.snap.Nodes()
	if err != nil || s.cs == nil {
		return base, err
	}
	out := make([]*Node, 0, len(base)+len(s.cs.newNodes))
	for _, n := range base {
		if _, replaced := s.cs.newNodes[n.Path]; replaced {
			continue
		}
		o, err := s.cs.overlay(n)
		if errors.Is(err, ErrNodeNotFound) {
			continue
		}
		out = append(out, o)
	}
	for _, n := range s.cs.newNodes {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

// sessionChunk returns the session's edit for a chunk, if any.
func (s *Session) sessionChunk(n *Node, coords []uint32) (ref *ChunkRef, edited bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.cs == nil {
		return nil, false
	}
	if e, ok := s.cs.chunks[n.ID][coordsKey(coords)]; ok {
		return e.ref, true
	}
	if _, isNew := s.cs.newNodes[n.Path]; isNew && s.cs.newNodes[n.Path].ID == n.ID {
		return nil, true // a new array has no chunks besides session ones
	}
	return nil, false
}

// ---------------------------------------------------------------------------
// Mutations

func (s *Session) writable() (storage.Writer, error) {
	s.mu.RLock()
	readOnly := s.cs == nil
	s.mu.RUnlock()
	if readOnly {
		return nil, ErrReadOnlySession
	}
	w, ok := s.repo.storage.(storage.Writer)
	if !ok {
		return nil, ErrReadOnlyStorage
	}
	return w, nil
}

// SetMetadata creates or updates the array or group at path from its
// zarr.json document, like writing "<path>/zarr.json" to a Zarr store.
func (s *Session) SetMetadata(ctx context.Context, path string, zarrJSON []byte) error {
	if _, err := s.writable(); err != nil {
		return err
	}
	path, err := normalizePath(path)
	if err != nil {
		return err
	}
	typ, edit, err := parseNodeMetadata(zarrJSON)
	if err != nil {
		return err
	}
	doc := append([]byte(nil), zarrJSON...)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cs == nil {
		return ErrReadOnlySession
	}
	existing, err := s.nodeLocked(path)
	switch {
	case errors.Is(err, ErrNodeNotFound):
		for _, anc := range ancestors(path) {
			if an, err := s.nodeLocked(anc); err == nil && an.Type != GroupNode {
				return fmt.Errorf("%w: cannot create %s inside array %s", ErrAlreadyExists, path, anc)
			}
		}
		n := &Node{ID: randomNodeID(), Path: path, Type: typ, ZarrMetadata: doc}
		if typ == ArrayNode {
			n.Array = &ArrayInfo{Shape: edit.shape, DimensionNames: edit.dims}
		}
		s.cs.newNodes[path] = n
		return nil
	case err != nil:
		return err
	case existing.Type != typ:
		return fmt.Errorf("%w: %s is a %s", ErrAlreadyExists, path, existing.Type)
	case bytes.Equal(existing.ZarrMetadata, doc):
		return nil
	}
	if n, isNew := s.cs.newNodes[path]; isNew {
		c := *n
		c.ZarrMetadata = doc
		if typ == ArrayNode {
			c.Array = &ArrayInfo{Shape: edit.shape, DimensionNames: edit.dims}
		}
		s.cs.newNodes[path] = &c
		return nil
	}
	if typ == GroupNode {
		s.cs.updatedGroups[existing.ID] = doc
	} else {
		s.cs.updatedArrays[existing.ID] = edit
	}
	return nil
}

// CreateGroup creates a group with the given attributes (nil for none),
// creating missing parent groups as zarr-python does.
func (s *Session) CreateGroup(ctx context.Context, path string, attributes map[string]any) error {
	if attributes == nil {
		attributes = map[string]any{}
	}
	doc, err := json.Marshal(map[string]any{"zarr_format": 3, "node_type": "group", "attributes": attributes})
	if err != nil {
		return err
	}
	if _, err := s.node(path); err == nil {
		return fmt.Errorf("%w: %s", ErrAlreadyExists, path)
	}
	if err := s.ensureParents(ctx, path); err != nil {
		return err
	}
	return s.SetMetadata(ctx, path, doc)
}

// ensureParents creates the missing ancestor groups of path.
func (s *Session) ensureParents(ctx context.Context, path string) error {
	path, err := normalizePath(path)
	if err != nil || path == "/" {
		return err
	}
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for i := 0; i < len(parts); i++ {
		parent := "/" + strings.Join(parts[:i], "/")
		n, err := s.node(parent)
		switch {
		case errors.Is(err, ErrNodeNotFound):
			doc := []byte(`{"zarr_format":3,"node_type":"group","attributes":{}}`)
			if err := s.SetMetadata(ctx, parent, doc); err != nil {
				return err
			}
		case err != nil:
			return err
		case n.Type != GroupNode:
			return fmt.Errorf("%w: %s is an array, not a group", ErrAlreadyExists, parent)
		}
	}
	return nil
}

// DeleteNode deletes the array or group at path; deleting a group deletes
// everything below it. Deleting a missing node is not an error.
func (s *Session) DeleteNode(ctx context.Context, path string) error {
	if _, err := s.writable(); err != nil {
		return err
	}
	path, err := normalizePath(path)
	if err != nil {
		return err
	}
	all, err := s.nodes()
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cs == nil {
		return ErrReadOnlySession // committed meanwhile
	}
	for _, n := range all {
		if n.Path != path && !(path == "/" || strings.HasPrefix(n.Path, path+"/")) {
			continue
		}
		if nn, isNew := s.cs.newNodes[n.Path]; isNew && nn.ID == n.ID {
			delete(s.cs.newNodes, n.Path)
		} else {
			s.cs.deleted[n.ID] = n
			delete(s.cs.updatedGroups, n.ID)
			delete(s.cs.updatedArrays, n.ID)
		}
		delete(s.cs.chunks, n.ID)
	}
	return nil
}

func (s *Session) checkCoords(n *Node, coords []uint32) error {
	if !n.Array.validChunkCoord(coords) {
		return fmt.Errorf("%w: %v for array %s with %d dimensions", ErrInvalidChunkCoords, coords, n.Path, len(n.Array.Shape))
	}
	return nil
}

func (s *Session) setChunkRef(n *Node, coords []uint32, ref *ChunkRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cs == nil {
		return ErrReadOnlySession
	}
	// The node must still be the same one (not deleted or replaced).
	cur, err := s.nodeLocked(n.Path)
	if err != nil {
		return err
	}
	if cur.ID != n.ID {
		return fmt.Errorf("%w: %s was replaced in this session", ErrNodeNotFound, n.Path)
	}
	m := s.cs.chunks[n.ID]
	if m == nil {
		m = map[string]chunkEntry{}
		s.cs.chunks[n.ID] = m
	}
	m[coordsKey(coords)] = chunkEntry{coords: append([]uint32(nil), coords...), ref: ref}
	return nil
}

// inlineThreshold is the largest chunk stored inside manifests (upstream's
// inline_chunk_threshold_bytes, default 512).
func (r *Repository) inlineThreshold() int {
	r.mu.Lock()
	info := r.info
	r.mu.Unlock()
	if r.spec >= 2 {
		if info != nil {
			if cfg, ok := info.Config.(map[string]any); ok {
				switch v := cfg["inline_chunk_threshold_bytes"].(type) {
				case uint64:
					return int(v)
				case int64:
					return int(v)
				}
			}
		}
	}
	return 512
}

// SetChunk stores the encoded bytes of one chunk. Small chunks are kept
// inline in the manifest; others are uploaded right away to chunks/.
func (s *Session) SetChunk(ctx context.Context, path string, coords []uint32, data []byte) error {
	w, err := s.writable()
	if err != nil {
		return err
	}
	n, err := s.arrayNode(path)
	if err != nil {
		return err
	}
	if err := s.checkCoords(n, coords); err != nil {
		return err
	}
	if len(data) == 0 {
		return fmt.Errorf("icechunk: refusing to store an empty chunk at %v of %s (use DeleteChunk)", coords, n.Path)
	}
	var ref *ChunkRef
	if len(data) <= s.repo.inlineThreshold() {
		ref = &ChunkRef{Kind: InlineChunk, Inline: append([]byte(nil), data...)}
	} else {
		id := randomID12()
		if _, err := w.Put(ctx, chunksPrefix+id.String(), data, nil); err != nil {
			return fmt.Errorf("icechunk: writing chunk: %w", err)
		}
		ref = &ChunkRef{Kind: NativeChunk, ID: id, Length: uint64(len(data))}
	}
	return s.setChunkRef(n, coords, ref)
}

// DeleteChunk removes a chunk; readers then see the fill value.
func (s *Session) DeleteChunk(ctx context.Context, path string, coords []uint32) error {
	if _, err := s.writable(); err != nil {
		return err
	}
	n, err := s.arrayNode(path)
	if err != nil {
		return err
	}
	if err := s.checkCoords(n, coords); err != nil {
		return err
	}
	return s.setChunkRef(n, coords, nil)
}

// VirtualRef points a chunk at a byte range of an external object.
type VirtualRef struct {
	// Location is an absolute URL such as "s3://bucket/file.nc". Readers
	// need a virtual chunk container for its prefix in the repository
	// config (set with icechunk-python), or their own mapping.
	Location       string
	Offset, Length uint64
	// Optional checksum: the object's ETag, or a time it was last known
	// unmodified at. Readers refuse the chunk if the object changed.
	ETag         string
	LastModified time.Time
}

// SetVirtualRef stores a virtual chunk reference.
func (s *Session) SetVirtualRef(ctx context.Context, path string, coords []uint32, v VirtualRef) error {
	if _, err := s.writable(); err != nil {
		return err
	}
	if !strings.Contains(v.Location, "://") {
		return fmt.Errorf("icechunk: virtual chunk location %q is not an absolute URL", v.Location)
	}
	n, err := s.arrayNode(path)
	if err != nil {
		return err
	}
	if err := s.checkCoords(n, coords); err != nil {
		return err
	}
	ref := &ChunkRef{Kind: VirtualChunk, Location: v.Location, Offset: v.Offset, Length: v.Length, ETag: v.ETag}
	if v.ETag == "" && !v.LastModified.IsZero() {
		ref.LastModified = v.LastModified.UTC().Truncate(time.Second)
	}
	return s.setChunkRef(n, coords, ref)
}

// HasUncommittedChanges reports whether the session has edits to commit.
func (s *Session) HasUncommittedChanges() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.cs == nil {
		return false
	}
	return !s.cs.isEmpty()
}

// CreateArray creates an array from a spec and opens it for writing.
func (s *Session) CreateArray(ctx context.Context, path string, spec zarr.ArraySpec) (*zarr.Array, error) {
	doc, err := spec.Metadata()
	if err != nil {
		return nil, err
	}
	if _, err := s.node(path); err == nil {
		return nil, fmt.Errorf("%w: %s", ErrAlreadyExists, path)
	}
	if err := s.ensureParents(ctx, path); err != nil {
		return nil, err
	}
	if err := s.SetMetadata(ctx, path, doc); err != nil {
		return nil, err
	}
	return s.OpenArray(ctx, path)
}
