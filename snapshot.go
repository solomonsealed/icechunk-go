package icechunk

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/solomonsealed/icechunk-go/internal/fbs"
	"github.com/solomonsealed/icechunk-go/internal/format"
)

// NodeType distinguishes groups from arrays.
type NodeType uint8

const (
	GroupNode NodeType = iota + 1
	ArrayNode
)

func (t NodeType) String() string {
	switch t {
	case GroupNode:
		return "group"
	case ArrayNode:
		return "array"
	}
	return "unknown"
}

// Node is an array or group in a snapshot.
type Node struct {
	ID   NodeID
	Path string
	Type NodeType
	// ZarrMetadata is the node's zarr.json document, verbatim.
	ZarrMetadata []byte
	// Array is set for array nodes only.
	Array *ArrayInfo
}

// ArrayInfo is the Icechunk-level description of an array node.
type ArrayInfo struct {
	Shape []DimensionShape
	// DimensionNames has one entry per dimension ("" when unnamed), or is
	// nil when the array has no dimension names.
	DimensionNames []string
	// Manifests lists the manifest files holding this array's chunk refs and
	// the region of the chunk grid each covers.
	Manifests []ManifestRef
}

// DimensionShape describes one dimension of an array.
type DimensionShape struct {
	ArrayLength uint64
	NumChunks   uint32
}

// ChunkRange is the half-open range [From, To) of chunk indices.
type ChunkRange struct {
	From, To uint32
}

// ManifestRef points at a manifest covering a region of an array's chunk grid.
// Empty Extents cover the whole grid.
type ManifestRef struct {
	ID      ManifestID
	Extents []ChunkRange
}

func (m *ManifestRef) contains(coords []uint32) bool {
	for i, r := range m.Extents {
		if i >= len(coords) {
			break
		}
		if coords[i] < r.From || coords[i] >= r.To {
			return false
		}
	}
	return true
}

// validChunkCoord mirrors upstream's ArrayShape::valid_chunk_coord.
func (a *ArrayInfo) validChunkCoord(coords []uint32) bool {
	if len(coords) != len(a.Shape) {
		return false
	}
	for i, d := range a.Shape {
		limit := d.NumChunks
		if limit == 0 {
			limit = 1
		}
		if coords[i] > limit-1 {
			return false
		}
	}
	return true
}

// ManifestFileInfo summarizes a manifest file referenced by a snapshot.
type ManifestFileInfo struct {
	ID           ManifestID
	SizeBytes    uint64
	NumChunkRefs uint32
}

// Snapshot is a decoded snapshot file. Nodes are decoded on demand straight
// from the flatbuffer, so large hierarchies stay cheap.
type Snapshot struct {
	header format.Header
	buf    []byte
	root   *fbs.Snapshot

	// Scalar fields decoded up front, so that their accessors cannot hit a
	// corrupt buffer after a successful parse.
	id        SnapshotID
	parent    *SnapshotID
	flushedAt time.Time
	message   string
	numNodes  int
}

func parseSnapshot(raw []byte) (s *Snapshot, err error) {
	defer recoverFormat(&err)
	h, buf, err := format.Decode(raw, format.FileTypeSnapshot)
	if err != nil {
		return nil, err
	}
	root := fbs.GetRootAsSnapshot(buf, 0)
	s = &Snapshot{header: h, buf: buf, root: root}
	s.id = idFrom12(root.Id(nil))
	if p := root.ParentId(nil); p != nil {
		pid := idFrom12(p)
		s.parent = &pid
	}
	s.flushedAt = microsToTime(root.FlushedAt())
	s.message = string(root.Message())
	s.numNodes = root.NodesLength()
	return s, nil
}

// ID returns the snapshot id.
func (s *Snapshot) ID() SnapshotID { return s.id }

// SpecVersion returns the on-disk spec version this file was written with.
func (s *Snapshot) SpecVersion() int { return int(s.header.SpecVersion) }

// Implementation returns the name of the library that wrote the file.
func (s *Snapshot) Implementation() string { return s.header.Implementation }

// ParentID returns the parent recorded in the file. Only spec v1 snapshots
// record parents; in v2 the snapshot graph lives in the repo info file.
func (s *Snapshot) ParentID() (SnapshotID, bool) {
	if s.parent == nil {
		return SnapshotID{}, false
	}
	return *s.parent, true
}

// FlushedAt returns the commit time.
func (s *Snapshot) FlushedAt() time.Time { return s.flushedAt }

// Message returns the commit message.
func (s *Snapshot) Message() string { return s.message }

// Metadata returns the commit properties.
func (s *Snapshot) Metadata() (md map[string]any, err error) {
	defer recoverFormat(&err)
	return decodeMetadataItems(s.buf, s.root.MetadataLength(), s.root.Metadata, s.header.SpecVersion)
}

// ManifestFiles lists every manifest file referenced by the snapshot.
func (s *Snapshot) ManifestFiles() (out []ManifestFileInfo, err error) {
	defer recoverFormat(&err)
	if n := s.root.ManifestFilesV2Length(); n > 0 {
		var mf fbs.ManifestFileInfoV2
		for i := 0; i < n; i++ {
			s.root.ManifestFilesV2(&mf, i)
			out = append(out, ManifestFileInfo{ID: idFrom12(mf.Id(nil)), SizeBytes: mf.SizeBytes(), NumChunkRefs: mf.NumChunkRefs()})
		}
		return out, nil
	}
	var mf fbs.ManifestFileInfo
	for i := 0; i < s.root.ManifestFilesLength(); i++ {
		s.root.ManifestFiles(&mf, i)
		out = append(out, ManifestFileInfo{ID: idFrom12(mf.Id(nil)), SizeBytes: mf.SizeBytes(), NumChunkRefs: mf.NumChunkRefs()})
	}
	return out, nil
}

// info returns the commit summary; parent comes from the file (v1 only).
func (s *Snapshot) info() (SnapshotInfo, error) {
	md, err := s.Metadata()
	if err != nil {
		return SnapshotInfo{}, err
	}
	si := SnapshotInfo{ID: s.ID(), FlushedAt: s.FlushedAt(), Message: s.Message(), Metadata: md}
	if p, ok := s.ParentID(); ok {
		si.ParentID = &p
	}
	return si, nil
}

// NumNodes returns the number of arrays and groups.
func (s *Snapshot) NumNodes() int { return s.numNodes }

// Node returns the node at an absolute path (e.g. "/", "/group/array").
func (s *Snapshot) Node(path string) (n *Node, err error) {
	defer recoverFormat(&err)
	path, err = normalizePath(path)
	if err != nil {
		return nil, err
	}
	key := []byte(path)
	count := s.root.NodesLength()
	var ns fbs.NodeSnapshot
	i := sort.Search(count, func(i int) bool {
		s.root.Nodes(&ns, i)
		return bytes.Compare(ns.Path(), key) >= 0
	})
	if i < count {
		s.root.Nodes(&ns, i)
		if bytes.Equal(ns.Path(), key) {
			return decodeNode(&ns)
		}
	}
	return nil, fmt.Errorf("%w: %s", ErrNodeNotFound, path)
}

// Nodes returns every node, sorted by path.
func (s *Snapshot) Nodes() (out []*Node, err error) {
	defer recoverFormat(&err)
	count := vecLen(s.root.NodesLength(), s.buf)
	out = make([]*Node, 0, count)
	var ns fbs.NodeSnapshot
	for i := 0; i < count; i++ {
		s.root.Nodes(&ns, i)
		n, err := decodeNode(&ns)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}

func decodeNode(ns *fbs.NodeSnapshot) (*Node, error) {
	n := &Node{
		Path:         string(ns.Path()),
		ZarrMetadata: append([]byte(nil), ns.UserDataBytes()...),
	}
	copy(n.ID[:], ns.Id(nil).Bytes())
	var t flatbuffers.Table
	if !ns.NodeData(&t) {
		return nil, fmt.Errorf("%w: node %s has no node data", ErrFormat, n.Path)
	}
	switch ns.NodeDataType() {
	case fbs.NodeDataGroup:
		n.Type = GroupNode
	case fbs.NodeDataArray:
		n.Type = ArrayNode
		var a fbs.ArrayNodeData
		a.Init(t.Bytes, t.Pos)
		info, err := decodeArrayData(&a, n.Path)
		if err != nil {
			return nil, err
		}
		n.Array = info
	default:
		return nil, fmt.Errorf("%w: node %s has unknown type %d", ErrFormat, n.Path, ns.NodeDataType())
	}
	return n, nil
}

func decodeArrayData(a *fbs.ArrayNodeData, path string) (*ArrayInfo, error) {
	info := &ArrayInfo{}
	// shape_v2 is absent in v1 snapshots; the v1 shape is [] for v2 snapshots
	// (and for scalars in v1), so branch on shape_v2's presence.
	if a.HasShapeV2() {
		var d fbs.DimensionShapeV2
		for i := 0; i < a.ShapeV2Length(); i++ {
			a.ShapeV2(&d, i)
			info.Shape = append(info.Shape, DimensionShape{ArrayLength: d.ArrayLength(), NumChunks: d.NumChunks()})
		}
	} else {
		var d fbs.DimensionShape
		for i := 0; i < a.ShapeLength(); i++ {
			a.Shape(&d, i)
			var nc uint64
			switch {
			case d.ChunkLength() == 0 && d.ArrayLength() != 0:
				return nil, fmt.Errorf("%w: array %s has chunk_length 0", ErrFormat, path)
			case d.ChunkLength() != 0:
				nc = (d.ArrayLength() + d.ChunkLength() - 1) / d.ChunkLength()
			}
			info.Shape = append(info.Shape, DimensionShape{ArrayLength: d.ArrayLength(), NumChunks: uint32(nc)})
		}
	}
	if n := vecLen(a.DimensionNamesLength(), a.Table().Bytes); n > 0 {
		var dn fbs.DimensionName
		info.DimensionNames = make([]string, n)
		for i := 0; i < n; i++ {
			a.DimensionNames(&dn, i)
			info.DimensionNames[i] = string(dn.Name())
		}
	}
	var mr fbs.ManifestRef
	var r fbs.ChunkIndexRange
	for i := 0; i < a.ManifestsLength(); i++ {
		a.Manifests(&mr, i)
		ref := ManifestRef{ID: idFrom12(mr.ObjectId(nil))}
		for j := 0; j < mr.ExtentsLength(); j++ {
			mr.Extents(&r, j)
			ref.Extents = append(ref.Extents, ChunkRange{From: r.From(), To: r.To()})
		}
		info.Manifests = append(info.Manifests, ref)
	}
	return info, nil
}

// normalizePath turns "a/b", "/a/b/" or "" into the canonical "/a/b" or "/".
func normalizePath(p string) (string, error) {
	p = strings.Trim(p, "/")
	if p == "" {
		return "/", nil
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return "", fmt.Errorf("icechunk: invalid node path %q", p)
		}
	}
	return "/" + p, nil
}
