package icechunk

import (
	"bytes"
	"sort"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/solomonsealed/icechunk-go/internal/fbs"
	"github.com/solomonsealed/icechunk-go/internal/format"
)

// Encoders for the immutable metadata files, mirroring upstream's writers
// (icechunk-format: Snapshot::from_iter, Manifest::from_sorted_vec,
// TransactionLog::new). All files are spec version 2.

const writeSpecVersion = 2

var fileIdentifier = []byte("Ichk")

func idVector8(b *flatbuffers.Builder, ids []NodeID) flatbuffers.UOffsetT {
	b.StartVector(8, len(ids), 1)
	for i := len(ids) - 1; i >= 0; i-- {
		createID8(b, ids[i])
	}
	return b.EndVector(len(ids))
}

func u32Vector(b *flatbuffers.Builder, vals []uint32) flatbuffers.UOffsetT {
	b.StartVector(4, len(vals), 4)
	for i := len(vals) - 1; i >= 0; i-- {
		b.PrependUint32(vals[i])
	}
	return b.EndVector(len(vals))
}

// snapshotSpec is everything a snapshot file contains.
type snapshotSpec struct {
	id            SnapshotID
	flushedAt     uint64 // microseconds
	message       string
	metadata      []rawMetadataItem
	manifestFiles []ManifestFileInfo
	nodes         []*Node // sorted by path bytes
}

func encodeNode(b *flatbuffers.Builder, n *Node) flatbuffers.UOffsetT {
	path := b.CreateString(n.Path)
	var dataType fbs.NodeData
	var data flatbuffers.UOffsetT
	if n.Type == ArrayNode {
		a := n.Array
		refs := make([]flatbuffers.UOffsetT, len(a.Manifests))
		for i, mr := range a.Manifests {
			fbs.ManifestRefStartExtentsVector(b, len(mr.Extents))
			for j := len(mr.Extents) - 1; j >= 0; j-- {
				fbs.CreateChunkIndexRange(b, mr.Extents[j].From, mr.Extents[j].To)
			}
			ext := b.EndVector(len(mr.Extents))
			fbs.ManifestRefStart(b)
			fbs.ManifestRefAddObjectId(b, createID12(b, mr.ID))
			fbs.ManifestRefAddExtents(b, ext)
			refs[i] = fbs.ManifestRefEnd(b)
		}
		manifests := offsetVector(b, refs)
		var dims flatbuffers.UOffsetT
		if a.DimensionNames != nil {
			offs := make([]flatbuffers.UOffsetT, len(a.DimensionNames))
			for i, name := range a.DimensionNames {
				var s flatbuffers.UOffsetT
				if name != "" {
					s = b.CreateSharedString(name)
				}
				fbs.DimensionNameStart(b)
				if name != "" {
					fbs.DimensionNameAddName(b, s)
				}
				offs[i] = fbs.DimensionNameEnd(b)
			}
			dims = offsetVector(b, offs)
		}
		shapes := make([]flatbuffers.UOffsetT, len(a.Shape))
		for i, d := range a.Shape {
			fbs.DimensionShapeV2Start(b)
			fbs.DimensionShapeV2AddArrayLength(b, d.ArrayLength)
			fbs.DimensionShapeV2AddNumChunks(b, d.NumChunks)
			shapes[i] = fbs.DimensionShapeV2End(b)
		}
		shapeV2 := offsetVector(b, shapes)
		fbs.ArrayNodeDataStartShapeVector(b, 0)
		emptyV1Shape := b.EndVector(0) // required field, empty in v2
		fbs.ArrayNodeDataStart(b)
		fbs.ArrayNodeDataAddShape(b, emptyV1Shape)
		if a.DimensionNames != nil {
			fbs.ArrayNodeDataAddDimensionNames(b, dims)
		}
		fbs.ArrayNodeDataAddManifests(b, manifests)
		fbs.ArrayNodeDataAddShapeV2(b, shapeV2)
		dataType, data = fbs.NodeDataArray, fbs.ArrayNodeDataEnd(b)
	} else {
		fbs.GroupNodeDataStart(b)
		dataType, data = fbs.NodeDataGroup, fbs.GroupNodeDataEnd(b)
	}
	userData := b.CreateByteVector(n.ZarrMetadata)
	fbs.NodeSnapshotStart(b)
	fbs.NodeSnapshotAddId(b, createID8(b, n.ID))
	fbs.NodeSnapshotAddPath(b, path)
	fbs.NodeSnapshotAddUserData(b, userData)
	fbs.NodeSnapshotAddNodeDataType(b, dataType)
	fbs.NodeSnapshotAddNodeData(b, data)
	return fbs.NodeSnapshotEnd(b)
}

func encodeSnapshot(s *snapshotSpec) ([]byte, error) {
	b := flatbuffers.NewBuilder(4096)
	files := append([]ManifestFileInfo(nil), s.manifestFiles...)
	sort.Slice(files, func(i, j int) bool { return bytes.Compare(files[i].ID[:], files[j].ID[:]) < 0 })
	fileOffs := make([]flatbuffers.UOffsetT, len(files))
	for i, f := range files {
		fbs.ManifestFileInfoV2Start(b)
		fbs.ManifestFileInfoV2AddId(b, createID12(b, f.ID))
		fbs.ManifestFileInfoV2AddSizeBytes(b, f.SizeBytes)
		fbs.ManifestFileInfoV2AddNumChunkRefs(b, f.NumChunkRefs)
		fileOffs[i] = fbs.ManifestFileInfoV2End(b)
	}
	filesV2 := offsetVector(b, fileOffs)
	fbs.SnapshotStartManifestFilesVector(b, 0)
	filesV1 := b.EndVector(0) // required field, empty in v2
	metadata := metadataVector(b, s.metadata)
	message := b.CreateString(s.message)
	nodeOffs := make([]flatbuffers.UOffsetT, len(s.nodes))
	for i, n := range s.nodes {
		nodeOffs[i] = encodeNode(b, n)
	}
	nodes := offsetVector(b, nodeOffs)
	fbs.SnapshotStart(b)
	fbs.SnapshotAddId(b, createID12(b, s.id))
	fbs.SnapshotAddNodes(b, nodes)
	fbs.SnapshotAddFlushedAt(b, s.flushedAt)
	fbs.SnapshotAddMessage(b, message)
	fbs.SnapshotAddMetadata(b, metadata)
	fbs.SnapshotAddManifestFiles(b, filesV1)
	fbs.SnapshotAddManifestFilesV2(b, filesV2)
	b.FinishWithFileIdentifier(fbs.SnapshotEnd(b), fileIdentifier)
	return format.Encode(format.FileTypeSnapshot, writeSpecVersion, b.FinishedBytes())
}

// chunkEntry is a chunk reference with its coordinates.
type chunkEntry struct {
	coords []uint32
	ref    *ChunkRef
}

func compareCoords(a, b []uint32) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return len(a) - len(b)
}

// encodeManifest writes a manifest holding the refs of one array, which
// must be sorted by coordinates. Locations are stored uncompressed.
func encodeManifest(id ManifestID, node NodeID, entries []chunkEntry) ([]byte, error) {
	b := flatbuffers.NewBuilder(1024 + 64*len(entries))
	refs := make([]flatbuffers.UOffsetT, len(entries))
	for i, e := range entries {
		index := u32Vector(b, e.coords)
		r := e.ref
		var inline, location, etag flatbuffers.UOffsetT
		switch r.Kind {
		case InlineChunk:
			inline = b.CreateByteVector(r.Inline)
		case VirtualChunk:
			location = b.CreateString(r.Location)
			if r.ETag != "" {
				etag = b.CreateString(r.ETag)
			}
		}
		fbs.ChunkRefStart(b)
		fbs.ChunkRefAddIndex(b, index)
		switch r.Kind {
		case InlineChunk:
			fbs.ChunkRefAddInline(b, inline)
		case NativeChunk:
			fbs.ChunkRefAddOffset(b, r.Offset)
			fbs.ChunkRefAddLength(b, r.Length)
			fbs.ChunkRefAddChunkId(b, createID12(b, r.ID))
		case VirtualChunk:
			fbs.ChunkRefAddOffset(b, r.Offset)
			fbs.ChunkRefAddLength(b, r.Length)
			fbs.ChunkRefAddLocation(b, location)
			if r.ETag != "" {
				fbs.ChunkRefAddChecksumEtag(b, etag)
			} else if !r.LastModified.IsZero() {
				fbs.ChunkRefAddChecksumLastModified(b, uint32(r.LastModified.Unix()))
			}
		}
		refs[i] = fbs.ChunkRefEnd(b)
	}
	refsVec := offsetVector(b, refs)
	fbs.ArrayManifestStart(b)
	fbs.ArrayManifestAddNodeId(b, createID8(b, node))
	fbs.ArrayManifestAddRefs(b, refsVec)
	arrays := offsetVector(b, []flatbuffers.UOffsetT{fbs.ArrayManifestEnd(b)})
	fbs.ManifestStart(b)
	fbs.ManifestAddId(b, createID12(b, id))
	fbs.ManifestAddArrays(b, arrays)
	fbs.ManifestAddCompressionAlgorithm(b, 0) // locations stored as plain strings
	b.FinishWithFileIdentifier(fbs.ManifestEnd(b), fileIdentifier)
	return format.Encode(format.FileTypeManifest, writeSpecVersion, b.FinishedBytes())
}

// txLog lists what a commit changed, for conflict detection and diffs.
type txLog struct {
	id                           SnapshotID
	newGroups, newArrays         []NodeID
	deletedGroups, deletedArrays []NodeID
	updatedGroups, updatedArrays []NodeID
	updatedChunks                map[NodeID][][]uint32
}

func sortNodeIDs(ids []NodeID) []NodeID {
	out := append([]NodeID(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i][:], out[j][:]) < 0 })
	return out
}

func encodeTxLog(t *txLog) ([]byte, error) {
	b := flatbuffers.NewBuilder(1024)
	nodes := make([]NodeID, 0, len(t.updatedChunks))
	for id := range t.updatedChunks {
		nodes = append(nodes, id)
	}
	nodes = sortNodeIDs(nodes)
	chunkOffs := make([]flatbuffers.UOffsetT, len(nodes))
	for i, id := range nodes {
		coords := append([][]uint32(nil), t.updatedChunks[id]...)
		sort.Slice(coords, func(a, c int) bool { return compareCoords(coords[a], coords[c]) < 0 })
		idx := make([]flatbuffers.UOffsetT, len(coords))
		for j, c := range coords {
			v := u32Vector(b, c)
			fbs.ChunkIndicesStart(b)
			fbs.ChunkIndicesAddCoords(b, v)
			idx[j] = fbs.ChunkIndicesEnd(b)
		}
		chunks := offsetVector(b, idx)
		fbs.ArrayUpdatedChunksStart(b)
		fbs.ArrayUpdatedChunksAddNodeId(b, createID8(b, id))
		fbs.ArrayUpdatedChunksAddChunks(b, chunks)
		chunkOffs[i] = fbs.ArrayUpdatedChunksEnd(b)
	}
	updatedChunks := offsetVector(b, chunkOffs)
	newGroups := idVector8(b, sortNodeIDs(t.newGroups))
	newArrays := idVector8(b, sortNodeIDs(t.newArrays))
	deletedGroups := idVector8(b, sortNodeIDs(t.deletedGroups))
	deletedArrays := idVector8(b, sortNodeIDs(t.deletedArrays))
	updatedGroups := idVector8(b, sortNodeIDs(t.updatedGroups))
	updatedArrays := idVector8(b, sortNodeIDs(t.updatedArrays))
	moved := offsetVector(b, nil) // present and empty, as upstream writes it
	fbs.TransactionLogStart(b)
	fbs.TransactionLogAddId(b, createID12(b, t.id))
	fbs.TransactionLogAddNewGroups(b, newGroups)
	fbs.TransactionLogAddNewArrays(b, newArrays)
	fbs.TransactionLogAddDeletedGroups(b, deletedGroups)
	fbs.TransactionLogAddDeletedArrays(b, deletedArrays)
	fbs.TransactionLogAddUpdatedArrays(b, updatedArrays)
	fbs.TransactionLogAddUpdatedGroups(b, updatedGroups)
	fbs.TransactionLogAddUpdatedChunks(b, updatedChunks)
	fbs.TransactionLogAddMovedNodes(b, moved)
	b.FinishWithFileIdentifier(fbs.TransactionLogEnd(b), fileIdentifier)
	return format.Encode(format.FileTypeTransactionLog, writeSpecVersion, b.FinishedBytes())
}

// parseTxLog decodes the parts of a transaction log used for conflict
// detection.
func parseTxLog(raw []byte) (t *txLog, moves int, err error) {
	defer recoverFormat(&err)
	_, buf, err := format.Decode(raw, format.FileTypeTransactionLog)
	if err != nil {
		return nil, 0, err
	}
	root := fbs.GetRootAsTransactionLog(buf, 0)
	t = &txLog{id: idFrom12(root.Id(nil)), updatedChunks: map[NodeID][][]uint32{}}
	ids := func(n int, get func(*fbs.ObjectId8, int) bool) []NodeID {
		out := make([]NodeID, vecLen(n, buf))
		var o fbs.ObjectId8
		for i := range out {
			get(&o, i)
			copy(out[i][:], o.Bytes())
		}
		return out
	}
	t.newGroups = ids(root.NewGroupsLength(), root.NewGroups)
	t.newArrays = ids(root.NewArraysLength(), root.NewArrays)
	t.deletedGroups = ids(root.DeletedGroupsLength(), root.DeletedGroups)
	t.deletedArrays = ids(root.DeletedArraysLength(), root.DeletedArrays)
	t.updatedGroups = ids(root.UpdatedGroupsLength(), root.UpdatedGroups)
	t.updatedArrays = ids(root.UpdatedArraysLength(), root.UpdatedArrays)
	var uc fbs.ArrayUpdatedChunks
	var ci fbs.ChunkIndices
	for i := 0; i < vecLen(root.UpdatedChunksLength(), buf); i++ {
		root.UpdatedChunks(&uc, i)
		var id NodeID
		copy(id[:], uc.NodeId(nil).Bytes())
		for j := 0; j < vecLen(uc.ChunksLength(), buf); j++ {
			uc.Chunks(&ci, j)
			c := make([]uint32, vecLen(ci.CoordsLength(), buf))
			for k := range c {
				c[k] = ci.Coords(k)
			}
			t.updatedChunks[id] = append(t.updatedChunks[id], c)
		}
	}
	return t, root.MovedNodesLength(), nil
}
