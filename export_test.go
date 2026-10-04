package icechunk

import (
	"context"

	"github.com/solomonsealed/icechunk-go/internal/fbs"
)

// Test-only exports for the external test package (consistency_test.go).

// ManifestArraySummary counts one array's chunk refs in a manifest file.
type ManifestArraySummary struct {
	NodeID       string `json:"node_id"`
	NumChunkRefs int    `json:"num_chunk_refs"`
	NumInline    int    `json:"num_inline"`
	NumNative    int    `json:"num_native"`
	NumVirtual   int    `json:"num_virtual"`
}

// ManifestSummary describes a manifest file the way icechunk-python's
// Repository.inspect_manifest does.
type ManifestSummary struct {
	Arrays                  []ManifestArraySummary `json:"arrays"`
	TotalChunkRefs          int                    `json:"total_chunk_refs"`
	UsesLocationCompression bool                   `json:"uses_location_compression"`
	NumCompressedRefs       int                    `json:"num_compressed_refs"`
}

// SummarizeManifest decodes every chunk ref of manifest id, in file order.
func SummarizeManifest(ctx context.Context, r *Repository, id ManifestID) (sum ManifestSummary, err error) {
	m, err := r.manifest(ctx, id)
	if err != nil {
		return sum, err
	}
	defer recoverFormat(&err)
	sum.Arrays = []ManifestArraySummary{}
	sum.UsesLocationCompression = m.root.CompressionAlgorithm() == compressionZstdDict && m.root.LocationDictionaryLength() > 0
	var am fbs.ArrayManifest
	var cr fbs.ChunkRef
	for i := 0; i < m.root.ArraysLength(); i++ {
		m.root.Arrays(&am, i)
		var node NodeID
		copy(node[:], am.NodeId(nil).Bytes())
		a := ManifestArraySummary{NodeID: node.String()}
		err := m.forEach(node, func(_ []uint32, ref *ChunkRef) error {
			a.NumChunkRefs++
			switch ref.Kind {
			case InlineChunk:
				a.NumInline++
			case NativeChunk:
				a.NumNative++
			case VirtualChunk:
				a.NumVirtual++
			}
			return nil
		})
		if err != nil {
			return sum, err
		}
		for j := 0; j < am.RefsLength(); j++ {
			am.Refs(&cr, j)
			if cr.CompressedLocationBytes() != nil {
				sum.NumCompressedRefs++
			}
		}
		sum.TotalChunkRefs += a.NumChunkRefs
		sum.Arrays = append(sum.Arrays, a)
	}
	return sum, nil
}
