package icechunk

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/solomonsealed/icechunk-go/internal/fbs"
	"github.com/solomonsealed/icechunk-go/storage"
)

// manifestArraySummary counts one array's chunk refs in a manifest file.
type manifestArraySummary struct {
	NodeID       string `json:"node_id"`
	NumChunkRefs int    `json:"num_chunk_refs"`
	NumInline    int    `json:"num_inline"`
	NumNative    int    `json:"num_native"`
	NumVirtual   int    `json:"num_virtual"`
}

// manifestSummary describes a manifest file the way icechunk-python's
// Repository.inspect_manifest does.
type manifestSummary struct {
	Arrays                  []manifestArraySummary `json:"arrays"`
	TotalChunkRefs          int                    `json:"total_chunk_refs"`
	UsesLocationCompression bool                   `json:"uses_location_compression"`
	NumCompressedRefs       int                    `json:"num_compressed_refs"`
}

// summarizeManifest decodes every chunk ref of manifest id, in file order.
func summarizeManifest(ctx context.Context, r *Repository, id ManifestID) (sum manifestSummary, err error) {
	m, err := r.manifest(ctx, id)
	if err != nil {
		return sum, err
	}
	defer recoverFormat(&err)
	sum.Arrays = []manifestArraySummary{}
	sum.UsesLocationCompression = m.root.CompressionAlgorithm() == compressionZstdDict && m.root.LocationDictionaryLength() > 0
	var am fbs.ArrayManifest
	var cr fbs.ChunkRef
	for i := 0; i < m.root.ArraysLength(); i++ {
		m.root.Arrays(&am, i)
		var node NodeID
		copy(node[:], am.NodeId(nil).Bytes())
		a := manifestArraySummary{NodeID: node.String()}
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

// TestPythonManifests: every manifest file's chunk refs, counted per array
// and kind as Repository.inspect_manifest counts them. It needs the
// manifest decoder, so unlike the other comparisons with icechunk-python
// (internal/conformance) it lives here and reads only the recorded answers
// in testdata/oracle, never a live ICECHUNK_PYTHON.
func TestPythonManifests(t *testing.T) {
	files, _ := filepath.Glob("testdata/oracle/*.json.gz")
	if len(files) == 0 {
		t.Skip("no oracle files in testdata/oracle (run testdata/oracle/oracle.py)")
	}
	slices.Sort(files)
	for _, file := range files {
		var o struct {
			Meta struct {
				Fixture string `json:"fixture"`
				Path    string `json:"path"`
			} `json:"_meta"`
			Manifests map[string]manifestSummary `json:"manifests"`
		}
		f, err := os.Open(file)
		if err != nil {
			t.Fatal(err)
		}
		zr, err := gzip.NewReader(f)
		if err == nil {
			err = json.NewDecoder(zr).Decode(&o)
		}
		f.Close()
		if err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		t.Run(o.Meta.Fixture, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			repo, err := Open(ctx, storage.NewLocal(o.Meta.Path), nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, mid := range slices.Sorted(maps.Keys(o.Manifests)) {
				id, err := ParseObjectID12(mid)
				if err != nil {
					t.Fatal(err)
				}
				got, err := summarizeManifest(ctx, repo, id)
				if err != nil {
					t.Errorf("manifest %s: %v", mid, err)
					continue
				}
				want := o.Manifests[mid]
				// inspect_manifest reports location compression for some manifests
				// written before it existed; it only matters for compressed refs.
				if want.NumCompressedRefs == 0 && got.NumCompressedRefs == 0 {
					want.UsesLocationCompression = got.UsesLocationCompression
				}
				if !reflect.DeepEqual(want, got) {
					t.Errorf("manifest %s differs from icechunk-python:\n  got  %+v\n  want %+v", mid, got, want)
				}
			}
		})
	}
}
