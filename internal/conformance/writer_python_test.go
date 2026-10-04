package conformance

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	icechunk "github.com/solomonsealed/icechunk-go"
	"github.com/solomonsealed/icechunk-go/storage"
	"github.com/solomonsealed/icechunk-go/zarr"
)

// int32Array is a zarr.json for an uncompressed little-endian int32 array,
// so chunk bytes can be written without the zarr package.
func int32Array(shape, chunks []uint64) []byte {
	return []byte(fmt.Sprintf(`{"zarr_format":3,"node_type":"array","shape":%s,"data_type":"int32",
		"chunk_grid":{"name":"regular","configuration":{"chunk_shape":%s}},
		"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
		"fill_value":-1,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}],
		"attributes":{},"dimension_names":["x"]}`, jsonInts(shape), jsonInts(chunks)))
}

func jsonInts(v []uint64) string {
	s := "["
	for i, x := range v {
		if i > 0 {
			s += ","
		}
		s += fmt.Sprint(x)
	}
	return s + "]"
}

func int32Bytes(vals ...int32) []byte {
	b := make([]byte, 4*len(vals))
	for i, v := range vals {
		binary.LittleEndian.PutUint32(b[4*i:], uint32(v))
	}
	return b
}

// testData builds deterministic data of a Zarr data type.
func testData(t *testing.T, dt string, shape []uint64, seed int64) *zarr.NDArray {
	t.Helper()
	r := rand.New(rand.NewSource(seed))
	n := 1
	for _, s := range shape {
		n *= int(s)
	}
	var nd *zarr.NDArray
	var err error
	switch dt {
	case "string":
		vals := make([]string, n)
		for i := range vals {
			vals[i] = fmt.Sprintf("v%d-%c", r.Intn(1000), 'α'+rune(i%20))
		}
		nd, err = zarr.FromStrings(shape, vals)
	case "float32":
		vals := make([]float32, n)
		for i := range vals {
			vals[i] = float32(r.NormFloat64() * 10)
		}
		nd, err = zarr.FromSlice(shape, vals)
	case "float64":
		vals := make([]float64, n)
		for i := range vals {
			vals[i] = r.NormFloat64() * 1e6
		}
		nd, err = zarr.FromSlice(shape, vals)
	case "int16":
		vals := make([]int16, n)
		for i := range vals {
			vals[i] = int16(r.Intn(60000) - 30000)
		}
		nd, err = zarr.FromSlice(shape, vals)
	case "uint64":
		vals := make([]uint64, n)
		for i := range vals {
			vals[i] = r.Uint64()
		}
		nd, err = zarr.FromSlice(shape, vals)
	case "complex64":
		vals := make([]complex64, n)
		for i := range vals {
			vals[i] = complex(float32(r.NormFloat64()), float32(i))
		}
		nd, err = zarr.FromSlice(shape, vals)
	case "bool":
		vals := make([]bool, n)
		for i := range vals {
			vals[i] = r.Intn(2) == 0
		}
		nd, err = zarr.FromSlice(shape, vals)
	default:
		t.Fatalf("no test data for %s", dt)
	}
	if err != nil {
		t.Fatal(err)
	}
	return nd
}

// TestWriteForPython writes a repository for testdata/check_go_writer.py to
// verify with icechunk-python. It runs only when ICECHUNK_GO_WRITE_DIR is
// set, and writes <dir>/repo plus <dir>/expected.json.
func TestWriteForPython(t *testing.T) {
	dir := os.Getenv("ICECHUNK_GO_WRITE_DIR")
	if dir == "" {
		t.Skip("set ICECHUNK_GO_WRITE_DIR to run")
	}
	ctx := context.Background()
	os.RemoveAll(filepath.Join(dir, "repo"))
	repo, err := icechunk.Create(ctx, storage.NewLocal(filepath.Join(dir, "repo")), nil)
	if err != nil {
		t.Fatal(err)
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	commitMeta := map[string]any{"author": "icechunk-go", "count": 42, "ratio": 0.5, "flag": true, "nothing": nil,
		"nested": map[string]any{"list": []any{1, "two", 3.5, false}, "empty": map[string]any{}},
		// Not JSON-native: stored as their JSON form (base64, exact integer).
		"digest": []byte{1, 2, 3}, "big": uint64(1)<<63 + 5}
	metaJSON, _ := json.Marshal(commitMeta)

	s, _ := repo.WritableSession(ctx, "main")
	must(s.CreateGroup(ctx, "/", map[string]any{"title": "written by go", "n": 7}))
	must(s.CreateGroup(ctx, "/group", map[string]any{"kind": "group"}))
	must(s.SetMetadata(ctx, "/group/small", int32Array([]uint64{6}, []uint64{2})))
	must(s.SetChunk(ctx, "/group/small", []uint32{0}, int32Bytes(1, 2)))
	must(s.SetChunk(ctx, "/group/small", []uint32{1}, int32Bytes(3, 4)))
	big := make([]int32, 300)
	for i := range big {
		big[i] = int32(i * 3)
	}
	must(s.SetMetadata(ctx, "/group/big", int32Array([]uint64{600}, []uint64{300})))
	must(s.SetChunk(ctx, "/group/big", []uint32{1}, int32Bytes(big...)))
	c1, err := s.Commit(ctx, "go commit 1", &icechunk.CommitOptions{Metadata: commitMeta})
	must(err)

	s, _ = repo.WritableSession(ctx, "main")
	must(s.SetChunk(ctx, "/group/small", []uint32{2}, int32Bytes(5, 6)))
	must(s.DeleteChunk(ctx, "/group/small", []uint32{0}))
	must(s.SetMetadata(ctx, "/group/big", int32Array([]uint64{450}, []uint64{300})))
	must(s.CreateGroup(ctx, "/doomed", nil))
	c2, err := s.Commit(ctx, "go commit 2", nil)
	must(err)

	s, _ = repo.WritableSession(ctx, "main")
	must(s.DeleteNode(ctx, "/doomed"))
	c3, err := s.Commit(ctx, "go commit 3", nil)
	must(err)

	// Arrays written through the zarr package with many codecs, for
	// zarr-python to decode.
	s, _ = repo.WritableSession(ctx, "main")
	arrays := map[string]any{}
	specs := map[string]zarr.ArraySpec{
		"default_f32": {DataType: "float32", FillValue: math.NaN()},
		"gzip_f64":    {DataType: "float64", Codecs: []zarr.CodecSpec{zarr.BytesLE(), zarr.Gzip(6)}, FillValue: math.Inf(-1)},
		"bigendian":   {DataType: "int16", Codecs: []zarr.CodecSpec{zarr.Codec("bytes", map[string]any{"endian": "big"})}, FillValue: 7},
		"crc32c":      {DataType: "float64", Codecs: []zarr.CodecSpec{zarr.BytesLE(), zarr.Zstd(5), zarr.Crc32c()}},
		"transpose":   {DataType: "int16", Codecs: []zarr.CodecSpec{zarr.Transpose([]int{1, 0}), zarr.BytesLE(), zarr.Zstd(1)}},
		"blosc_lz4":   {DataType: "float32", Codecs: []zarr.CodecSpec{zarr.BytesLE(), zarr.Blosc("lz4", 5, "shuffle")}},
		"blosc_zstd":  {DataType: "float64", Codecs: []zarr.CodecSpec{zarr.BytesLE(), zarr.Blosc("zstd", 3, "bitshuffle")}},
		"blosc_zlib":  {DataType: "int16", Codecs: []zarr.CodecSpec{zarr.BytesLE(), zarr.Blosc("zlib", 3, "noshuffle")}},
		"numcodecs":   {DataType: "float64", Codecs: []zarr.CodecSpec{zarr.BytesLE(), zarr.Codec("numcodecs.shuffle", map[string]any{"elementsize": 8}), zarr.Codec("numcodecs.zlib", map[string]any{"level": 3}), zarr.Codec("numcodecs.crc32", nil), zarr.Codec("numcodecs.adler32", map[string]any{"location": "end"}), zarr.Codec("numcodecs.fletcher32", nil), zarr.Codec("numcodecs.crc32c", nil), zarr.Codec("numcodecs.lz4", nil), zarr.Codec("numcodecs.zstd", map[string]any{"level": 1})}},
		"bitround":    {DataType: "float32", Codecs: []zarr.CodecSpec{zarr.Codec("numcodecs.bitround", map[string]any{"keepbits": 7}), zarr.BytesLE(), zarr.Zstd(0)}},
		"strings":     {DataType: "string", FillValue: "-"},
		"complex":     {DataType: "complex64", FillValue: complex(1, 2)},
		"bool":        {DataType: "bool", FillValue: true},
		"sharded":     {DataType: "int16", ShardShape: []uint64{8, 6}},
		"sharded_str": {DataType: "string", ShardShape: []uint64{8, 6}, Codecs: []zarr.CodecSpec{zarr.Codec("vlen-utf8", nil), zarr.Gzip(1)}},
		"uint64":      {DataType: "uint64", FillValue: uint64(1) << 63},
	}
	names := make([]string, 0, len(specs))
	for name := range specs {
		names = append(names, name)
	}
	sort.Strings(names)
	for i, name := range names {
		spec := specs[name]
		spec.Shape, spec.ChunkShape = []uint64{13, 11}, []uint64{4, 3}
		spec.DimensionNames = []string{"y", ""}
		spec.Attributes = map[string]any{"codec": name}
		arr, err := s.CreateArray(ctx, "/codecs/"+name, spec)
		must(err)
		// Leave rows 9.. unwritten so fill values show.
		data := testData(t, spec.DataType, []uint64{9, 11}, int64(i))
		must(arr.Write(ctx, []uint64{0, 0}, data))
		nd, err := arr.ReadAll(ctx)
		must(err)
		entry := map[string]any{"dtype": spec.DataType}
		if nd.Strings != nil {
			entry["strings"] = nd.Strings
		} else {
			sum := sha256.Sum256(nd.Data)
			entry["sha256"] = hex.EncodeToString(sum[:])
		}
		arrays["codecs/"+name] = entry
	}
	_, err = s.Commit(ctx, "go commit 4: codecs", nil)
	must(err)

	must(repo.CreateBranch(ctx, "feature/go", c1))
	must(repo.CreateTag(ctx, "v1", c2))
	must(repo.CreateTag(ctx, "gone", c1))
	must(repo.DeleteTag(ctx, "gone"))

	exp := map[string]any{
		"history":     []string{"go commit 4: codecs", "go commit 3", "go commit 2", "go commit 1", "Repository initialized"},
		"arrays":      arrays,
		"commit_meta": json.RawMessage(metaJSON),
		"snapshots":   map[string]string{"c1": c1.String(), "c2": c2.String(), "c3": c3.String()},
		"small":       []int32{-1, -1, 3, 4, 5, 6},
		"big_values":  append(make([]int32, 300), big[:150]...),
	}
	for i := 0; i < 300; i++ {
		exp["big_values"].([]int32)[i] = -1
	}
	b, _ := json.MarshalIndent(exp, "", " ")
	must(os.WriteFile(filepath.Join(dir, "expected.json"), b, 0o644))
}
