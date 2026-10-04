package zarr

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"slices"
	"sync"
	"testing"
)

// memChunks is an in-memory ChunkWriter.
type memChunks struct {
	mu      sync.Mutex
	chunks  map[string][]byte
	deletes []string // DeleteChunk calls, by key
}

func newMemChunks() *memChunks { return &memChunks{chunks: map[string][]byte{}} }

func (m *memChunks) key(c []uint32) string { return fmt.Sprint(c) }

func (m *memChunks) GetChunk(_ context.Context, c []uint32, off, n int64) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.chunks[m.key(c)]
	if !ok {
		return nil, false, nil
	}
	if n < 0 {
		n = int64(len(d)) - off
	}
	return append([]byte(nil), d[off:off+n]...), true, nil
}

func (m *memChunks) ChunkSize(_ context.Context, c []uint32) (int64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.chunks[m.key(c)]
	return int64(len(d)), ok, nil
}

func (m *memChunks) SetChunk(_ context.Context, c []uint32, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.chunks[m.key(c)] = append([]byte(nil), data...)
	return nil
}

func (m *memChunks) DeleteChunk(_ context.Context, c []uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.chunks, m.key(c))
	m.deletes = append(m.deletes, m.key(c))
	return nil
}

func TestLZ4RoundTrip(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for _, n := range []int{0, 1, 11, 12, 13, 100, 1000, 70000} {
		for _, kind := range []string{"random", "repetitive", "zeros"} {
			src := make([]byte, n)
			switch kind {
			case "random":
				r.Read(src)
			case "repetitive":
				for i := range src {
					src[i] = byte(i % 7 * (i / 1000))
				}
			}
			comp := lz4Compress(src)
			out := make([]byte, n)
			if err := lz4Block(comp, out); err != nil || !bytes.Equal(out, src) {
				t.Errorf("lz4 %s n=%d: %v", kind, n, err)
			}
		}
	}
}

func TestBloscRoundTrip(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	for _, cname := range []string{"lz4", "zstd", "zlib", "blosclz"} {
		for _, shuf := range []string{"noshuffle", "shuffle", "bitshuffle"} {
			for _, n := range []int{0, 4, 1000, 300000, 600004} {
				src := make([]byte, n)
				for i := 0; i+4 <= n; i += 4 {
					src[i] = byte(r.Intn(3))
				}
				enc, _ := parseBloscConfig([]byte(fmt.Sprintf(`{"cname":%q,"clevel":5,"shuffle":%q,"typesize":4,"blocksize":0}`, cname, shuf)), 4)
				comp, err := enc.compress(src)
				if err != nil {
					t.Fatal(err)
				}
				out, err := bloscDecompress(comp)
				if err != nil || !bytes.Equal(out, src) {
					t.Errorf("blosc %s/%s n=%d: %v", cname, shuf, n, err)
				}
			}
		}
	}
}

func randomNDArray(t *testing.T, dt string, shape []uint64, seed int64) *NDArray {
	t.Helper()
	r := rand.New(rand.NewSource(seed))
	n := int(numElements(shape))
	switch dt {
	case "string":
		vals := make([]string, n)
		for i := range vals {
			vals[i] = fmt.Sprintf("s%d-%s", r.Intn(100), string(rune('α'+i%20)))
		}
		nd, _ := FromStrings(shape, vals)
		return nd
	case "float32":
		vals := make([]float32, n)
		for i := range vals {
			vals[i] = float32(r.NormFloat64())
		}
		nd, _ := FromSlice(shape, vals)
		return nd
	case "float64":
		vals := make([]float64, n)
		for i := range vals {
			vals[i] = r.NormFloat64() * 1000
		}
		nd, _ := FromSlice(shape, vals)
		return nd
	case "int16":
		vals := make([]int16, n)
		for i := range vals {
			vals[i] = int16(r.Intn(2000) - 1000)
		}
		nd, _ := FromSlice(shape, vals)
		return nd
	case "complex64":
		vals := make([]complex64, n)
		for i := range vals {
			vals[i] = complex(float32(i), -float32(i))
		}
		nd, _ := FromSlice(shape, vals)
		return nd
	case "bool":
		vals := make([]bool, n)
		for i := range vals {
			vals[i] = r.Intn(2) == 0
		}
		nd, _ := FromSlice(shape, vals)
		return nd
	}
	t.Fatalf("no generator for %s", dt)
	return nil
}

func TestWriteReadAllCodecs(t *testing.T) {
	ctx := context.Background()
	cases := map[string]ArraySpec{
		"default":    {DataType: "float32"},
		"gzip":       {DataType: "float64", Codecs: []CodecSpec{BytesLE(), Gzip(6)}},
		"bigendian":  {DataType: "int16", Codecs: []CodecSpec{Codec("bytes", map[string]any{"endian": "big"})}},
		"crc32c":     {DataType: "float64", Codecs: []CodecSpec{BytesLE(), Zstd(5), Crc32c()}},
		"transpose":  {DataType: "int16", Codecs: []CodecSpec{Transpose([]int{1, 0}), BytesLE(), Zstd(1)}},
		"blosc-lz4":  {DataType: "float32", Codecs: []CodecSpec{BytesLE(), Blosc("lz4", 5, "shuffle")}},
		"blosc-zstd": {DataType: "float64", Codecs: []CodecSpec{BytesLE(), Blosc("zstd", 3, "bitshuffle")}},
		"blosc-zlib": {DataType: "int16", Codecs: []CodecSpec{BytesLE(), Blosc("zlib", 3, "noshuffle")}},
		"numcodecs": {DataType: "float64", Codecs: []CodecSpec{BytesLE(),
			Codec("numcodecs.shuffle", map[string]any{"elementsize": 8}), Codec("numcodecs.zlib", map[string]any{"level": 3}),
			Codec("numcodecs.crc32", nil), Codec("numcodecs.adler32", map[string]any{"location": "end"}),
			Codec("numcodecs.fletcher32", nil), Codec("numcodecs.crc32c", nil), Codec("numcodecs.lz4", nil), Codec("numcodecs.zstd", map[string]any{"level": 1})}},
		"strings":   {DataType: "string", FillValue: "-"},
		"complex":   {DataType: "complex64", FillValue: complex(1, 2)},
		"bool":      {DataType: "bool", FillValue: true},
		"sharded":   {DataType: "int16", ShardShape: []uint64{8, 6}},
		"sharded-s": {DataType: "string", ShardShape: []uint64{8, 6}, Codecs: []CodecSpec{Codec("vlen-utf8", nil), Gzip(1)}},
	}
	for name, spec := range cases {
		t.Run(name, func(t *testing.T) {
			spec.Shape = []uint64{13, 11}
			spec.ChunkShape = []uint64{4, 3}
			doc, err := spec.Metadata()
			if err != nil {
				t.Fatal(err)
			}
			store := newMemChunks()
			arr, err := OpenArray(doc, store, nil)
			if err != nil {
				t.Fatal(err)
			}
			full := randomNDArray(t, spec.DataType, spec.Shape, 7)
			if err := arr.Write(ctx, []uint64{0, 0}, full); err != nil {
				t.Fatalf("write: %v", err)
			}
			got, err := arr.ReadAll(ctx)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if !reflect.DeepEqual(got.Data, full.Data) || !reflect.DeepEqual(got.Strings, full.Strings) {
				t.Fatalf("round trip mismatch")
			}
			// Partial overwrite spanning chunk boundaries.
			patch := randomNDArray(t, spec.DataType, []uint64{5, 4}, 8)
			if err := arr.Write(ctx, []uint64{3, 2}, patch); err != nil {
				t.Fatalf("partial write: %v", err)
			}
			got, _ = arr.ReadAll(ctx)
			region, _ := arr.Read(ctx, []uint64{3, 2}, []uint64{5, 4})
			if !reflect.DeepEqual(region.Data, patch.Data) || !reflect.DeepEqual(region.Strings, patch.Strings) {
				t.Errorf("partial write not visible")
			}
			// Rows 0-2 lie outside the patch: still the first 33 elements of full.
			outside, _ := arr.Read(ctx, []uint64{0, 0}, []uint64{3, 11})
			es := full.DataType.Size
			if full.Strings != nil {
				if !reflect.DeepEqual(outside.Strings, full.Strings[:33]) {
					t.Errorf("partial write clobbered other data")
				}
			} else if !bytes.Equal(outside.Data, full.Data[:33*es]) {
				t.Errorf("partial write clobbered other data")
			}
		})
	}
}

func TestWriteDeletesFillChunks(t *testing.T) {
	ctx := context.Background()
	doc, _ := ArraySpec{Shape: []uint64{4}, ChunkShape: []uint64{2}, DataType: "float64", FillValue: math.NaN()}.Metadata()
	store := newMemChunks()
	arr, _ := OpenArray(doc, store, nil)
	// A never-written chunk left all-fill is deleted anyway, as zarr-python
	// does (Icechunk records the deletion).
	nan, _ := FromSlice([]uint64{2}, []float64{math.NaN(), math.NaN()})
	if err := arr.Write(ctx, []uint64{0}, nan); err != nil {
		t.Fatal(err)
	}
	if len(store.chunks) != 0 || !slices.Equal(store.deletes, []string{"[0]"}) {
		t.Fatalf("writing fill to a new chunk: %d chunks, deletes %v", len(store.chunks), store.deletes)
	}
	vals, _ := FromSlice([]uint64{4}, []float64{1, 2, 3, 4})
	if err := arr.Write(ctx, []uint64{0}, vals); err != nil {
		t.Fatal(err)
	}
	if len(store.chunks) != 2 {
		t.Fatalf("%d chunks stored", len(store.chunks))
	}
	if err := arr.Write(ctx, []uint64{2}, nan); err != nil {
		t.Fatal(err)
	}
	if len(store.chunks) != 1 {
		t.Errorf("all-fill chunk was not deleted: %d chunks", len(store.chunks))
	}
}

func TestBitRoundEncode(t *testing.T) {
	c := &chunkData{shape: []uint64{3}, dtype: DataType{Name: "float32", Kind: KindFloat, Size: 4}}
	nd, _ := FromSlice([]uint64{3}, []float32{1.2345678, -9.876543, 3})
	c.data = nd.Data
	out, err := bitRound{keepbits: 7}.encodeArr(c)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := (&NDArray{Shape: c.shape, DataType: c.dtype, Data: out.data}).Values()
	// numpy: BitRound(7).encode(np.float32([1.2345678, -9.876543, 3])).view('f4')
	want := []float32{1.234375, -9.875, 3}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("bitround = %v, want %v", got, want)
	}
}
