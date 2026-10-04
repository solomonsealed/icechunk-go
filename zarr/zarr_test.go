package zarr

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestParseSelection(t *testing.T) {
	shape := []uint64{10, 20, 30}
	for _, tc := range []struct {
		sel          string
		start, count []uint64
		squeeze      []bool
	}{
		{"", []uint64{0, 0, 0}, []uint64{10, 20, 30}, []bool{false, false, false}},
		{"2:5", []uint64{2, 0, 0}, []uint64{3, 20, 30}, []bool{false, false, false}},
		{"3, :, -5:", []uint64{3, 0, 25}, []uint64{1, 20, 5}, []bool{true, false, false}},
		{"[:4, 19]", []uint64{0, 19, 0}, []uint64{4, 1, 30}, []bool{false, true, false}},
		{"8:100", []uint64{8, 0, 0}, []uint64{2, 20, 30}, []bool{false, false, false}},
		{"-1", []uint64{9, 0, 0}, []uint64{1, 20, 30}, []bool{true, false, false}},
	} {
		s, c, q, err := ParseSelection(tc.sel, shape)
		if err != nil || !slices.Equal(s, tc.start) || !slices.Equal(c, tc.count) || !slices.Equal(q, tc.squeeze) {
			t.Errorf("ParseSelection(%q) = %v %v %v %v", tc.sel, s, c, q, err)
		}
	}
	for _, bad := range []string{"10", "1,2,3,4", "1:2:3", "x"} {
		if _, _, _, err := ParseSelection(bad, shape); err == nil {
			t.Errorf("ParseSelection(%q) accepted", bad)
		}
	}
}

func TestHalfFloat(t *testing.T) {
	for _, f := range []float32{0, 1, -2.5, 65504, 6.103515625e-05, 5.960464477539063e-08} {
		if got := halfToFloat32(float32ToHalf(f)); got != f {
			t.Errorf("half round trip %v -> %v", f, got)
		}
	}
	if !math.IsNaN(float64(halfToFloat32(0x7e00))) || !math.IsInf(float64(halfToFloat32(0xfc00)), -1) {
		t.Error("half NaN/Inf")
	}
}

func TestNDArrayJSON(t *testing.T) {
	n := &NDArray{Shape: []uint64{3}, DataType: DataType{Name: "float64", Kind: KindFloat, Size: 8}, Data: make([]byte, 24)}
	putUint(n.Data[8:16], math.Float64bits(math.NaN()))
	putUint(n.Data[16:24], math.Float64bits(2.5))
	b, err := json.Marshal(n)
	if err != nil || string(b) != `{"shape":[3],"dtype":"float64","data":[0,null,2.5]}` {
		t.Errorf("json = %s, %v", b, err)
	}
}

func TestLZ4AndBloscLZ(t *testing.T) {
	// "abcabcabcabc..." as an LZ4 block: 3 literals then a match at offset 3.
	src := []byte{0x3f, 'a', 'b', 'c', 3, 0, 0x05}
	dst := make([]byte, 3+4+15+5)
	if err := lz4Block(src, dst); err != nil {
		t.Fatal(err)
	}
	for i, b := range dst {
		if b != "abc"[i%3] {
			t.Fatalf("lz4 output %q", dst)
		}
	}
	if err := lz4Block([]byte{0x10, 'a', 9, 0}, make([]byte, 10)); err == nil {
		t.Error("lz4 accepted a match before the start of output")
	}
}

func TestFloat16FillRounding(t *testing.T) {
	for v, want := range map[float64]uint16{0.7: 0x399a, 65520: 0x7c00, 65519: 0x7bff, -0.1: 0xae66, 1e-8: 0} {
		if got := float64ToHalf(v); got != want {
			t.Errorf("float64ToHalf(%v) = %#04x, want %#04x", v, got, want)
		}
	}
}

type panickySource struct{}

func (panickySource) GetChunk(context.Context, []uint32, int64, int64) ([]byte, bool, error) {
	panic("boom")
}
func (panickySource) ChunkSize(context.Context, []uint32) (int64, bool, error) { return 0, false, nil }

// A panicking chunk source becomes an error instead of crashing the program.
func TestPanicInChunkReadIsAnError(t *testing.T) {
	doc := []byte(`{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"int8",
		"chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2]}},
		"chunk_key_encoding":{"name":"default"},"fill_value":0,"codecs":[{"name":"bytes"}]}`)
	arr, err := OpenArray(doc, panickySource{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := arr.ReadAll(context.Background()); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Errorf("err = %v, want the panic as an error", err)
	}
}

// Attributes keep integers exact, as Python does.
func TestAttributeNumbers(t *testing.T) {
	attrs, err := Attributes([]byte(`{"attributes": {"big": 4611686018427387904, "neg": -5, "f": 1.5,
		"e": 1e3, "u": 18446744073709551615, "huge": 123456789012345678901234567890, "list": [1, {"x": 2.0}]}}`))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"big": int64(4611686018427387904), "neg": int64(-5), "f": 1.5, "e": 1000.0,
		"u": uint64(18446744073709551615), "huge": json.Number("123456789012345678901234567890"),
		"list": []any{int64(1), map[string]any{"x": 2.0}},
	}
	if !reflect.DeepEqual(attrs, want) {
		t.Errorf("attributes = %#v", attrs)
	}
}
