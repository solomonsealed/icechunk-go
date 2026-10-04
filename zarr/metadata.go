// Package zarr decodes Zarr v3 arrays: metadata (zarr.json), chunk grids,
// fill values and the codec pipeline, and assembles chunk data into
// N-dimensional results.
//
// It is storage-agnostic: chunks come from a ChunkSource, which the
// icechunk package implements on top of a repository snapshot.
package zarr

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ErrUnsupported is returned for metadata features this package cannot decode.
var ErrUnsupported = errors.New("zarr: unsupported")

// Kind classifies data types.
type Kind uint8

const (
	KindBool Kind = iota + 1
	KindInt
	KindUint
	KindFloat
	KindComplex
	// KindRaw is an opaque fixed-size type ("r8", "r16", ...).
	KindRaw
	// KindString is variable-length UTF-8 ("string").
	KindString
	// KindBytes is variable-length binary ("bytes", "variable_length_bytes").
	KindBytes
	// KindDatetime / KindTimedelta are numpy datetime64 / timedelta64,
	// stored as int64 counts of Unit.
	KindDatetime
	KindTimedelta
	// KindFixedString is numpy's fixed-length UTF-32 ("fixed_length_utf32").
	KindFixedString
	// KindFixedBytes is numpy's fixed-length bytes ("null_terminated_bytes",
	// "raw_bytes").
	KindFixedBytes
)

// DataType describes an array's element type.
type DataType struct {
	// Name is the Zarr data type name, e.g. "float32" or "numpy.datetime64".
	Name string
	Kind Kind
	// Size is the element size in bytes; 0 for variable-length types.
	Size int
	// Unit is the datetime/timedelta unit (e.g. "ns", "s", "D").
	Unit string
	// ScaleFactor is the datetime/timedelta unit multiplier.
	ScaleFactor int
	// Configuration is the raw data type configuration, if any.
	Configuration map[string]any
}

// Variable reports whether elements have variable length.
func (d DataType) Variable() bool { return d.Kind == KindString || d.Kind == KindBytes }

// swapSize is the unit to byte-swap for big-endian data (0 = no swap).
func (d DataType) swapSize() int {
	switch d.Kind {
	case KindInt, KindUint, KindFloat, KindDatetime, KindTimedelta:
		return d.Size
	case KindComplex:
		return d.Size / 2
	case KindFixedString:
		return 4
	}
	return 0
}

func (d DataType) String() string { return d.Name }

func parseDataType(raw json.RawMessage) (DataType, error) {
	var name string
	var cfg map[string]any
	if err := json.Unmarshal(raw, &name); err != nil {
		var obj struct {
			Name          string         `json:"name"`
			Configuration map[string]any `json:"configuration"`
		}
		if err := json.Unmarshal(raw, &obj); err != nil {
			return DataType{}, fmt.Errorf("zarr: invalid data_type %s", raw)
		}
		name, cfg = obj.Name, obj.Configuration
	}
	d := DataType{Name: name, Configuration: cfg}
	switch name {
	case "bool":
		d.Kind, d.Size = KindBool, 1
	case "int8", "int16", "int32", "int64":
		d.Kind = KindInt
		d.Size, _ = strconv.Atoi(name[3:])
		d.Size /= 8
	case "uint8", "uint16", "uint32", "uint64":
		d.Kind = KindUint
		d.Size, _ = strconv.Atoi(name[4:])
		d.Size /= 8
	case "float16", "float32", "float64":
		d.Kind = KindFloat
		d.Size, _ = strconv.Atoi(name[5:])
		d.Size /= 8
	case "complex64", "complex128":
		d.Kind = KindComplex
		d.Size, _ = strconv.Atoi(name[7:])
		d.Size /= 8
	case "string":
		d.Kind = KindString
	case "bytes", "variable_length_bytes":
		d.Kind = KindBytes
	case "numpy.datetime64", "numpy.timedelta64":
		d.Kind, d.Size = KindDatetime, 8
		if name == "numpy.timedelta64" {
			d.Kind = KindTimedelta
		}
		d.Unit, _ = cfg["unit"].(string)
		d.ScaleFactor = 1
		if f, ok := cfg["scale_factor"].(float64); ok {
			d.ScaleFactor = int(f)
		}
	case "fixed_length_utf32", "null_terminated_bytes", "raw_bytes":
		d.Kind = KindFixedBytes
		if name == "fixed_length_utf32" {
			d.Kind = KindFixedString
		}
		f, ok := cfg["length_bytes"].(float64)
		if !ok || f < 0 {
			return d, fmt.Errorf("zarr: data type %s needs configuration.length_bytes", name)
		}
		d.Size = int(f)
	default:
		if strings.HasPrefix(name, "r") {
			if bits, err := strconv.Atoi(name[1:]); err == nil && bits > 0 && bits%8 == 0 {
				d.Kind, d.Size = KindRaw, bits/8
				return d, nil
			}
		}
		return d, fmt.Errorf("%w: data type %q", ErrUnsupported, name)
	}
	return d, nil
}

// ---------------------------------------------------------------------------
// Fill values

// parseFillValue encodes the JSON fill value as one element in the decoded
// in-memory representation (little-endian for fixed-size types).
func parseFillValue(raw json.RawMessage, d DataType) (fixed []byte, str string, err error) {
	if len(raw) == 0 || string(raw) == "null" {
		if d.Variable() {
			return nil, "", nil
		}
		return make([]byte, d.Size), "", nil
	}
	bad := func() ([]byte, string, error) {
		return nil, "", fmt.Errorf("zarr: invalid fill_value %s for %s", raw, d.Name)
	}
	out := make([]byte, d.Size)
	switch d.Kind {
	case KindBool:
		var b bool
		if err := json.Unmarshal(raw, &b); err != nil {
			var n float64
			if json.Unmarshal(raw, &n) != nil {
				return bad()
			}
			b = n != 0
		}
		if b {
			out[0] = 1
		}
	case KindInt, KindUint, KindDatetime, KindTimedelta:
		var s string
		if json.Unmarshal(raw, &s) == nil {
			if s == "NaT" {
				putInt(out, math.MinInt64)
				break
			}
			if bits, ok := parseHexBits(s, d.Size); ok {
				putUint(out, bits)
				break
			}
			return bad()
		}
		n := json.Number(strings.TrimSpace(string(raw)))
		if d.Kind == KindUint {
			u, err := strconv.ParseUint(n.String(), 10, 64)
			if err != nil {
				f, ferr := n.Float64()
				if ferr != nil {
					return bad()
				}
				u = uint64(f)
			}
			putUint(out, u)
		} else {
			i, err := n.Int64()
			if err != nil {
				f, ferr := n.Float64()
				if ferr != nil {
					return bad()
				}
				i = int64(f)
			}
			putInt(out, i)
		}
	case KindFloat:
		bits, ok := parseFloatFill(raw, d.Size)
		if !ok {
			return bad()
		}
		putUint(out, bits)
	case KindComplex:
		var parts []json.RawMessage
		if json.Unmarshal(raw, &parts) != nil || len(parts) != 2 {
			return bad()
		}
		for i, p := range parts {
			bits, ok := parseFloatFill(p, d.Size/2)
			if !ok {
				return bad()
			}
			putUint(out[i*d.Size/2:(i+1)*d.Size/2], bits)
		}
	case KindRaw, KindFixedBytes:
		var ints []int
		if json.Unmarshal(raw, &ints) == nil {
			for i := 0; i < len(ints) && i < len(out); i++ {
				out[i] = byte(ints[i])
			}
			break
		}
		var s string
		if json.Unmarshal(raw, &s) != nil {
			return bad()
		}
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			b = []byte(s)
		}
		copy(out, b)
	case KindFixedString:
		var s string
		if json.Unmarshal(raw, &s) != nil {
			return bad()
		}
		for i, r := range []rune(s) {
			if (i+1)*4 > len(out) {
				break
			}
			binary.LittleEndian.PutUint32(out[i*4:], uint32(r))
		}
	case KindString:
		var s string
		if json.Unmarshal(raw, &s) != nil {
			return bad()
		}
		return nil, s, nil
	case KindBytes:
		var ints []int
		if json.Unmarshal(raw, &ints) == nil {
			b := make([]byte, len(ints))
			for i, v := range ints {
				b[i] = byte(v)
			}
			return nil, string(b), nil
		}
		var s string
		if json.Unmarshal(raw, &s) != nil {
			return bad()
		}
		if b, err := base64.StdEncoding.DecodeString(s); err == nil {
			return nil, string(b), nil
		}
		return nil, s, nil
	}
	return out, "", nil
}

func parseHexBits(s string, size int) (uint64, bool) {
	if !strings.HasPrefix(s, "0x") || len(s) > 2+2*size {
		return 0, false
	}
	u, err := strconv.ParseUint(s[2:], 16, 64)
	return u, err == nil
}

// parseFloatFill returns the IEEE bits of a float fill value of size bytes.
func parseFloatFill(raw json.RawMessage, size int) (uint64, bool) {
	var f float64
	var s string
	if json.Unmarshal(raw, &s) == nil {
		switch s {
		case "NaN":
			// Canonical quiet NaNs, as numpy writes them (math.NaN() has a
			// different payload).
			switch size {
			case 2:
				return 0x7e00, true
			case 4:
				return 0x7fc00000, true
			case 8:
				return 0x7ff8000000000000, true
			}
			return 0, false
		case "Infinity":
			f = math.Inf(1)
		case "-Infinity":
			f = math.Inf(-1)
		default:
			return parseHexBits(s, size)
		}
	} else if json.Unmarshal(raw, &f) != nil {
		return 0, false
	}
	switch size {
	case 2:
		return uint64(float64ToHalf(f)), true
	case 4:
		return uint64(math.Float32bits(float32(f))), true
	case 8:
		return math.Float64bits(f), true
	}
	return 0, false
}

func putUint(b []byte, v uint64) {
	for i := range b {
		b[i] = byte(v >> (8 * i))
	}
}

func putInt(b []byte, v int64) { putUint(b, uint64(v)) }

// ---------------------------------------------------------------------------
// Half precision

func halfToFloat32(h uint16) float32 {
	sign := uint32(h>>15) << 31
	exp := uint32(h>>10) & 0x1f
	mant := uint32(h) & 0x3ff
	switch {
	case exp == 0 && mant == 0:
		return math.Float32frombits(sign)
	case exp == 0: // subnormal
		f := float32(mant) / 1024 / 16384
		if sign != 0 {
			f = -f
		}
		return f
	case exp == 0x1f:
		return math.Float32frombits(sign | 0x7f800000 | mant<<13)
	}
	return math.Float32frombits(sign | (exp+112)<<23 | mant<<13)
}

// float64ToHalf converts to IEEE half precision, rounding to nearest even
// like numpy.
func float64ToHalf(f float64) uint16 {
	var sign uint16
	if math.Signbit(f) {
		sign = 0x8000
	}
	a := math.Abs(f)
	switch {
	case math.IsNaN(f):
		return sign | 0x7e00
	case a >= 65520: // halfway to 2^16 rounds (to even) to infinity
		return sign | 0x7c00
	case a < 0x1p-14: // subnormal: multiples of 2^-24
		return sign | uint16(math.RoundToEven(a*0x1p24))
	}
	_, exp := math.Frexp(a) // a = frac * 2^exp, frac in [0.5, 1)
	e := exp - 1            // a in [2^e, 2^(e+1))
	m := math.RoundToEven(math.Ldexp(a, 10-e))
	if m == 2048 {
		m, e = 1024, e+1
	}
	return sign | uint16(e+15)<<10 | uint16(m-1024)
}

func float32ToHalf(f float32) uint16 {
	b := math.Float32bits(f)
	sign := uint16(b>>16) & 0x8000
	exp := int((b>>23)&0xff) - 127 + 15
	mant := b & 0x7fffff
	switch {
	case (b>>23)&0xff == 0xff:
		if mant != 0 {
			return sign | 0x7e00
		}
		return sign | 0x7c00
	case exp >= 0x1f:
		return sign | 0x7c00
	case exp <= 0:
		if exp < -10 {
			return sign
		}
		mant |= 0x800000
		return sign | uint16(mant>>uint(14-exp))
	}
	return sign | uint16(exp)<<10 | uint16(mant>>13)
}

// ---------------------------------------------------------------------------
// Chunk grids

// chunkGrid maps array positions to chunks along each dimension.
type chunkGrid interface {
	// numChunks returns the number of chunks along dim for an array of length n.
	numChunks(dim int, n uint64) uint64
	// span returns the start position and length of chunk idx along dim.
	span(dim int, idx uint64) (start, size uint64)
	// index returns the chunk containing position pos along dim.
	index(dim int, pos uint64) uint64
}

type regularGrid []uint64

func (g regularGrid) numChunks(dim int, n uint64) uint64 {
	return (n + g[dim] - 1) / g[dim]
}
func (g regularGrid) span(dim int, idx uint64) (uint64, uint64) { return idx * g[dim], g[dim] }
func (g regularGrid) index(dim int, pos uint64) uint64          { return pos / g[dim] }

// rectilinearGrid has explicit chunk edges per dimension; a dimension with
// no explicit edges repeats a regular step.
type rectilinearGrid struct {
	step   []uint64   // >0 for regular-step dimensions
	starts [][]uint64 // cumulative starts (len = nchunks+1) for explicit dims
}

func (g *rectilinearGrid) numChunks(dim int, n uint64) uint64 {
	if s := g.step[dim]; s > 0 {
		return (n + s - 1) / s
	}
	st := g.starts[dim]
	for i := 1; i < len(st); i++ {
		if st[i] >= n {
			return uint64(i)
		}
	}
	return uint64(len(st) - 1)
}

func (g *rectilinearGrid) span(dim int, idx uint64) (uint64, uint64) {
	if s := g.step[dim]; s > 0 {
		return idx * s, s
	}
	st := g.starts[dim]
	if idx+1 >= uint64(len(st)) {
		return st[len(st)-1], 0
	}
	return st[idx], st[idx+1] - st[idx]
}

func (g *rectilinearGrid) index(dim int, pos uint64) uint64 {
	if s := g.step[dim]; s > 0 {
		return pos / s
	}
	st := g.starts[dim]
	lo, hi := 0, len(st)-1
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if st[mid] <= pos {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return uint64(lo)
}

func parseChunkGrid(raw json.RawMessage, ndim int) (chunkGrid, []uint64, error) {
	var g struct {
		Name          string `json:"name"`
		Configuration struct {
			ChunkShape  []uint64          `json:"chunk_shape"`
			Kind        string            `json:"kind"`
			ChunkShapes []json.RawMessage `json:"chunk_shapes"`
		} `json:"configuration"`
	}
	if err := json.Unmarshal(raw, &g); err != nil {
		return nil, nil, fmt.Errorf("zarr: invalid chunk_grid: %w", err)
	}
	switch g.Name {
	case "regular":
		cs := g.Configuration.ChunkShape
		if len(cs) != ndim {
			return nil, nil, fmt.Errorf("zarr: chunk_shape has %d dimensions, array has %d", len(cs), ndim)
		}
		for _, c := range cs {
			if c == 0 {
				return nil, nil, fmt.Errorf("zarr: chunk_shape contains 0")
			}
		}
		return regularGrid(cs), cs, nil
	case "rectilinear":
		shapes := g.Configuration.ChunkShapes
		if len(shapes) != ndim {
			return nil, nil, fmt.Errorf("zarr: chunk_shapes has %d dimensions, array has %d", len(shapes), ndim)
		}
		grid := &rectilinearGrid{step: make([]uint64, ndim), starts: make([][]uint64, ndim)}
		for d, spec := range shapes {
			var step uint64
			if json.Unmarshal(spec, &step) == nil {
				if step == 0 {
					return nil, nil, fmt.Errorf("zarr: rectilinear step 0")
				}
				grid.step[d] = step
				continue
			}
			var entries []json.RawMessage
			if err := json.Unmarshal(spec, &entries); err != nil {
				return nil, nil, fmt.Errorf("zarr: invalid rectilinear chunk_shapes entry %s", spec)
			}
			starts := []uint64{0}
			push := func(edge uint64) { starts = append(starts, starts[len(starts)-1]+edge) }
			for _, e := range entries {
				var edge uint64
				if json.Unmarshal(e, &edge) == nil {
					push(edge)
					continue
				}
				var rle []uint64
				if json.Unmarshal(e, &rle) != nil || len(rle) != 2 {
					return nil, nil, fmt.Errorf("zarr: invalid rectilinear edge %s", e)
				}
				for i := uint64(0); i < rle[1]; i++ {
					push(rle[0])
				}
			}
			grid.starts[d] = starts
		}
		return grid, nil, nil
	}
	return nil, nil, fmt.Errorf("%w: chunk grid %q", ErrUnsupported, g.Name)
}

// ---------------------------------------------------------------------------
// Array metadata

// CodecSpec is one entry of the codecs list.
type CodecSpec struct {
	Name          string
	Configuration json.RawMessage
}

func (c *CodecSpec) UnmarshalJSON(b []byte) error {
	var name string
	if json.Unmarshal(b, &name) == nil {
		c.Name = name
		return nil
	}
	var obj struct {
		Name          string          `json:"name"`
		Configuration json.RawMessage `json:"configuration"`
	}
	if err := json.Unmarshal(b, &obj); err != nil {
		return err
	}
	c.Name, c.Configuration = obj.Name, obj.Configuration
	return nil
}

// Metadata is a parsed Zarr v3 zarr.json document for an array.
type Metadata struct {
	ZarrFormat int
	NodeType   string
	Shape      []uint64
	DataType   DataType
	// ChunkShape is the regular chunk shape; nil for rectilinear grids.
	ChunkShape     []uint64
	FillValue      json.RawMessage
	Codecs         []CodecSpec
	Attributes     map[string]any
	DimensionNames []*string

	grid      chunkGrid
	fill      []byte
	fillStr   string
	separator string
	v2Keys    bool
}

type rawMetadata struct {
	ZarrFormat       int             `json:"zarr_format"`
	NodeType         string          `json:"node_type"`
	Shape            []uint64        `json:"shape"`
	DataType         json.RawMessage `json:"data_type"`
	ChunkGrid        json.RawMessage `json:"chunk_grid"`
	ChunkKeyEncoding struct {
		Name          string `json:"name"`
		Configuration struct {
			Separator string `json:"separator"`
		} `json:"configuration"`
	} `json:"chunk_key_encoding"`
	FillValue           json.RawMessage `json:"fill_value"`
	Codecs              []CodecSpec     `json:"codecs"`
	Attributes          json.RawMessage `json:"attributes"`
	DimensionNames      []*string       `json:"dimension_names"`
	StorageTransformers []CodecSpec     `json:"storage_transformers"`
}

// ParseMetadata parses an array's zarr.json.
func ParseMetadata(doc []byte) (*Metadata, error) {
	var raw rawMetadata
	if err := json.Unmarshal(doc, &raw); err != nil {
		return nil, fmt.Errorf("zarr: invalid zarr.json: %w", err)
	}
	if raw.ZarrFormat != 3 {
		return nil, fmt.Errorf("%w: zarr_format %d", ErrUnsupported, raw.ZarrFormat)
	}
	if raw.NodeType != "array" {
		return nil, fmt.Errorf("zarr: node_type is %q, not an array", raw.NodeType)
	}
	if len(raw.StorageTransformers) > 0 {
		return nil, fmt.Errorf("%w: storage transformer %q", ErrUnsupported, raw.StorageTransformers[0].Name)
	}
	m := &Metadata{
		ZarrFormat:     raw.ZarrFormat,
		NodeType:       raw.NodeType,
		Shape:          raw.Shape,
		FillValue:      raw.FillValue,
		Codecs:         raw.Codecs,
		DimensionNames: raw.DimensionNames,
		separator:      raw.ChunkKeyEncoding.Configuration.Separator,
		v2Keys:         raw.ChunkKeyEncoding.Name == "v2",
	}
	var err error
	if m.Attributes, err = decodeAttributes(raw.Attributes); err != nil {
		return nil, err
	}
	if m.Shape == nil {
		m.Shape = []uint64{}
	}
	if m.DataType, err = parseDataType(raw.DataType); err != nil {
		return nil, err
	}
	if m.grid, m.ChunkShape, err = parseChunkGrid(raw.ChunkGrid, len(m.Shape)); err != nil {
		return nil, err
	}
	if m.fill, m.fillStr, err = parseFillValue(raw.FillValue, m.DataType); err != nil {
		return nil, err
	}
	if m.separator == "" {
		m.separator = "/"
		if m.v2Keys {
			m.separator = "."
		}
	}
	return m, nil
}

// ChunkKey returns the chunk's store key relative to the array, following
// the array's chunk_key_encoding (e.g. "c/0/1").
func (m *Metadata) ChunkKey(coords []uint64) string {
	parts := make([]string, len(coords))
	for i, c := range coords {
		parts[i] = strconv.FormatUint(c, 10)
	}
	if m.v2Keys {
		if len(parts) == 0 {
			return "0"
		}
		return strings.Join(parts, m.separator)
	}
	return strings.Join(append([]string{"c"}, parts...), m.separator)
}

// Attributes extracts the attributes of any zarr.json document (array or
// group), with numbers decoded as decodeAttributes does.
func Attributes(doc []byte) (map[string]any, error) {
	var raw struct {
		Attributes json.RawMessage `json:"attributes"`
	}
	if err := json.Unmarshal(doc, &raw); err != nil {
		return nil, fmt.Errorf("zarr: invalid zarr.json: %w", err)
	}
	return decodeAttributes(raw.Attributes)
}

// decodeAttributes decodes user attributes keeping integers exact, as
// zarr-python (Python's json) does: integers become int64, or uint64 above
// int64's range, or json.Number beyond 64 bits; other numbers float64.
func decodeAttributes(raw json.RawMessage) (map[string]any, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return map[string]any{}, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var attrs map[string]any
	if err := dec.Decode(&attrs); err != nil {
		return nil, fmt.Errorf("zarr: invalid attributes: %w", err)
	}
	if attrs == nil {
		return map[string]any{}, nil
	}
	return exactNumbers(attrs).(map[string]any), nil
}

func exactNumbers(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			x[k] = exactNumbers(e)
		}
	case []any:
		for i, e := range x {
			x[i] = exactNumbers(e)
		}
	case json.Number:
		s := x.String()
		if !strings.ContainsAny(s, ".eE") {
			if i, err := strconv.ParseInt(s, 10, 64); err == nil {
				return i
			}
			if u, err := strconv.ParseUint(s, 10, 64); err == nil {
				return u
			}
			return x
		}
		f, _ := x.Float64()
		return f
	}
	return v
}
