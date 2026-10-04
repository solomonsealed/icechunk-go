package zarr

import (
	"encoding/json"
	"fmt"
	"math"
	"math/cmplx"
)

// ArraySpec describes a new array; Metadata renders its zarr.json.
type ArraySpec struct {
	Shape []uint64
	// ChunkShape is the chunk shape (inner chunk shape when sharded);
	// nil means one chunk for the whole array.
	ChunkShape []uint64
	// DataType is a Zarr v3 data type name such as "float32" or "string".
	DataType string
	// FillValue defaults to the zero value (false, 0, 0.0 or "").
	// Use math.NaN() or math.Inf for special floats.
	FillValue any
	// Codecs defaults to zarr-python's: bytes + zstd (vlen-utf8 + zstd for
	// strings). When ShardShape is set, these are the inner-chunk codecs.
	Codecs []CodecSpec
	// ShardShape, if set, stores chunks in shards of this shape (a multiple
	// of ChunkShape) using the sharding_indexed codec.
	ShardShape []uint64
	// DimensionNames are optional; "" leaves a dimension unnamed.
	DimensionNames []string
	Attributes     map[string]any
}

// Codec builds a CodecSpec from a name and configuration. A nil config is
// written as {} (zarr-python requires a configuration for numcodecs.*).
func Codec(name string, config map[string]any) CodecSpec {
	if config == nil {
		config = map[string]any{}
	}
	c := CodecSpec{Name: name}
	c.Configuration, _ = json.Marshal(config)
	return c
}

// Common codecs.
func BytesLE() CodecSpec { return Codec("bytes", map[string]any{"endian": "little"}) }
func Zstd(level int) CodecSpec {
	return Codec("zstd", map[string]any{"level": level, "checksum": false})
}
func Gzip(level int) CodecSpec { return Codec("gzip", map[string]any{"level": level}) }
func Crc32c() CodecSpec        { return CodecSpec{Name: "crc32c"} } // no configuration, as zarr-python writes it

// Blosc configures blosc: cname "lz4", "lz4hc", "zstd" or "zlib"; shuffle
// "noshuffle", "shuffle" or "bitshuffle".
func Blosc(cname string, clevel int, shuffle string) CodecSpec {
	return Codec("blosc", map[string]any{"cname": cname, "clevel": clevel, "shuffle": shuffle, "blocksize": 0})
}

// Transpose reorders dimensions in storage.
func Transpose(order []int) CodecSpec { return Codec("transpose", map[string]any{"order": order}) }

func (c CodecSpec) MarshalJSON() ([]byte, error) {
	m := map[string]any{"name": c.Name}
	if len(c.Configuration) > 0 {
		m["configuration"] = c.Configuration
	}
	return json.Marshal(m)
}

func jsonFloat(f float64) any {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}
	return f
}

func fillJSON(dt DataType, v any) (any, error) {
	bad := func() (any, error) { return nil, fmt.Errorf("zarr: fill value %v (%T) does not fit %s", v, v, dt.Name) }
	switch dt.Kind {
	case KindBool:
		if v == nil {
			return false, nil
		}
		b, ok := v.(bool)
		if !ok {
			return bad()
		}
		return b, nil
	case KindString, KindBytes:
		if v == nil {
			return "", nil
		}
		s, ok := v.(string)
		if !ok {
			return bad()
		}
		return s, nil
	case KindComplex:
		var c complex128
		switch x := v.(type) {
		case nil:
		case complex128:
			c = x
		case complex64:
			c = complex128(x)
		default:
			f, ok := toFloat(v)
			if !ok {
				return bad()
			}
			c = complex(f, 0)
		}
		if cmplx.IsNaN(c) && !math.IsNaN(real(c)) && !math.IsNaN(imag(c)) {
			return bad()
		}
		return []any{jsonFloat(real(c)), jsonFloat(imag(c))}, nil
	case KindFloat:
		if v == nil {
			return 0.0, nil
		}
		f, ok := toFloat(v)
		if !ok {
			return bad()
		}
		return jsonFloat(f), nil
	case KindInt, KindUint, KindDatetime, KindTimedelta:
		switch x := v.(type) {
		case nil:
			return 0, nil
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return x, nil
		}
		return bad()
	}
	if v == nil {
		return 0, nil
	}
	return v, nil
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case int32:
		return float64(x), true
	case uint64:
		return float64(x), true
	}
	return 0, false
}

// Metadata renders the array's zarr.json and validates it by parsing it back.
func (s ArraySpec) Metadata() ([]byte, error) {
	dt, err := ParseDataType(s.DataType)
	if err != nil {
		return nil, err
	}
	if s.Shape == nil {
		s.Shape = []uint64{}
	}
	chunks := s.ChunkShape
	if chunks == nil {
		chunks = make([]uint64, len(s.Shape))
		for i, n := range s.Shape {
			chunks[i] = max(n, 1)
		}
	}
	if len(chunks) != len(s.Shape) {
		return nil, fmt.Errorf("zarr: chunk shape %v does not match shape %v", chunks, s.Shape)
	}
	codecs := s.Codecs
	if codecs == nil {
		if dt.Variable() {
			codecs = []CodecSpec{Codec("vlen-utf8", nil), Zstd(0)}
		} else {
			codecs = []CodecSpec{BytesLE(), Zstd(0)}
		}
	}
	gridShape := chunks
	if s.ShardShape != nil {
		if len(s.ShardShape) != len(s.Shape) {
			return nil, fmt.Errorf("zarr: shard shape %v does not match shape %v", s.ShardShape, s.Shape)
		}
		cfg, _ := json.Marshal(map[string]any{
			"chunk_shape":    chunks,
			"codecs":         codecs,
			"index_codecs":   []CodecSpec{BytesLE(), Crc32c()},
			"index_location": "end",
		})
		codecs = []CodecSpec{{Name: "sharding_indexed", Configuration: cfg}}
		gridShape = s.ShardShape
	}
	fill, err := fillJSON(dt, s.FillValue)
	if err != nil {
		return nil, err
	}
	attrs := s.Attributes
	if attrs == nil {
		attrs = map[string]any{}
	}
	var dataType any = dt.Name
	if dt.Configuration != nil {
		dataType = map[string]any{"name": dt.Name, "configuration": dt.Configuration}
	}
	doc := map[string]any{
		"zarr_format":          3,
		"node_type":            "array",
		"shape":                s.Shape,
		"data_type":            dataType,
		"chunk_grid":           map[string]any{"name": "regular", "configuration": map[string]any{"chunk_shape": gridShape}},
		"chunk_key_encoding":   map[string]any{"name": "default", "configuration": map[string]any{"separator": "/"}},
		"fill_value":           fill,
		"codecs":               codecs,
		"attributes":           attrs,
		"storage_transformers": []any{},
	}
	if s.DimensionNames != nil {
		if len(s.DimensionNames) != len(s.Shape) {
			return nil, fmt.Errorf("zarr: %d dimension names for %d dimensions", len(s.DimensionNames), len(s.Shape))
		}
		names := make([]any, len(s.DimensionNames))
		for i, n := range s.DimensionNames {
			if n != "" {
				names[i] = n
			}
		}
		doc["dimension_names"] = names
	}
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, err
	}
	if _, err := ParseMetadata(out); err != nil {
		return nil, fmt.Errorf("zarr: invalid array spec: %w", err)
	}
	return out, nil
}
