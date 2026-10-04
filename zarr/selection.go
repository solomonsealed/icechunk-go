package zarr

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ParseSelection parses a NumPy-style basic selection such as "0:10, 5, :"
// or "-3:" against shape. Each comma-separated item is an index (which
// selects a single element along that dimension) or a start:stop range;
// negative values count from the end and missing trailing items select
// everything. Steps are not supported. An empty string selects the whole
// array. squeeze reports which dimensions were selected by a single index.
func ParseSelection(sel string, shape []uint64) (start, count []uint64, squeeze []bool, err error) {
	nd := len(shape)
	start, count, squeeze = make([]uint64, nd), append([]uint64(nil), shape...), make([]bool, nd)
	sel = strings.TrimSpace(sel)
	sel = strings.TrimSuffix(strings.TrimPrefix(sel, "["), "]")
	if sel == "" || sel == "..." {
		return start, count, squeeze, nil
	}
	items := strings.Split(sel, ",")
	if len(items) > nd {
		return nil, nil, nil, fmt.Errorf("zarr: selection %q has %d items for %d dimensions", sel, len(items), nd)
	}
	resolve := func(s string, n uint64, dflt uint64) (uint64, error) {
		s = strings.TrimSpace(s)
		if s == "" {
			return dflt, nil
		}
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("zarr: invalid index %q", s)
		}
		if v < 0 {
			v += int64(n)
		}
		if v < 0 {
			v = 0
		}
		return min(uint64(v), n), nil
	}
	for d, item := range items {
		n := shape[d]
		lo, hi, isRange := strings.Cut(item, ":")
		if !isRange {
			s := strings.TrimSpace(item)
			v, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("zarr: invalid index %q", s)
			}
			if v < 0 {
				v += int64(n)
			}
			if v < 0 || uint64(v) >= n {
				return nil, nil, nil, fmt.Errorf("zarr: index %s out of bounds for dimension %d of length %d", s, d, n)
			}
			start[d], count[d], squeeze[d] = uint64(v), 1, true
			continue
		}
		if strings.Contains(hi, ":") {
			return nil, nil, nil, fmt.Errorf("zarr: steps are not supported in %q", item)
		}
		a, err := resolve(lo, n, 0)
		if err != nil {
			return nil, nil, nil, err
		}
		b, err := resolve(hi, n, n)
		if err != nil {
			return nil, nil, nil, err
		}
		start[d] = a
		if b > a {
			count[d] = b - a
		} else {
			count[d] = 0
		}
	}
	return start, count, squeeze, nil
}

// MarshalJSON encodes {"shape": [...], "dtype": "...", "data": [...]}
// with data flattened in C order. NaN and infinite floats, which JSON
// cannot represent, are encoded as null; complex numbers as [re, im];
// raw/fixed bytes as base64 strings.
func (n *NDArray) MarshalJSON() ([]byte, error) {
	vals, err := n.Values()
	if err != nil {
		return nil, err
	}
	var data any = vals
	switch v := vals.(type) {
	case []float32:
		data = floatsJSON(len(v), func(i int) float64 { return float64(v[i]) })
	case []float64:
		data = floatsJSON(len(v), func(i int) float64 { return v[i] })
	case []complex64:
		out := make([][2]any, len(v))
		for i, c := range v {
			out[i] = [2]any{floatJSON(float64(real(c))), floatJSON(float64(imag(c)))}
		}
		data = out
	case []complex128:
		out := make([][2]any, len(v))
		for i, c := range v {
			out[i] = [2]any{floatJSON(real(c)), floatJSON(imag(c))}
		}
		data = out
	case []uint8:
		// Avoid encoding/json's base64 for []byte.
		out := make([]uint16, len(v))
		for i, b := range v {
			out[i] = uint16(b)
		}
		data = out
	}
	shape := n.Shape
	if shape == nil {
		shape = []uint64{}
	}
	return json.Marshal(struct {
		Shape []uint64 `json:"shape"`
		DType string   `json:"dtype"`
		Data  any      `json:"data"`
	}{shape, n.DataType.Name, data})
}

func floatJSON(f float64) any {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil
	}
	return f
}

func floatsJSON(n int, at func(int) float64) []any {
	out := make([]any, n)
	for i := range out {
		out[i] = floatJSON(at(i))
	}
	return out
}
