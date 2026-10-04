package serve

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	icechunk "github.com/solomonsealed/icechunk-go"
	zarr "github.com/solomonsealed/zarr-go"
)

// Write endpoints, enabled when Service.WriteToken is set and authorized
// with "Authorization: Bearer <token>". Each request is one commit (rebased
// onto concurrent commits when they do not conflict):
//
//	POST /arrays/<path>?branch=main&message=...   body: array spec (JSON)
//	PUT  /array/<path>?branch=main&message=...    body: {"start": [...], "shape": [...], "data": [...]}
//
// The array spec has the fields shape, chunk_shape, shard_shape,
// data_type, fill_value, codecs, dimension_names and attributes. Data is a
// flat list in C order: numbers ("NaN", "Infinity" or null for special
// floats), booleans, strings, or [re, im] pairs for complex types.

func (s *Service) authorized(r *Request) error {
	if s.WriteToken == "" {
		return &httpError{405, "writes are disabled"}
	}
	got := strings.TrimPrefix(r.Header["authorization"], "Bearer ")
	if subtle.ConstantTimeCompare([]byte(got), []byte(s.WriteToken)) != 1 {
		return &httpError{401, "missing or invalid bearer token"}
	}
	return nil
}

func decodeJSON(body []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		return badRequest("invalid JSON body: %v", err)
	}
	return nil
}

type arraySpecJSON struct {
	Shape          []uint64         `json:"shape"`
	ChunkShape     []uint64         `json:"chunk_shape"`
	ShardShape     []uint64         `json:"shard_shape"`
	DataType       string           `json:"data_type"`
	FillValue      any              `json:"fill_value"`
	Codecs         []zarr.CodecSpec `json:"codecs"`
	DimensionNames []*string        `json:"dimension_names"`
	Attributes     map[string]any   `json:"attributes"`
}

func scalarFromJSON(v any) any {
	switch x := v.(type) {
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i
		}
		if u, err := strconv.ParseUint(x.String(), 10, 64); err == nil {
			return u
		}
		f, _ := x.Float64()
		return f
	case string:
		switch x {
		case "NaN":
			return math.NaN()
		case "Infinity":
			return math.Inf(1)
		case "-Infinity":
			return math.Inf(-1)
		}
	case []any:
		if len(x) == 2 {
			re, ok1 := toF64(scalarFromJSON(x[0]))
			im, ok2 := toF64(scalarFromJSON(x[1]))
			if ok1 && ok2 {
				return complex(re, im)
			}
		}
	}
	return v
}

func toF64(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int64:
		return float64(x), true
	case uint64:
		return float64(x), true
	case nil:
		return math.NaN(), true
	}
	return 0, false
}

func (s *Service) write(ctx context.Context, r *Request) (*Response, error) {
	if err := s.authorized(r); err != nil {
		return nil, err
	}
	q := r.URL.Query()
	branch := q.Get("branch")
	if branch == "" {
		branch = s.DefaultRef
	}
	if branch == "" {
		branch = "main"
	}
	path := r.URL.Path
	var message string
	session, err := s.Repo.WritableSession(ctx, branch)
	if err != nil {
		return nil, err
	}
	switch {
	case r.Method == "POST" && strings.HasPrefix(path, "/arrays/"):
		var spec arraySpecJSON
		if err := decodeJSON(r.Body, &spec); err != nil {
			return nil, err
		}
		as := zarr.ArraySpec{
			Shape: spec.Shape, ChunkShape: spec.ChunkShape, ShardShape: spec.ShardShape, DataType: spec.DataType,
			FillValue: scalarFromJSON(spec.FillValue), Codecs: spec.Codecs, Attributes: spec.Attributes,
		}
		if spec.DimensionNames != nil {
			as.DimensionNames = make([]string, len(spec.DimensionNames))
			for i, n := range spec.DimensionNames {
				if n != nil {
					as.DimensionNames[i] = *n
				}
			}
		}
		target := strings.TrimPrefix(path, "/arrays/")
		if _, err := session.CreateArray(ctx, target, as); err != nil {
			return nil, badRequest("%v", err)
		}
		message = "create array /" + strings.Trim(target, "/")
	case r.Method == "PUT" && strings.HasPrefix(path, "/array/"):
		var body struct {
			Start []uint64 `json:"start"`
			Shape []uint64 `json:"shape"`
			Data  []any    `json:"data"`
		}
		if err := decodeJSON(r.Body, &body); err != nil {
			return nil, err
		}
		target := strings.TrimPrefix(path, "/array/")
		arr, err := session.OpenArray(ctx, target)
		if err != nil {
			return nil, err
		}
		if body.Start == nil {
			body.Start = make([]uint64, len(arr.Shape()))
		}
		nd, err := ndFromJSON(arr.DataType(), body.Shape, body.Data)
		if err != nil {
			return nil, badRequest("%v", err)
		}
		if err := arr.Write(ctx, body.Start, nd); err != nil {
			return nil, badRequest("%v", err)
		}
		message = fmt.Sprintf("write /%s at %v", strings.Trim(target, "/"), body.Start)
	default:
		return nil, &httpError{405, "method not allowed"}
	}
	if m := q.Get("message"); m != "" {
		message = m
	}
	id, err := session.Commit(ctx, message, &icechunk.CommitOptions{Rebase: true})
	if err != nil {
		return nil, err
	}
	resp, err := jsonResponse(map[string]string{"snapshot": id.String(), "branch": branch})
	if resp != nil {
		resp.Status = 201
	}
	return resp, err
}

// ndFromJSON converts a flat JSON list into an NDArray of data type dt.
func ndFromJSON(dt zarr.DataType, shape []uint64, data []any) (*zarr.NDArray, error) {
	n := uint64(1)
	for _, d := range shape {
		n *= d
	}
	if uint64(len(data)) != n {
		return nil, fmt.Errorf("%d values for shape %v", len(data), shape)
	}
	num := func(v any) (json.Number, error) {
		switch x := v.(type) {
		case json.Number:
			return x, nil
		case string:
			return json.Number(x), nil
		}
		return "", fmt.Errorf("expected a number, got %v", v)
	}
	var nd *zarr.NDArray
	var err error
	switch dt.Kind {
	case zarr.KindString, zarr.KindBytes:
		vals := make([]string, len(data))
		for i, v := range data {
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("expected a string, got %v", v)
			}
			vals[i] = s
		}
		nd, err = zarr.FromStrings(shape, vals)
	case zarr.KindBool:
		vals := make([]bool, len(data))
		for i, v := range data {
			b, ok := v.(bool)
			if !ok {
				return nil, fmt.Errorf("expected a boolean, got %v", v)
			}
			vals[i] = b
		}
		nd, err = zarr.FromSlice(shape, vals)
	case zarr.KindFloat:
		vals := make([]float64, len(data))
		for i, v := range data {
			f, ok := toF64(scalarFromJSON(v))
			if !ok {
				return nil, fmt.Errorf("expected a number, got %v", v)
			}
			vals[i] = f
		}
		switch dt.Size {
		case 4:
			f32 := make([]float32, len(vals))
			for i, v := range vals {
				f32[i] = float32(v)
			}
			nd, err = zarr.FromSlice(shape, f32)
		case 8:
			nd, err = zarr.FromSlice(shape, vals)
		default:
			return nil, fmt.Errorf("writing %s from JSON is not supported", dt.Name)
		}
	case zarr.KindComplex:
		vals := make([]complex128, len(data))
		for i, v := range data {
			c, ok := scalarFromJSON(v).(complex128)
			if !ok {
				return nil, fmt.Errorf("expected [re, im], got %v", v)
			}
			vals[i] = c
		}
		if dt.Size == 8 {
			c64 := make([]complex64, len(vals))
			for i, v := range vals {
				c64[i] = complex64(v)
			}
			nd, err = zarr.FromSlice(shape, c64)
		} else {
			nd, err = zarr.FromSlice(shape, vals)
		}
	case zarr.KindInt, zarr.KindDatetime, zarr.KindTimedelta:
		vals := make([]int64, len(data))
		for i, v := range data {
			x, err := num(v)
			if err != nil {
				return nil, err
			}
			if vals[i], err = x.Int64(); err != nil {
				return nil, fmt.Errorf("invalid integer %v", v)
			}
		}
		nd, err = intsAs(shape, vals, dt.Size)
	case zarr.KindUint:
		vals := make([]uint64, len(data))
		for i, v := range data {
			x, err := num(v)
			if err != nil {
				return nil, err
			}
			if vals[i], err = strconv.ParseUint(x.String(), 10, 64); err != nil {
				return nil, fmt.Errorf("invalid unsigned integer %v", v)
			}
		}
		nd, err = uintsAs(shape, vals, dt.Size)
	default:
		return nil, fmt.Errorf("writing %s from JSON is not supported", dt.Name)
	}
	if err != nil {
		return nil, err
	}
	nd.DataType = dt // e.g. datetime64 stored as int64
	return nd, nil
}

func intsAs(shape []uint64, v []int64, size int) (*zarr.NDArray, error) {
	switch size {
	case 1:
		return zarr.FromSlice(shape, convert[int64, int8](v))
	case 2:
		return zarr.FromSlice(shape, convert[int64, int16](v))
	case 4:
		return zarr.FromSlice(shape, convert[int64, int32](v))
	}
	return zarr.FromSlice(shape, v)
}

func uintsAs(shape []uint64, v []uint64, size int) (*zarr.NDArray, error) {
	switch size {
	case 1:
		return zarr.FromSlice(shape, convert[uint64, uint8](v))
	case 2:
		return zarr.FromSlice(shape, convert[uint64, uint16](v))
	case 4:
		return zarr.FromSlice(shape, convert[uint64, uint32](v))
	}
	return zarr.FromSlice(shape, v)
}

func convert[F, T int64 | int8 | int16 | int32 | uint64 | uint8 | uint16 | uint32](in []F) []T {
	out := make([]T, len(in))
	for i, v := range in {
		out[i] = T(v)
	}
	return out
}
