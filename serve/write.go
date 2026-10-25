package serve

import (
	"bytes"
	"cmp"
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
	DimensionNames []string         `json:"dimension_names"` // null names decode as ""
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
	branch := cmp.Or(q.Get("branch"), s.DefaultRef, "main")
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
			FillValue: scalarFromJSON(spec.FillValue), Codecs: spec.Codecs, DimensionNames: spec.DimensionNames, Attributes: spec.Attributes,
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
	id, err := session.Commit(ctx, cmp.Or(q.Get("message"), message), &icechunk.CommitOptions{Rebase: true})
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
	switch {
	case dt.Variable():
		nd, err = fromEach(shape, dt, data, "a string", func(v any) (string, bool) { s, ok := v.(string); return s, ok })
	case dt.Kind == zarr.KindBool:
		nd, err = fromEach(shape, dt, data, "a boolean", func(v any) (bool, bool) { b, ok := v.(bool); return b, ok })
	// Floats and complex numbers are written as float32 or float64 parts.
	case dt.Kind == zarr.KindFloat && (dt.Size == 4 || dt.Size == 8):
		nd, err = fromEach(shape, dt, data, "a number", func(v any) (float64, bool) { return toF64(scalarFromJSON(v)) })
	case dt.Kind == zarr.KindComplex && (dt.Size == 8 || dt.Size == 16):
		nd, err = fromEach(shape, dt, data, "[re, im]", func(v any) (complex128, bool) { c, ok := scalarFromJSON(v).(complex128); return c, ok })
	case dt.Kind == zarr.KindInt, dt.Kind == zarr.KindDatetime, dt.Kind == zarr.KindTimedelta:
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
	case dt.Kind == zarr.KindUint:
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

// fromEach builds an NDArray of type dt from data, converting each value
// with conv, which reports whether the value is what the type needs.
func fromEach[T any](shape []uint64, dt zarr.DataType, data []any, what string, conv func(any) (T, bool)) (*zarr.NDArray, error) {
	vals := make([]T, len(data))
	for i, v := range data {
		x, ok := conv(v)
		if !ok {
			return nil, fmt.Errorf("expected %s, got %v", what, v)
		}
		vals[i] = x
	}
	return zarr.FromValues(shape, dt, vals)
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
