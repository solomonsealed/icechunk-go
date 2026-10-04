package format

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
)

// EncodeFlexBuffer encodes a JSON-compatible value (nil, bool, integers,
// floats, string, []byte, slices, maps with string keys, json.Number) as a
// FlexBuffer. Every slot uses an 8-byte width, which keeps the encoder
// simple and is valid for any reader.
func EncodeFlexBuffer(v any) ([]byte, error) {
	e := &flexEncoder{}
	root, err := e.value(v, 0)
	if err != nil {
		return nil, err
	}
	e.align()
	e.slot(root)
	e.buf = append(e.buf, root.packedType(), 8)
	return e.buf, nil
}

const flexW = 8 // byte width of every slot

type flexEncoder struct {
	buf []byte
}

// flexItem is an encoded value: inline scalar bits, or the position of
// out-of-line data the parent will point to.
type flexItem struct {
	typ    int
	inline bool
	bits   uint64
	pos    int
}

func (it flexItem) packedType() byte { return byte(it.typ<<2) | 3 } // width code 3 = 8 bytes

func (e *flexEncoder) align() {
	for len(e.buf)%flexW != 0 {
		e.buf = append(e.buf, 0)
	}
}

func (e *flexEncoder) u64(v uint64) {
	e.buf = binary.LittleEndian.AppendUint64(e.buf, v)
}

// slot writes an element: inline bits, or a backwards offset to the data.
func (e *flexEncoder) slot(it flexItem) {
	if it.inline {
		e.u64(it.bits)
		return
	}
	e.u64(uint64(len(e.buf) - it.pos))
}

func (e *flexEncoder) value(v any, depth int) (flexItem, error) {
	if depth > maxFlexDepth {
		return flexItem{}, fmt.Errorf("flexbuffer: value nested too deeply")
	}
	switch x := v.(type) {
	case nil:
		return flexItem{typ: fbtNull, inline: true}, nil
	case bool:
		var b uint64
		if x {
			b = 1
		}
		return flexItem{typ: fbtBool, inline: true, bits: b}, nil
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return flexItem{typ: fbtInt, inline: true, bits: uint64(i)}, nil
		}
		if u, err := strconv.ParseUint(x.String(), 10, 64); err == nil {
			return flexItem{typ: fbtUint, inline: true, bits: u}, nil
		}
		f, err := x.Float64()
		if err != nil {
			return flexItem{}, fmt.Errorf("flexbuffer: invalid number %q", x)
		}
		return flexItem{typ: fbtFloat, inline: true, bits: math.Float64bits(f)}, nil
	case string:
		e.align()
		e.u64(uint64(len(x)))
		pos := len(e.buf)
		e.buf = append(append(e.buf, x...), 0)
		return flexItem{typ: fbtString, pos: pos}, nil
	case []byte:
		e.align()
		e.u64(uint64(len(x)))
		pos := len(e.buf)
		e.buf = append(e.buf, x...)
		return flexItem{typ: fbtBlob, pos: pos}, nil
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return flexItem{typ: fbtInt, inline: true, bits: uint64(rv.Int())}, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return flexItem{typ: fbtUint, inline: true, bits: rv.Uint()}, nil
	case reflect.Float32, reflect.Float64:
		return flexItem{typ: fbtFloat, inline: true, bits: math.Float64bits(rv.Float())}, nil
	case reflect.Slice, reflect.Array:
		items := make([]flexItem, rv.Len())
		for i := range items {
			it, err := e.value(rv.Index(i).Interface(), depth+1)
			if err != nil {
				return flexItem{}, err
			}
			items[i] = it
		}
		return flexItem{typ: fbtVector, pos: e.vector(items)}, nil
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return flexItem{}, fmt.Errorf("flexbuffer: map keys must be strings, not %s", rv.Type().Key())
		}
		keys := make([]string, 0, rv.Len())
		for _, k := range rv.MapKeys() {
			keys = append(keys, k.String())
		}
		sort.Strings(keys) // maps are looked up by binary search on keys
		values := make([]flexItem, len(keys))
		for i, k := range keys {
			it, err := e.value(rv.MapIndex(reflect.ValueOf(k).Convert(rv.Type().Key())).Interface(), depth+1)
			if err != nil {
				return flexItem{}, err
			}
			values[i] = it
		}
		// Keys: null-terminated strings, then a typed vector of offsets.
		keyPos := make([]int, len(keys))
		for i, k := range keys {
			keyPos[i] = len(e.buf)
			e.buf = append(append(e.buf, k...), 0)
		}
		e.align()
		e.u64(uint64(len(keys)))
		keysVec := len(e.buf)
		for _, p := range keyPos {
			e.u64(uint64(len(e.buf) - p))
		}
		// Values vector, prefixed by (keys offset, keys width, length).
		e.align()
		e.u64(uint64(len(e.buf) - keysVec))
		e.u64(flexW)
		return flexItem{typ: fbtMap, pos: e.vectorBody(values)}, nil
	}
	return flexItem{}, fmt.Errorf("flexbuffer: unsupported value of type %T", v)
}

// vector writes an untyped vector and returns the position of its data.
func (e *flexEncoder) vector(items []flexItem) int {
	e.align()
	return e.vectorBody(items)
}

// vectorBody writes length, elements and element types; the caller has
// aligned the buffer (and, for maps, written the key prefix).
func (e *flexEncoder) vectorBody(items []flexItem) int {
	e.u64(uint64(len(items)))
	pos := len(e.buf)
	for _, it := range items {
		e.slot(it)
	}
	for _, it := range items {
		e.buf = append(e.buf, it.packedType())
	}
	return pos
}
