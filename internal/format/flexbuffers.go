package format

import (
	"encoding/binary"
	"fmt"
	"math"
)

// FlexBuffers decoding into JSON-compatible Go values (nil, bool, int64,
// uint64, float64, string, []byte, []any, map[string]any).
//
// Icechunk spec v2 stores snapshot metadata values, repo metadata values and
// the repository config as FlexBuffers. The format reference is
// https://flatbuffers.dev/internals/#flexbuffers.

const (
	fbtNull           = 0
	fbtInt            = 1
	fbtUint           = 2
	fbtFloat          = 3
	fbtKey            = 4
	fbtString         = 5
	fbtIndirectInt    = 6
	fbtIndirectUint   = 7
	fbtIndirectFloat  = 8
	fbtMap            = 9
	fbtVector         = 10
	fbtVectorInt      = 11
	fbtVectorUint     = 12
	fbtVectorFloat    = 13
	fbtVectorKey      = 14
	fbtVectorStringDp = 15
	fbtVectorInt2     = 16
	fbtVectorFloat4   = 24
	fbtBlob           = 25
	fbtBool           = 26
	fbtVectorBool     = 36
)

// maxFlexDepth bounds recursion on hostile input.
const maxFlexDepth = 128

type flexReader struct {
	buf []byte
}

type flexRef struct {
	off         int // position of the value (or of the offset to it)
	parentWidth int
	byteWidth   int
	typ         int
}

// DecodeFlexBuffer decodes a complete FlexBuffer.
func DecodeFlexBuffer(buf []byte) (any, error) {
	if len(buf) < 3 {
		return nil, fmt.Errorf("%w: flexbuffer too short", ErrFormat)
	}
	r := &flexReader{buf: buf}
	width := int(buf[len(buf)-1])
	packed := buf[len(buf)-2]
	off := len(buf) - 2 - width
	if !validWidth(width) || off < 0 {
		return nil, fmt.Errorf("%w: invalid flexbuffer root", ErrFormat)
	}
	return r.value(flexRef{off: off, parentWidth: width, byteWidth: 1 << (packed & 3), typ: int(packed >> 2)}, 0)
}

func validWidth(w int) bool { return w == 1 || w == 2 || w == 4 || w == 8 }

func (r *flexReader) bounds(off, n int) error {
	if off < 0 || n < 0 || off+n > len(r.buf) {
		return fmt.Errorf("%w: flexbuffer read out of bounds", ErrFormat)
	}
	return nil
}

func (r *flexReader) uint(off, width int) (uint64, error) {
	if err := r.bounds(off, width); err != nil {
		return 0, err
	}
	b := r.buf[off:]
	switch width {
	case 1:
		return uint64(b[0]), nil
	case 2:
		return uint64(binary.LittleEndian.Uint16(b)), nil
	case 4:
		return uint64(binary.LittleEndian.Uint32(b)), nil
	case 8:
		return binary.LittleEndian.Uint64(b), nil
	}
	return 0, fmt.Errorf("%w: invalid flexbuffer width %d", ErrFormat, width)
}

func (r *flexReader) int(off, width int) (int64, error) {
	u, err := r.uint(off, width)
	if err != nil {
		return 0, err
	}
	switch width {
	case 1:
		return int64(int8(u)), nil
	case 2:
		return int64(int16(u)), nil
	case 4:
		return int64(int32(u)), nil
	}
	return int64(u), nil
}

func (r *flexReader) float(off, width int) (float64, error) {
	u, err := r.uint(off, width)
	if err != nil {
		return 0, err
	}
	switch width {
	case 4:
		return float64(math.Float32frombits(uint32(u))), nil
	case 8:
		return math.Float64frombits(u), nil
	}
	return 0, fmt.Errorf("%w: invalid flexbuffer float width %d", ErrFormat, width)
}

// indirect follows the backwards offset stored at off.
func (r *flexReader) indirect(off, width int) (int, error) {
	u, err := r.uint(off, width)
	if err != nil {
		return 0, err
	}
	target := off - int(u)
	if u > uint64(len(r.buf)) || target < 0 {
		return 0, fmt.Errorf("%w: flexbuffer offset out of bounds", ErrFormat)
	}
	return target, nil
}

func (r *flexReader) value(ref flexRef, depth int) (any, error) {
	if depth > maxFlexDepth {
		return nil, fmt.Errorf("%w: flexbuffer nested too deeply", ErrFormat)
	}
	switch ref.typ {
	case fbtNull:
		return nil, nil
	case fbtInt:
		return r.int(ref.off, ref.parentWidth)
	case fbtUint:
		return r.uint(ref.off, ref.parentWidth)
	case fbtFloat:
		return r.float(ref.off, ref.parentWidth)
	case fbtBool:
		u, err := r.uint(ref.off, ref.parentWidth)
		return u != 0, err
	}

	data, err := r.indirect(ref.off, ref.parentWidth)
	if err != nil {
		return nil, err
	}
	w := ref.byteWidth
	switch ref.typ {
	case fbtIndirectInt:
		return r.int(data, w)
	case fbtIndirectUint:
		return r.uint(data, w)
	case fbtIndirectFloat:
		return r.float(data, w)
	case fbtKey:
		return r.key(data)
	case fbtString, fbtBlob:
		n, err := r.uint(data-w, w)
		if err != nil {
			return nil, err
		}
		if err := r.bounds(data, int(n)); err != nil {
			return nil, err
		}
		if ref.typ == fbtBlob {
			return append([]byte(nil), r.buf[data:data+int(n)]...), nil
		}
		return string(r.buf[data : data+int(n)]), nil
	case fbtVector:
		n, err := r.length(data, w)
		if err != nil {
			return nil, err
		}
		types := data + n*w
		if err := r.bounds(types, n); err != nil {
			return nil, err
		}
		out := make([]any, n)
		for i := range out {
			packed := r.buf[types+i]
			v, err := r.value(flexRef{off: data + i*w, parentWidth: w, byteWidth: 1 << (packed & 3), typ: int(packed >> 2)}, depth+1)
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		return out, nil
	case fbtMap:
		n, err := r.length(data, w)
		if err != nil {
			return nil, err
		}
		keysOff, err := r.indirect(data-3*w, w)
		if err != nil {
			return nil, err
		}
		kw, err := r.uint(data-2*w, w)
		if err != nil {
			return nil, err
		}
		if !validWidth(int(kw)) {
			return nil, fmt.Errorf("%w: invalid flexbuffer map key width", ErrFormat)
		}
		types := data + n*w
		if err := r.bounds(types, n); err != nil {
			return nil, err
		}
		out := make(map[string]any, n)
		for i := 0; i < n; i++ {
			kd, err := r.indirect(keysOff+i*int(kw), int(kw))
			if err != nil {
				return nil, err
			}
			k, err := r.key(kd)
			if err != nil {
				return nil, err
			}
			packed := r.buf[types+i]
			v, err := r.value(flexRef{off: data + i*w, parentWidth: w, byteWidth: 1 << (packed & 3), typ: int(packed >> 2)}, depth+1)
			if err != nil {
				return nil, err
			}
			out[k] = v
		}
		return out, nil
	}

	// Typed vectors: element type implied by the vector type.
	var elem, n int
	switch {
	case ref.typ >= fbtVectorInt && ref.typ <= fbtVectorStringDp:
		elem = ref.typ - fbtVectorInt + fbtInt
		if n, err = r.length(data, w); err != nil {
			return nil, err
		}
	case ref.typ == fbtVectorBool:
		elem = fbtBool
		if n, err = r.length(data, w); err != nil {
			return nil, err
		}
	case ref.typ >= fbtVectorInt2 && ref.typ <= fbtVectorFloat4:
		k := ref.typ - fbtVectorInt2
		elem = k%3 + fbtInt
		n = k/3 + 2
	default:
		return nil, fmt.Errorf("%w: unknown flexbuffer type %d", ErrFormat, ref.typ)
	}
	if err := r.bounds(data, n*w); err != nil {
		return nil, err
	}
	out := make([]any, n)
	for i := range out {
		v, err := r.value(flexRef{off: data + i*w, parentWidth: w, byteWidth: 1, typ: elem}, depth+1)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

func (r *flexReader) length(data, w int) (int, error) {
	n, err := r.uint(data-w, w)
	if err != nil {
		return 0, err
	}
	if n > uint64(len(r.buf)) {
		return 0, fmt.Errorf("%w: flexbuffer length out of bounds", ErrFormat)
	}
	return int(n), nil
}

func (r *flexReader) key(off int) (string, error) {
	for i := off; i < len(r.buf); i++ {
		if r.buf[i] == 0 {
			return string(r.buf[off:i]), nil
		}
	}
	return "", fmt.Errorf("%w: unterminated flexbuffer key", ErrFormat)
}
