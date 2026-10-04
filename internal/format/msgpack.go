package format

import (
	"encoding/binary"
	"fmt"
	"math"
)

// DecodeMsgpack decodes one MessagePack value into JSON-compatible Go values
// (nil, bool, int64, uint64, float64, string, []byte, []any, map[string]any).
// Icechunk spec v1 stores snapshot metadata values as MessagePack.
func DecodeMsgpack(buf []byte) (any, error) {
	d := msgpackDecoder{buf: buf}
	v, err := d.value(0)
	if err != nil {
		return nil, err
	}
	if d.pos != len(buf) {
		return nil, fmt.Errorf("%w: trailing bytes after msgpack value", ErrFormat)
	}
	return v, nil
}

type msgpackDecoder struct {
	buf []byte
	pos int
}

func (d *msgpackDecoder) take(n int) ([]byte, error) {
	if n < 0 || d.pos+n > len(d.buf) {
		return nil, fmt.Errorf("%w: truncated msgpack value", ErrFormat)
	}
	b := d.buf[d.pos : d.pos+n]
	d.pos += n
	return b, nil
}

func (d *msgpackDecoder) uintN(n int) (uint64, error) {
	b, err := d.take(n)
	if err != nil {
		return 0, err
	}
	switch n {
	case 1:
		return uint64(b[0]), nil
	case 2:
		return uint64(binary.BigEndian.Uint16(b)), nil
	case 4:
		return uint64(binary.BigEndian.Uint32(b)), nil
	}
	return binary.BigEndian.Uint64(b), nil
}

func (d *msgpackDecoder) value(depth int) (any, error) {
	if depth > maxFlexDepth {
		return nil, fmt.Errorf("%w: msgpack nested too deeply", ErrFormat)
	}
	b, err := d.take(1)
	if err != nil {
		return nil, err
	}
	c := b[0]
	switch {
	case c <= 0x7f:
		return int64(c), nil
	case c >= 0xe0:
		return int64(int8(c)), nil
	case c&0xf0 == 0x80:
		return d.mapN(int(c&0x0f), depth)
	case c&0xf0 == 0x90:
		return d.arrayN(int(c&0x0f), depth)
	case c&0xe0 == 0xa0:
		return d.str(int(c & 0x1f))
	}
	switch c {
	case 0xc0:
		return nil, nil
	case 0xc2:
		return false, nil
	case 0xc3:
		return true, nil
	case 0xc4, 0xc5, 0xc6:
		n, err := d.uintN(1 << (c - 0xc4))
		if err != nil {
			return nil, err
		}
		raw, err := d.take(int(n))
		return append([]byte(nil), raw...), err
	case 0xca:
		u, err := d.uintN(4)
		return float64(math.Float32frombits(uint32(u))), err
	case 0xcb:
		u, err := d.uintN(8)
		return math.Float64frombits(u), err
	case 0xcc, 0xcd, 0xce, 0xcf:
		return d.uintN(1 << (c - 0xcc))
	case 0xd0:
		u, err := d.uintN(1)
		return int64(int8(u)), err
	case 0xd1:
		u, err := d.uintN(2)
		return int64(int16(u)), err
	case 0xd2:
		u, err := d.uintN(4)
		return int64(int32(u)), err
	case 0xd3:
		u, err := d.uintN(8)
		return int64(u), err
	case 0xd9, 0xda, 0xdb:
		n, err := d.uintN(1 << (c - 0xd9))
		if err != nil {
			return nil, err
		}
		return d.str(int(n))
	case 0xdc, 0xdd:
		n, err := d.uintN(2 << (c - 0xdc))
		if err != nil {
			return nil, err
		}
		return d.arrayN(int(n), depth)
	case 0xde, 0xdf:
		n, err := d.uintN(2 << (c - 0xde))
		if err != nil {
			return nil, err
		}
		return d.mapN(int(n), depth)
	}
	return nil, fmt.Errorf("%w: unsupported msgpack type 0x%02x", ErrFormat, c)
}

func (d *msgpackDecoder) str(n int) (any, error) {
	b, err := d.take(n)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

func (d *msgpackDecoder) arrayN(n int, depth int) (any, error) {
	if n > len(d.buf)-d.pos {
		return nil, fmt.Errorf("%w: truncated msgpack array", ErrFormat)
	}
	out := make([]any, n)
	for i := range out {
		v, err := d.value(depth + 1)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

func (d *msgpackDecoder) mapN(n int, depth int) (any, error) {
	if n > len(d.buf)-d.pos {
		return nil, fmt.Errorf("%w: truncated msgpack map", ErrFormat)
	}
	out := make(map[string]any, n)
	for i := 0; i < n; i++ {
		k, err := d.value(depth + 1)
		if err != nil {
			return nil, err
		}
		ks, ok := k.(string)
		if !ok {
			ks = fmt.Sprint(k)
		}
		v, err := d.value(depth + 1)
		if err != nil {
			return nil, err
		}
		out[ks] = v
	}
	return out, nil
}
