package zarr

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
)

// NDArray is an N-dimensional result in C (row-major) order.
type NDArray struct {
	Shape    []uint64
	DataType DataType
	// Data holds fixed-size elements, little-endian, DataType.Size bytes each.
	Data []byte
	// Strings holds the elements of variable-length (string/bytes) types.
	Strings []string
}

func newFilled(shape []uint64, dt DataType, fill []byte, fillStr string) *NDArray {
	n := numElements(shape)
	out := &NDArray{Shape: append([]uint64(nil), shape...), DataType: dt}
	if dt.Variable() {
		out.Strings = make([]string, n)
		if fillStr != "" {
			for i := range out.Strings {
				out.Strings[i] = fillStr
			}
		}
		return out
	}
	out.Data = make([]byte, n*uint64(dt.Size))
	allZero := true
	for _, b := range fill {
		if b != 0 {
			allZero = false
			break
		}
	}
	if !allZero && len(out.Data) > 0 {
		copy(out.Data, fill)
		// Doubling copy fills the buffer in O(log n) copy calls.
		for filled := len(fill); filled < len(out.Data); filled *= 2 {
			copy(out.Data[filled:], out.Data[:filled])
		}
	}
	return out
}

// Len returns the number of elements.
func (n *NDArray) Len() int { return int(numElements(n.Shape)) }

// Values returns the elements as a typed slice: []bool, []int8 … []uint64,
// []float32 (also for float16), []float64, []complex64, []complex128,
// []int64 (datetime/timedelta counts), []string (string, bytes and
// fixed-length UTF-32 types) or [][]byte (raw and fixed-length bytes types).
func (n *NDArray) Values() (any, error) {
	dt, d, cnt := n.DataType, n.Data, n.Len()
	le := binary.LittleEndian
	switch dt.Kind {
	case KindBool:
		out := make([]bool, cnt)
		for i := range out {
			out[i] = d[i] != 0
		}
		return out, nil
	case KindInt, KindDatetime, KindTimedelta:
		switch dt.Size {
		case 1:
			out := make([]int8, cnt)
			for i := range out {
				out[i] = int8(d[i])
			}
			return out, nil
		case 2:
			out := make([]int16, cnt)
			for i := range out {
				out[i] = int16(le.Uint16(d[2*i:]))
			}
			return out, nil
		case 4:
			out := make([]int32, cnt)
			for i := range out {
				out[i] = int32(le.Uint32(d[4*i:]))
			}
			return out, nil
		case 8:
			out := make([]int64, cnt)
			for i := range out {
				out[i] = int64(le.Uint64(d[8*i:]))
			}
			return out, nil
		}
	case KindUint:
		switch dt.Size {
		case 1:
			return append([]uint8(nil), d...), nil
		case 2:
			out := make([]uint16, cnt)
			for i := range out {
				out[i] = le.Uint16(d[2*i:])
			}
			return out, nil
		case 4:
			out := make([]uint32, cnt)
			for i := range out {
				out[i] = le.Uint32(d[4*i:])
			}
			return out, nil
		case 8:
			out := make([]uint64, cnt)
			for i := range out {
				out[i] = le.Uint64(d[8*i:])
			}
			return out, nil
		}
	case KindFloat:
		switch dt.Size {
		case 2:
			out := make([]float32, cnt)
			for i := range out {
				out[i] = halfToFloat32(le.Uint16(d[2*i:]))
			}
			return out, nil
		case 4:
			out := make([]float32, cnt)
			for i := range out {
				out[i] = math.Float32frombits(le.Uint32(d[4*i:]))
			}
			return out, nil
		case 8:
			out := make([]float64, cnt)
			for i := range out {
				out[i] = math.Float64frombits(le.Uint64(d[8*i:]))
			}
			return out, nil
		}
	case KindComplex:
		if dt.Size == 8 {
			out := make([]complex64, cnt)
			for i := range out {
				out[i] = complex(math.Float32frombits(le.Uint32(d[8*i:])), math.Float32frombits(le.Uint32(d[8*i+4:])))
			}
			return out, nil
		}
		out := make([]complex128, cnt)
		for i := range out {
			out[i] = complex(math.Float64frombits(le.Uint64(d[16*i:])), math.Float64frombits(le.Uint64(d[16*i+8:])))
		}
		return out, nil
	case KindString, KindBytes:
		return append([]string(nil), n.Strings...), nil
	case KindFixedString:
		out := make([]string, cnt)
		for i := range out {
			var sb strings.Builder
			for j := 0; j+4 <= dt.Size; j += 4 {
				r := le.Uint32(d[i*dt.Size+j:])
				if r == 0 {
					break
				}
				sb.WriteRune(rune(r))
			}
			out[i] = sb.String()
		}
		return out, nil
	case KindRaw, KindFixedBytes:
		out := make([][]byte, cnt)
		for i := range out {
			b := d[i*dt.Size : (i+1)*dt.Size]
			if dt.Name == "null_terminated_bytes" {
				if k := strings.IndexByte(string(b), 0); k >= 0 {
					b = b[:k]
				}
			}
			out[i] = append([]byte(nil), b...)
		}
		return out, nil
	}
	return nil, fmt.Errorf("%w: values of %s", ErrUnsupported, dt.Name)
}

// Float64s returns the elements of a real numeric or bool array converted to
// float64.
func (n *NDArray) Float64s() ([]float64, error) {
	cnt := n.Len()
	out := make([]float64, cnt)
	for i := range out {
		v, err := n.float64At(i)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

func (n *NDArray) float64At(i int) (float64, error) {
	dt, d := n.DataType, n.Data
	le := binary.LittleEndian
	o := i * dt.Size
	switch dt.Kind {
	case KindBool:
		if d[o] != 0 {
			return 1, nil
		}
		return 0, nil
	case KindInt, KindDatetime, KindTimedelta:
		switch dt.Size {
		case 1:
			return float64(int8(d[o])), nil
		case 2:
			return float64(int16(le.Uint16(d[o:]))), nil
		case 4:
			return float64(int32(le.Uint32(d[o:]))), nil
		case 8:
			return float64(int64(le.Uint64(d[o:]))), nil
		}
	case KindUint:
		switch dt.Size {
		case 1:
			return float64(d[o]), nil
		case 2:
			return float64(le.Uint16(d[o:])), nil
		case 4:
			return float64(le.Uint32(d[o:])), nil
		case 8:
			return float64(le.Uint64(d[o:])), nil
		}
	case KindFloat:
		switch dt.Size {
		case 2:
			return float64(halfToFloat32(le.Uint16(d[o:]))), nil
		case 4:
			return float64(math.Float32frombits(le.Uint32(d[o:]))), nil
		case 8:
			return math.Float64frombits(le.Uint64(d[o:])), nil
		}
	}
	return 0, fmt.Errorf("%w: cannot convert %s to float64", ErrUnsupported, dt.Name)
}

// Number is the set of Go numeric types Convert can produce.
type Number interface {
	~int8 | ~int16 | ~int32 | ~int64 | ~int | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uint | ~float32 | ~float64
}

// Convert returns the elements of a real numeric or bool array converted to
// T with Go conversion semantics. Integer sources convert exactly (no
// round-trip through float64); float to integer conversion of NaN or
// out-of-range values is implementation-defined, as in Go.
func Convert[T Number](n *NDArray) ([]T, error) {
	dt, d := n.DataType, n.Data
	le := binary.LittleEndian
	out := make([]T, n.Len())
	switch {
	case dt.Kind == KindInt || dt.Kind == KindDatetime || dt.Kind == KindTimedelta:
		for i := range out {
			o := i * dt.Size
			var v int64
			switch dt.Size {
			case 1:
				v = int64(int8(d[o]))
			case 2:
				v = int64(int16(le.Uint16(d[o:])))
			case 4:
				v = int64(int32(le.Uint32(d[o:])))
			default:
				v = int64(le.Uint64(d[o:]))
			}
			out[i] = T(v)
		}
	case dt.Kind == KindUint:
		for i := range out {
			o := i * dt.Size
			var v uint64
			switch dt.Size {
			case 1:
				v = uint64(d[o])
			case 2:
				v = uint64(le.Uint16(d[o:]))
			case 4:
				v = uint64(le.Uint32(d[o:]))
			default:
				v = le.Uint64(d[o:])
			}
			out[i] = T(v)
		}
	default:
		for i := range out {
			v, err := n.float64At(i)
			if err != nil {
				return nil, err
			}
			out[i] = T(v)
		}
	}
	return out, nil
}
