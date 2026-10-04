package zarr

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
)

// ChunkWriter is a ChunkSource that also stores chunks. Array.Write needs
// one; icechunk's writable sessions provide it.
type ChunkWriter interface {
	ChunkSource
	// SetChunk stores the encoded bytes of a chunk.
	SetChunk(ctx context.Context, coords []uint32, data []byte) error
	// DeleteChunk removes a chunk, so readers see the fill value.
	DeleteChunk(ctx context.Context, coords []uint32) error
}

// ---------------------------------------------------------------------------
// Encoding through the pipeline

func (p *pipeline) encode(ctx context.Context, c *chunkData) ([]byte, error) {
	var err error
	for _, aa := range p.aa {
		if c, err = aa.encodeArr(c); err != nil {
			return nil, err
		}
	}
	out, err := p.ab.encode(ctx, c)
	if err != nil {
		return nil, err
	}
	for _, bb := range p.bb {
		enc, ok := bb.(BytesEncoder)
		if !ok {
			return nil, fmt.Errorf("%w: a codec of this array cannot encode", ErrUnsupported)
		}
		if out, err = enc.Encode(out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (identityCodec) encodeArr(c *chunkData) (*chunkData, error) { return c, nil }

// bitRound is numcodecs.bitround: lossy rounding of float mantissas to
// keepbits bits (decoding is the identity).
type bitRound struct {
	identityCodec
	keepbits int
}

func (b bitRound) encodeArr(c *chunkData) (*chunkData, error) {
	var bits, size int
	switch {
	case c.dtype.Kind == KindFloat && c.dtype.Size == 2:
		bits, size = 10, 2
	case c.dtype.Kind == KindFloat && c.dtype.Size == 4:
		bits, size = 23, 4
	case c.dtype.Kind == KindFloat && c.dtype.Size == 8:
		bits, size = 52, 8
	default:
		return nil, fmt.Errorf("zarr: bitround needs float data, not %s", c.dtype.Name)
	}
	if b.keepbits > bits {
		return nil, fmt.Errorf("zarr: bitround keepbits %d too large for %s", b.keepbits, c.dtype.Name)
	}
	if b.keepbits == bits {
		return c, nil
	}
	maskbits := uint(bits - b.keepbits)
	mask := ^uint64(0) << maskbits
	half := uint64(1)<<(maskbits-1) - 1
	out := &chunkData{shape: c.shape, dtype: c.dtype, data: make([]byte, len(c.data))}
	for i := 0; i+size <= len(c.data); i += size {
		v := getUint(c.data[i:i+size], size)
		v += (v>>maskbits)&1 + half
		putUint(out.data[i:i+size], v&mask)
	}
	return out, nil
}

func getUint(b []byte, size int) uint64 {
	var v uint64
	for i := size - 1; i >= 0; i-- {
		v = v<<8 | uint64(b[i])
	}
	return v
}

func (t *transpose) encodeArr(c *chunkData) (*chunkData, error) {
	n := len(c.shape)
	p, err := t.perm(n)
	if err != nil {
		return nil, err
	}
	encShape, _, err := t.encoded(c.shape, c.dtype)
	if err != nil {
		return nil, err
	}
	stride := stridesOf(c.shape)
	encStride := make([]uint64, n)
	for i := range p {
		encStride[i] = stride[p[i]]
	}
	total := numElements(c.shape)
	out := &chunkData{shape: encShape, dtype: c.dtype}
	if c.dtype.Variable() {
		out.strs = make([]string, total)
	} else {
		out.data = make([]byte, len(c.data))
	}
	idx := make([]uint64, n)
	var src uint64
	es := uint64(c.dtype.Size)
	for dst := uint64(0); dst < total; dst++ {
		if c.dtype.Variable() {
			out.strs[dst] = c.strs[src]
		} else {
			copy(out.data[dst*es:(dst+1)*es], c.data[src*es:(src+1)*es])
		}
		for d := n - 1; d >= 0; d-- {
			idx[d]++
			src += encStride[d]
			if idx[d] < encShape[d] {
				break
			}
			src -= encStride[d] * idx[d]
			idx[d] = 0
		}
	}
	return out, nil
}

func (b *bytesCodec) encode(_ context.Context, c *chunkData) ([]byte, error) {
	if !b.big {
		return c.data, nil
	}
	sw := c.dtype.swapSize()
	if sw <= 1 {
		return c.data, nil
	}
	out := make([]byte, len(c.data))
	for i := 0; i+sw <= len(c.data); i += sw {
		for j := 0; j < sw; j++ {
			out[i+j] = c.data[i+sw-1-j]
		}
	}
	return out, nil
}

func (vlenCodec) encode(_ context.Context, c *chunkData) ([]byte, error) {
	size := 4
	for _, s := range c.strs {
		size += 4 + len(s)
	}
	out := make([]byte, 0, size)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(c.strs)))
	for _, s := range c.strs {
		out = binary.LittleEndian.AppendUint32(out, uint32(len(s)))
		out = append(out, s...)
	}
	return out, nil
}

// encode writes a complete shard: inner chunks that hold only the fill
// value are omitted (marked missing in the index).
func (s *shardingCodec) encode(ctx context.Context, c *chunkData) ([]byte, error) {
	grid, err := s.gridShape(c.shape)
	if err != nil {
		return nil, err
	}
	n := numElements(grid)
	index := make([]byte, 16*n)
	var body []byte
	idxSize := int(s.indexSize(grid))
	base := 0
	if s.indexAtStart {
		base = idxSize
	}
	var firstErr error
	forEachIndex(grid, func(i uint64, coords []uint64) bool {
		inner := newFilled(s.inner, c.dtype, s.fill, s.fillStr)
		srcOff := make([]uint64, len(coords))
		for d := range coords {
			srcOff[d] = coords[d] * s.inner[d]
		}
		copyChunk(inner, make([]uint64, len(coords)), c, srcOff, s.inner)
		if isFill(inner, s.fill, s.fillStr) {
			binary.LittleEndian.PutUint64(index[16*i:], math.MaxUint64)
			binary.LittleEndian.PutUint64(index[16*i+8:], math.MaxUint64)
			return true
		}
		enc, err := s.innerPipe.encode(ctx, &chunkData{shape: inner.Shape, dtype: c.dtype, data: inner.Data, strs: inner.Strings})
		if err != nil {
			firstErr = err
			return false
		}
		binary.LittleEndian.PutUint64(index[16*i:], uint64(base+len(body)))
		binary.LittleEndian.PutUint64(index[16*i+8:], uint64(len(enc)))
		body = append(body, enc...)
		return true
	})
	if firstErr != nil {
		return nil, firstErr
	}
	shape := append(append([]uint64{}, grid...), 2)
	encIndex, err := s.indexPipe.encode(ctx, &chunkData{shape: shape, dtype: DataType{Name: "uint64", Kind: KindUint, Size: 8}, data: index})
	if err != nil {
		return nil, err
	}
	if len(encIndex) != idxSize {
		return nil, fmt.Errorf("zarr: shard index encoded to %d bytes, expected %d", len(encIndex), idxSize)
	}
	if s.indexAtStart {
		return append(encIndex, body...), nil
	}
	return append(body, encIndex...), nil
}

// isFill reports whether every element equals the fill value.
func isFill(n *NDArray, fill []byte, fillStr string) bool {
	if n.Strings != nil {
		for _, s := range n.Strings {
			if s != fillStr {
				return false
			}
		}
		return true
	}
	es := len(fill)
	if es == 0 {
		return len(n.Data) == 0
	}
	// Any NaN matches a NaN fill value, as in zarr-python; otherwise
	// compare bytes (so -0.0 is kept even with a 0.0 fill value).
	part := 0
	switch n.DataType.Kind {
	case KindFloat:
		part = es
	case KindComplex:
		part = es / 2
	}
	for i := 0; i+es <= len(n.Data); i += es {
		if bytes.Equal(n.Data[i:i+es], fill) {
			continue
		}
		if part == 0 {
			return false
		}
		for p := 0; p < es; p += part {
			a, f := n.Data[i+p:i+p+part], fill[p:p+part]
			if !bytes.Equal(a, f) && !(isNaNBits(a) && isNaNBits(f)) {
				return false
			}
		}
	}
	return true
}

func isNaNBits(b []byte) bool {
	v := getUint(b, len(b))
	switch len(b) {
	case 2:
		return v&0x7c00 == 0x7c00 && v&0x3ff != 0
	case 4:
		return math.IsNaN(float64(math.Float32frombits(uint32(v))))
	case 8:
		return math.IsNaN(math.Float64frombits(v))
	}
	return false
}

// ---------------------------------------------------------------------------
// Writing arrays

// Write stores data at the region starting at start (data.Shape is the
// region's extent). Chunks only partly covered are read, updated and
// rewritten; chunks that end up holding only the fill value are deleted, as
// zarr-python does by default. The array's source must be a ChunkWriter.
// Concurrent writes to the same chunk are not coordinated.
func (a *Array) Write(ctx context.Context, start []uint64, data *NDArray) error {
	w, ok := a.src.(ChunkWriter)
	if !ok {
		return fmt.Errorf("zarr: array is read-only")
	}
	nd := len(a.meta.Shape)
	if len(start) != nd || len(data.Shape) != nd {
		return fmt.Errorf("zarr: write of shape %v at %v does not match %d dimensions", data.Shape, start, nd)
	}
	dt := a.meta.DataType
	if data.DataType.Name != dt.Name || data.DataType.Size != dt.Size {
		return fmt.Errorf("zarr: writing %s data to a %s array", data.DataType.Name, dt.Name)
	}
	if want := numElements(data.Shape); (dt.Variable() && uint64(len(data.Strings)) != want) ||
		(!dt.Variable() && uint64(len(data.Data)) != want*uint64(dt.Size)) {
		return fmt.Errorf("zarr: data has the wrong number of elements for shape %v", data.Shape)
	}
	for d := range start {
		if start[d]+data.Shape[d] > a.meta.Shape[d] {
			return fmt.Errorf("zarr: write [%d, %d) out of bounds for dimension %d of length %d", start[d], start[d]+data.Shape[d], d, a.meta.Shape[d])
		}
	}
	if numElements(data.Shape) == 0 {
		return nil
	}
	p := newPool(ctx, a.concurrency)
	forEachChunk(a.meta.grid, start, data.Shape, func(coords []uint64) {
		coords = append([]uint64(nil), coords...)
		p.do(func() error { return a.writeChunk(p.ctx, w, start, data, coords) })
	})
	return p.wait()
}

func (a *Array) writeChunk(ctx context.Context, w ChunkWriter, start []uint64, data *NDArray, coords []uint64) error {
	nd := len(coords)
	cStart, cShape := make([]uint64, nd), make([]uint64, nd)
	whole := true
	for d, c := range coords {
		cStart[d], cShape[d] = a.meta.grid.span(d, c)
		// Covered if the write spans the chunk's in-bounds part.
		end := min(cStart[d]+cShape[d], a.meta.Shape[d])
		if start[d] > cStart[d] || start[d]+data.Shape[d] < end {
			whole = false
		}
	}
	c32 := toU32(coords)
	var chunk *NDArray
	if whole {
		chunk = newFilled(cShape, a.meta.DataType, a.meta.fill, a.meta.fillStr)
	} else {
		var err error
		if chunk, err = a.ReadChunk(ctx, coords); err != nil {
			return err
		}
	}
	// Copy the overlap of data (at start) into the chunk (at cStart).
	dstOff, srcOff, box := make([]uint64, nd), make([]uint64, nd), make([]uint64, nd)
	for d := 0; d < nd; d++ {
		lo := max(start[d], cStart[d])
		hi := min(start[d]+data.Shape[d], cStart[d]+cShape[d])
		dstOff[d], srcOff[d], box[d] = lo-cStart[d], lo-start[d], hi-lo
	}
	copyChunk(chunk, dstOff, &chunkData{shape: data.Shape, dtype: data.DataType, data: data.Data, strs: data.Strings}, srcOff, box)
	if isFill(chunk, a.meta.fill, a.meta.fillStr) {
		if _, exists, err := w.ChunkSize(ctx, c32); err != nil || !exists {
			return err
		}
		return w.DeleteChunk(ctx, c32)
	}
	enc, err := a.pipe.encode(ctx, &chunkData{shape: chunk.Shape, dtype: chunk.DataType, data: chunk.Data, strs: chunk.Strings})
	if err != nil {
		return fmt.Errorf("zarr: encoding chunk %v: %w", coords, err)
	}
	return w.SetChunk(ctx, c32, enc)
}

// ---------------------------------------------------------------------------
// Building NDArrays

// Scalar is the set of Go element types FromSlice accepts.
type Scalar interface {
	~bool | ~int8 | ~int16 | ~int32 | ~int64 | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | ~complex64 | ~complex128
}

// FromSlice wraps Go values (C order) as an NDArray of the matching Zarr
// data type: bool, int8 … uint64, float32, float64, complex64, complex128.
func FromSlice[T Scalar](shape []uint64, values []T) (*NDArray, error) {
	if uint64(len(values)) != numElements(shape) {
		return nil, fmt.Errorf("zarr: %d values for shape %v", len(values), shape)
	}
	var zero T
	var name string
	switch any(zero).(type) {
	case bool:
		name = "bool"
	case int8:
		name = "int8"
	case int16:
		name = "int16"
	case int32:
		name = "int32"
	case int64:
		name = "int64"
	case uint8:
		name = "uint8"
	case uint16:
		name = "uint16"
	case uint32:
		name = "uint32"
	case uint64:
		name = "uint64"
	case float32:
		name = "float32"
	case float64:
		name = "float64"
	case complex64:
		name = "complex64"
	case complex128:
		name = "complex128"
	default:
		return nil, fmt.Errorf("zarr: unsupported element type %T (use the base type, not a named type)", zero)
	}
	dt, err := ParseDataType(name)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, len(values)*dt.Size)
	le := binary.LittleEndian
	for _, v := range values {
		switch x := any(v).(type) {
		case bool:
			b := byte(0)
			if x {
				b = 1
			}
			buf = append(buf, b)
		case int8:
			buf = append(buf, byte(x))
		case int16:
			buf = le.AppendUint16(buf, uint16(x))
		case int32:
			buf = le.AppendUint32(buf, uint32(x))
		case int64:
			buf = le.AppendUint64(buf, uint64(x))
		case uint8:
			buf = append(buf, x)
		case uint16:
			buf = le.AppendUint16(buf, x)
		case uint32:
			buf = le.AppendUint32(buf, x)
		case uint64:
			buf = le.AppendUint64(buf, x)
		case float32:
			buf = le.AppendUint32(buf, math.Float32bits(x))
		case float64:
			buf = le.AppendUint64(buf, math.Float64bits(x))
		case complex64:
			buf = le.AppendUint32(buf, math.Float32bits(real(x)))
			buf = le.AppendUint32(buf, math.Float32bits(imag(x)))
		case complex128:
			buf = le.AppendUint64(buf, math.Float64bits(real(x)))
			buf = le.AppendUint64(buf, math.Float64bits(imag(x)))
		}
	}
	return &NDArray{Shape: append([]uint64(nil), shape...), DataType: dt, Data: buf}, nil
}

// FromStrings wraps strings (C order) as an NDArray of the "string" type.
func FromStrings(shape []uint64, values []string) (*NDArray, error) {
	if uint64(len(values)) != numElements(shape) {
		return nil, fmt.Errorf("zarr: %d values for shape %v", len(values), shape)
	}
	return &NDArray{Shape: append([]uint64(nil), shape...), DataType: DataType{Name: "string", Kind: KindString},
		Strings: append([]string(nil), values...)}, nil
}

// ParseDataType parses a Zarr v3 data type name such as "float32".
func ParseDataType(name string) (DataType, error) {
	raw, _ := json.Marshal(name)
	return parseDataType(raw)
}
