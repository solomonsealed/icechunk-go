package zarr

import (
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// BytesCodec is a bytes-to-bytes codec (compressor, checksum, ...). Only
// decoding is needed for reading.
type BytesCodec interface {
	Decode(in []byte) ([]byte, error)
}

// BytesCodecFunc adapts a function to BytesCodec.
type BytesCodecFunc func(in []byte) ([]byte, error)

// Decode implements BytesCodec.
func (f BytesCodecFunc) Decode(in []byte) ([]byte, error) { return f(in) }

// BytesCodecFactory builds a codec from its JSON configuration. elemSize is
// the size in bytes of the array's elements at that pipeline stage.
type BytesCodecFactory func(config json.RawMessage, elemSize int) (BytesCodec, error)

var (
	registryMu  sync.RWMutex
	bytesCodecs = map[string]BytesCodecFactory{}
)

// RegisterBytesCodec makes a bytes-to-bytes codec available by name,
// replacing any existing codec with that name.
func RegisterBytesCodec(name string, f BytesCodecFactory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	bytesCodecs[name] = f
}

func lookupBytesCodec(name string) (BytesCodecFactory, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	f, ok := bytesCodecs[name]
	return f, ok
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// fletcher32 is numcodecs' Fletcher-32, HDF5's variant: big-endian 16-bit
// words, a trailing odd byte as the high byte of a final word. numcodecs
// stores the result as a little-endian uint32 after the data.
func fletcher32(data []byte) uint32 {
	sum1, sum2 := uint32(0xffff), uint32(0xffff)
	words := len(data) / 2
	for i := 0; i < words; {
		n := min(360, words-i)
		for ; n > 0; n-- {
			sum1 += uint32(binary.BigEndian.Uint16(data[2*i:]))
			sum2 += sum1
			i++
		}
		sum1 = (sum1 & 0xffff) + (sum1 >> 16)
		sum2 = (sum2 & 0xffff) + (sum2 >> 16)
	}
	if len(data)%2 == 1 {
		sum1 += uint32(data[len(data)-1]) << 8
		sum2 += sum1
		sum1 = (sum1 & 0xffff) + (sum1 >> 16)
		sum2 = (sum2 & 0xffff) + (sum2 >> 16)
	}
	sum1 = (sum1 & 0xffff) + (sum1 >> 16)
	sum2 = (sum2 & 0xffff) + (sum2 >> 16)
	return sum2<<16 | sum1
}

func stripChecksum(in []byte, location, kind string, sum func([]byte) uint32) ([]byte, error) {
	if len(in) < 4 {
		return nil, fmt.Errorf("zarr: %s buffer too short", kind)
	}
	var data, stored []byte
	if location == "end" {
		data, stored = in[:len(in)-4], in[len(in)-4:]
	} else {
		data, stored = in[4:], in[:4]
	}
	if got, want := sum(data), binary.LittleEndian.Uint32(stored); got != want {
		return nil, fmt.Errorf("zarr: %s checksum mismatch (stored %08x, computed %08x)", kind, want, got)
	}
	return data, nil
}

func gunzip(in []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		return nil, fmt.Errorf("zarr: gzip: %w", err)
	}
	defer r.Close()
	return io.ReadAll(r)
}

func unzlib(in []byte) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(in))
	if err != nil {
		return nil, fmt.Errorf("zarr: zlib: %w", err)
	}
	defer r.Close()
	return io.ReadAll(r)
}

func unbzip2(in []byte) ([]byte, error) {
	return io.ReadAll(bzip2.NewReader(bytes.NewReader(in)))
}

var (
	zstdOnce sync.Once
	zstdDec  *zstd.Decoder
	zstdErr  error
)

func unzstd(in []byte) ([]byte, error) {
	zstdOnce.Do(func() {
		zstdDec, zstdErr = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	})
	if zstdErr != nil {
		return nil, zstdErr
	}
	out, err := zstdDec.DecodeAll(in, nil)
	if err != nil {
		return nil, fmt.Errorf("zarr: zstd: %w", err)
	}
	return out, nil
}

// unlz4Numcodecs decodes numcodecs' LZ4 framing: a little-endian uint32
// uncompressed size followed by one LZ4 block.
func unlz4Numcodecs(in []byte) ([]byte, error) {
	if len(in) < 4 {
		return nil, fmt.Errorf("zarr: lz4 buffer too short")
	}
	n := binary.LittleEndian.Uint32(in)
	out := make([]byte, n)
	if err := lz4Block(in[4:], out); err != nil {
		return nil, err
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Decoded chunk data and the pipeline

// chunkData holds a chunk at some pipeline stage: fixed-size elements in
// C order (little-endian), or strings for variable-length types.
type chunkData struct {
	shape []uint64
	dtype DataType
	data  []byte
	strs  []string
}

func numElements(shape []uint64) uint64 {
	n := uint64(1)
	for _, s := range shape {
		n *= s
	}
	return n
}

// arrayCodec is an array-to-array codec.
type arrayCodec interface {
	// encoded returns the shape and type produced when encoding an input of
	// the given shape and type.
	encoded(shape []uint64, dt DataType) ([]uint64, DataType, error)
	// decode reverses the codec, producing data of shape/dt.
	decode(c *chunkData, shape []uint64, dt DataType) (*chunkData, error)
	// encodeArr applies the codec.
	encodeArr(c *chunkData) (*chunkData, error)
}

// arrayBytesCodec is the array-to-bytes codec (exactly one per pipeline).
type arrayBytesCodec interface {
	decode(ctx context.Context, in []byte, shape []uint64, dt DataType) (*chunkData, error)
	encode(ctx context.Context, c *chunkData) ([]byte, error)
}

type pipeline struct {
	aa []arrayCodec
	ab arrayBytesCodec
	bb []BytesCodec
}

// buildPipeline classifies codecs and validates their order. fill and
// fillStr are needed by sharding to fill missing inner chunks.
func buildPipeline(specs []CodecSpec, dt DataType, fill []byte, fillStr string) (*pipeline, error) {
	p := &pipeline{}
	cur := dt
	stage := 0 // 0: array->array, 1: bytes->bytes
	for _, s := range specs {
		if aa, ok, err := newArrayCodec(s); err != nil {
			return nil, err
		} else if ok {
			if stage != 0 || p.ab != nil {
				return nil, fmt.Errorf("zarr: array-to-array codec %q after the array-to-bytes codec", s.Name)
			}
			p.aa = append(p.aa, aa)
			if _, cur, err = aa.encoded(nil, cur); err != nil {
				return nil, err
			}
			continue
		}
		if ab, ok, err := newArrayBytesCodec(s, cur, fill, fillStr); err != nil {
			return nil, err
		} else if ok {
			if p.ab != nil {
				return nil, fmt.Errorf("zarr: more than one array-to-bytes codec (%q)", s.Name)
			}
			p.ab, stage = ab, 1
			continue
		}
		f, ok := lookupBytesCodec(s.Name)
		if !ok {
			return nil, fmt.Errorf("%w: codec %q", ErrUnsupported, s.Name)
		}
		if p.ab == nil {
			return nil, fmt.Errorf("zarr: bytes-to-bytes codec %q before the array-to-bytes codec", s.Name)
		}
		c, err := f(s.Configuration, cur.Size)
		if err != nil {
			return nil, fmt.Errorf("zarr: codec %q: %w", s.Name, err)
		}
		p.bb = append(p.bb, c)
	}
	if p.ab == nil {
		// The bytes codec is implied when missing (e.g. single-byte types).
		ab, _, err := newArrayBytesCodec(CodecSpec{Name: "bytes"}, cur, fill, fillStr)
		if err != nil {
			return nil, err
		}
		p.ab = ab
	}
	return p, nil
}

func (p *pipeline) decode(ctx context.Context, in []byte, shape []uint64, dt DataType) (*chunkData, error) {
	var err error
	for i := len(p.bb) - 1; i >= 0; i-- {
		if in, err = p.bb[i].Decode(in); err != nil {
			return nil, err
		}
	}
	shapes := make([][]uint64, len(p.aa)+1)
	types := make([]DataType, len(p.aa)+1)
	shapes[0], types[0] = shape, dt
	for i, c := range p.aa {
		if shapes[i+1], types[i+1], err = c.encoded(shapes[i], types[i]); err != nil {
			return nil, err
		}
	}
	c, err := p.ab.decode(ctx, in, shapes[len(p.aa)], types[len(p.aa)])
	if err != nil {
		return nil, err
	}
	for i := len(p.aa) - 1; i >= 0; i-- {
		if c, err = p.aa[i].decode(c, shapes[i], types[i]); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// ---------------------------------------------------------------------------
// Array-to-array codecs

func newArrayCodec(s CodecSpec) (arrayCodec, bool, error) {
	switch s.Name {
	case "transpose":
		var cfg struct {
			Order json.RawMessage `json:"order"`
		}
		if err := json.Unmarshal(s.Configuration, &cfg); err != nil {
			return nil, true, fmt.Errorf("zarr: transpose: %w", err)
		}
		var order []int
		var named string
		if json.Unmarshal(cfg.Order, &order) != nil {
			if json.Unmarshal(cfg.Order, &named) != nil || (named != "C" && named != "F") {
				return nil, true, fmt.Errorf("zarr: invalid transpose order %s", cfg.Order)
			}
		}
		return &transpose{order: order, fortran: named == "F"}, true, nil
	case "numcodecs.bitround":
		// Bit rounding is lossy at encode time; decoding is the identity.
		var cfg struct {
			Keepbits int `json:"keepbits"`
		}
		if err := parseConfig(s.Configuration, &cfg); err != nil {
			return nil, true, fmt.Errorf("zarr: bitround: %w", err)
		}
		return bitRound{keepbits: cfg.Keepbits}, true, nil
	}
	return nil, false, nil
}

type identityCodec struct{}

func (identityCodec) encoded(shape []uint64, dt DataType) ([]uint64, DataType, error) {
	return shape, dt, nil
}
func (identityCodec) decode(c *chunkData, _ []uint64, _ DataType) (*chunkData, error) { return c, nil }

type transpose struct {
	order   []int
	fortran bool
}

func (t *transpose) perm(n int) ([]int, error) {
	if t.order == nil {
		p := make([]int, n)
		for i := range p {
			p[i] = i
			if t.fortran {
				p[i] = n - 1 - i
			}
		}
		return p, nil
	}
	if len(t.order) != n {
		return nil, fmt.Errorf("zarr: transpose order has %d entries for %d dimensions", len(t.order), n)
	}
	seen := make([]bool, n)
	for _, o := range t.order {
		if o < 0 || o >= n || seen[o] {
			return nil, fmt.Errorf("zarr: invalid transpose order %v", t.order)
		}
		seen[o] = true
	}
	return t.order, nil
}

func (t *transpose) encoded(shape []uint64, dt DataType) ([]uint64, DataType, error) {
	if shape == nil {
		return nil, dt, nil
	}
	p, err := t.perm(len(shape))
	if err != nil {
		return nil, dt, err
	}
	out := make([]uint64, len(shape))
	for i, o := range p {
		out[i] = shape[o]
	}
	return out, dt, nil
}

func (t *transpose) decode(c *chunkData, shape []uint64, dt DataType) (*chunkData, error) {
	n := len(shape)
	p, err := t.perm(n)
	if err != nil {
		return nil, err
	}
	// Output strides (in elements) for the original C-order layout.
	stride := make([]uint64, n)
	acc := uint64(1)
	for i := n - 1; i >= 0; i-- {
		stride[i] = acc
		acc *= shape[i]
	}
	// Walking the encoded array in C order, encoded dim i advances output
	// dim p[i].
	encStride := make([]uint64, n)
	for i := range p {
		encStride[i] = stride[p[i]]
	}
	total := numElements(shape)
	out := &chunkData{shape: shape, dtype: dt}
	if dt.Variable() {
		out.strs = make([]string, total)
	} else {
		out.data = make([]byte, len(c.data))
	}
	idx := make([]uint64, n)
	var dst uint64
	es := uint64(dt.Size)
	for src := uint64(0); src < total; src++ {
		if dt.Variable() {
			out.strs[dst] = c.strs[src]
		} else {
			copy(out.data[dst*es:(dst+1)*es], c.data[src*es:(src+1)*es])
		}
		for d := n - 1; d >= 0; d-- {
			idx[d]++
			dst += encStride[d]
			if idx[d] < c.shape[d] {
				break
			}
			dst -= encStride[d] * idx[d]
			idx[d] = 0
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Array-to-bytes codecs

func newArrayBytesCodec(s CodecSpec, dt DataType, fill []byte, fillStr string) (arrayBytesCodec, bool, error) {
	switch s.Name {
	case "bytes":
		var cfg struct {
			Endian string `json:"endian"`
		}
		if len(s.Configuration) > 0 {
			if err := json.Unmarshal(s.Configuration, &cfg); err != nil {
				return nil, true, fmt.Errorf("zarr: bytes codec: %w", err)
			}
		}
		if dt.Variable() {
			return nil, true, fmt.Errorf("zarr: the bytes codec cannot encode variable-length %s data", dt.Name)
		}
		return &bytesCodec{big: cfg.Endian == "big"}, true, nil
	case "vlen-utf8", "vlen-bytes", "numcodecs.vlen-utf8", "numcodecs.vlen-bytes":
		return vlenCodec{}, true, nil
	case "sharding_indexed":
		sc, err := newShardingCodec(s.Configuration, dt, fill, fillStr)
		return sc, true, err
	}
	return nil, false, nil
}

type bytesCodec struct{ big bool }

func (b *bytesCodec) decode(_ context.Context, in []byte, shape []uint64, dt DataType) (*chunkData, error) {
	want := numElements(shape) * uint64(dt.Size)
	if uint64(len(in)) != want {
		return nil, fmt.Errorf("zarr: decoded chunk has %d bytes, expected %d", len(in), want)
	}
	if b.big {
		if sw := dt.swapSize(); sw > 1 {
			out := make([]byte, len(in))
			for i := 0; i+sw <= len(in); i += sw {
				for j := 0; j < sw; j++ {
					out[i+j] = in[i+sw-1-j]
				}
			}
			in = out
		}
	}
	return &chunkData{shape: shape, dtype: dt, data: in}, nil
}

// vlenCodec decodes vlen-utf8 / vlen-bytes: a uint32 item count followed by
// (uint32 length, bytes) per item, all little-endian.
type vlenCodec struct{}

func (vlenCodec) decode(_ context.Context, in []byte, shape []uint64, dt DataType) (*chunkData, error) {
	if len(in) < 4 {
		return nil, fmt.Errorf("zarr: vlen chunk too short")
	}
	n := uint64(binary.LittleEndian.Uint32(in))
	if want := numElements(shape); n != want {
		return nil, fmt.Errorf("zarr: vlen chunk has %d items, expected %d", n, want)
	}
	strs := make([]string, n)
	pos := 4
	for i := range strs {
		if pos+4 > len(in) {
			return nil, fmt.Errorf("zarr: truncated vlen chunk")
		}
		l := int(binary.LittleEndian.Uint32(in[pos:]))
		pos += 4
		if l < 0 || pos+l > len(in) {
			return nil, fmt.Errorf("zarr: truncated vlen chunk")
		}
		strs[i] = string(in[pos : pos+l])
		pos += l
	}
	return &chunkData{shape: shape, dtype: dt, strs: strs}, nil
}
