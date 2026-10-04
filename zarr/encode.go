package zarr

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/adler32"
	"hash/crc32"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// BytesEncoder is implemented by bytes-to-bytes codecs that can encode,
// which writing arrays requires.
type BytesEncoder interface {
	Encode(in []byte) ([]byte, error)
}

// codecFuncs is a BytesCodec built from a decode and an optional encode
// function.
type codecFuncs struct {
	name string
	dec  func([]byte) ([]byte, error)
	enc  func([]byte) ([]byte, error)
}

func (c codecFuncs) Decode(in []byte) ([]byte, error) { return c.dec(in) }

func (c codecFuncs) Encode(in []byte) ([]byte, error) {
	if c.enc == nil {
		return nil, fmt.Errorf("%w: encoding with codec %q", ErrUnsupported, c.name)
	}
	return c.enc(in)
}

func parseConfig(cfg json.RawMessage, v any) error {
	if len(cfg) == 0 || string(cfg) == "null" {
		return nil
	}
	return json.Unmarshal(cfg, v)
}

func init() {
	fixed := func(name string, dec, enc func([]byte) ([]byte, error)) BytesCodecFactory {
		return func(json.RawMessage, int) (BytesCodec, error) { return codecFuncs{name, dec, enc}, nil }
	}
	for _, name := range []string{"gzip", "numcodecs.gzip"} {
		RegisterBytesCodec(name, func(cfg json.RawMessage, _ int) (BytesCodec, error) {
			c := struct {
				Level int `json:"level"`
			}{Level: 5}
			if err := parseConfig(cfg, &c); err != nil {
				return nil, err
			}
			return codecFuncs{name, gunzip, func(in []byte) ([]byte, error) { return gzipBytes(in, c.Level) }}, nil
		})
	}
	for _, name := range []string{"zstd", "numcodecs.zstd"} {
		RegisterBytesCodec(name, func(cfg json.RawMessage, _ int) (BytesCodec, error) {
			var c struct {
				Level    int  `json:"level"`
				Checksum bool `json:"checksum"`
			}
			if err := parseConfig(cfg, &c); err != nil {
				return nil, err
			}
			return codecFuncs{name, unzstd, func(in []byte) ([]byte, error) { return zstdBytes(in, c.Level, c.Checksum) }}, nil
		})
	}
	for _, name := range []string{"blosc", "numcodecs.blosc"} {
		RegisterBytesCodec(name, func(cfg json.RawMessage, elemSize int) (BytesCodec, error) {
			bc, err := parseBloscConfig(cfg, elemSize)
			if err != nil {
				return nil, err
			}
			return codecFuncs{name, bloscDecompress, bc.compress}, nil
		})
	}
	RegisterBytesCodec("numcodecs.zlib", func(cfg json.RawMessage, _ int) (BytesCodec, error) {
		c := struct {
			Level int `json:"level"`
		}{Level: 1}
		if err := parseConfig(cfg, &c); err != nil {
			return nil, err
		}
		return codecFuncs{"numcodecs.zlib", unzlib, func(in []byte) ([]byte, error) { return zlibBytes(in, c.Level) }}, nil
	})
	RegisterBytesCodec("numcodecs.bz2", fixed("numcodecs.bz2", unbzip2, nil))
	RegisterBytesCodec("numcodecs.lz4", fixed("numcodecs.lz4", unlz4Numcodecs, func(in []byte) ([]byte, error) {
		out := binary.LittleEndian.AppendUint32(nil, uint32(len(in)))
		return append(out, lz4Compress(in)...), nil
	}))
	crc32cSum := func(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }
	RegisterBytesCodec("crc32c", fixed("crc32c",
		func(in []byte) ([]byte, error) { return stripChecksum(in, "end", "crc32c", crc32cSum) },
		func(in []byte) ([]byte, error) { return addChecksum(in, "end", crc32cSum), nil }))
	// numcodecs' default checksum location: "start", except "end" for CRC32C.
	checksum := func(name, kind, defaultLocation string, sum func([]byte) uint32) BytesCodecFactory {
		return func(cfg json.RawMessage, _ int) (BytesCodec, error) {
			var c struct {
				Location string `json:"location"`
			}
			if err := parseConfig(cfg, &c); err != nil {
				return nil, err
			}
			if c.Location == "" {
				c.Location = defaultLocation
			}
			return codecFuncs{name,
				func(in []byte) ([]byte, error) { return stripChecksum(in, c.Location, kind, sum) },
				func(in []byte) ([]byte, error) { return addChecksum(in, c.Location, sum), nil }}, nil
		}
	}
	RegisterBytesCodec("numcodecs.crc32", checksum("numcodecs.crc32", "crc32", "start", crc32.ChecksumIEEE))
	RegisterBytesCodec("numcodecs.crc32c", checksum("numcodecs.crc32c", "crc32c", "end", crc32cSum))
	RegisterBytesCodec("numcodecs.adler32", checksum("numcodecs.adler32", "adler32", "start", adler32.Checksum))
	RegisterBytesCodec("numcodecs.fletcher32", fixed("numcodecs.fletcher32",
		func(in []byte) ([]byte, error) { return stripChecksum(in, "end", "fletcher32", fletcher32) },
		func(in []byte) ([]byte, error) { return addChecksum(in, "end", fletcher32), nil }))
	RegisterBytesCodec("numcodecs.shuffle", func(cfg json.RawMessage, elemSize int) (BytesCodec, error) {
		c := struct {
			ElementSize int `json:"elementsize"`
		}{ElementSize: 4}
		if err := parseConfig(cfg, &c); err != nil {
			return nil, err
		}
		apply := func(f func(int, []byte, []byte)) func([]byte) ([]byte, error) {
			return func(in []byte) ([]byte, error) {
				if c.ElementSize <= 1 {
					return in, nil
				}
				out := make([]byte, len(in))
				f(c.ElementSize, in, out)
				return out, nil
			}
		}
		return codecFuncs{"numcodecs.shuffle", apply(unshuffle), apply(shuffle)}, nil
	})
}

func addChecksum(in []byte, location string, sum func([]byte) uint32) []byte {
	cs := binary.LittleEndian.AppendUint32(nil, sum(in))
	if location == "end" {
		return append(append([]byte(nil), in...), cs...)
	}
	return append(cs, in...)
}

func gzipBytes(in []byte, level int) ([]byte, error) {
	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, level)
	if err != nil {
		return nil, fmt.Errorf("zarr: gzip: %w", err)
	}
	w.Write(in)
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func zlibBytes(in []byte, level int) ([]byte, error) {
	var buf bytes.Buffer
	w, err := zlib.NewWriterLevel(&buf, level)
	if err != nil {
		return nil, fmt.Errorf("zarr: zlib: %w", err)
	}
	w.Write(in)
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type zstdKey struct {
	level    int
	checksum bool
}

var zstdEncoders sync.Map // zstdKey -> *zstd.Encoder

// zstdBytes compresses one frame. Level follows zstd's numbering (0 means
// the default, 3); checksum adds the frame checksum.
func zstdBytes(in []byte, level int, checksum bool) ([]byte, error) {
	k := zstdKey{level, checksum}
	e, ok := zstdEncoders.Load(k)
	if !ok {
		lvl := level
		if lvl == 0 {
			lvl = 3
		}
		enc, err := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(lvl)),
			zstd.WithEncoderCRC(checksum),
			zstd.WithEncoderConcurrency(1))
		if err != nil {
			return nil, err
		}
		e, _ = zstdEncoders.LoadOrStore(k, enc)
	}
	return e.(*zstd.Encoder).EncodeAll(in, nil), nil
}

// shuffle groups byte j of every element together (the inverse of unshuffle).
func shuffle(typesize int, src, dst []byte) {
	n := len(src) / typesize
	for j := 0; j < typesize; j++ {
		for i := 0; i < n; i++ {
			dst[j*n+i] = src[i*typesize+j]
		}
	}
	copy(dst[n*typesize:], src[n*typesize:])
}

// bitShuffle transposes bits (the inverse of bitUnshuffle, format version 2:
// blocks whose element count is not a multiple of 8 are left as is).
func bitShuffle(typesize int, src, dst []byte) {
	n := len(src) / typesize
	if n%8 != 0 {
		copy(dst, src)
		return
	}
	rowBytes := n / 8
	for i := range dst[:n*typesize] {
		dst[i] = 0
	}
	for i := 0; i < n; i++ {
		for j := 0; j < typesize; j++ {
			v := src[i*typesize+j]
			if v == 0 {
				continue
			}
			for k := 0; k < 8; k++ {
				if v&(1<<k) != 0 {
					dst[(j*8+k)*rowBytes+i/8] |= 1 << (i % 8)
				}
			}
		}
	}
	copy(dst[n*typesize:], src[n*typesize:])
}

// ---------------------------------------------------------------------------
// LZ4

// lz4Compress produces one raw LZ4 block (greedy, 64 KiB window),
// decodable by any LZ4 implementation.
func lz4Compress(src []byte) []byte {
	const (
		minMatch     = 4
		mfLimit      = 12 // no match may start within the last 12 bytes
		lastLiterals = 5  // the last 5 bytes are always literals
	)
	dst := make([]byte, 0, len(src)+len(src)/255+16)
	emit := func(lits []byte, offset, matchLen int) {
		lit := len(lits)
		token := byte(min(lit, 15)) << 4
		if matchLen > 0 {
			token |= byte(min(matchLen-minMatch, 15))
		}
		dst = append(dst, token)
		if lit >= 15 {
			n := lit - 15
			for ; n >= 255; n -= 255 {
				dst = append(dst, 255)
			}
			dst = append(dst, byte(n))
		}
		dst = append(dst, lits...)
		if matchLen == 0 {
			return
		}
		dst = append(dst, byte(offset), byte(offset>>8))
		if matchLen-minMatch >= 15 {
			n := matchLen - minMatch - 15
			for ; n >= 255; n -= 255 {
				dst = append(dst, 255)
			}
			dst = append(dst, byte(n))
		}
	}
	if len(src) <= mfLimit {
		emit(src, 0, 0)
		return dst
	}
	var table [1 << 16]int32 // position+1 of the last occurrence of a hash
	anchor, i := 0, 0
	for limit := len(src) - mfLimit; i < limit; {
		seq := binary.LittleEndian.Uint32(src[i:])
		h := (seq * 2654435761) >> 16
		cand := int(table[h]) - 1
		table[h] = int32(i + 1)
		if cand >= 0 && i-cand <= 65535 && binary.LittleEndian.Uint32(src[cand:]) == seq {
			ml := minMatch
			for i+ml < len(src)-lastLiterals && src[cand+ml] == src[i+ml] {
				ml++
			}
			emit(src[anchor:i], i-cand, ml)
			i += ml
			anchor = i
			continue
		}
		i++
	}
	emit(src[anchor:], 0, 0)
	return dst
}

// ---------------------------------------------------------------------------
// Blosc

type bloscEncoder struct {
	compformat int // 1 lz4, 3 zlib, 4 zstd
	clevel     int
	shuffle    int // 0 none, 1 byte, 2 bit
	typesize   int
	blocksize  int
}

func parseBloscConfig(cfg json.RawMessage, elemSize int) (*bloscEncoder, error) {
	var c struct {
		Cname     string          `json:"cname"`
		Clevel    *int            `json:"clevel"`
		Shuffle   json.RawMessage `json:"shuffle"`
		Typesize  int             `json:"typesize"`
		Blocksize int             `json:"blocksize"`
	}
	if err := parseConfig(cfg, &c); err != nil {
		return nil, err
	}
	b := &bloscEncoder{clevel: 5, typesize: c.Typesize, blocksize: c.Blocksize}
	if c.Clevel != nil {
		b.clevel = *c.Clevel
	}
	if b.typesize <= 0 {
		b.typesize = max(elemSize, 1)
	}
	switch c.Cname {
	case "zstd":
		b.compformat = 4
	case "zlib":
		b.compformat = 3
	default:
		// lz4 and lz4hc share a format; blosclz and snappy streams are
		// written with lz4, which every blosc decoder reads.
		b.compformat = 1
	}
	var s string
	var n int
	switch {
	case json.Unmarshal(c.Shuffle, &s) == nil:
		switch s {
		case "noshuffle":
		case "shuffle":
			b.shuffle = 1
		case "bitshuffle":
			b.shuffle = 2
		default:
			return nil, fmt.Errorf("zarr: blosc shuffle %q", s)
		}
	case json.Unmarshal(c.Shuffle, &n) == nil:
		b.shuffle = n // numcodecs uses 0/1/2 (and -1 = auto: byte shuffle)
		if n < 0 {
			b.shuffle = 1
		}
	default:
		b.shuffle = 1
	}
	return b, nil
}

// compress writes a c-blosc 1.x frame (format version 2) with unsplit
// blocks, falling back to a stored (memcpyed) frame when that is smaller.
func (b *bloscEncoder) compress(in []byte) ([]byte, error) {
	const header = 16
	nbytes := len(in)
	typesize := min(b.typesize, 255)
	blocksize := b.blocksize
	if blocksize <= 0 {
		blocksize = 256 << 10
	}
	blocksize = max(blocksize/typesize*typesize, typesize)
	if blocksize > nbytes {
		blocksize = nbytes
	}
	flags := byte(0x10) | byte(b.compformat<<5) // blocks are not split
	switch {
	case b.shuffle == 1 && typesize > 1:
		flags |= 0x1
	case b.shuffle == 2:
		flags |= 0x4
	}
	stored := func() []byte {
		out := make([]byte, header, header+nbytes)
		out[0], out[1], out[2], out[3] = 2, 1, flags|0x2, byte(typesize)
		binary.LittleEndian.PutUint32(out[4:], uint32(nbytes))
		binary.LittleEndian.PutUint32(out[8:], uint32(max(blocksize, 1)))
		binary.LittleEndian.PutUint32(out[12:], uint32(header+nbytes))
		return append(out, in...)
	}
	if nbytes == 0 {
		return stored(), nil
	}
	nblocks := (nbytes + blocksize - 1) / blocksize
	out := make([]byte, header+4*nblocks, header+4*nblocks+nbytes)
	out[0], out[1], out[2], out[3] = 2, 1, flags, byte(typesize)
	binary.LittleEndian.PutUint32(out[4:], uint32(nbytes))
	binary.LittleEndian.PutUint32(out[8:], uint32(blocksize))
	tmp := make([]byte, blocksize)
	for i := 0; i < nblocks; i++ {
		block := in[i*blocksize : min((i+1)*blocksize, nbytes)]
		src := block
		switch {
		case flags&0x1 != 0:
			shuffle(typesize, block, tmp[:len(block)])
			src = tmp[:len(block)]
		case flags&0x4 != 0 && len(block) >= typesize:
			bitShuffle(typesize, block, tmp[:len(block)])
			src = tmp[:len(block)]
		}
		var comp []byte
		var err error
		switch b.compformat {
		case 4:
			comp, err = zstdBytes(src, b.clevel, false)
		case 3:
			comp, err = zlibBytes(src, min(max(b.clevel, 1), 9))
		default:
			comp = lz4Compress(src)
		}
		if err != nil {
			return nil, err
		}
		if len(comp) >= len(src) {
			comp = src // stored block: the decoder copies when sizes match
		}
		binary.LittleEndian.PutUint32(out[header+4*i:], uint32(len(out)))
		out = binary.LittleEndian.AppendUint32(out, uint32(len(comp)))
		out = append(out, comp...)
		if len(out) >= header+nbytes {
			return stored(), nil
		}
	}
	binary.LittleEndian.PutUint32(out[12:], uint32(len(out)))
	return out, nil
}
