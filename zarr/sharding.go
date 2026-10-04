package zarr

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
)

// shardingCodec implements the sharding_indexed array-to-bytes codec: a
// shard holds a grid of inner chunks, each encoded with the inner codecs,
// plus an index of (offset, nbytes) pairs at the start or end of the shard.
type shardingCodec struct {
	inner        []uint64
	innerPipe    *pipeline
	indexPipe    *pipeline
	indexAtStart bool
	// indexTrailer is the number of bytes the index codecs add beyond the
	// raw 16 bytes per inner chunk (e.g. 4 for crc32c).
	indexTrailer int
	fill         []byte
	fillStr      string
}

func newShardingCodec(raw json.RawMessage, dt DataType, fill []byte, fillStr string) (*shardingCodec, error) {
	var cfg struct {
		ChunkShape    []uint64    `json:"chunk_shape"`
		Codecs        []CodecSpec `json:"codecs"`
		IndexCodecs   []CodecSpec `json:"index_codecs"`
		IndexLocation string      `json:"index_location"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("zarr: sharding_indexed: %w", err)
	}
	for _, c := range cfg.ChunkShape {
		if c == 0 {
			return nil, fmt.Errorf("zarr: sharding_indexed chunk_shape contains 0")
		}
	}
	s := &shardingCodec{inner: cfg.ChunkShape, indexAtStart: cfg.IndexLocation == "start", fill: fill, fillStr: fillStr}
	var err error
	if s.innerPipe, err = buildPipeline(cfg.Codecs, dt, fill, fillStr); err != nil {
		return nil, fmt.Errorf("zarr: sharding_indexed inner codecs: %w", err)
	}
	if len(cfg.IndexCodecs) == 0 {
		cfg.IndexCodecs = []CodecSpec{{Name: "bytes"}, {Name: "crc32c"}}
	}
	u64 := DataType{Name: "uint64", Kind: KindUint, Size: 8}
	for _, c := range cfg.IndexCodecs {
		switch c.Name {
		case "bytes":
		case "crc32c":
			s.indexTrailer += 4
		default:
			return nil, fmt.Errorf("%w: sharding index codec %q", ErrUnsupported, c.Name)
		}
	}
	if s.indexPipe, err = buildPipeline(cfg.IndexCodecs, u64, make([]byte, 8), ""); err != nil {
		return nil, fmt.Errorf("zarr: sharding_indexed index codecs: %w", err)
	}
	return s, nil
}

// gridShape returns the number of inner chunks per dimension of a shard.
func (s *shardingCodec) gridShape(shard []uint64) ([]uint64, error) {
	if len(shard) != len(s.inner) {
		return nil, fmt.Errorf("zarr: shard has %d dimensions, inner chunk_shape has %d", len(shard), len(s.inner))
	}
	g := make([]uint64, len(shard))
	for i := range shard {
		if shard[i]%s.inner[i] != 0 {
			return nil, fmt.Errorf("zarr: shard shape %v is not a multiple of inner chunk shape %v", shard, s.inner)
		}
		g[i] = shard[i] / s.inner[i]
	}
	return g, nil
}

func (s *shardingCodec) indexSize(grid []uint64) int64 {
	return int64(numElements(grid))*16 + int64(s.indexTrailer)
}

// shardIndex holds the decoded (offset, nbytes) pairs; missing chunks are
// marked with offset = nbytes = 2^64-1.
type shardIndex []uint64

func (ix shardIndex) entry(i uint64) (off, n uint64, ok bool) {
	off, n = ix[2*i], ix[2*i+1]
	return off, n, !(off == math.MaxUint64 && n == math.MaxUint64)
}

func (s *shardingCodec) decodeIndex(ctx context.Context, raw []byte, grid []uint64) (shardIndex, error) {
	shape := append(append([]uint64{}, grid...), 2)
	c, err := s.indexPipe.decode(ctx, raw, shape, DataType{Name: "uint64", Kind: KindUint, Size: 8})
	if err != nil {
		return nil, fmt.Errorf("zarr: shard index: %w", err)
	}
	ix := make(shardIndex, len(c.data)/8)
	for i := range ix {
		ix[i] = binary.LittleEndian.Uint64(c.data[8*i:])
	}
	return ix, nil
}

// indexRange returns where the index lives in a shard of the given size.
func (s *shardingCodec) indexRange(shardSize int64, grid []uint64) (int64, int64, error) {
	n := s.indexSize(grid)
	if n > shardSize {
		return 0, 0, fmt.Errorf("zarr: shard of %d bytes is smaller than its %d byte index", shardSize, n)
	}
	if s.indexAtStart {
		return 0, n, nil
	}
	return shardSize - n, n, nil
}

// decode decodes a complete shard.
func (s *shardingCodec) decode(ctx context.Context, in []byte, shape []uint64, dt DataType) (*chunkData, error) {
	grid, err := s.gridShape(shape)
	if err != nil {
		return nil, err
	}
	ioff, ilen, err := s.indexRange(int64(len(in)), grid)
	if err != nil {
		return nil, err
	}
	ix, err := s.decodeIndex(ctx, in[ioff:ioff+ilen], grid)
	if err != nil {
		return nil, err
	}
	out := newFilled(shape, dt, s.fill, s.fillStr)
	var firstErr error
	forEachIndex(grid, func(i uint64, coords []uint64) bool {
		off, n, ok := ix.entry(i)
		if !ok {
			return true
		}
		if off > uint64(len(in)) || n > uint64(len(in))-off {
			firstErr = fmt.Errorf("zarr: inner chunk %v range [%d, %d) outside shard of %d bytes", coords, off, off+n, len(in))
			return false
		}
		c, err := s.innerPipe.decode(ctx, in[off:off+n], s.inner, dt)
		if err != nil {
			firstErr = err
			return false
		}
		dstOff := make([]uint64, len(coords))
		for d := range coords {
			dstOff[d] = coords[d] * s.inner[d]
		}
		copyChunk(out, dstOff, c, make([]uint64, len(coords)), s.inner)
		return true
	})
	if firstErr != nil {
		return nil, firstErr
	}
	return &chunkData{shape: out.Shape, dtype: dt, data: out.Data, strs: out.Strings}, nil
}

// forEachIndex visits every multi-index in grid in C order with its linear
// index, stopping early when fn returns false.
func forEachIndex(grid []uint64, fn func(linear uint64, coords []uint64) bool) {
	total := numElements(grid)
	coords := make([]uint64, len(grid))
	for i := uint64(0); i < total; i++ {
		if !fn(i, coords) {
			return
		}
		for d := len(grid) - 1; d >= 0; d-- {
			coords[d]++
			if coords[d] < grid[d] {
				break
			}
			coords[d] = 0
		}
	}
}
