package zarr

import (
	"context"
	"fmt"
	"sync"
)

// ChunkSource supplies the encoded bytes of an array's chunks (for sharded
// arrays: of its shards).
type ChunkSource interface {
	// GetChunk returns length bytes of the chunk at coords starting at
	// offset; length < 0 reads to the end. ok is false if the chunk was never
	// written, in which case readers use the fill value.
	GetChunk(ctx context.Context, coords []uint32, offset, length int64) (data []byte, ok bool, err error)
	// ChunkSize returns the encoded size of a chunk; ok is false if absent.
	ChunkSize(ctx context.Context, coords []uint32) (size int64, ok bool, err error)
}

// Options tunes reads.
type Options struct {
	// Concurrency bounds parallel chunk fetches per Read call. Zero means 16.
	Concurrency int
}

// Array reads a Zarr v3 array from a ChunkSource. It is safe for concurrent use.
type Array struct {
	meta        *Metadata
	src         ChunkSource
	pipe        *pipeline
	shard       *shardingCodec // set when shards can be read partially
	concurrency int
}

// OpenArray parses an array's zarr.json and prepares its codec pipeline.
func OpenArray(doc []byte, src ChunkSource, opts *Options) (*Array, error) {
	meta, err := ParseMetadata(doc)
	if err != nil {
		return nil, err
	}
	a := &Array{meta: meta, src: src, concurrency: 16}
	if opts != nil && opts.Concurrency > 0 {
		a.concurrency = opts.Concurrency
	}
	if a.pipe, err = buildPipeline(meta.Codecs, meta.DataType, meta.fill, meta.fillStr); err != nil {
		return nil, err
	}
	// With sharding as the only codec, shards can be read inner chunk by
	// inner chunk using ranged requests.
	if sc, ok := a.pipe.ab.(*shardingCodec); ok && len(a.pipe.aa) == 0 && len(a.pipe.bb) == 0 {
		a.shard = sc
	}
	return a, nil
}

// Metadata returns the parsed zarr.json.
func (a *Array) Metadata() *Metadata { return a.meta }

// Shape returns the array shape.
func (a *Array) Shape() []uint64 { return append([]uint64(nil), a.meta.Shape...) }

// DataType returns the element type.
func (a *Array) DataType() DataType { return a.meta.DataType }

// Attributes returns the user attributes.
func (a *Array) Attributes() map[string]any { return a.meta.Attributes }

// DimensionNames returns the dimension names ("" for unnamed dimensions).
func (a *Array) DimensionNames() []string {
	out := make([]string, len(a.meta.Shape))
	for i, n := range a.meta.DimensionNames {
		if n != nil && i < len(out) {
			out[i] = *n
		}
	}
	return out
}

// ChunkGridShape returns the number of chunks along each dimension.
func (a *Array) ChunkGridShape() []uint64 {
	out := make([]uint64, len(a.meta.Shape))
	for d, n := range a.meta.Shape {
		out[d] = a.meta.grid.numChunks(d, n)
	}
	return out
}

// ReadAll reads the whole array.
func (a *Array) ReadAll(ctx context.Context) (*NDArray, error) {
	return a.Read(ctx, make([]uint64, len(a.meta.Shape)), a.meta.Shape)
}

// ReadChunk decodes one complete chunk (shard, for sharded arrays). Missing
// chunks are returned filled with the fill value.
func (a *Array) ReadChunk(ctx context.Context, coords []uint64) (*NDArray, error) {
	if len(coords) != len(a.meta.Shape) {
		return nil, fmt.Errorf("zarr: chunk coordinates %v do not match %d dimensions", coords, len(a.meta.Shape))
	}
	shape := make([]uint64, len(coords))
	for d, c := range coords {
		if c >= a.meta.grid.numChunks(d, a.meta.Shape[d]) {
			return nil, fmt.Errorf("zarr: chunk coordinates %v out of range", coords)
		}
		_, shape[d] = a.meta.grid.span(d, c)
	}
	data, ok, err := a.src.GetChunk(ctx, toU32(coords), 0, -1)
	if err != nil {
		return nil, err
	}
	if !ok {
		return newFilled(shape, a.meta.DataType, a.meta.fill, a.meta.fillStr), nil
	}
	c, err := a.pipe.decode(ctx, data, shape, a.meta.DataType)
	if err != nil {
		return nil, fmt.Errorf("zarr: decoding chunk %v: %w", coords, err)
	}
	return &NDArray{Shape: c.shape, DataType: c.dtype, Data: c.data, Strings: c.strs}, nil
}

// Read reads the region starting at start with extent count.
func (a *Array) Read(ctx context.Context, start, count []uint64) (*NDArray, error) {
	nd := len(a.meta.Shape)
	if len(start) != nd || len(count) != nd {
		return nil, fmt.Errorf("zarr: selection has %d/%d dimensions, array has %d", len(start), len(count), nd)
	}
	for d := range start {
		if start[d]+count[d] > a.meta.Shape[d] || start[d]+count[d] < start[d] {
			return nil, fmt.Errorf("zarr: selection [%d, %d) out of bounds for dimension %d of length %d", start[d], start[d]+count[d], d, a.meta.Shape[d])
		}
	}
	out := newFilled(count, a.meta.DataType, a.meta.fill, a.meta.fillStr)
	if numElements(count) == 0 {
		return out, nil
	}
	p := newPool(ctx, a.concurrency)
	forEachChunk(a.meta.grid, start, count, func(coords []uint64) {
		coords = append([]uint64(nil), coords...)
		p.do(func() error { return a.readInto(p.ctx, out, start, count, coords) })
	})
	if err := p.wait(); err != nil {
		return nil, err
	}
	return out, nil
}

// readInto decodes the chunk at coords and copies its overlap with the
// selection into out.
func (a *Array) readInto(ctx context.Context, out *NDArray, start, count, coords []uint64) error {
	nd := len(coords)
	cStart, cShape := make([]uint64, nd), make([]uint64, nd)
	for d, c := range coords {
		cStart[d], cShape[d] = a.meta.grid.span(d, c)
	}
	if a.shard != nil {
		return a.readShardInto(ctx, out, start, count, coords, cStart, cShape)
	}
	data, ok, err := a.src.GetChunk(ctx, toU32(coords), 0, -1)
	if err != nil || !ok {
		return err
	}
	c, err := a.pipe.decode(ctx, data, cShape, a.meta.DataType)
	if err != nil {
		return fmt.Errorf("zarr: decoding chunk %v: %w", coords, err)
	}
	copyOverlap(out, start, count, c, cStart)
	return nil
}

// readShardInto reads only the inner chunks of a shard that overlap the
// selection: one ranged read for the index, then one per inner chunk. When
// every inner chunk is needed, the whole shard is fetched at once instead.
func (a *Array) readShardInto(ctx context.Context, out *NDArray, start, count, coords, sStart, sShape []uint64) error {
	sc := a.shard
	grid, err := sc.gridShape(sShape)
	if err != nil {
		return err
	}
	// Inner chunk range overlapping the selection, relative to the shard.
	nd := len(coords)
	lo, hi := make([]uint64, nd), make([]uint64, nd)
	whole := true
	for d := 0; d < nd; d++ {
		from := max(start[d], sStart[d]) - sStart[d]
		to := min(start[d]+count[d], sStart[d]+sShape[d]) - sStart[d]
		lo[d], hi[d] = from/sc.inner[d], (to-1)/sc.inner[d]
		if lo[d] != 0 || hi[d] != grid[d]-1 {
			whole = false
		}
	}
	c32 := toU32(coords)
	if whole {
		data, ok, err := a.src.GetChunk(ctx, c32, 0, -1)
		if err != nil || !ok {
			return err
		}
		c, err := sc.decode(ctx, data, sShape, a.meta.DataType)
		if err != nil {
			return fmt.Errorf("zarr: decoding shard %v: %w", coords, err)
		}
		copyOverlap(out, start, count, c, sStart)
		return nil
	}
	size, ok, err := a.src.ChunkSize(ctx, c32)
	if err != nil || !ok {
		return err
	}
	ioff, ilen, err := sc.indexRange(size, grid)
	if err != nil {
		return err
	}
	raw, _, err := a.src.GetChunk(ctx, c32, ioff, ilen)
	if err != nil {
		return err
	}
	ix, err := sc.decodeIndex(ctx, raw, grid)
	if err != nil {
		return err
	}
	strides := stridesOf(grid)
	span := make([]uint64, nd)
	for d := range span {
		span[d] = hi[d] - lo[d] + 1
	}
	// A separate pool: waiting on the outer one from inside a task could deadlock.
	p := newPool(ctx, a.concurrency)
	forEachIndex(span, func(_ uint64, rel []uint64) bool {
		var linear uint64
		innerStart := make([]uint64, nd)
		for d := range rel {
			linear += (lo[d] + rel[d]) * strides[d]
			innerStart[d] = sStart[d] + (lo[d]+rel[d])*sc.inner[d]
		}
		off, n, ok := ix.entry(linear)
		if !ok {
			return true
		}
		if off > uint64(size) || n > uint64(size)-off {
			p.fail(fmt.Errorf("zarr: shard %v inner chunk range outside shard", coords))
			return false
		}
		p.do(func() error {
			data, _, err := a.src.GetChunk(p.ctx, c32, int64(off), int64(n))
			if err != nil {
				return err
			}
			c, err := sc.innerPipe.decode(p.ctx, data, sc.inner, a.meta.DataType)
			if err != nil {
				return fmt.Errorf("zarr: decoding shard %v inner chunk: %w", coords, err)
			}
			copyOverlap(out, start, count, c, innerStart)
			return nil
		})
		return true
	})
	return p.wait()
}

// forEachChunk visits the coordinates of every chunk overlapping the region.
func forEachChunk(g chunkGrid, start, count []uint64, fn func(coords []uint64)) {
	nd := len(start)
	lo, span := make([]uint64, nd), make([]uint64, nd)
	for d := 0; d < nd; d++ {
		lo[d] = g.index(d, start[d])
		span[d] = g.index(d, start[d]+count[d]-1) - lo[d] + 1
	}
	coords := make([]uint64, nd)
	forEachIndex(span, func(_ uint64, rel []uint64) bool {
		for d := range rel {
			coords[d] = lo[d] + rel[d]
		}
		fn(coords)
		return true
	})
}

// copyOverlap copies the part of chunk c (positioned at cStart in the
// array) that falls inside the selection [start, start+count) into out.
func copyOverlap(out *NDArray, start, count []uint64, c *chunkData, cStart []uint64) {
	nd := len(start)
	dstOff, srcOff, box := make([]uint64, nd), make([]uint64, nd), make([]uint64, nd)
	for d := 0; d < nd; d++ {
		lo := max(start[d], cStart[d])
		hi := min(start[d]+count[d], cStart[d]+c.shape[d])
		if hi <= lo {
			return
		}
		dstOff[d], srcOff[d], box[d] = lo-start[d], lo-cStart[d], hi-lo
	}
	copyChunk(out, dstOff, c, srcOff, box)
}

// copyChunk copies a box from chunk c (at srcOff) into out (at dstOff).
func copyChunk(out *NDArray, dstOff []uint64, c *chunkData, srcOff []uint64, box []uint64) {
	es := uint64(out.DataType.Size)
	forEachRun(out.Shape, dstOff, c.shape, srcOff, box, func(d, s, n uint64) {
		if out.Strings != nil {
			copy(out.Strings[d:d+n], c.strs[s:s+n])
		} else {
			copy(out.Data[d*es:(d+n)*es], c.data[s*es:(s+n)*es])
		}
	})
}

func stridesOf(shape []uint64) []uint64 {
	st := make([]uint64, len(shape))
	acc := uint64(1)
	for i := len(shape) - 1; i >= 0; i-- {
		st[i] = acc
		acc *= shape[i]
	}
	return st
}

// forEachRun calls fn(dstIndex, srcIndex, n) for each contiguous run of the
// box along the last dimension; indices are linear element offsets.
func forEachRun(dstShape, dstOff, srcShape, srcOff, box []uint64, fn func(d, s, n uint64)) {
	nd := len(box)
	if nd == 0 {
		fn(0, 0, 1)
		return
	}
	for _, b := range box {
		if b == 0 {
			return
		}
	}
	ds, ss := stridesOf(dstShape), stridesOf(srcShape)
	outer := box[:nd-1]
	forEachIndex(outer, func(_ uint64, idx []uint64) bool {
		d, s := dstOff[nd-1], srcOff[nd-1]
		for k, i := range idx {
			d += (dstOff[k] + i) * ds[k]
			s += (srcOff[k] + i) * ss[k]
		}
		fn(d, s, box[nd-1])
		return true
	})
}

func toU32(c []uint64) []uint32 {
	out := make([]uint32, len(c))
	for i, v := range c {
		out[i] = uint32(v)
	}
	return out
}

// pool runs tasks with bounded concurrency and remembers the first error.
// The first failure cancels the pool's context, stopping pending tasks.
type pool struct {
	ctx    context.Context
	cancel context.CancelFunc
	sem    chan struct{}
	wg     sync.WaitGroup
	once   sync.Once
	err    error
}

func newPool(ctx context.Context, n int) *pool {
	ctx, cancel := context.WithCancel(ctx)
	return &pool{ctx: ctx, cancel: cancel, sem: make(chan struct{}, n)}
}

func (p *pool) do(task func() error) {
	select {
	case p.sem <- struct{}{}:
	case <-p.ctx.Done():
		p.fail(p.ctx.Err())
		return
	}
	p.wg.Add(1)
	go func() {
		defer func() { <-p.sem; p.wg.Done() }()
		// A panic here would take down the whole program (in a Worker: the
		// isolate serving every in-flight request).
		defer func() {
			if r := recover(); r != nil {
				p.fail(fmt.Errorf("zarr: panic while reading a chunk: %v", r))
			}
		}()
		if p.ctx.Err() != nil {
			return
		}
		if err := task(); err != nil {
			p.fail(err)
		}
	}()
}

func (p *pool) fail(err error) {
	p.once.Do(func() {
		p.err = err
		p.cancel()
	})
}

func (p *pool) wait() error {
	p.wg.Wait()
	p.cancel()
	return p.err
}
