package icechunk

import (
	"container/list"
	"context"
	"errors"
	"sync"

	"github.com/solomonsealed/icechunk-go/storage"
)

// assetCache is a byte-budgeted LRU for immutable metadata objects
// (snapshots and manifests) that also collapses concurrent loads of the same
// key into one fetch. Loads are only shared within one storage.Scope (see
// storage.WithScope); completed entries are shared by everyone.
type assetCache struct {
	mu       sync.Mutex
	budget   int64
	used     int64
	order    *list.List // front = most recently used
	entries  map[cacheKey]*list.Element
	inflight map[inflightKey]*inflightLoad
}

type inflightKey struct {
	key   cacheKey
	scope any
}

type cacheKey struct {
	kind byte // 's' snapshot, 'm' manifest
	id   ObjectID12
}

type cacheEntry struct {
	key  cacheKey
	val  any
	size int64
}

type inflightLoad struct {
	done chan struct{}
	val  any
	err  error
}

func newAssetCache(budget int64) *assetCache {
	return &assetCache{
		budget:   budget,
		order:    list.New(),
		entries:  map[cacheKey]*list.Element{},
		inflight: map[inflightKey]*inflightLoad{},
	}
}

// get returns the cached value for key, calling load at most once across
// concurrent callers when it is missing. load returns the value and its size.
func (c *assetCache) get(ctx context.Context, key cacheKey, load func() (any, int64, error)) (any, error) {
	for {
		v, err, retry := c.tryGet(ctx, key, load)
		if !retry {
			return v, err
		}
	}
}

// tryGet does one attempt. retry is set when this caller waited on a load
// that failed only because the loading caller's context ended.
func (c *assetCache) tryGet(ctx context.Context, key cacheKey, load func() (any, int64, error)) (v any, err error, retry bool) {
	c.mu.Lock()
	if el, ok := c.entries[key]; ok {
		c.order.MoveToFront(el)
		v := el.Value.(*cacheEntry).val
		c.mu.Unlock()
		return v, nil, false
	}
	ik := inflightKey{key: key, scope: storage.Scope(ctx)}
	if fl, ok := c.inflight[ik]; ok {
		c.mu.Unlock()
		select {
		case <-fl.done:
			ctxErr := errors.Is(fl.err, context.Canceled) || errors.Is(fl.err, context.DeadlineExceeded)
			if ctxErr && ctx.Err() == nil {
				return nil, nil, true
			}
			return fl.val, fl.err, false
		case <-ctx.Done():
			return nil, ctx.Err(), false
		}
	}
	fl := &inflightLoad{done: make(chan struct{})}
	c.inflight[ik] = fl
	c.mu.Unlock()

	val, size, err := load()
	fl.val, fl.err = val, err

	c.mu.Lock()
	delete(c.inflight, ik)
	_, cached := c.entries[key] // another scope may have stored it meanwhile
	if err == nil && size <= c.budget && !cached {
		c.entries[key] = c.order.PushFront(&cacheEntry{key: key, val: val, size: size})
		c.used += size
		for c.used > c.budget {
			last := c.order.Back()
			e := last.Value.(*cacheEntry)
			c.order.Remove(last)
			delete(c.entries, e.key)
			c.used -= e.size
		}
	}
	c.mu.Unlock()
	close(fl.done)
	return val, err, false
}
