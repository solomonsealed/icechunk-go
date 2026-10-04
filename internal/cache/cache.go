// Package cache provides the cache a Repository keeps its decoded immutable
// objects (snapshots and manifests) in.
package cache

import (
	"container/list"
	"context"
	"errors"
	"sync"

	"github.com/solomonsealed/icechunk-go/storage"
)

// Cache is a byte-budgeted LRU for immutable values that also collapses
// concurrent loads of the same key into one fetch. Loads are only shared
// within one storage.Scope (see storage.WithScope); completed entries are
// shared by everyone.
type Cache[K comparable] struct {
	mu       sync.Mutex
	budget   int64
	used     int64
	order    *list.List // front = most recently used
	entries  map[K]*list.Element
	inflight map[inflightKey[K]]*inflightLoad
}

type inflightKey[K comparable] struct {
	key   K
	scope any
}

type entry[K comparable] struct {
	key  K
	val  any
	size int64
}

type inflightLoad struct {
	done chan struct{}
	val  any
	err  error
}

// New returns a cache that holds values of at most budget bytes in total.
func New[K comparable](budget int64) *Cache[K] {
	return &Cache[K]{
		budget:   budget,
		order:    list.New(),
		entries:  map[K]*list.Element{},
		inflight: map[inflightKey[K]]*inflightLoad{},
	}
}

// Get returns the cached value for key, calling load at most once across
// concurrent callers when it is missing. load returns the value and its size.
func (c *Cache[K]) Get(ctx context.Context, key K, load func() (any, int64, error)) (any, error) {
	for {
		v, err, retry := c.tryGet(ctx, key, load)
		if !retry {
			return v, err
		}
	}
}

// tryGet does one attempt. retry is set when this caller waited on a load
// that failed only because the loading caller's context ended.
func (c *Cache[K]) tryGet(ctx context.Context, key K, load func() (any, int64, error)) (v any, err error, retry bool) {
	c.mu.Lock()
	if el, ok := c.entries[key]; ok {
		c.order.MoveToFront(el)
		v := el.Value.(*entry[K]).val
		c.mu.Unlock()
		return v, nil, false
	}
	ik := inflightKey[K]{key: key, scope: storage.Scope(ctx)}
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
		c.entries[key] = c.order.PushFront(&entry[K]{key: key, val: val, size: size})
		c.used += size
		for c.used > c.budget {
			last := c.order.Back()
			e := last.Value.(*entry[K])
			c.order.Remove(last)
			delete(c.entries, e.key)
			c.used -= e.size
		}
	}
	c.mu.Unlock()
	close(fl.done)
	return val, err, false
}
