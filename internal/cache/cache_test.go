package cache

import (
	"context"
	"testing"
	"time"

	"github.com/solomonsealed/icechunk-go/storage"
)

// Loads are coalesced within a scope but never across scopes.
func TestCacheScopes(t *testing.T) {
	c := New[string](1 << 20)
	key := "m"
	release := make(chan struct{})
	loads := make(chan string, 8)
	load := func(name string) func() (any, int64, error) {
		return func() (any, int64, error) {
			loads <- name
			<-release
			return name, 1, nil
		}
	}
	ctxA := storage.WithScope(context.Background(), "a")
	ctxB := storage.WithScope(context.Background(), "b")
	results := make(chan any, 3)
	go func() { v, _ := c.Get(ctxA, key, load("a1")); results <- v }()
	<-loads                                                            // a1 is in flight
	go func() { v, _ := c.Get(ctxA, key, load("a2")); results <- v }() // same scope: must not load
	go func() { v, _ := c.Get(ctxB, key, load("b1")); results <- v }() // other scope: loads itself
	if got := <-loads; got != "b1" {
		t.Fatalf("second load = %s, want b1", got)
	}
	close(release)
	for i := 0; i < 3; i++ {
		if v := <-results; v != "a1" && v != "b1" {
			t.Errorf("result %v", v)
		}
	}
	// Whichever load finished first was cached; later reads must not load.
	if v, _ := c.Get(context.Background(), key, load("late")); v != "a1" && v != "b1" {
		t.Errorf("cached value = %v, want a1 or b1", v)
	}
	close(loads)
	for name := range loads {
		t.Errorf("unexpected load %s", name)
	}
}

// A waiter whose own context is fine retries when the shared load failed
// only because the loading caller's context was cancelled.
func TestCacheRetriesAfterLoaderCancellation(t *testing.T) {
	c := New[string](1 << 20)
	key := "s"
	started, release := make(chan struct{}), make(chan struct{})
	go c.Get(context.Background(), key, func() (any, int64, error) {
		close(started)
		<-release
		return nil, 0, context.Canceled
	})
	<-started
	got := make(chan any)
	go func() {
		v, err := c.Get(context.Background(), key, func() (any, int64, error) { return "fresh", 1, nil })
		if err != nil {
			t.Error(err)
		}
		got <- v
	}()
	time.Sleep(10 * time.Millisecond) // let the second caller start waiting
	close(release)
	if v := <-got; v != "fresh" {
		t.Errorf("waiter got %v, want its own successful load", v)
	}
}
