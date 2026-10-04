package storage

import "context"

type scopeKey struct{}

// WithScope marks ctx as belonging to an I/O scope, such as one incoming
// request. Concurrent reads of the same object are only coalesced between
// callers in the same scope (completed results are still shared by all).
//
// Cloudflare Workers require this: an I/O object created while handling
// one request must never be awaited by another, and in the Go WebAssembly
// runtime a goroutine woken by another request's I/O runs in that request's
// context. cfworker.Serve gives every request its own scope. scope must be
// comparable (a pointer is a good choice).
func WithScope(ctx context.Context, scope any) context.Context {
	return context.WithValue(ctx, scopeKey{}, scope)
}

// Scope returns the scope set by WithScope, or nil.
func Scope(ctx context.Context) any {
	return ctx.Value(scopeKey{})
}
