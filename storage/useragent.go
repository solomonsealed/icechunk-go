package storage

import "context"

type userAgentKey struct{}

// WithUserAgent returns a copy of ctx whose requests identify themselves
// with userAgent. Storages that make HTTP requests send it as the
// User-Agent header, after any User-Agent of their own configuration.
// Repositories set it on every request they make, with their attribution
// (see icechunk.Attribution), as upstream Icechunk does since 2.3.0.
func WithUserAgent(ctx context.Context, userAgent string) context.Context {
	return context.WithValue(ctx, userAgentKey{}, userAgent)
}

// UserAgent returns the User-Agent set on ctx with WithUserAgent, or "".
func UserAgent(ctx context.Context) string {
	ua, _ := ctx.Value(userAgentKey{}).(string)
	return ua
}
