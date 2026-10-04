// Package nethttp serves a serve.Service with net/http. It lives apart from
// package serve so that Cloudflare Worker builds, which use serve without
// net/http, do not link it.
package nethttp

import (
	"net/http"

	"github.com/solomonsealed/icechunk-go/serve"
)

// Handler adapts s to net/http.
func Handler(s *serve.Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := s.Handle(r.Context(), &serve.Request{
			Method: r.Method,
			URL:    r.URL,
			Header: map[string]string{"range": r.Header.Get("Range")},
		})
		for k, v := range resp.Header {
			w.Header().Set(k, v)
		}
		w.WriteHeader(resp.Status)
		if r.Method != http.MethodHead {
			w.Write(resp.Body)
		}
	})
}
