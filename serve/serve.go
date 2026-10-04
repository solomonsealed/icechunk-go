// Package serve exposes an Icechunk repository over a small HTTP API:
//
//	GET /                          repository summary (spec version, branches, tags)
//	GET /log?ref=main&limit=50     commit history
//	GET /nodes?ref=main            arrays and groups with shape, dtype, chunking
//	GET /array/<path>?ref=main&slice=0:10,5
//	                               array values as JSON (or raw bytes with format=binary)
//	GET /chunks/<path>?ref=main    chunk references (inline / native / virtual)
//	GET /zarr/<ref>/<key>          read-only Zarr v3 store (with Range support),
//	                               so any Zarr client can read the repository
//
// <ref> is a branch, a tag or a snapshot id (tried in that order); in
// /zarr/ paths, escape "/" inside ref names as %2F. The
// package does not depend on net/http: Service.Handle takes and returns
// plain structs, so it runs unchanged inside a Cloudflare Worker (see
// cfworker) or behind net/http (see package serve/nethttp).
package serve

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	icechunk "github.com/solomonsealed/icechunk-go"
	"github.com/solomonsealed/icechunk-go/zarr"
)

// Request is the part of an HTTP request the service looks at.
type Request struct {
	Method string
	URL    *url.URL
	// Header holds request headers with lower-case names (only "range" is used).
	Header map[string]string
}

// Response is an HTTP response.
type Response struct {
	Status int
	Header map[string]string
	Body   []byte
}

// Service serves one repository.
type Service struct {
	Repo *icechunk.Repository
	// DefaultRef is used when a request names no ref ("main" when empty).
	DefaultRef string
	// MaxElements bounds the size of /array responses (default 4 Mi elements).
	MaxElements uint64
}

type httpError struct {
	status int
	msg    string
}

func (e *httpError) Error() string { return e.msg }

func badRequest(format string, args ...any) error {
	return &httpError{400, fmt.Sprintf(format, args...)}
}

// Handle serves one request.
func (s *Service) Handle(ctx context.Context, r *Request) *Response {
	resp, err := s.route(ctx, r)
	if err != nil {
		resp = errorResponse(err)
	}
	if resp.Header == nil {
		resp.Header = map[string]string{}
	}
	resp.Header["access-control-allow-origin"] = "*"
	resp.Header["access-control-expose-headers"] = "content-range, content-length, x-shape, x-dtype, x-snapshot-id"
	if r.Method == "HEAD" {
		resp.Header["content-length"] = strconv.Itoa(len(resp.Body))
		resp.Body = nil
	}
	return resp
}

func errorResponse(err error) *Response {
	status := 500
	var he *httpError
	switch {
	case errors.As(err, &he):
		status = he.status
	case errors.Is(err, icechunk.ErrRefNotFound), errors.Is(err, icechunk.ErrNodeNotFound),
		errors.Is(err, icechunk.ErrKeyNotFound), errors.Is(err, icechunk.ErrSnapshotNotFound):
		status = 404
	case errors.Is(err, icechunk.ErrNotAnArray), errors.Is(err, zarr.ErrUnsupported):
		status = 400
	case errors.Is(err, icechunk.ErrChunkModified):
		// The object behind a virtual chunk changed since it was referenced.
		status = 409
	}
	body, _ := json.Marshal(map[string]string{"error": err.Error()})
	return &Response{Status: status, Header: map[string]string{"content-type": "application/json"}, Body: body}
}

func jsonResponse(v any) (*Response, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &Response{Status: 200, Header: map[string]string{"content-type": "application/json"}, Body: body}, nil
}

func (s *Service) route(ctx context.Context, r *Request) (*Response, error) {
	switch r.Method {
	case "OPTIONS":
		return &Response{Status: 204, Header: map[string]string{
			"access-control-allow-methods": "GET, HEAD, OPTIONS",
			"access-control-allow-headers": "range",
		}}, nil
	case "GET", "HEAD":
	default:
		return nil, &httpError{405, "method not allowed"}
	}
	path := r.URL.Path
	q := r.URL.Query()
	if raw := r.URL.EscapedPath(); strings.HasPrefix(raw, "/zarr/") {
		// The ref is one escaped segment ("feature%2Fx" for "feature/x").
		rawRef, rawKey, _ := strings.Cut(strings.TrimPrefix(raw, "/zarr/"), "/")
		ref, err1 := url.PathUnescape(rawRef)
		key, err2 := url.PathUnescape(rawKey)
		if err1 != nil || err2 != nil {
			return nil, badRequest("invalid path %q", raw)
		}
		return s.zarr(ctx, ref, key, r.Header["range"])
	}
	switch {
	case path == "/" || path == "":
		return s.summary(ctx)
	case path == "/log":
		return s.log(ctx, q)
	case path == "/nodes":
		return s.nodes(ctx, q)
	case strings.HasPrefix(path, "/array/"):
		return s.array(ctx, strings.TrimPrefix(path, "/array/"), q)
	case strings.HasPrefix(path, "/chunks/"):
		return s.chunks(ctx, strings.TrimPrefix(path, "/chunks/"), q)
	}
	return nil, &httpError{404, "not found: " + path}
}

// resolve turns a ref string into a session: branch, then tag, then id.
func (s *Service) resolve(ctx context.Context, ref string) (*icechunk.Session, error) {
	if ref == "" {
		ref = s.DefaultRef
	}
	if ref == "" {
		ref = "main"
	}
	sess, err := s.Repo.ReadonlySession(ctx, icechunk.AtBranch(ref))
	if !errors.Is(err, icechunk.ErrRefNotFound) {
		return sess, err
	}
	sess, err = s.Repo.ReadonlySession(ctx, icechunk.AtTag(ref))
	if !errors.Is(err, icechunk.ErrRefNotFound) {
		return sess, err
	}
	if id, perr := icechunk.ParseSnapshotID(ref); perr == nil {
		return s.Repo.ReadonlySession(ctx, icechunk.AtSnapshot(id))
	}
	return nil, err
}

func (s *Service) summary(ctx context.Context) (*Response, error) {
	out := map[string]any{"spec_version": s.Repo.SpecVersion()}
	refs := func(list func(context.Context) ([]string, error), lookup func(context.Context, string) (icechunk.SnapshotID, error)) any {
		names, err := list(ctx)
		if err != nil {
			return map[string]string{"error": err.Error()}
		}
		m := map[string]string{}
		for _, n := range names {
			if id, err := lookup(ctx, n); err == nil {
				m[n] = id.String()
			}
		}
		return m
	}
	out["branches"] = refs(s.Repo.ListBranches, s.Repo.LookupBranch)
	out["tags"] = refs(s.Repo.ListTags, s.Repo.LookupTag)
	return jsonResponse(out)
}

type commitJSON struct {
	ID        string         `json:"id"`
	Parent    string         `json:"parent,omitempty"`
	FlushedAt string         `json:"flushed_at"`
	Message   string         `json:"message"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

func (s *Service) log(ctx context.Context, q url.Values) (*Response, error) {
	limit := 100
	if l := q.Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n <= 0 {
			return nil, badRequest("invalid limit %q", l)
		}
		limit = n
	}
	sess, err := s.resolve(ctx, q.Get("ref"))
	if err != nil {
		return nil, err
	}
	out := []commitJSON{}
	for si, err := range s.Repo.Ancestry(ctx, icechunk.AtSnapshot(sess.SnapshotID())) {
		if err != nil {
			return nil, err
		}
		c := commitJSON{ID: si.ID.String(), FlushedAt: si.FlushedAt.Format("2006-01-02T15:04:05.000000Z07:00"), Message: si.Message, Metadata: si.Metadata}
		if si.ParentID != nil {
			c.Parent = si.ParentID.String()
		}
		out = append(out, c)
		if len(out) >= limit {
			break
		}
	}
	return jsonResponse(out)
}

type nodeJSON struct {
	Path           string         `json:"path"`
	Type           string         `json:"type"`
	Shape          []uint64       `json:"shape,omitempty"`
	ChunkShape     []uint64       `json:"chunk_shape,omitempty"`
	DType          string         `json:"dtype,omitempty"`
	DimensionNames []string       `json:"dimension_names,omitempty"`
	Attributes     map[string]any `json:"attributes,omitempty"`
}

func (s *Service) nodes(ctx context.Context, q url.Values) (*Response, error) {
	sess, err := s.resolve(ctx, q.Get("ref"))
	if err != nil {
		return nil, err
	}
	nodes, err := sess.Nodes()
	if err != nil {
		return nil, err
	}
	out := make([]nodeJSON, 0, len(nodes))
	for _, n := range nodes {
		nj := nodeJSON{Path: n.Path, Type: n.Type.String()}
		if n.Type == icechunk.ArrayNode {
			if md, err := zarr.ParseMetadata(n.ZarrMetadata); err == nil {
				nj.Shape, nj.ChunkShape, nj.DType, nj.Attributes = md.Shape, md.ChunkShape, md.DataType.Name, md.Attributes
			}
			nj.DimensionNames = n.Array.DimensionNames
		} else if attrs, err := zarr.Attributes(n.ZarrMetadata); err == nil && len(attrs) > 0 {
			nj.Attributes = attrs
		}
		out = append(out, nj)
	}
	resp, err := jsonResponse(out)
	if resp != nil {
		resp.Header["x-snapshot-id"] = sess.SnapshotID().String()
	}
	return resp, err
}

func (s *Service) array(ctx context.Context, path string, q url.Values) (*Response, error) {
	sess, err := s.resolve(ctx, q.Get("ref"))
	if err != nil {
		return nil, err
	}
	arr, err := sess.OpenArray(ctx, path)
	if err != nil {
		return nil, err
	}
	start, count, squeeze, err := zarr.ParseSelection(q.Get("slice"), arr.Shape())
	if err != nil {
		return nil, badRequest("%v", err)
	}
	n := uint64(1)
	for _, c := range count {
		n *= c
	}
	limit := s.MaxElements
	if limit == 0 {
		limit = 4 << 20
	}
	if n > limit {
		return nil, badRequest("selection has %d elements, more than the limit of %d; narrow it with ?slice=", n, limit)
	}
	nd, err := arr.Read(ctx, start, count)
	if err != nil {
		return nil, err
	}
	shape := []uint64{}
	for d, c := range nd.Shape {
		if !squeeze[d] {
			shape = append(shape, c)
		}
	}
	nd.Shape = shape
	shapeStr := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(shape)), ","), "[]")
	if q.Get("format") == "binary" {
		if nd.DataType.Variable() {
			return nil, badRequest("format=binary is not available for %s data", nd.DataType.Name)
		}
		return &Response{Status: 200, Body: nd.Data, Header: map[string]string{
			"content-type":  "application/octet-stream",
			"x-shape":       shapeStr,
			"x-dtype":       nd.DataType.Name,
			"x-snapshot-id": sess.SnapshotID().String(),
		}}, nil
	}
	resp, err := jsonResponse(nd)
	if resp != nil {
		resp.Header["x-snapshot-id"] = sess.SnapshotID().String()
	}
	return resp, err
}

func (s *Service) chunks(ctx context.Context, path string, q url.Values) (*Response, error) {
	sess, err := s.resolve(ctx, q.Get("ref"))
	if err != nil {
		return nil, err
	}
	limit := 1000
	if l := q.Get("limit"); l != "" {
		if limit, err = strconv.Atoi(l); err != nil || limit <= 0 {
			return nil, badRequest("invalid limit %q", l)
		}
	}
	type chunkJSON struct {
		Coords   []uint32 `json:"coords"`
		Kind     string   `json:"kind"`
		Size     uint64   `json:"size"`
		ChunkID  string   `json:"chunk_id,omitempty"`
		Location string   `json:"location,omitempty"`
		Offset   uint64   `json:"offset,omitempty"`
	}
	out := []chunkJSON{}
	for e, err := range sess.ChunkRefs(ctx, path) {
		if err != nil {
			return nil, err
		}
		c := chunkJSON{Coords: e.Coords, Kind: e.Ref.Kind.String(), Size: e.Ref.Size(), Location: e.Ref.Location}
		if e.Ref.Kind != icechunk.InlineChunk {
			c.Offset = e.Ref.Offset
		}
		if e.Ref.Kind == icechunk.NativeChunk {
			c.ChunkID = e.Ref.ID.String()
		}
		out = append(out, c)
		if len(out) >= limit {
			break
		}
	}
	return jsonResponse(out)
}

func (s *Service) zarr(ctx context.Context, ref, key, rangeHeader string) (*Response, error) {
	sess, err := s.resolve(ctx, ref)
	if err != nil {
		return nil, err
	}
	store := sess.Store()
	header := map[string]string{"content-type": "application/octet-stream", "accept-ranges": "bytes"}
	if key == "zarr.json" || strings.HasSuffix(key, "/zarr.json") {
		header["content-type"] = "application/json"
	}
	if id, err := icechunk.ParseSnapshotID(ref); err == nil && id == sess.SnapshotID() {
		header["cache-control"] = "public, max-age=31536000, immutable"
	}
	if rangeHeader == "" {
		body, err := store.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		return &Response{Status: 200, Header: header, Body: body}, nil
	}
	size, err := store.Size(ctx, key)
	if err != nil {
		return nil, err
	}
	off, n, err := parseRange(rangeHeader, size)
	if err != nil {
		return &Response{Status: 416, Header: map[string]string{"content-range": fmt.Sprintf("bytes */%d", size)}}, nil
	}
	body, err := store.GetRange(ctx, key, off, n)
	if err != nil {
		return nil, err
	}
	header["content-range"] = fmt.Sprintf("bytes %d-%d/%d", off, off+n-1, size)
	return &Response{Status: 206, Header: header, Body: body}, nil
}

// parseRange parses a single-range "bytes=a-b", "bytes=a-" or "bytes=-n".
func parseRange(h string, size int64) (off, n int64, err error) {
	spec, ok := strings.CutPrefix(strings.TrimSpace(h), "bytes=")
	if !ok || strings.Contains(spec, ",") {
		return 0, 0, fmt.Errorf("unsupported range %q", h)
	}
	a, b, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, 0, fmt.Errorf("invalid range %q", h)
	}
	switch {
	case a == "":
		suffix, err := strconv.ParseInt(b, 10, 64)
		if err != nil || suffix <= 0 || size == 0 {
			return 0, 0, fmt.Errorf("invalid range %q", h)
		}
		suffix = min(suffix, size)
		return size - suffix, suffix, nil
	default:
		start, err := strconv.ParseInt(a, 10, 64)
		if err != nil || start < 0 || start >= size {
			return 0, 0, fmt.Errorf("invalid range %q", h)
		}
		end := size - 1
		if b != "" {
			if end, err = strconv.ParseInt(b, 10, 64); err != nil || end < start {
				return 0, 0, fmt.Errorf("invalid range %q", h)
			}
			end = min(end, size-1)
		}
		return start, end - start + 1, nil
	}
}
