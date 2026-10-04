package icechunk

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Store exposes a session as a read-only Zarr v3 key/value store, using the
// same key layout as upstream Icechunk: "zarr.json", "<path>/zarr.json" for
// metadata and "<path>/c/<i>/<j>..." for chunks.
type Store struct {
	s *Session
}

// Store returns the session's Zarr key/value view.
func (s *Session) Store() *Store { return &Store{s: s} }

// Session returns the session backing the store.
func (st *Store) Session() *Session { return st.s }

type parsedKey struct {
	path     string // absolute node path
	metadata bool
	coords   []uint32
}

func parseKey(key string) (parsedKey, error) {
	invalid := fmt.Errorf("%w: invalid key %q", ErrKeyNotFound, key)
	if key == "zarr.json" {
		return parsedKey{path: "/", metadata: true}, nil
	}
	if p, ok := strings.CutSuffix(key, "/zarr.json"); ok {
		path, err := normalizePath(p)
		if err != nil {
			return parsedKey{}, invalid
		}
		return parsedKey{path: path, metadata: true}, nil
	}
	segs := strings.Split(key, "/")
	// The chunk marker is the last "c" segment followed only by integers.
	for i := len(segs) - 1; i >= 0; i-- {
		if segs[i] == "c" {
			coords := make([]uint32, 0, len(segs)-i-1)
			for _, s := range segs[i+1:] {
				v, err := strconv.ParseUint(s, 10, 32)
				if err != nil {
					return parsedKey{}, invalid
				}
				coords = append(coords, uint32(v))
			}
			path, err := normalizePath(strings.Join(segs[:i], "/"))
			if err != nil {
				return parsedKey{}, invalid
			}
			return parsedKey{path: path, coords: coords}, nil
		}
		if _, err := strconv.ParseUint(segs[i], 10, 32); err != nil {
			break
		}
	}
	return parsedKey{}, invalid
}

// Get returns the value of key, or an error wrapping ErrKeyNotFound.
func (st *Store) Get(ctx context.Context, key string) ([]byte, error) {
	return st.GetRange(ctx, key, 0, -1)
}

// GetRange returns length bytes of key's value starting at offset
// (length < 0 reads to the end).
func (st *Store) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	k, err := parseKey(key)
	if err != nil {
		return nil, err
	}
	node, err := st.s.node(k.path)
	if err != nil {
		if errors.Is(err, ErrNodeNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, key)
		}
		return nil, err
	}
	if k.metadata {
		md := node.ZarrMetadata
		if length < 0 {
			length = int64(len(md)) - offset
		}
		if offset < 0 || length < 0 || offset+length > int64(len(md)) {
			return nil, fmt.Errorf("icechunk: byte range outside %s", key)
		}
		return append([]byte(nil), md[offset:offset+length]...), nil
	}
	if node.Type != ArrayNode {
		return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, key)
	}
	ref, err := st.s.chunkRef(ctx, node, k.coords)
	if err != nil {
		return nil, err
	}
	if ref == nil {
		return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, key)
	}
	return st.s.repo.fetchChunk(ctx, ref, offset, length)
}

// Exists reports whether key has a value.
func (st *Store) Exists(ctx context.Context, key string) (bool, error) {
	k, err := parseKey(key)
	if err != nil {
		return false, nil
	}
	node, err := st.s.node(k.path)
	if err != nil {
		if errors.Is(err, ErrNodeNotFound) {
			return false, nil
		}
		return false, err
	}
	if k.metadata {
		return true, nil
	}
	if node.Type != ArrayNode {
		return false, nil
	}
	ref, err := st.s.chunkRef(ctx, node, k.coords)
	return ref != nil, err
}

// ListDir lists the immediate children (keys and prefixes) under prefix,
// sorted. For an array this is always ["c", "zarr.json"], as upstream does.
func (st *Store) ListDir(ctx context.Context, prefix string) ([]string, error) {
	prefix = strings.Trim(prefix, "/")
	path, err := normalizePath(prefix)
	if err != nil {
		return nil, err
	}
	node, err := st.s.node(path)
	switch {
	case err == nil && node.Type == ArrayNode:
		return []string{"c", "zarr.json"}, nil
	case err == nil:
		nodes, err := st.s.nodes()
		if err != nil {
			return nil, err
		}
		out := []string{"zarr.json"}
		base := path
		if base != "/" {
			base += "/"
		}
		for _, n := range nodes {
			rest, ok := strings.CutPrefix(n.Path, base)
			if ok && rest != "" && !strings.Contains(rest, "/") {
				out = append(out, rest)
			}
		}
		sort.Strings(out)
		return out, nil
	case !errors.Is(err, ErrNodeNotFound):
		return nil, err
	}

	// Not a node: maybe a prefix inside an array's chunk keys ("a/c/0").
	segs := strings.Split(prefix, "/")
	for i := len(segs) - 1; i >= 1; i-- {
		anc, err := st.s.node("/" + strings.Join(segs[:i], "/"))
		if err != nil {
			continue
		}
		if anc.Type != ArrayNode {
			return nil, nil
		}
		seen := map[string]bool{}
		base := prefix + "/"
		for e, err := range st.s.ChunkRefs(ctx, anc.Path) {
			if err != nil {
				return nil, err
			}
			key := chunkKey(anc.Path, e.Coords)
			if rest, ok := strings.CutPrefix(key, base); ok {
				next, _, _ := strings.Cut(rest, "/")
				seen[next] = true
			}
		}
		out := make([]string, 0, len(seen))
		for k := range seen {
			out = append(out, k)
		}
		sort.Strings(out)
		return out, nil
	}
	return nil, nil
}

// ListPrefix returns, sorted, every key of the group or array that prefix
// names (with or without a trailing slash) and of everything below it, or
// every key for "". As upstream, a prefix that names no group or array is
// an error (wrapping ErrNodeNotFound): "a/b" lists /a/b's keys, not those
// of a sibling /a/bc. Listing chunk keys reads every manifest of the
// arrays listed.
func (st *Store) ListPrefix(ctx context.Context, prefix string) ([]string, error) {
	root := "/"
	if strings.Trim(prefix, "/") != "" {
		path, err := normalizePath(prefix)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid prefix %q", ErrNodeNotFound, prefix)
		}
		if _, err := st.s.node(path); err != nil {
			return nil, fmt.Errorf("listing prefix %q: %w", prefix, err)
		}
		root = path
	}
	nodes, err := st.s.nodes()
	if err != nil {
		return nil, err
	}
	var out []string
	for _, n := range nodes {
		if root != "/" && n.Path != root && !strings.HasPrefix(n.Path, root+"/") {
			continue
		}
		if n.Path == "/" {
			out = append(out, "zarr.json")
		} else {
			out = append(out, strings.TrimPrefix(n.Path, "/")+"/zarr.json")
		}
		if n.Type != ArrayNode {
			continue
		}
		for e, err := range st.s.ChunkRefs(ctx, n.Path) {
			if err != nil {
				return nil, err
			}
			out = append(out, chunkKey(n.Path, e.Coords))
		}
	}
	sort.Strings(out)
	return out, nil
}

// chunkKey formats the store key of a chunk: "<path>/c/<i>/<j>".
func chunkKey(path string, coords []uint32) string {
	var b strings.Builder
	if path != "/" {
		b.WriteString(strings.TrimPrefix(path, "/"))
		b.WriteString("/")
	}
	b.WriteString("c")
	for _, c := range coords {
		b.WriteString("/")
		b.WriteString(strconv.FormatUint(uint64(c), 10))
	}
	return b.String()
}

// Size returns the size in bytes of key's value without reading it (chunk
// sizes come from their manifest references).
func (st *Store) Size(ctx context.Context, key string) (int64, error) {
	k, err := parseKey(key)
	if err != nil {
		return 0, err
	}
	node, err := st.s.node(k.path)
	if err != nil {
		if errors.Is(err, ErrNodeNotFound) {
			return 0, fmt.Errorf("%w: %s", ErrKeyNotFound, key)
		}
		return 0, err
	}
	if k.metadata {
		return int64(len(node.ZarrMetadata)), nil
	}
	if node.Type != ArrayNode {
		return 0, fmt.Errorf("%w: %s", ErrKeyNotFound, key)
	}
	ref, err := st.s.chunkRef(ctx, node, k.coords)
	if err != nil {
		return 0, err
	}
	if ref == nil {
		return 0, fmt.Errorf("%w: %s", ErrKeyNotFound, key)
	}
	return int64(ref.Size()), nil
}

// Set writes a value: "…/zarr.json" creates or updates a node, chunk keys
// store encoded chunk bytes (like upstream's IcechunkStore.set). The
// session must be writable.
func (st *Store) Set(ctx context.Context, key string, value []byte) error {
	k, err := parseKey(key)
	if err != nil {
		return err
	}
	if k.metadata {
		return st.s.SetMetadata(ctx, k.path, value)
	}
	return st.s.SetChunk(ctx, k.path, k.coords, value)
}

// Delete removes a key: a node (with everything below it) for metadata
// keys, a chunk for chunk keys.
func (st *Store) Delete(ctx context.Context, key string) error {
	k, err := parseKey(key)
	if err != nil {
		return err
	}
	if k.metadata {
		return st.s.DeleteNode(ctx, k.path)
	}
	return st.s.DeleteChunk(ctx, k.path, k.coords)
}
