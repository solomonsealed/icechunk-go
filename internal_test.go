package icechunk

import (
	"context"
	"errors"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/solomonsealed/icechunk-go/internal/fbs"
	"github.com/solomonsealed/icechunk-go/internal/format"
	"github.com/solomonsealed/icechunk-go/storage"
)

func TestCrockford(t *testing.T) {
	id := ObjectID12{0x0b, 0x1c, 0xc8, 0xd6, 0x78, 0x75, 0x80, 0xf0, 0xe3, 0x3a, 0x65, 0x34}
	if got := id.String(); got != "1CECHNKREP0F1RSTCMT0" {
		t.Fatalf("encode = %s", got)
	}
	back, err := ParseSnapshotID("1cechnkrep0f1rstcmt0")
	if err != nil || back != id {
		t.Fatalf("decode = %v, %v", back, err)
	}
	nid := NodeID{1, 2, 3, 4, 5, 6, 7, 8}
	if p, err := ParseNodeID(nid.String()); err != nil || p != nid || len(nid.String()) != 13 {
		t.Fatalf("node id round trip: %s %v", nid, err)
	}
	if _, err := ParseSnapshotID("short"); err == nil {
		t.Fatal("expected error for short id")
	}
}

// The v2 virtual fixture must store its 2000 locations dictionary-compressed,
// so that the decompression path is really exercised.
func TestVirtualLocationsAreCompressed(t *testing.T) {
	ctx := context.Background()
	repo, err := Open(ctx, storage.NewLocal("testdata/generated/virtual-v2"), nil)
	if err != nil {
		t.Skip(err)
	}
	s, err := repo.ReadonlySession(ctx, AtBranch("main"))
	if err != nil {
		t.Fatal(err)
	}
	n, err := s.Node("/virtual")
	if err != nil {
		t.Fatal(err)
	}
	m, err := repo.manifest(ctx, n.Array.Manifests[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if m.root.CompressionAlgorithm() != compressionZstdDict || m.root.LocationDictionaryLength() == 0 {
		t.Fatalf("manifest is not dictionary compressed (alg %d)", m.root.CompressionAlgorithm())
	}
	am, _ := m.arrayManifest(n.ID)
	var cr fbs.ChunkRef
	am.Refs(&cr, 0)
	if cr.CompressedLocationBytes() == nil || cr.Location() != nil {
		t.Fatal("chunk ref does not use compressed_location")
	}
}

func TestCorruptFilesAreErrors(t *testing.T) {
	ctx := context.Background()
	raw, err := storage.NewLocal("testdata/upstream/test-repo-v2").Get(ctx, "repo", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseRepoInfo(raw[:20]); err == nil {
		t.Error("truncated header accepted")
	}
	// Valid header and zstd frame around garbage flatbuffer bytes.
	for _, cut := range []int{len(raw) / 2, len(raw) - 3} {
		mem := storage.NewMemory(map[string][]byte{"repo": raw[:cut]})
		if _, err := Open(ctx, mem, nil); err == nil {
			t.Errorf("repo truncated at %d accepted", cut)
		}
	}
}

// mutateFile decompresses an icechunk file, flips bytes of the flatbuffer
// payload and re-wraps it, so the header and zstd frame stay valid.
func mutateFile(t *testing.T, raw []byte, seed int64) []byte {
	t.Helper()
	body, err := format.Decompress(raw[format.HeaderLen:])
	if err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewSource(seed))
	for i := 0; i < 1+r.Intn(8); i++ {
		body[r.Intn(len(body))] = byte(r.Intn(256))
	}
	enc, _ := zstd.NewWriter(nil)
	return append(append([]byte(nil), raw[:format.HeaderLen]...), enc.EncodeAll(body, nil)...)
}

// Corrupt flatbuffers must surface as errors, never as panics.
func TestMutatedFlatbuffersDoNotPanic(t *testing.T) {
	ctx := context.Background()
	src := storage.NewLocal("testdata/upstream/test-repo-v2")
	keys := []string{"repo"}
	for _, prefix := range []string{"snapshots/", "manifests/"} {
		ks, _ := src.List(ctx, prefix)
		keys = append(keys, ks...)
	}
	for _, key := range keys {
		raw, err := src.Get(ctx, key, nil)
		if err != nil {
			t.Fatal(err)
		}
		for seed := int64(0); seed < 300; seed++ {
			bad := mutateFile(t, raw, seed)
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("%s seed %d: panic: %v", key, seed, r)
					}
				}()
				switch {
				case key == "repo":
					parseRepoInfo(bad)
				case strings.HasPrefix(key, "snapshots/"):
					s, err := parseSnapshot(bad)
					if err != nil {
						return
					}
					_, _ = s.ID(), s.Message()
					s.Metadata()
					s.ManifestFiles()
					s.info()
					if nodes, err := s.Nodes(); err == nil {
						for _, n := range nodes {
							s.Node(n.Path)
						}
					}
					s.Node("/group1/big_chunks")
				default:
					m, err := parseManifest(bad)
					if err != nil {
						return
					}
					var ids []NodeID
					for i := 0; i < 4; i++ {
						var id NodeID
						id[0] = byte(i)
						ids = append(ids, id)
					}
					func() {
						defer recoverFormat(&err)
						for i := 0; i < m.root.ArraysLength(); i++ {
							var am fbs.ArrayManifest
							m.root.Arrays(&am, i)
							var id NodeID
							copy(id[:], am.NodeId(nil).Bytes())
							ids = append(ids, id)
						}
					}()
					for _, id := range ids {
						m.forEach(id, func([]uint32, *ChunkRef) error { return nil })
						m.lookup(id, []uint32{0, 0})
						m.lookup(id, []uint32{1})
					}
				}
			}()
		}
	}
}

// Loads are coalesced within a scope but never across scopes.
func TestCacheScopes(t *testing.T) {
	c := newAssetCache(1 << 20)
	key := cacheKey{kind: 'm'}
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
	go func() { v, _ := c.get(ctxA, key, load("a1")); results <- v }()
	<-loads                                                            // a1 is in flight
	go func() { v, _ := c.get(ctxA, key, load("a2")); results <- v }() // same scope: must not load
	go func() { v, _ := c.get(ctxB, key, load("b1")); results <- v }() // other scope: loads itself
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
	if v, _ := c.get(context.Background(), key, load("late")); v != "a1" && v != "b1" {
		t.Errorf("cached value = %v, want a1 or b1", v)
	}
	close(loads)
	for name := range loads {
		t.Errorf("unexpected load %s", name)
	}
}

func TestResolveVirtual(t *testing.T) {
	r := &Repository{containers: []virtualContainer{
		{prefix: "file:///data/", store: storage.NewMemory(nil)},
		{prefix: "https://host/files/", store: storage.NewMemory(nil)},
	}}
	for loc, want := range map[string]string{
		"file:///data/a/b.nc":                      "a/b.nc",
		"file:///data/my%20file.nc":                "my file.nc",
		"https://host/files/x.bin?versionId=3#f":   "x.bin",
		"https://host/files/sub%2Fdir/x.bin?a=b/c": "sub/dir/x.bin",
	} {
		_, key, err := r.resolveVirtual(loc)
		if err != nil || key != want {
			t.Errorf("resolveVirtual(%q) = %q, %v; want %q", loc, key, err, want)
		}
	}
	for _, loc := range []string{
		"file:///data/../etc/passwd",
		"file:///data/sub/../../etc/passwd",
		"file:///data/%2e%2e/etc/passwd",
		"https://host/files/./x",
	} {
		if _, key, err := r.resolveVirtual(loc); !errors.Is(err, ErrFormat) {
			t.Errorf("resolveVirtual(%q) = %q, %v; want a rejection", loc, key, err)
		}
	}
	if _, _, err := r.resolveVirtual("s3://elsewhere/x"); !errors.Is(err, ErrNoVirtualContainer) {
		t.Errorf("unmapped location: %v", err)
	}
}

// A waiter whose own context is fine retries when the shared load failed
// only because the loading caller's context was cancelled.
func TestCacheRetriesAfterLoaderCancellation(t *testing.T) {
	c := newAssetCache(1 << 20)
	key := cacheKey{kind: 's'}
	started, release := make(chan struct{}), make(chan struct{})
	go c.get(context.Background(), key, func() (any, int64, error) {
		close(started)
		<-release
		return nil, 0, context.Canceled
	})
	<-started
	got := make(chan any)
	go func() {
		v, err := c.get(context.Background(), key, func() (any, int64, error) { return "fresh", 1, nil })
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
