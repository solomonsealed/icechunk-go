package icechunk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

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

// PrunedLogsFor exposes a snapshot's pruned ancestor tx logs to tests.
func PrunedLogsFor(t *testing.T, dir string, id SnapshotID) []string {
	raw, err := os.ReadFile(filepath.Join(dir, "repo"))
	if err != nil {
		t.Fatal(err)
	}
	doc, err := parseRepoDoc(raw)
	if err != nil {
		t.Fatal(err)
	}
	i, ok := doc.snapshotIndex(id)
	if !ok {
		return nil
	}
	var out []string
	for _, p := range doc.snapshots[i].pruned {
		out = append(out, p.String())
	}
	return out
}

// The ops log honours num_updates_per_repo_info_file from the repo config.
func TestOpsLogLimitFromConfig(t *testing.T) {
	cfg, err := format.EncodeFlexBuffer(map[string]any{"num_updates_per_repo_info_file": uint64(3)})
	if err != nil {
		t.Fatal(err)
	}
	doc := &repoDoc{hasConfig: true, config: cfg}
	if doc.updatesLimit() != 3 {
		t.Fatalf("limit = %d", doc.updatesLimit())
	}
	for i := 0; i < 5; i++ {
		doc.pushUpdate(docUpdate{typ: fbs.UpdateTypeGCRanUpdate, updatedAt: uint64(i + 1)}, fmt.Sprintf("repo.backup%d", i), doc.updatesLimit())
	}
	if len(doc.updates) != 3 || doc.repoBeforeUpdates == nil || *doc.repoBeforeUpdates != "repo.backup2" { // u1 was pushed out; its backup (written at push 2) still holds u1 and u0
		t.Errorf("updates %d, before %v", len(doc.updates), doc.repoBeforeUpdates)
	}
	if (&repoDoc{}).updatesLimit() != 1000 {
		t.Error("default limit")
	}
}

// Spec v1 ref keys are object_store paths: upstream percent-encodes them.
func TestV1RefKeys(t *testing.T) {
	for name, want := range map[string]string{
		"main":       "refs/branch.main/ref.json",
		"ünïcode":    "refs/branch.%C3%BCn%C3%AFcode/ref.json",
		"with space": "refs/branch.with space/ref.json",
		"a%b*?#":     "refs/branch.a%25b%2A%3F%23/ref.json",
		"tab\t":      "refs/branch.tab%09/ref.json",
		"":           "refs/branch./ref.json",
	} {
		if got := v1BranchKey(name); got != want {
			t.Errorf("v1BranchKey(%q) = %q, want %q", name, got, want)
		}
	}
	if got := objectStorePart(".."); got != "%2E%2E" {
		t.Errorf("objectStorePart(..) = %q", got)
	}
	repo, err := Open(context.Background(), storage.NewLocal("testdata/generated/features-v1"), nil)
	if err != nil {
		t.Skip(err)
	}
	if _, err := repo.LookupBranch(context.Background(), "ünïcode"); err != nil {
		t.Errorf("branch ünïcode: %v", err)
	}
}

// OpsLog follows repo_before_updates through older repo info files.
func TestOpsLogFollowsOlderFiles(t *testing.T) {
	ctx := context.Background()
	repo, err := Open(ctx, storage.NewLocal("testdata/generated/features-v2"), nil)
	if err != nil {
		t.Skip(err)
	}
	info, err := repo.RepoInfo(ctx)
	if err != nil || info.RepoBeforeUpdates == "" {
		t.Fatalf("fixture should have older repo info files: %v", err)
	}
	var kinds []string
	for u, err := range repo.OpsLog(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		kinds = append(kinds, u.Kind)
	}
	if len(kinds) <= len(info.Updates) || kinds[len(kinds)-1] != "RepoInitialized" {
		t.Errorf("ops log has %d updates (latest file %d), last %q", len(kinds), len(info.Updates), kinds[len(kinds)-1])
	}
}

// ListPrefix lists the subtree of the node a prefix names, as upstream does.
func TestListPrefixNamesANode(t *testing.T) {
	ctx := context.Background()
	repo, err := Open(ctx, storage.NewLocal("testdata/generated/features-v2"), nil)
	if err != nil {
		t.Skip(err)
	}
	s, err := repo.ReadonlySession(ctx, AtTag("v-c2"))
	if err != nil {
		t.Fatal(err)
	}
	keys, err := s.Store().ListPrefix(ctx, "order/a")
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if k != "order/a/zarr.json" && !strings.HasPrefix(k, "order/a/") {
			t.Errorf("ListPrefix(order/a) lists %q", k)
		}
	}
	if _, err := s.Store().ListPrefix(ctx, "ord"); !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("ListPrefix(ord): %v, want ErrNodeNotFound", err)
	}
}

// splitSizes reads upstream's manifest.splitting config: the first matching
// array condition applies, and in it the first condition matching each
// dimension. The config is the one upstream stored for these rules (checked
// against icechunk-python: /first splits by 2 chunks, /second by 4).
func TestSplitSizes(t *testing.T) {
	var cfg map[string]any
	raw := `{"manifest": {"splitting": {"split_sizes": [
		[{"or": [{"name_matches": {"regex": "^zz$"}}, {"and": [{"path_matches": {"regex": "first"}}, "any_array"]}]},
		 [{"condition": "Any", "num_chunks": 2}, {"condition": {"Axis": 0}, "num_chunks": 3}]],
		[{"path_matches": {"regex": ".*"}},
		 [{"condition": {"Axis": 0}, "num_chunks": 4}, {"condition": {"DimensionName": "x"}, "num_chunks": 5}]]]}}}`
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatal(err)
	}
	r := &Repository{info: &RepoInfo{Config: cfg}}
	node := func(path string, dims ...string) *Node {
		a := &ArrayInfo{DimensionNames: dims}
		for range dims {
			a.Shape = append(a.Shape, DimensionShape{ArrayLength: 9, NumChunks: 9})
		}
		return &Node{Path: path, Type: ArrayNode, Array: a}
	}
	for _, c := range []struct {
		n    *Node
		want []uint32
	}{
		{node("/first", ""), []uint32{2}},
		{node("/second", "", "x", "y"), []uint32{4, 5, 0}},
		{node("/zz", "", ""), []uint32{2, 2}},
	} {
		if got := r.splitSizes(c.n); !slices.Equal(got, c.want) {
			t.Errorf("splitSizes(%s) = %v, want %v", c.n.Path, got, c.want)
		}
	}
}
