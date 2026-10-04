package icechunk_test

// These tests read the repositories that upstream Icechunk keeps as
// on-disk compatibility fixtures (icechunk-python/tests/data), and assert
// the same facts as upstream's tests/test_can_read_old.py.

import (
	"context"
	"errors"
	"math"
	"slices"
	"testing"

	icechunk "github.com/solomonsealed/icechunk-go"
	"github.com/solomonsealed/icechunk-go/storage"
	"github.com/solomonsealed/icechunk-go/zarr"
)

func openFixture(t *testing.T, name string, opts *icechunk.Options) *icechunk.Repository {
	t.Helper()
	repo, err := icechunk.Open(context.Background(), storage.NewLocal("testdata/upstream/"+name), opts)
	if err != nil {
		t.Fatalf("open %s: %v", name, err)
	}
	return repo
}

func messages(t *testing.T, repo *icechunk.Repository, v icechunk.Version) []string {
	t.Helper()
	var out []string
	for si, err := range repo.Ancestry(context.Background(), v) {
		if err != nil {
			t.Fatalf("ancestry of %v: %v", v, err)
		}
		out = append(out, si.Message)
	}
	return out
}

func readFloats(t *testing.T, s *icechunk.Session, path string) []float64 {
	t.Helper()
	ctx := context.Background()
	arr, err := s.OpenArray(ctx, path)
	if err != nil {
		t.Fatalf("open array %s: %v", path, err)
	}
	nd, err := arr.ReadAll(ctx)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	v, err := nd.Float64s()
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func allEqual(vs []float64, want float64) bool {
	for _, v := range vs {
		if !(v == want || (math.IsNaN(want) && math.IsNaN(v))) {
			return false
		}
	}
	return len(vs) > 0
}

func TestCanReadOldRepo(t *testing.T) {
	for _, fixture := range []string{"test-repo-v1", "test-repo-v2", "test-repo-v2-migrated"} {
		t.Run(fixture, func(t *testing.T) {
			ctx := context.Background()
			virtual := storage.NewMemory(nil)
			repo := openFixture(t, fixture, &icechunk.Options{
				VirtualChunkContainers: map[string]storage.Storage{"s3://testbucket/": virtual},
			})
			wantSpec := 2
			if fixture == "test-repo-v1" {
				wantSpec = 1
			}
			if repo.SpecVersion() != wantSpec {
				t.Fatalf("spec version = %d, want %d", repo.SpecVersion(), wantSpec)
			}

			mainHistory := []string{"set virtual chunk", "fill data", "empty structure", "Repository initialized"}
			if got := messages(t, repo, icechunk.AtBranch("main")); !slices.Equal(got, mainHistory) {
				t.Errorf("main history = %q", got)
			}
			branchHistory := append([]string{"some more structure", "delete a chunk"}, mainHistory...)
			if got := messages(t, repo, icechunk.AtBranch("my-branch")); !slices.Equal(got, branchHistory) {
				t.Errorf("my-branch history = %q", got)
			}
			if got := messages(t, repo, icechunk.AtTag("it also works!")); !slices.Equal(got, branchHistory) {
				t.Errorf("tag history = %q", got)
			}
			if got := messages(t, repo, icechunk.AtTag("it works!")); !slices.Equal(got, branchHistory[1:]) {
				t.Errorf("tag history = %q", got)
			}
			if _, err := repo.ReadonlySession(ctx, icechunk.AtTag("deleted")); !errors.Is(err, icechunk.ErrRefNotFound) {
				t.Errorf("deleted tag: err = %v, want ErrRefNotFound", err)
			}

			branches, err := repo.ListBranches(ctx)
			if err != nil || !slices.Equal(branches, []string{"main", "my-branch"}) {
				t.Errorf("branches = %q, %v", branches, err)
			}
			tags, err := repo.ListTags(ctx)
			if err != nil || !slices.Equal(tags, []string{"it also works!", "it works!"}) {
				t.Errorf("tags = %q, %v", tags, err)
			}

			s, err := repo.ReadonlySession(ctx, icechunk.AtBranch("my-branch"))
			if err != nil {
				t.Fatal(err)
			}
			store := s.Store()
			for dir, want := range map[string][]string{
				"":                                  {"group1", "group2", "zarr.json"},
				"group1":                            {"big_chunks", "small_chunks", "zarr.json"},
				"group2":                            {"group3", "zarr.json"},
				"group2/group3/group4":              {"group5", "zarr.json"},
				"group2/group3/group4/group5":       {"inner", "zarr.json"},
				"group2/group3/group4/group5/inner": {"c", "zarr.json"},
			} {
				got, err := store.ListDir(ctx, dir)
				if err != nil || !slices.Equal(got, want) {
					t.Errorf("ListDir(%q) = %q, %v; want %q", dir, got, err, want)
				}
			}

			// inner was never written: all fill value (NaN).
			if v := readFloats(t, s, "group2/group3/group4/group5/inner"); !allEqual(v, math.NaN()) || len(v) != 100 {
				t.Errorf("inner = %v", v)
			}
			// small_chunks: 5 inline chunks, the last one deleted (fill 8).
			if v := readFloats(t, s, "group1/small_chunks"); !slices.Equal(v, []float64{84, 84, 84, 84, 8}) {
				t.Errorf("small_chunks = %v", v)
			}
			ref, err := s.ChunkRef(ctx, "group1/small_chunks", []uint32{0})
			if err != nil || ref == nil || ref.Kind != icechunk.InlineChunk {
				t.Errorf("small_chunks c/0 ref = %+v, %v; want inline", ref, err)
			}

			// big_chunks c/0/0 is virtual, pointing at a copy of chunk c/0/1.
			ref, err = s.ChunkRef(ctx, "group1/big_chunks", []uint32{0, 0})
			if err != nil || ref == nil || ref.Kind != icechunk.VirtualChunk || ref.Location != "s3://testbucket/can_read_old/chunk-1" {
				t.Fatalf("virtual ref = %+v, %v", ref, err)
			}
			native, err := store.Get(ctx, "group1/big_chunks/c/0/1")
			if err != nil {
				t.Fatal(err)
			}
			if ref.Length != uint64(len(native)) {
				t.Errorf("virtual ref length %d, native chunk %d bytes", ref.Length, len(native))
			}
			nref, _ := s.ChunkRef(ctx, "group1/big_chunks", []uint32{0, 1})
			if nref == nil || nref.Kind != icechunk.NativeChunk {
				t.Errorf("c/0/1 ref = %+v; want native", nref)
			}
			virtual.Put("can_read_old/chunk-1", native)
			if v := readFloats(t, s, "group1/big_chunks"); !allEqual(v, 42) || len(v) != 100 {
				t.Errorf("big_chunks = %v", v)
			}

			// Group attributes come straight from zarr.json.
			attrs, err := s.Attributes("group1")
			if err != nil || attrs["this"] != "is a nice group" || attrs["size"] != 42.0 {
				t.Errorf("group1 attributes = %v, %v", attrs, err)
			}
		})
	}
}

func TestVirtualChunkNeedsContainer(t *testing.T) {
	ctx := context.Background()
	repo := openFixture(t, "test-repo-v2", nil)
	s, err := repo.ReadonlySession(ctx, icechunk.AtBranch("main"))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.GetChunk(ctx, "group1/big_chunks", []uint32{0, 0})
	if !errors.Is(err, icechunk.ErrNoVirtualContainer) {
		t.Fatalf("err = %v, want ErrNoVirtualContainer", err)
	}
}

func TestCanReadOldRepoWithManifestSplitting(t *testing.T) {
	for _, fixture := range []string{"split-repo-v1", "split-repo-v2", "split-repo-v2-migrated"} {
		t.Run(fixture, func(t *testing.T) {
			ctx := context.Background()
			repo := openFixture(t, fixture, nil)
			var history []icechunk.SnapshotInfo
			for si, err := range repo.Ancestry(ctx, icechunk.AtBranch("main")) {
				if err != nil {
					t.Fatal(err)
				}
				history = append(history, si)
			}
			slices.Reverse(history)
			want := []struct {
				msg       string
				manifests int
			}{
				{"Repository initialized", 0},
				{"empty structure", 0},
				{"write data", 9},
				{"write data again", 9},
				{"write data again with more splits", 17},
			}
			if len(history) != len(want) {
				t.Fatalf("history has %d snapshots, want %d", len(history), len(want))
			}
			for i, w := range want {
				if history[i].Message != w.msg {
					t.Errorf("snapshot %d message = %q, want %q", i, history[i].Message, w.msg)
				}
				snap, err := repo.Snapshot(ctx, history[i].ID)
				if err != nil {
					t.Fatal(err)
				}
				files, err := snap.ManifestFiles()
				if err != nil || len(files) != w.manifests {
					t.Errorf("%q: %d manifest files (%v), want %d", w.msg, len(files), err, w.manifests)
				}
			}

			s, err := repo.ReadonlySession(ctx, icechunk.AtBranch("main"))
			if err != nil {
				t.Fatal(err)
			}
			n, err := s.Node("/group1/split")
			if err != nil {
				t.Fatal(err)
			}
			if len(n.Array.Manifests) < 2 {
				t.Errorf("split array has %d manifest refs, expected several", len(n.Array.Manifests))
			}
			if v := readFloats(t, s, "group1/split"); !allEqual(v, 14) || len(v) != 100 {
				t.Errorf("split = %v", v)
			}
			if v := readFloats(t, s, "group1/small_chunks"); !allEqual(v, 3) || len(v) != 5 {
				t.Errorf("small_chunks = %v", v)
			}
			// Every chunk ref is reachable through the extents of its manifest.
			count := 0
			for e, err := range s.ChunkRefs(ctx, "group1/split") {
				if err != nil {
					t.Fatal(err)
				}
				if e.Ref.Kind != icechunk.NativeChunk {
					t.Errorf("chunk %v is %v", e.Coords, e.Ref.Kind)
				}
				count++
			}
			if count != 16 {
				t.Errorf("split has %d chunk refs, want 16", count)
			}
		})
	}
}

func TestReadExpiredRepo(t *testing.T) {
	for _, fixture := range []string{"expire-repo-v1-by-2.0.5", "expire-repo-v2-by-working-copy"} {
		t.Run(fixture, func(t *testing.T) {
			ctx := context.Background()
			repo := openFixture(t, fixture, nil)
			branches, err := repo.ListBranches(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Contains(branches, "main") || !slices.Contains(branches, "feature") ||
				slices.Contains(branches, "doomed1") || slices.Contains(branches, "doomed2") {
				t.Errorf("branches = %q", branches)
			}
			tags, err := repo.ListTags(ctx)
			if err != nil || !slices.Contains(tags, "protect-b") {
				t.Errorf("tags = %q, %v", tags, err)
			}
			for v, want := range map[icechunk.Version][]string{
				icechunk.AtBranch("main"):    {"write j", "Repository initialized"},
				icechunk.AtBranch("feature"): {"feature post-t2", "Repository initialized"},
				icechunk.AtTag("protect-b"):  {"write b", "Repository initialized"},
			} {
				if got := messages(t, repo, v); !slices.Equal(got, want) {
					t.Errorf("%v history = %q, want %q", v, got, want)
				}
			}
			s, err := repo.ReadonlySession(ctx, icechunk.AtBranch("main"))
			if err != nil {
				t.Fatal(err)
			}
			arr, err := s.OpenArray(ctx, "group1/data")
			if err != nil {
				t.Fatal(err)
			}
			for _, c := range []struct {
				row  uint64
				want float64
			}{{0, 99}, {5, 5}, {8, 8}, {11, 0}} {
				nd, err := arr.Read(ctx, []uint64{c.row, 0}, []uint64{1, 1})
				if err != nil {
					t.Fatal(err)
				}
				v, _ := nd.Float64s()
				if v[0] != c.want {
					t.Errorf("data[%d, 0] = %v, want %v", c.row, v[0], c.want)
				}
			}
			if v := readFloats(t, s, "group1/small"); !allEqual(v, 0) {
				t.Errorf("small = %v", v)
			}
		})
	}
}

func TestRepoInfoDetails(t *testing.T) {
	ctx := context.Background()
	repo := openFixture(t, "test-repo-v2", nil)
	info, err := repo.RepoInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Status.Availability != "online" {
		t.Errorf("status = %+v", info.Status)
	}
	if !slices.Contains(info.DeletedTags, "deleted") {
		t.Errorf("deleted tags = %q", info.DeletedTags)
	}
	if len(info.Updates) == 0 || info.Updates[len(info.Updates)-1].Kind != "RepoInitialized" {
		t.Errorf("ops log = %+v", info.Updates)
	}
	cfg, ok := info.Config.(map[string]any)
	if !ok {
		t.Fatalf("config = %#v", info.Config)
	}
	if v, ok := cfg["inline_chunk_threshold_bytes"]; !ok || v != uint64(12) && v != int64(12) {
		t.Errorf("inline_chunk_threshold_bytes = %#v", v)
	}
	initial := info.Snapshots
	found := false
	for _, si := range initial {
		if si.ID.String() == "1CECHNKREP0F1RSTCMT0" && si.ParentID == nil {
			found = true
		}
	}
	if !found {
		t.Errorf("initial snapshot 1CECHNKREP0F1RSTCMT0 missing or has a parent")
	}
	// The zarr layer sees the stored metadata.
	s, _ := repo.ReadonlySession(ctx, icechunk.AtBranch("main"))
	arr, err := s.OpenArray(ctx, "/group1/big_chunks")
	if err != nil {
		t.Fatal(err)
	}
	if arr.DataType().Name != "float32" || !slices.Equal(arr.Shape(), []uint64{10, 10}) || !slices.Equal(arr.ChunkGridShape(), []uint64{2, 2}) {
		t.Errorf("array = %s %v %v", arr.DataType(), arr.Shape(), arr.ChunkGridShape())
	}
	if arr.DataType().Kind != zarr.KindFloat {
		t.Errorf("kind = %v", arr.DataType().Kind)
	}
}
