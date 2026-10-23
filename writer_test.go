package icechunk_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	icechunk "github.com/solomonsealed/icechunk-go"
	"github.com/solomonsealed/icechunk-go/storage"
	zarr "github.com/solomonsealed/zarr-go"
)

// int32Array is a zarr.json for an uncompressed little-endian int32 array,
// so chunk bytes can be written without the zarr package.
func int32Array(shape, chunks []uint64) []byte {
	return fmt.Appendf(nil, `{"zarr_format":3,"node_type":"array","shape":%s,"data_type":"int32",
		"chunk_grid":{"name":"regular","configuration":{"chunk_shape":%s}},
		"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
		"fill_value":-1,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}],
		"attributes":{},"dimension_names":["x"]}`, jsonInts(shape), jsonInts(chunks))
}

func jsonInts(v []uint64) string {
	s := "["
	for i, x := range v {
		if i > 0 {
			s += ","
		}
		s += fmt.Sprint(x)
	}
	return s + "]"
}

func int32Bytes(vals ...int32) []byte {
	b := make([]byte, 4*len(vals))
	for i, v := range vals {
		binary.LittleEndian.PutUint32(b[4*i:], uint32(v))
	}
	return b
}

func readInt32s(t *testing.T, s *icechunk.Session, path string) []int32 {
	t.Helper()
	ctx := context.Background()
	arr, err := s.OpenArray(ctx, path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	nd, err := arr.ReadAll(ctx)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	v, err := nd.Values()
	if err != nil {
		t.Fatal(err)
	}
	return v.([]int32)
}

func TestWriteCommitRead(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemory(nil)
	repo, err := icechunk.Create(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := icechunk.Create(ctx, st, nil); !errors.Is(err, icechunk.ErrAlreadyExists) {
		t.Errorf("second Create: %v", err)
	}

	s, err := repo.WritableSession(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CreateGroup(ctx, "/", map[string]any{"title": "written by go"}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateGroup(ctx, "/data", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.SetMetadata(ctx, "/data/x", int32Array([]uint64{6}, []uint64{2})); err != nil {
		t.Fatal(err)
	}
	// Chunk 0 inline (8 bytes), chunk 1 native (forced by a big write below), chunk 2 missing.
	if err := s.SetChunk(ctx, "/data/x", []uint32{0}, int32Bytes(1, 2)); err != nil {
		t.Fatal(err)
	}
	if err := s.SetChunk(ctx, "/data/x", []uint32{1}, int32Bytes(3, 4)); err != nil {
		t.Fatal(err)
	}
	if err := s.SetChunk(ctx, "/data/x", []uint32{3}, int32Bytes(9, 9)); !errors.Is(err, icechunk.ErrInvalidChunkCoords) {
		t.Errorf("out-of-grid chunk: %v", err)
	}
	// Reads inside the session see uncommitted data.
	if got := readInt32s(t, s, "/data/x"); !slices.Equal(got, []int32{1, 2, 3, 4, -1, -1}) {
		t.Errorf("uncommitted read = %v", got)
	}
	big := make([]int32, 1000)
	for i := range big {
		big[i] = int32(i)
	}
	if err := s.SetMetadata(ctx, "/data/big", int32Array([]uint64{1000}, []uint64{1000})); err != nil {
		t.Fatal(err)
	}
	if err := s.SetChunk(ctx, "/data/big", []uint32{0}, int32Bytes(big...)); err != nil {
		t.Fatal(err)
	}
	if err := s.SetVirtualRef(ctx, "/data/x", []uint32{2}, icechunk.VirtualRef{Location: "s3://bucket/file.bin", Offset: 4, Length: 8, ETag: "abc"}); err != nil {
		t.Fatal(err)
	}
	c1, err := s.Commit(ctx, "first commit", &icechunk.CommitOptions{Metadata: map[string]any{"author": "go", "n": 1}})
	if err != nil {
		t.Fatal(err)
	}
	if s.Writable() {
		t.Error("session still writable after commit")
	}

	// A fresh handle sees the commit.
	virtual := storage.NewMemory(map[string][]byte{"file.bin": int32Bytes(0, 5, 6, 0)})
	repo2, err := icechunk.Open(ctx, st, &icechunk.Options{VirtualChunkContainers: map[string]storage.Storage{"s3://bucket/": virtual}})
	if err != nil {
		t.Fatal(err)
	}
	r, err := repo2.ReadonlySession(ctx, icechunk.AtBranch("main"))
	if err != nil {
		t.Fatal(err)
	}
	if r.SnapshotID() != c1 {
		t.Errorf("main = %s, want %s", r.SnapshotID(), c1)
	}
	if got := readInt32s(t, r, "/data/x"); !slices.Equal(got, []int32{1, 2, 3, 4, 5, 6}) {
		t.Errorf("committed read = %v", got)
	}
	if got := readInt32s(t, r, "/data/big"); !slices.Equal(got, big) {
		t.Errorf("big array mismatch")
	}
	ref, _ := r.ChunkRef(ctx, "/data/big", []uint32{0})
	if ref == nil || ref.Kind != icechunk.NativeChunk {
		t.Errorf("big chunk ref = %+v, want native", ref)
	}
	ref, _ = r.ChunkRef(ctx, "/data/x", []uint32{0})
	if ref == nil || ref.Kind != icechunk.InlineChunk {
		t.Errorf("small chunk ref = %+v, want inline", ref)
	}
	attrs, _ := r.Attributes("/")
	if attrs["title"] != "written by go" {
		t.Errorf("root attributes = %v", attrs)
	}
	var log []icechunk.SnapshotInfo
	for si, err := range repo2.Ancestry(ctx, icechunk.AtBranch("main")) {
		if err != nil {
			t.Fatal(err)
		}
		log = append(log, si)
	}
	if len(log) != 2 || log[0].Message != "first commit" || log[1].Message != "Repository initialized" ||
		log[0].Metadata["author"] != "go" || log[1].ID != icechunk.InitialSnapshotID {
		t.Errorf("history = %+v", log)
	}

	// Second commit: update metadata (shrink), delete a chunk and a node.
	s, _ = repo2.WritableSession(ctx, "main")
	if err := s.SetMetadata(ctx, "/data/x", int32Array([]uint64{4}, []uint64{2})); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteChunk(ctx, "/data/x", []uint32{0}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteNode(ctx, "/data/big"); err != nil {
		t.Fatal(err)
	}
	c2, err := s.Commit(ctx, "second", nil)
	if err != nil {
		t.Fatal(err)
	}
	r, _ = repo2.ReadonlySession(ctx, icechunk.AtBranch("main"))
	if got := readInt32s(t, r, "/data/x"); !slices.Equal(got, []int32{-1, -1, 3, 4}) {
		t.Errorf("after shrink/delete = %v", got)
	}
	if _, err := r.Node("/data/big"); !errors.Is(err, icechunk.ErrNodeNotFound) {
		t.Errorf("deleted node: %v", err)
	}
	n, _ := r.Node("/data/x")
	if len(n.Array.Manifests) != 1 || n.Array.Manifests[0].Extents[0] != (icechunk.ChunkRange{From: 1, To: 2}) {
		t.Errorf("manifest refs after shrink = %+v", n.Array.Manifests)
	}
	// The old snapshot is untouched.
	old, _ := repo2.ReadonlySession(ctx, icechunk.AtSnapshot(c1))
	if got := readInt32s(t, old, "/data/x"); !slices.Equal(got, []int32{1, 2, 3, 4, 5, 6}) {
		t.Errorf("time travel = %v", got)
	}

	// Refs.
	if err := repo2.CreateBranch(ctx, "dev", c1); err != nil {
		t.Fatal(err)
	}
	if err := repo2.CreateBranch(ctx, "dev", c1); !errors.Is(err, icechunk.ErrAlreadyExists) {
		t.Errorf("duplicate branch: %v", err)
	}
	if err := repo2.CreateTag(ctx, "v1", c2); err != nil {
		t.Fatal(err)
	}
	if err := repo2.DeleteTag(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if err := repo2.CreateTag(ctx, "v1", c2); !errors.Is(err, icechunk.ErrAlreadyExists) {
		t.Errorf("recreating a deleted tag: %v", err)
	}
	if err := repo2.ResetBranch(ctx, "dev", c2); err != nil {
		t.Fatal(err)
	}
	if err := repo2.DeleteBranch(ctx, "dev"); err != nil {
		t.Fatal(err)
	}
	if err := repo2.DeleteBranch(ctx, "main"); !errors.Is(err, icechunk.ErrCannotDeleteMain) {
		t.Errorf("deleting main: %v", err)
	}
	info, err := repo2.RepoInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for _, u := range info.Updates {
		kinds = append(kinds, u.Kind)
	}
	want := []string{"BranchDeleted", "BranchReset", "TagDeleted", "TagCreated", "BranchCreated", "NewCommit", "NewCommit", "RepoInitialized"}
	if !slices.Equal(kinds, want) {
		t.Errorf("ops log = %v, want %v", kinds, want)
	}
	backups, _ := st.List(ctx, "overwritten/")
	if len(backups) != 7 {
		t.Errorf("%d backups of the repo file, want 7", len(backups))
	}
}

func TestCommitConflictsAndRebase(t *testing.T) {
	ctx := context.Background()
	repo, err := icechunk.Create(ctx, storage.NewMemory(nil), nil)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := repo.WritableSession(ctx, "main")
	s.SetMetadata(ctx, "/x", int32Array([]uint64{8}, []uint64{2}))
	if _, err := s.Commit(ctx, "create", nil); err != nil {
		t.Fatal(err)
	}

	a, _ := repo.WritableSession(ctx, "main")
	b, _ := repo.WritableSession(ctx, "main")
	c, _ := repo.WritableSession(ctx, "main")
	a.SetChunk(ctx, "/x", []uint32{0}, int32Bytes(1, 1))
	b.SetChunk(ctx, "/x", []uint32{1}, int32Bytes(2, 2))
	c.SetChunk(ctx, "/x", []uint32{0}, int32Bytes(3, 3))
	if _, err := a.Commit(ctx, "a", nil); err != nil {
		t.Fatal(err)
	}
	// b changed different chunks: without rebase it conflicts, with rebase it lands.
	var ce *icechunk.ConflictError
	if _, err := b.Commit(ctx, "b", nil); !errors.As(err, &ce) {
		t.Fatalf("b without rebase: %v", err)
	}
	md := map[string]any{"who": "b", "__icechunk": map[string]any{"note": "kept"}}
	if _, err := b.Commit(ctx, "b", &icechunk.CommitOptions{Rebase: true, Metadata: md}); err != nil {
		t.Fatalf("b with rebase: %v", err)
	}
	// c wrote the same chunk as a: a real conflict.
	if _, err := c.Commit(ctx, "c", &icechunk.CommitOptions{Rebase: true}); !errors.Is(err, icechunk.ErrConflict) {
		t.Fatalf("c: %v", err)
	}
	r, _ := repo.ReadonlySession(ctx, icechunk.AtBranch("main"))
	if got := readInt32s(t, r, "/x"); !slices.Equal(got, []int32{1, 1, 2, 2, -1, -1, -1, -1}) {
		t.Errorf("after rebase = %v", got)
	}
	// Like upstream, rebasing commits record the rebases they took in
	// "__icechunk", keeping the caller's entries.
	for si, err := range repo.Ancestry(ctx, icechunk.AtBranch("main")) {
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]any{"who": "b", "__icechunk": map[string]any{"note": "kept", "rebase_attempts": int64(1)}}
		if !reflect.DeepEqual(si.Metadata, want) {
			t.Errorf("rebased commit metadata = %#v, want %#v", si.Metadata, want)
		}
		break
	}
	empty, _ := repo.WritableSession(ctx, "main")
	if _, err := empty.Commit(ctx, "nothing", nil); !errors.Is(err, icechunk.ErrNoChanges) {
		t.Errorf("empty commit: %v", err)
	}
}

// Many writers committing at once all succeed with rebase, and the repo
// info file stays consistent (no lost updates).
func TestConcurrentCommits(t *testing.T) {
	ctx := context.Background()
	for name, st := range map[string]storage.Storage{"memory": storage.NewMemory(nil), "local": storage.NewLocal(t.TempDir())} {
		t.Run(name, func(t *testing.T) {
			repo, err := icechunk.Create(ctx, st, nil)
			if err != nil {
				t.Fatal(err)
			}
			s, _ := repo.WritableSession(ctx, "main")
			s.SetMetadata(ctx, "/x", int32Array([]uint64{16}, []uint64{1}))
			if _, err := s.Commit(ctx, "create", nil); err != nil {
				t.Fatal(err)
			}
			const writers = 8
			errs := make(chan error, writers)
			for i := 0; i < writers; i++ {
				go func(i int) {
					r, err := icechunk.Open(ctx, st, nil) // separate handles, like separate processes
					if err != nil {
						errs <- err
						return
					}
					s, err := r.WritableSession(ctx, "main")
					if err != nil {
						errs <- err
						return
					}
					if err := s.SetChunk(ctx, "/x", []uint32{uint32(i)}, int32Bytes(int32(i))); err != nil {
						errs <- err
						return
					}
					_, err = s.Commit(ctx, fmt.Sprintf("writer %d", i), &icechunk.CommitOptions{Rebase: true})
					errs <- err
				}(i)
			}
			for i := 0; i < writers; i++ {
				if err := <-errs; err != nil {
					t.Error(err)
				}
			}
			r, _ := icechunk.Open(ctx, st, nil)
			rs, _ := r.ReadonlySession(ctx, icechunk.AtBranch("main"))
			got := readInt32s(t, rs, "/x")
			for i := 0; i < writers; i++ {
				if got[i] != int32(i) {
					t.Errorf("chunk %d = %d (lost update?)", i, got[i])
				}
			}
			n := 0
			for _, err := range r.Ancestry(ctx, icechunk.AtBranch("main")) {
				if err != nil {
					t.Fatal(err)
				}
				n++
			}
			if n != writers+2 {
				t.Errorf("history has %d commits, want %d", n, writers+2)
			}
		})
	}
}

// The Zarr key/value view accepts writes, like a Zarr store.
func TestStoreSetDelete(t *testing.T) {
	ctx := context.Background()
	repo, _ := icechunk.Create(ctx, storage.NewMemory(nil), nil)
	s, _ := repo.WritableSession(ctx, "main")
	st := s.Store()
	if err := st.Set(ctx, "zarr.json", []byte(`{"zarr_format":3,"node_type":"group","attributes":{}}`)); err != nil {
		t.Fatal(err)
	}
	if err := st.Set(ctx, "a/zarr.json", int32Array([]uint64{4}, []uint64{2})); err != nil {
		t.Fatal(err)
	}
	if err := st.Set(ctx, "a/c/1", int32Bytes(7, 8)); err != nil {
		t.Fatal(err)
	}
	if got := readInt32s(t, s, "a"); !slices.Equal(got, []int32{-1, -1, 7, 8}) {
		t.Errorf("after Set = %v", got)
	}
	keys, _ := st.ListPrefix(ctx, "")
	if !slices.Equal(keys, []string{"a/c/1", "a/zarr.json", "zarr.json"}) {
		t.Errorf("keys = %v", keys)
	}
	if err := st.Delete(ctx, "a/c/1"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := st.Exists(ctx, "a/c/1"); ok {
		t.Error("deleted chunk still exists")
	}
	if err := st.Delete(ctx, "a/zarr.json"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Node("/a"); !errors.Is(err, icechunk.ErrNodeNotFound) {
		t.Errorf("deleted node: %v", err)
	}
	if _, err := s.Commit(ctx, "via store", nil); err != nil {
		t.Fatal(err)
	}
	r, _ := repo.ReadonlySession(ctx, icechunk.AtBranch("main"))
	if err := r.Store().Set(ctx, "b/zarr.json", nil); !errors.Is(err, icechunk.ErrReadOnlySession) {
		t.Errorf("write to read-only session: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Regression tests for issues found in review of the writer

// Commit metadata is stored as JSON values: []byte would otherwise become
// a FlexBuffer blob that upstream cannot parse.
func TestCommitMetadataIsJSON(t *testing.T) {
	ctx := context.Background()
	repo, _ := icechunk.Create(ctx, storage.NewMemory(nil), nil)
	s, _ := repo.WritableSession(ctx, "main")
	s.CreateGroup(ctx, "/", nil)
	type info struct {
		Name string `json:"name"`
	}
	_, err := s.Commit(ctx, "m", &icechunk.CommitOptions{Metadata: map[string]any{
		"digest": []byte{1, 2, 3}, "struct": info{"x"}, "big": uint64(1)<<63 + 5,
	}})
	if err != nil {
		t.Fatal(err)
	}
	for si, err := range repo.Ancestry(ctx, icechunk.AtBranch("main")) {
		if err != nil {
			t.Fatal(err)
		}
		md := si.Metadata
		if md["digest"] != "AQID" || md["struct"].(map[string]any)["name"] != "x" || md["big"] != uint64(1)<<63+5 {
			t.Errorf("metadata = %#v", md)
		}
		break
	}
	s, _ = repo.WritableSession(ctx, "main")
	s.CreateGroup(ctx, "/g", nil)
	if _, err := s.Commit(ctx, "nan", &icechunk.CommitOptions{Metadata: map[string]any{"bad": func() {}}}); err == nil {
		t.Error("non-JSON metadata accepted")
	}
}

// Reads racing with a commit must not panic (run with -race).
func TestReadsDuringCommit(t *testing.T) {
	ctx := context.Background()
	repo, _ := icechunk.Create(ctx, storage.NewMemory(nil), nil)
	for round := 0; round < 20; round++ {
		s, _ := repo.WritableSession(ctx, "main")
		path := "/a" + string(rune('a'+round))
		s.SetMetadata(ctx, path, int32Array([]uint64{4}, []uint64{2}))
		s.SetChunk(ctx, path, []uint32{0}, int32Bytes(1, 2))
		var wg sync.WaitGroup
		stop := make(chan struct{})
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					s.Node(path)
					s.Nodes()
					s.ChunkRef(ctx, path, []uint32{0})
					for range s.ChunkRefs(ctx, path) {
					}
					s.SnapshotID()
					s.HasUncommittedChanges()
				}
			}()
		}
		if _, err := s.Commit(ctx, "c", nil); err != nil {
			t.Fatal(err)
		}
		close(stop)
		wg.Wait()
	}
}

// A new node must not end up under an array, or under a parent group that
// a concurrent commit deleted.
func TestRebaseRejectsInvalidParents(t *testing.T) {
	ctx := context.Background()
	repo, _ := icechunk.Create(ctx, storage.NewMemory(nil), nil)
	s, _ := repo.WritableSession(ctx, "main")
	s.CreateGroup(ctx, "/g", nil)
	s.CreateGroup(ctx, "/h", nil)
	if _, err := s.Commit(ctx, "groups", nil); err != nil {
		t.Fatal(err)
	}
	a, _ := repo.WritableSession(ctx, "main")
	a.CreateGroup(ctx, "/g/b", nil)
	c, _ := repo.WritableSession(ctx, "main")
	c.CreateGroup(ctx, "/h/x", nil)
	other, _ := repo.WritableSession(ctx, "main")
	other.DeleteNode(ctx, "/g")
	other.SetMetadata(ctx, "/g", int32Array([]uint64{2}, []uint64{1}))
	other.DeleteNode(ctx, "/h")
	if _, err := other.Commit(ctx, "replace /g with an array, delete /h", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Commit(ctx, "a", &icechunk.CommitOptions{Rebase: true}); !errors.Is(err, icechunk.ErrConflict) {
		t.Errorf("new node under an array: %v", err)
	}
	if _, err := c.Commit(ctx, "c", &icechunk.CommitOptions{Rebase: true}); !errors.Is(err, icechunk.ErrConflict) {
		t.Errorf("new node under a deleted group: %v", err)
	}
	// Creating a node inside an array is refused up front.
	d, _ := repo.WritableSession(ctx, "main")
	if err := d.CreateGroup(ctx, "/g/inside", nil); !errors.Is(err, icechunk.ErrAlreadyExists) {
		t.Errorf("group inside an array: %v", err)
	}
	if err := d.SetChunk(ctx, "/g", []uint32{0}, nil); err == nil {
		t.Error("empty chunk accepted")
	}
}

// Rebase consults the transaction logs of expired ancestors folded into a
// snapshot (pruned_ancestor_tx_logs, spec 2.1).
func TestRebaseReadsPrunedTxLogs(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	if err := os.CopyFS(dir, os.DirFS("testdata/upstream/expire-repo-v2-by-working-copy")); err != nil {
		t.Fatal(err)
	}
	st := storage.NewLocal(dir)
	repo, err := icechunk.Open(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateBranch(ctx, "b", icechunk.InitialSnapshotID); err != nil {
		t.Fatal(err)
	}
	try := func() error {
		repo.ResetBranch(ctx, "b", icechunk.InitialSnapshotID)
		s, _ := repo.WritableSession(ctx, "b")
		// SetMetadata, not CreateGroup: a new root group would (rightly)
		// collide with the tip's root.
		s.SetMetadata(ctx, "/fresh", []byte(`{"zarr_format":3,"node_type":"group","attributes":{}}`))
		tip, _ := repo.LookupBranch(ctx, "main")
		repo.ResetBranch(ctx, "b", tip) // someone moved the branch meanwhile
		_, err := s.Commit(ctx, "rebased", &icechunk.CommitOptions{Rebase: true})
		return err
	}
	if err := try(); err != nil {
		t.Fatalf("rebase over pruned history: %v", err)
	}
	// Remove a pruned ancestor's log: the rebase can no longer be verified.
	main, _ := repo.LookupBranch(ctx, "main")
	pruned := icechunk.PrunedLogsFor(t, dir, main)
	if len(pruned) == 0 {
		t.Skip("fixture has no pruned ancestor logs on main")
	}
	if err := os.Remove(filepath.Join(dir, "transactions", pruned[0])); err != nil {
		t.Fatal(err)
	}
	if err := try(); !errors.Is(err, icechunk.ErrConflict) {
		t.Errorf("rebase with a missing pruned log: %v", err)
	}
}

// clockStore reports object times from a clock that is off by skew.
type clockStore struct {
	*storage.Memory
	skew time.Duration
}

func (c clockStore) LastModified(ctx context.Context, key string) (time.Time, error) {
	return time.Now().Add(c.skew), nil
}

func TestClockSkewGuard(t *testing.T) {
	ctx := context.Background()
	mem := storage.NewMemory(nil)
	repo, _ := icechunk.Create(ctx, clockStore{mem, time.Minute}, nil)
	s, _ := repo.WritableSession(ctx, "main")
	s.CreateGroup(ctx, "/", nil)
	if _, err := s.Commit(ctx, "fine", nil); err != nil {
		t.Fatalf("1 minute skew: %v", err)
	}
	repo2, _ := icechunk.Open(ctx, clockStore{mem, -time.Hour}, nil)
	if err := repo2.CreateBranch(ctx, "x", icechunk.InitialSnapshotID); !errors.Is(err, icechunk.ErrClockSkew) {
		t.Errorf("1 hour skew: %v", err)
	}
}

// Repositories whose status is not "online" refuse writes.
func TestReadOnlyStatusRefusesWrites(t *testing.T) {
	ctx := context.Background()
	src := "testdata/generated/features-v2"
	if _, err := os.Stat(src); err != nil {
		t.Skip("features-v2 fixture not present")
	}
	dir := t.TempDir()
	if err := os.CopyFS(dir, os.DirFS(src)); err != nil {
		t.Fatal(err)
	}
	repo, err := icechunk.Open(ctx, storage.NewLocal(dir), nil)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := repo.RepoInfo(ctx)
	if info.Status.Availability == "online" {
		t.Skip("fixture is online")
	}
	if err := repo.CreateBranch(ctx, "x", icechunk.InitialSnapshotID); !errors.Is(err, icechunk.ErrRepositoryNotOnline) {
		t.Errorf("write to a %s repository: %v", info.Status.Availability, err)
	}
}

// TestChunkGridsAsUpstream checks which chunk grids SetMetadata accepts, and
// the chunks it counts, against upstream's store (ArrayMetadata::num_chunks).
func TestChunkGridsAsUpstream(t *testing.T) {
	ctx := context.Background()
	repo, err := icechunk.Create(ctx, storage.NewMemory(nil), nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := repo.WritableSession(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		shape, grid string
		chunks      []uint32 // nil: refused
	}{
		{`[10, 7]`, `{"name": "regular", "configuration": {"chunk_shape": [3, 7]}}`, []uint32{4, 1}},
		{`[4]`, `{"name": "regular", "configuration": {"chunk_shape": [0]}}`, []uint32{0}},
		{`[4]`, `{"name": "regular", "configuration": {"chunk_shape": [2, 3]}}`, []uint32{2}},
		{`[]`, `{"name": "regular", "configuration": {"chunk_shape": []}}`, []uint32{}},
		{`[8, 6]`, `{"name": "rectilinear", "configuration": {"kind": "inline", "chunk_shapes": [[[4, 2]], [2, 4]]}}`, []uint32{2, 2}},
		{`[8]`, `{"name": "rectilinear", "configuration": {"kind": "inline", "chunk_shapes": [[[4.0, 2]]]}}`, []uint32{2}},
		{`[7]`, `{"name": "rectilinear", "configuration": {"kind": "inline", "chunk_shapes": [[4.0, 3.0]]}}`, []uint32{2}},
		{`[9]`, `{"name": "rectilinear", "configuration": {"kind": "inline", "chunk_shapes": [[[2, 3, 99], 3]]}}`, []uint32{4}},
		{`[4]`, `{"name": "regular", "configuration": {"chunk_shape": [false]}}`, nil},
		{`[4]`, `{"name": "regular", "configuration": {"chunk_shape": [true]}}`, nil},
		{`[4]`, `{"name": "regular", "configuration": {"chunk_shape": [2.0]}}`, nil},
		{`[4]`, `{"name": "regular", "configuration": {"chunk_shape": [-2]}}`, nil},
		{`[4]`, `{"name": "regular", "configuration": {"chunk_shape": null}}`, nil},
		{`[4, 4]`, `{"name": "regular", "configuration": {"chunk_shape": [2]}}`, nil},
		{`[8, 6]`, `{"name": "regular", "configuration": {"chunk_shape": [4, [2, 4]]}}`, nil},
		{`[2]`, `{"name": "rectilinear", "configuration": {"kind": "inline", "chunk_shapes": [[true, true]]}}`, nil},
		{`[8, 6]`, `{"name": "rectilinear", "configuration": {"kind": "inline", "chunk_shapes": [4, [2, 4]]}}`, nil},
		{`[8]`, `{"name": "rectilinear", "configuration": {"kind": "inline", "chunk_shapes": [[[4, 2.0]]]}}`, nil},
		{`[8]`, `{"name": "rectilinear", "configuration": {"kind": "inline", "chunk_shapes": [[[4]]]}}`, nil},
		{`[8, 6]`, `{"name": "rectilinear", "configuration": {"kind": "inline", "chunk_shapes": [[4, 4]]}}`, nil},
		{`[4]`, `{"name": "other", "configuration": {"chunk_shape": [2]}}`, nil},
		{`[4]`, `{"name": "regular", "configuration": [2]}`, nil},
	} {
		doc := `{"zarr_format": 3, "node_type": "array", "shape": ` + c.shape + `, "data_type": "int32", "chunk_grid": ` + c.grid +
			`, "chunk_key_encoding": {"name": "default", "configuration": {"separator": "/"}}, "fill_value": 0, "codecs": [{"name": "bytes", "configuration": {"endian": "little"}}]}`
		err := s.SetMetadata(ctx, "/a", []byte(doc))
		if c.chunks == nil {
			if !errors.Is(err, icechunk.ErrInvalidMetadata) {
				t.Errorf("shape %s, grid %s: got %v, want ErrInvalidMetadata", c.shape, c.grid, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("shape %s, grid %s: %v", c.shape, c.grid, err)
			continue
		}
		n, err := s.Node("/a")
		if err != nil {
			t.Fatal(err)
		}
		got := []uint32{}
		for _, d := range n.Array.Shape {
			got = append(got, d.NumChunks)
		}
		if !slices.Equal(got, c.chunks) {
			t.Errorf("shape %s, grid %s: %v chunks, want %v", c.shape, c.grid, got, c.chunks)
		}
		if err := s.DeleteNode(ctx, "/a"); err != nil {
			t.Fatal(err)
		}
	}
}

// TestWriteStoresRepairedMetadata: an array whose stored chunk size zarr-go
// repairs in a way that moves chunks (0 read as 1) gets its repaired
// zarr.json stored before the first chunk write, as zarr-python does.
func TestWriteStoresRepairedMetadata(t *testing.T) {
	ctx := context.Background()
	repo, err := icechunk.Create(ctx, storage.NewMemory(nil), nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := repo.WritableSession(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	stored := func(chunk string) []byte {
		return []byte(`{"shape": [64], "data_type": "int32", "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [` + chunk +
			`]}}, "chunk_key_encoding": {"name": "default", "configuration": {"separator": "/"}}, "fill_value": -1, "codecs": [{"name": "bytes", "configuration": {"endian": "little"}}], "attributes": {"k": "v"}, "zarr_format": 3, "node_type": "array", "storage_transformers": []}`)
	}
	if err := s.SetMetadata(ctx, "/a", stored("0")); err != nil {
		t.Fatal(err)
	}
	oldID, err := s.Commit(ctx, "stored by an old zarr-python", nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err = repo.WritableSession(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	arr, err := s.OpenArray(ctx, "/a")
	if err != nil {
		t.Fatal(err)
	}
	if !arr.Metadata().RepairPending() {
		t.Fatal("chunk size 0 should need a repair that moves chunks")
	}
	early, err := s.OpenArray(ctx, "/a") // opened before the repair is stored
	if err != nil {
		t.Fatal(err)
	}
	// 64 chunks of one element, written concurrently: the repair is stored once.
	vals := make([]int32, 64)
	for i := range vals {
		vals[i] = int32(i * 3)
	}
	nd, err := zarr.FromSlice([]uint64{64}, vals)
	if err != nil {
		t.Fatal(err)
	}
	if err := arr.Write(ctx, []uint64{0}, nd); err != nil {
		t.Fatal(err)
	}
	n, err := s.Node("/a")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(n.ZarrMetadata, mustDocument(t, arr.Metadata())) || n.Array.Shape[0].NumChunks != 64 {
		t.Errorf("after the first write: zarr.json %s, %d chunks", n.ZarrMetadata, n.Array.Shape[0].NumChunks)
	}
	if attrs, _ := s.Attributes("/a"); attrs["k"] != "v" {
		t.Errorf("attributes after the repair: %v", attrs)
	}
	// The repair is stored: a handle opened before it writes without storing it again.
	one, _ := zarr.FromSlice([]uint64{1}, []int32{1000})
	if err := early.Write(ctx, []uint64{5}, one); err != nil {
		t.Fatal(err)
	}
	vals[5] = 1000
	if got := readInt32s(t, s, "/a"); !slices.Equal(got, vals) {
		t.Errorf("read back %v", got)
	}
	// A handle whose zarr.json was replaced with other chunks must be reopened.
	stale, err := s.OpenArray(ctx, "/a")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetMetadata(ctx, "/a", int32Array([]uint64{64}, []uint64{8})); err != nil {
		t.Fatal(err)
	}
	if err := stale.Write(ctx, []uint64{0}, one); err == nil {
		t.Error("write through a handle whose chunk grid changed: no error")
	}
	if err := s.SetMetadata(ctx, "/a", stored("1")); err != nil {
		t.Fatal(err)
	}
	id, err := s.Commit(ctx, "write", nil)
	if err != nil {
		t.Fatal(err)
	}
	ro, err := repo.ReadonlySession(ctx, icechunk.AtSnapshot(id))
	if err != nil {
		t.Fatal(err)
	}
	if got := readInt32s(t, ro, "/a"); !slices.Equal(got, vals) {
		t.Errorf("committed %v", got)
	}

	// Reading the stored document needs no repair to be stored: the array
	// holds only its fill value.
	ro, err = repo.ReadonlySession(ctx, icechunk.AtSnapshot(oldID))
	if err != nil {
		t.Fatal(err)
	}
	if got := readInt32s(t, ro, "/a"); !slices.Equal(got, slices.Repeat([]int32{-1}, 64)) {
		t.Errorf("read before the repair: %v", got)
	}
	if n, _ := ro.Node("/a"); !bytes.Equal(n.ZarrMetadata, stored("0")) {
		t.Errorf("read-only zarr.json changed: %s", n.ZarrMetadata)
	}
}

func mustDocument(t *testing.T, m *zarr.Metadata) []byte {
	t.Helper()
	b, err := m.Document()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestChunkKeyEncodings: Icechunk stores chunks only under default keys.
func TestChunkKeyEncodings(t *testing.T) {
	ctx := context.Background()
	repo, err := icechunk.Create(ctx, storage.NewMemory(nil), nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := repo.WritableSession(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	one, _ := zarr.FromSlice([]uint64{2, 2}, []int32{1, 2, 3, 4})
	for _, enc := range []zarr.ChunkKeyEncoding{
		{Name: "v2"},
		{Name: "v2", Separator: "/"},
		{Name: "default", Separator: "."},
		{Name: "template", Template: "{0}/{1}.bin"},
	} {
		// Created as upstream's store stores the metadata, but zarr writes
		// are refused, as zarr-python's are there.
		spec := zarr.ArraySpec{Shape: []uint64{4, 4}, ChunkShape: []uint64{2, 2}, DataType: "int32", ChunkKeyEncoding: enc,
			Codecs: []zarr.CodecSpec{{Name: "bytes", Configuration: []byte(`{"endian": "little"}`)}}}
		arr, err := s.CreateArray(ctx, "/x", spec)
		if err != nil {
			t.Fatalf("create with %+v: %v", enc, err)
		}
		if err := arr.Write(ctx, []uint64{0, 0}, one); !errors.Is(err, icechunk.ErrInvalidMetadata) {
			t.Errorf("write with %+v: %v", enc, err)
		}
		// Chunks written by coordinates are read back by coordinates.
		if err := s.SetChunk(ctx, "/x", []uint32{1, 1}, int32Bytes(5, 6, 7, 8)); err != nil {
			t.Fatal(err)
		}
		if nd, err := arr.Read(ctx, []uint64{2, 2}, []uint64{2, 2}); err != nil {
			t.Error(err)
		} else if v, _ := nd.Values(); !slices.Equal(v.([]int32), []int32{5, 6, 7, 8}) {
			t.Errorf("read with %+v: %v", enc, v)
		}
		if err := s.DeleteNode(ctx, "/x"); err != nil {
			t.Fatal(err)
		}
	}
	spec := zarr.ArraySpec{Shape: []uint64{4, 4}, ChunkShape: []uint64{2, 2}, DataType: "int32", ChunkKeyEncoding: zarr.ChunkKeyEncoding{Name: "default", Separator: "/"}}
	arr, err := s.CreateArray(ctx, "/ok", spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := arr.Write(ctx, []uint64{2, 2}, one); err != nil {
		t.Fatal(err)
	}
}
