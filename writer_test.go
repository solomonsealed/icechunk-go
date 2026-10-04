package icechunk_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"testing"

	icechunk "github.com/solomonsealed/icechunk-go"
	"github.com/solomonsealed/icechunk-go/storage"
)

// int32Array is a zarr.json for an uncompressed little-endian int32 array,
// so chunk bytes can be written without the zarr package.
func int32Array(shape, chunks []uint64) []byte {
	return []byte(fmt.Sprintf(`{"zarr_format":3,"node_type":"array","shape":%s,"data_type":"int32",
		"chunk_grid":{"name":"regular","configuration":{"chunk_shape":%s}},
		"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
		"fill_value":-1,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}],
		"attributes":{},"dimension_names":["x"]}`, jsonInts(shape), jsonInts(chunks)))
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
	if err := repo2.DeleteBranch(ctx, "main"); err == nil {
		t.Error("deleting main succeeded")
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
	if _, err := b.Commit(ctx, "b", &icechunk.CommitOptions{Rebase: true}); err != nil {
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
	if _, err := r.SnapshotID(), error(nil); err != nil {
		t.Fatal(err)
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
