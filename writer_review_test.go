package icechunk_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	icechunk "github.com/solomonsealed/icechunk-go"
	"github.com/solomonsealed/icechunk-go/storage"
)

// Regression tests for issues found in review of the writer.

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
	copyDir(t, "testdata/upstream/expire-repo-v2-by-working-copy", dir)
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
	copyDir(t, src, dir)
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
