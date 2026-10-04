package conformance

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	icechunk "github.com/solomonsealed/icechunk-go"
	"github.com/solomonsealed/icechunk-go/storage"
	zarr "github.com/solomonsealed/zarr-go"
)

func readAllArrays(t *testing.T, s *icechunk.Session) map[string]*zarr.NDArray {
	t.Helper()
	ctx := context.Background()
	nodes, err := s.Nodes()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*zarr.NDArray{}
	for _, n := range nodes {
		if n.Type != icechunk.ArrayNode {
			continue
		}
		arr, err := s.OpenArray(ctx, n.Path)
		if err != nil {
			continue // arrays the reader cannot decode are not compared
		}
		nd, err := arr.ReadAll(ctx)
		if err != nil {
			t.Fatalf("read %s: %v", n.Path, err)
		}
		out[n.Path] = nd
	}
	return out
}

// Go commits on top of repositories written by icechunk-python: split
// manifests, expired history (pruned tx logs), sharded arrays. Everything
// the commit does not touch must read back unchanged. With
// ICECHUNK_GO_WRITE_DIR set, the results are kept in <dir>/onpython for
// testdata/check_go_on_python.py.
func TestWriteOnPythonRepos(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		fixture, branch, array string
		start                  []uint64
		patch                  func() *zarr.NDArray
	}{
		{"upstream/split-repo-v2", "main", "/group1/split", []uint64{2, 2},
			func() *zarr.NDArray { nd, _ := zarr.FromSlice([]uint64{4, 5}, make([]float32, 20)); return nd }},
		{"upstream/expire-repo-v2-by-working-copy", "main", "/group1/data", []uint64{3, 0},
			func() *zarr.NDArray {
				nd, _ := zarr.FromSlice([]uint64{1, 1}, []int64{-42})
				return nd
			}},
		{"generated/codecs-v2", "main", "/codecs/sharded", []uint64{5, 4},
			func() *zarr.NDArray { nd, _ := zarr.FromSlice([]uint64{2, 3}, []float64{1, 2, 3, 4, 5, 6}); return nd }},
	}
	keep := os.Getenv("ICECHUNK_GO_WRITE_DIR")
	for _, tc := range cases {
		t.Run(tc.fixture, func(t *testing.T) {
			dir := t.TempDir()
			if keep != "" {
				dir = filepath.Join(keep, "onpython", filepath.Base(tc.fixture))
				os.RemoveAll(dir)
			}
			if err := os.CopyFS(dir, os.DirFS(testdata+"/"+tc.fixture)); err != nil {
				t.Fatal(err)
			}
			st := storage.NewLocal(dir)
			repo, err := icechunk.Open(ctx, st, nil)
			if err != nil {
				t.Fatal(err)
			}
			before, _ := repo.ReadonlySession(ctx, icechunk.AtBranch(tc.branch))
			oldData := readAllArrays(t, before)
			var oldHistory []string
			for si, err := range repo.Ancestry(ctx, icechunk.AtBranch(tc.branch)) {
				if err != nil {
					t.Fatal(err)
				}
				oldHistory = append(oldHistory, si.Message)
			}
			oldInfo, _ := repo.RepoInfo(ctx)

			s, err := repo.WritableSession(ctx, tc.branch)
			if err != nil {
				t.Fatal(err)
			}
			arr, err := s.OpenArray(ctx, tc.array)
			if err != nil {
				t.Fatal(err)
			}
			patch := tc.patch()
			if patch.DataType.Name != arr.DataType().Name {
				p2, err := convertPatch(patch, arr.DataType().Name)
				if err != nil {
					t.Fatal(err)
				}
				patch = p2
			}
			if err := arr.Write(ctx, tc.start, patch); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Commit(ctx, "go commit on python repo", &icechunk.CommitOptions{Metadata: map[string]any{"by": "go"}}); err != nil {
				t.Fatal(err)
			}

			repo2, _ := icechunk.Open(ctx, st, nil)
			after, _ := repo2.ReadonlySession(ctx, icechunk.AtBranch(tc.branch))
			newData := readAllArrays(t, after)
			for path, old := range oldData {
				nd := newData[path]
				if path == tc.array {
					region, _ := mustArray(t, after, path).Read(ctx, tc.start, patch.Shape)
					if !reflect.DeepEqual(region.Data, patch.Data) {
						t.Errorf("%s: patch not visible", path)
					}
					continue
				}
				if nd == nil || !reflect.DeepEqual(nd.Data, old.Data) || !reflect.DeepEqual(nd.Strings, old.Strings) {
					t.Errorf("%s changed although the commit did not touch it", path)
				}
			}
			var history []string
			for si, err := range repo2.Ancestry(ctx, icechunk.AtBranch(tc.branch)) {
				if err != nil {
					t.Fatal(err)
				}
				history = append(history, si.Message)
			}
			if !slices.Equal(history, append([]string{"go commit on python repo"}, oldHistory...)) {
				t.Errorf("history = %q", history)
			}
			// Everything else in the repo info survives the rewrite.
			newInfo, _ := repo2.RepoInfo(ctx)
			if len(newInfo.Snapshots) != len(oldInfo.Snapshots)+1 || !reflect.DeepEqual(newInfo.Tags, oldInfo.Tags) ||
				!reflect.DeepEqual(newInfo.Config, oldInfo.Config) || !reflect.DeepEqual(newInfo.Metadata, oldInfo.Metadata) ||
				!reflect.DeepEqual(newInfo.DeletedTags, oldInfo.DeletedTags) || newInfo.Status != oldInfo.Status ||
				!reflect.DeepEqual(newInfo.EnabledFeatureFlags, oldInfo.EnabledFeatureFlags) ||
				!reflect.DeepEqual(newInfo.DisabledFeatureFlags, oldInfo.DisabledFeatureFlags) {
				t.Errorf("repo info not preserved")
			}
			for _, old := range oldInfo.Snapshots {
				found := false
				for _, n := range newInfo.Snapshots {
					if n.ID == old.ID {
						found = reflect.DeepEqual(n, old)
					}
				}
				if !found {
					t.Errorf("snapshot %s changed", old.ID)
				}
			}
			// The ops log gains one entry; the previous newest entry now names
			// the backup of the file this commit replaced (upstream's rule).
			u := newInfo.Updates
			if u[0].Kind != "NewCommit" || u[1].BackupPath == "" {
				t.Errorf("ops log head = %+v, %+v", u[0], u[1])
			}
			if _, err := os.Stat(filepath.Join(dir, "overwritten", u[1].BackupPath)); err != nil {
				t.Errorf("backup %s missing: %v", u[1].BackupPath, err)
			}
			prev := u[1]
			prev.BackupPath = oldInfo.Updates[0].BackupPath
			if !reflect.DeepEqual(prev, oldInfo.Updates[0]) || !reflect.DeepEqual(u[2:], oldInfo.Updates[1:len(u)-1]) {
				t.Errorf("older ops log entries changed")
			}
		})
	}
}

func mustArray(t *testing.T, s *icechunk.Session, path string) *zarr.Array {
	t.Helper()
	a, err := s.OpenArray(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// convertPatch re-types a patch's values to the array's data type.
func convertPatch(p *zarr.NDArray, dt string) (*zarr.NDArray, error) {
	f, err := p.Float64s()
	if err != nil {
		return nil, err
	}
	switch dt {
	case "float32":
		v := make([]float32, len(f))
		for i := range f {
			v[i] = float32(f[i])
		}
		return zarr.FromSlice(p.Shape, v)
	case "float64":
		return zarr.FromSlice(p.Shape, f)
	case "int64":
		v := make([]int64, len(f))
		for i := range f {
			v[i] = int64(f[i])
		}
		return zarr.FromSlice(p.Shape, v)
	case "int32":
		v := make([]int32, len(f))
		for i := range f {
			v[i] = int32(f[i])
		}
		return zarr.FromSlice(p.Shape, v)
	}
	return p, nil
}
