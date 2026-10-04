package conformance

// Tests against repositories written by icechunk-python (see
// testdata/generate.py), covering every data type and codec the zarr
// package supports, in both spec versions.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"os"
	"reflect"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	icechunk "github.com/solomonsealed/icechunk-go"
	"github.com/solomonsealed/icechunk-go/storage"
	zarr "github.com/solomonsealed/zarr-go"
)

type expectedArray struct {
	DType  json.RawMessage   `json:"dtype"`
	Shape  []uint64          `json:"shape"`
	SHA256 string            `json:"sha256"`
	Values []json.RawMessage `json:"values"`
}

type expectations struct {
	Meta struct {
		CommitProperties map[string]any `json:"commit_properties"`
	} `json:"_meta"`
	Repos map[string]map[string]map[string]json.RawMessage
}

func loadExpected(t *testing.T) expectations {
	t.Helper()
	raw, err := os.ReadFile(testdata + "/generated/expected.json")
	if err != nil {
		t.Skipf("generated fixtures missing (run testdata/generate.py): %v", err)
	}
	var e expectations
	if err := json.Unmarshal(raw, &e); err != nil {
		t.Fatal(err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		t.Fatal(err)
	}
	e.Repos = map[string]map[string]map[string]json.RawMessage{}
	for name, v := range top {
		if name == "_meta" {
			continue
		}
		var r map[string]map[string]json.RawMessage
		if err := json.Unmarshal(v, &r); err != nil {
			t.Fatal(err)
		}
		e.Repos[name] = r
	}
	return e
}

func openGenerated(t *testing.T, name string, opts *icechunk.Options) *icechunk.Repository {
	t.Helper()
	repo, err := icechunk.Open(context.Background(), storage.NewLocal(testdata+"/generated/"+name), opts)
	if err != nil {
		t.Fatalf("open %s: %v", name, err)
	}
	return repo
}

// normalize round-trips a value through JSON so decoded metadata compares
// equal to expectations regardless of integer/float representation.
func normalize(t *testing.T, v any) any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestGeneratedCodecMatrix(t *testing.T) {
	exp := loadExpected(t)
	for _, name := range []string{"codecs-v2", "codecs-v1"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			repo := openGenerated(t, name, nil)
			wantSpec := map[string]int{"codecs-v2": 2, "codecs-v1": 1}[name]
			if repo.SpecVersion() != wantSpec {
				t.Errorf("spec version %d, want %d", repo.SpecVersion(), wantSpec)
			}

			// Commit metadata: FlexBuffers in v2, MessagePack in v1.
			var tip icechunk.SnapshotInfo
			for si, err := range repo.Ancestry(ctx, icechunk.AtBranch("main")) {
				if err != nil {
					t.Fatal(err)
				}
				tip = si
				break
			}
			if tip.Message != "codec matrix" {
				t.Errorf("tip message %q", tip.Message)
			}
			if got, want := normalize(t, tip.Metadata), normalize(t, exp.Meta.CommitProperties); !reflect.DeepEqual(got, want) {
				t.Errorf("commit metadata = %v, want %v", got, want)
			}

			for branch, arrays := range exp.Repos[name] {
				s, err := repo.ReadonlySession(ctx, icechunk.AtBranch(branch))
				if err != nil {
					t.Fatal(err)
				}
				paths := make([]string, 0, len(arrays))
				for p := range arrays {
					paths = append(paths, p)
				}
				sort.Strings(paths)
				for _, path := range paths {
					var e expectedArray
					if err := json.Unmarshal(arrays[path], &e); err != nil {
						t.Fatal(err)
					}
					t.Run(branch+"/"+path, func(t *testing.T) { checkArray(t, s, path, e) })
				}
			}

			if repo.SpecVersion() >= 2 {
				// Spec v2 ref names may contain "/".
				if _, err := repo.LookupBranch(ctx, "feature/x"); err != nil {
					t.Errorf("branch feature/x: %v", err)
				}
				if _, err := repo.LookupTag(ctx, "v1/rc"); err != nil {
					t.Errorf("tag v1/rc: %v", err)
				}
			}

			tagged, err := repo.ReadonlySession(ctx, icechunk.AtTag("v1.0"))
			if err != nil {
				t.Fatal(err)
			}
			main, _ := repo.LookupBranch(ctx, "main")
			if tagged.SnapshotID() != main {
				t.Errorf("tag v1.0 = %s, main = %s", tagged.SnapshotID(), main)
			}
		})
	}
}

func checkArray(t *testing.T, s *icechunk.Session, path string, e expectedArray) {
	ctx := context.Background()
	arr, err := s.OpenArray(ctx, path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !slices.Equal(arr.Shape(), e.Shape) {
		t.Fatalf("shape %v, want %v", arr.Shape(), e.Shape)
	}
	nd, err := arr.ReadAll(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if e.SHA256 != "" {
		sum := sha256.Sum256(nd.Data)
		if got := hex.EncodeToString(sum[:]); got != e.SHA256 {
			t.Errorf("sha256 %s, want %s (dtype %s)", got, e.SHA256, arr.DataType())
		}
	}
	if e.Values != nil {
		checkValues(t, nd, e.Values)
	}
	// Region reads agree with the full read.
	if len(e.Shape) > 0 && nd.Len() > 0 {
		start, count := make([]uint64, len(e.Shape)), make([]uint64, len(e.Shape))
		for d, n := range e.Shape {
			start[d] = n / 3
			count[d] = max(1, n/2)
			if start[d]+count[d] > n {
				count[d] = n - start[d]
			}
		}
		sub, err := arr.Read(ctx, start, count)
		if err != nil {
			t.Fatalf("region read: %v", err)
		}
		if want := slice(nd, start, count); !reflect.DeepEqual(sub.Data, want.Data) || !reflect.DeepEqual(sub.Strings, want.Strings) {
			t.Errorf("region [%v +%v] differs from full read", start, count)
		}
	}
}

// slice extracts a region from an NDArray (reference implementation).
func slice(nd *zarr.NDArray, start, count []uint64) *zarr.NDArray {
	out := &zarr.NDArray{Shape: count, DataType: nd.DataType}
	es := uint64(nd.DataType.Size)
	total := uint64(1)
	for _, c := range count {
		total *= c
	}
	idx := make([]uint64, len(count))
	for i := uint64(0); i < total; i++ {
		var lin uint64
		for d := range count {
			lin = lin*nd.Shape[d] + start[d] + idx[d]
		}
		if nd.Strings != nil {
			out.Strings = append(out.Strings, nd.Strings[lin])
		} else {
			out.Data = append(out.Data, nd.Data[lin*es:(lin+1)*es]...)
		}
		for d := len(count) - 1; d >= 0; d-- {
			idx[d]++
			if idx[d] < count[d] {
				break
			}
			idx[d] = 0
		}
	}
	return out
}

func checkValues(t *testing.T, nd *zarr.NDArray, want []json.RawMessage) {
	t.Helper()
	vals, err := nd.Values()
	if err != nil {
		t.Fatalf("values: %v", err)
	}
	got := reflect.ValueOf(vals)
	if got.Len() != len(want) {
		t.Fatalf("%d values, want %d", got.Len(), len(want))
	}
	for i := range want {
		g := got.Index(i).Interface()
		if !valueMatches(g, want[i]) {
			t.Errorf("value[%d] = %v, want %s", i, g, want[i])
			return
		}
	}
}

func parseExpFloat(raw json.RawMessage) float64 {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		switch s {
		case "NaN":
			return math.NaN()
		case "Infinity":
			return math.Inf(1)
		case "-Infinity":
			return math.Inf(-1)
		}
	}
	var f float64
	json.Unmarshal(raw, &f)
	return f
}

func floatEq(a, b float64) bool { return a == b || (math.IsNaN(a) && math.IsNaN(b)) }

func valueMatches(g any, raw json.RawMessage) bool {
	switch v := g.(type) {
	case bool:
		var w bool
		return json.Unmarshal(raw, &w) == nil && v == w
	case string:
		var w string
		return json.Unmarshal(raw, &w) == nil && v == w
	case float32:
		return floatEq(float64(v), float64(float32(parseExpFloat(raw))))
	case float64:
		return floatEq(v, parseExpFloat(raw))
	case complex64:
		var parts []json.RawMessage
		json.Unmarshal(raw, &parts)
		return floatEq(float64(real(v)), float64(float32(parseExpFloat(parts[0])))) && floatEq(float64(imag(v)), float64(float32(parseExpFloat(parts[1]))))
	case complex128:
		var parts []json.RawMessage
		json.Unmarshal(raw, &parts)
		return floatEq(real(v), parseExpFloat(parts[0])) && floatEq(imag(v), parseExpFloat(parts[1]))
	case uint64:
		var w uint64
		return json.Unmarshal(raw, &w) == nil && v == w
	}
	rv := reflect.ValueOf(g)
	switch rv.Kind() {
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var w int64
		return json.Unmarshal(raw, &w) == nil && rv.Int() == w
	case reflect.Uint8, reflect.Uint16, reflect.Uint32:
		var w uint64
		return json.Unmarshal(raw, &w) == nil && rv.Uint() == w
	}
	return false
}

// countingStorage records the bytes and requests served per key prefix.
type countingStorage struct {
	storage.Storage
	mu       sync.Mutex
	requests []string
	bytes    int
}

func (c *countingStorage) Get(ctx context.Context, key string, opts *storage.GetOptions) ([]byte, error) {
	b, err := c.Storage.Get(ctx, key, opts)
	c.mu.Lock()
	c.requests = append(c.requests, key)
	c.bytes += len(b)
	c.mu.Unlock()
	return b, err
}

func TestShardedPartialReads(t *testing.T) {
	ctx := context.Background()
	full := openGenerated(t, "codecs-v2", nil)
	fs, _ := full.ReadonlySession(ctx, icechunk.AtBranch("main"))
	farr, _ := fs.OpenArray(ctx, "big/sharded_int16")
	all, err := farr.ReadAll(ctx)
	if err != nil {
		t.Fatal(err)
	}

	cs := &countingStorage{Storage: storage.NewLocal(testdata + "/generated/codecs-v2")}
	repo, err := icechunk.Open(ctx, cs, &icechunk.Options{CacheBytes: -1})
	if err != nil {
		t.Fatal(err)
	}
	s, _ := repo.ReadonlySession(ctx, icechunk.AtBranch("main"))
	arr, _ := s.OpenArray(ctx, "big/sharded_int16")
	cs.mu.Lock()
	cs.requests, cs.bytes = nil, 0
	cs.mu.Unlock()
	// A 3x3 region inside one 32x32 shard touches a single 8x8 inner chunk.
	start, count := []uint64{9, 9}, []uint64{3, 3}
	sub, err := arr.Read(ctx, start, count)
	if err != nil {
		t.Fatal(err)
	}
	if want := slice(all, start, count); !reflect.DeepEqual(sub.Data, want.Data) {
		t.Fatalf("partial shard read mismatch")
	}
	chunkReqs := 0
	for _, k := range cs.requests {
		if len(k) > 7 && k[:7] == "chunks/" {
			chunkReqs++
		}
	}
	// One request for the shard index, one for the inner chunk.
	if chunkReqs != 2 {
		t.Errorf("partial read made %d chunk requests (%v), want 2", chunkReqs, cs.requests)
	}
}

func TestGeneratedVirtualChunks(t *testing.T) {
	exp := loadExpected(t)
	for _, name := range []string{"virtual-v2", "virtual-v1"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			var prefix string
			json.Unmarshal(exp.Repos[name]["main"]["_virtual_prefix"], &prefix)
			var e expectedArray
			json.Unmarshal(exp.Repos[name]["main"]["virtual"], &e)

			local := storage.NewLocal(testdata + "/generated/virtual-data")
			repo := openGenerated(t, name, &icechunk.Options{
				VirtualChunkContainers: map[string]storage.Storage{prefix: local},
			})
			s, err := repo.ReadonlySession(ctx, icechunk.AtBranch("main"))
			if err != nil {
				t.Fatal(err)
			}
			arr, err := s.OpenArray(ctx, "virtual")
			if err != nil {
				t.Fatal(err)
			}
			nd, err := arr.ReadAll(ctx)
			if err != nil {
				t.Fatal(err)
			}
			checkValues(t, nd, e.Values)

			// "my%20file.bin" is read as "my file.bin"; "?versionId=7#frag" is dropped.
			var enc expectedArray
			json.Unmarshal(exp.Repos[name]["main"]["encoded"], &enc)
			encArr, err := s.OpenArray(ctx, "encoded")
			if err != nil {
				t.Fatal(err)
			}
			encND, err := encArr.ReadAll(ctx)
			if err != nil {
				t.Fatalf("encoded locations: %v", err)
			}
			checkValues(t, encND, enc.Values)

			ref, err := s.ChunkRef(ctx, "virtual", []uint32{1999})
			if err != nil || ref.Kind != icechunk.VirtualChunk || ref.Location != prefix+"part-39.bin" || ref.Offset != 6+4*49 {
				t.Errorf("ref 1999 = %+v, %v", ref, err)
			}

			// Checksums are passed through as conditional reads.
			rec := &recordingStorage{Storage: local}
			repo2 := openGenerated(t, name, &icechunk.Options{VirtualChunkContainers: map[string]storage.Storage{prefix: rec}})
			s2, _ := repo2.ReadonlySession(ctx, icechunk.AtBranch("main"))
			if _, _, err := s2.GetChunk(ctx, "checksummed", []uint32{0}); err != nil {
				t.Fatal(err)
			}
			if _, _, err := s2.GetChunk(ctx, "checksummed", []uint32{1}); err != nil {
				t.Fatal(err)
			}
			if len(rec.opts) != 2 || rec.opts[0].IfMatch != "etag-123" ||
				!rec.opts[1].IfUnmodifiedSince.Equal(time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)) {
				t.Errorf("conditional options = %+v", rec.opts)
			}
			rec.fail = true
			if _, _, err := s2.GetChunk(ctx, "checksummed", []uint32{0}); !errors.Is(err, icechunk.ErrChunkModified) {
				t.Errorf("modified object: err = %v, want ErrChunkModified", err)
			}
			// storage.Local enforces last-modified checks against file mtimes:
			// the fixture files were written after the recorded 2020 date, but
			// before 2100.
			if _, _, err := s.GetChunk(ctx, "checksummed", []uint32{1}); !errors.Is(err, icechunk.ErrChunkModified) {
				t.Errorf("stale last-modified checksum: err = %v, want ErrChunkModified", err)
			}
			if data, ok, err := s.GetChunk(ctx, "checksummed", []uint32{3}); err != nil || !ok || len(data) != 4 {
				t.Errorf("current last-modified checksum: %v %v %v", data, ok, err)
			}
			// Local files have no ETag, so ETag checksums cannot be satisfied.
			if _, _, err := s.GetChunk(ctx, "checksummed", []uint32{2}); !errors.Is(err, icechunk.ErrChunkModified) {
				t.Errorf("etag checksum on local storage: err = %v, want ErrChunkModified", err)
			}
		})
	}
}

type recordingStorage struct {
	storage.Storage
	opts []storage.GetOptions
	fail bool
}

func (r *recordingStorage) Get(ctx context.Context, key string, opts *storage.GetOptions) ([]byte, error) {
	if r.fail {
		return nil, storage.ErrPreconditionFailed
	}
	r.opts = append(r.opts, *opts)
	return r.Storage.Get(ctx, key, &storage.GetOptions{Range: opts.Range})
}
