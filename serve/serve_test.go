package serve

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"testing"

	icechunk "github.com/solomonsealed/icechunk-go"
	"github.com/solomonsealed/icechunk-go/storage"
)

func service(t *testing.T, fixture string) *Service {
	t.Helper()
	repo, err := icechunk.Open(context.Background(), storage.NewLocal("../testdata/"+fixture), nil)
	if err != nil {
		t.Fatal(err)
	}
	return &Service{Repo: repo}
}

func get(t *testing.T, s *Service, target string, header map[string]string) *Response {
	t.Helper()
	u, err := url.Parse(target)
	if err != nil {
		t.Fatal(err)
	}
	return s.Handle(context.Background(), &Request{Method: "GET", URL: u, Header: header})
}

func TestSummaryLogNodes(t *testing.T) {
	s := service(t, "upstream/test-repo-v2")
	r := get(t, s, "/", nil)
	var sum struct {
		Spec     int               `json:"spec_version"`
		Branches map[string]string `json:"branches"`
		Tags     map[string]string `json:"tags"`
	}
	if err := json.Unmarshal(r.Body, &sum); err != nil || r.Status != 200 || sum.Spec != 2 || len(sum.Branches) != 2 || len(sum.Tags) != 2 {
		t.Fatalf("summary %d %s", r.Status, r.Body)
	}
	r = get(t, s, "/log?ref=it%20works!&limit=2", nil)
	var log []commitJSON
	if err := json.Unmarshal(r.Body, &log); err != nil || len(log) != 2 || log[0].Message != "delete a chunk" {
		t.Fatalf("log %d %s", r.Status, r.Body)
	}
	r = get(t, s, "/nodes?ref=my-branch", nil)
	if r.Status != 200 || !strings.Contains(string(r.Body), `"path":"/group2/group3/group4/group5/inner","type":"array","shape":[10,10],"chunk_shape":[5,5],"dtype":"float32"`) {
		t.Fatalf("nodes %d %s", r.Status, r.Body)
	}
	if r := get(t, s, "/log?ref=nope", nil); r.Status != 404 {
		t.Errorf("unknown ref status %d", r.Status)
	}
}

func TestArrayEndpoint(t *testing.T) {
	s := service(t, "upstream/test-repo-v2")
	r := get(t, s, "/array/group1/small_chunks?ref=my-branch", nil)
	if r.Status != 200 || string(r.Body) != `{"shape":[5],"dtype":"int8","data":[84,84,84,84,8]}` {
		t.Fatalf("array %d %s", r.Status, r.Body)
	}
	r = get(t, s, "/array/group2/group3/group4/group5/inner?ref=my-branch&slice=2,1:3", nil)
	if r.Status != 200 || string(r.Body) != `{"shape":[2],"dtype":"float32","data":[null,null]}` {
		t.Fatalf("slice %d %s", r.Status, r.Body)
	}
	r = get(t, s, "/array/group1/small_chunks?ref=my-branch&format=binary&slice=3:", nil)
	if r.Status != 200 || string(r.Body) != "\x54\x08" || r.Header["x-shape"] != "2" || r.Header["x-dtype"] != "int8" {
		t.Fatalf("binary %d %q %v", r.Status, r.Body, r.Header)
	}
	// Steps and integer lists select like NumPy's orthogonal indexing.
	r = get(t, s, "/array/group1/small_chunks?ref=my-branch&slice=[4,0,4]", nil)
	if r.Status != 200 || string(r.Body) != `{"shape":[3],"dtype":"int8","data":[8,84,8]}` {
		t.Fatalf("list %d %s", r.Status, r.Body)
	}
	r = get(t, s, "/array/group1/small_chunks?ref=my-branch&slice=::2", nil)
	if r.Status != 200 || string(r.Body) != `{"shape":[3],"dtype":"int8","data":[84,84,8]}` {
		t.Fatalf("step %d %s", r.Status, r.Body)
	}
	if r := get(t, s, "/array/group1?ref=main", nil); r.Status != 400 {
		t.Errorf("group as array: %d %s", r.Status, r.Body)
	}
	if r := get(t, s, "/array/group1/small_chunks?slice=9", nil); r.Status != 400 {
		t.Errorf("bad slice: %d %s", r.Status, r.Body)
	}
}

func TestZarrPassthrough(t *testing.T) {
	s := service(t, "upstream/test-repo-v2")
	r := get(t, s, "/zarr/main/group1/zarr.json", nil)
	var md map[string]any
	if r.Status != 200 || r.Header["content-type"] != "application/json" || json.Unmarshal(r.Body, &md) != nil || md["node_type"] != "group" {
		t.Fatalf("zarr.json %d %s", r.Status, r.Body)
	}
	full := get(t, s, "/zarr/main/group1/big_chunks/c/1/1", nil)
	if full.Status != 200 || len(full.Body) == 0 {
		t.Fatalf("chunk %d", full.Status)
	}
	n := len(full.Body)
	for _, tc := range []struct {
		rng  string
		want string
	}{
		{"bytes=0-3", string(full.Body[:4])},
		{"bytes=4-", string(full.Body[4:])},
		{"bytes=-5", string(full.Body[n-5:])},
	} {
		r := get(t, s, "/zarr/main/group1/big_chunks/c/1/1", map[string]string{"range": tc.rng})
		if r.Status != 206 || string(r.Body) != tc.want || !strings.HasPrefix(r.Header["content-range"], "bytes ") {
			t.Errorf("range %s: %d %v", tc.rng, r.Status, r.Header)
		}
	}
	if r := get(t, s, "/zarr/main/group1/big_chunks/c/1/1", map[string]string{"range": "bytes=999999-"}); r.Status != 416 {
		t.Errorf("unsatisfiable range: %d", r.Status)
	}
	// Missing chunks are 404, which Zarr clients read as the fill value.
	if r := get(t, s, "/zarr/my-branch/group1/small_chunks/c/4", nil); r.Status != 404 {
		t.Errorf("deleted chunk: %d", r.Status)
	}
	// Snapshot-pinned URLs are immutable and cacheable.
	id, _ := s.Repo.LookupBranch(context.Background(), "main")
	r = get(t, s, "/zarr/"+id.String()+"/zarr.json", nil)
	if r.Status != 200 || !strings.Contains(r.Header["cache-control"], "immutable") {
		t.Errorf("snapshot ref: %d %v", r.Status, r.Header)
	}
	head := s.Handle(context.Background(), &Request{Method: "HEAD", URL: &url.URL{Path: "/zarr/main/zarr.json"}})
	if head.Status != 200 || head.Body != nil || head.Header["content-length"] == "" {
		t.Errorf("HEAD: %d %v", head.Status, head.Header)
	}
}

func TestRefNamesWithSlash(t *testing.T) {
	s := service(t, "generated/codecs-v2")
	if r := get(t, s, "/zarr/feature%2Fx/types/int32/zarr.json", nil); r.Status != 200 {
		t.Errorf("/zarr/feature%%2Fx: %d %s", r.Status, r.Body)
	}
	if r := get(t, s, "/array/types/int32?ref=v1/rc&slice=0,0", nil); r.Status != 200 {
		t.Errorf("?ref=v1/rc: %d %s", r.Status, r.Body)
	}
	r := get(t, s, "/", nil)
	if !strings.Contains(string(r.Body), `"feature/x"`) || !strings.Contains(string(r.Body), `"v1/rc"`) {
		t.Errorf("summary misses refs with slashes: %s", r.Body)
	}
}

func TestParseRange(t *testing.T) {
	for _, tc := range []struct {
		h       string
		size    int64
		off, n  int64
		wantErr bool
	}{
		{"bytes=0-9", 100, 0, 10, false},
		{"bytes=90-", 100, 90, 10, false},
		{"bytes=-10", 100, 90, 10, false},
		{"bytes=-500", 100, 0, 100, false},
		{"bytes=50-1000", 100, 50, 50, false},
		{"bytes=-5", 0, 0, 0, true},
		{"bytes=0-", 0, 0, 0, true},
		{"bytes=5-2", 100, 0, 0, true},
		{"bytes=0-1,4-5", 100, 0, 0, true},
	} {
		off, n, err := parseRange(tc.h, tc.size)
		if (err != nil) != tc.wantErr || (!tc.wantErr && (off != tc.off || n != tc.n)) {
			t.Errorf("parseRange(%q, %d) = %d, %d, %v", tc.h, tc.size, off, n, err)
		}
	}
}
