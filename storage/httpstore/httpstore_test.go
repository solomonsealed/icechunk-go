package httpstore_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	icechunk "github.com/solomonsealed/icechunk-go"
	"github.com/solomonsealed/icechunk-go/storage"
	"github.com/solomonsealed/icechunk-go/storage/httpstore"
)

// fixtureServer serves a directory like a static file host / public bucket,
// honouring Range requests.
func fixtureServer(t *testing.T, dir string, requests *[]string) *httptest.Server {
	fs := http.FileServer(http.Dir(dir))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests != nil {
			*requests = append(*requests, r.URL.Path+" "+r.Header.Get("Range"))
		}
		if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(r.URL.Path))); err != nil {
			http.NotFound(w, r)
			return
		}
		fs.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestHTTPStoreReadsRepo(t *testing.T) {
	ctx := context.Background()
	var reqs []string
	srv := fixtureServer(t, "../../testdata/upstream/test-repo-v2", &reqs)
	repo, err := icechunk.Open(ctx, httpstore.New(srv.URL, nil), nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := repo.ReadonlySession(ctx, icechunk.AtTag("it also works!"))
	if err != nil {
		t.Fatal(err)
	}
	arr, err := s.OpenArray(ctx, "group1/small_chunks")
	if err != nil {
		t.Fatal(err)
	}
	nd, err := arr.ReadAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := nd.Float64s(); !slices.Equal(v, []float64{84, 84, 84, 84, 8}) {
		t.Errorf("small_chunks = %v", v)
	}
	// Native chunks are fetched with range requests.
	data, ok, err := s.GetChunk(ctx, "group1/big_chunks", []uint32{1, 1})
	if err != nil || !ok || len(data) == 0 {
		t.Fatalf("GetChunk = %d bytes, %v, %v", len(data), ok, err)
	}
	ranged := false
	for _, r := range reqs {
		if strings.HasPrefix(r, "/chunks/") && strings.Contains(r, "bytes=0-") {
			ranged = true
		}
	}
	if !ranged {
		t.Errorf("no ranged chunk request in %q", reqs)
	}
	if _, err := httpstore.New(srv.URL, nil).Get(ctx, "missing", nil); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("missing object: %v", err)
	}
}

func TestHTTPStoreConditionalAndRangeFallback(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if m := r.Header.Get("If-Match"); m != "" && m != `"v1"` {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		w.Write([]byte("0123456789")) // ignores Range, always 200
	}))
	defer srv.Close()
	st := httpstore.New(srv.URL, nil)
	got, err := st.Get(ctx, "x", storage.RangeOf(2, 3))
	if err != nil || string(got) != "234" {
		t.Errorf("range fallback = %q, %v", got, err)
	}
	opts := storage.RangeOf(0, 1)
	opts.IfMatch = "v2"
	if _, err := st.Get(ctx, "x", opts); !errors.Is(err, storage.ErrPreconditionFailed) {
		t.Errorf("precondition: %v", err)
	}
}

// A fake S3 endpoint checks path-style addressing, signing and listing.
func TestS3StoreListsV1Refs(t *testing.T) {
	ctx := context.Background()
	dir := "../../testdata/upstream/test-repo-v1"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential=AK/") {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.URL.Query().Get("list-type") == "2" {
			prefix := strings.TrimPrefix(r.URL.Query().Get("prefix"), "repos/v1/")
			w.Write([]byte("<ListBucketResult>"))
			filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
				rel, _ := filepath.Rel(dir, p)
				if !info.IsDir() && strings.HasPrefix(rel, prefix) {
					w.Write([]byte("<Contents><Key>repos/v1/" + rel + "</Key></Contents>"))
				}
				return nil
			})
			w.Write([]byte("<IsTruncated>false</IsTruncated></ListBucketResult>"))
			return
		}
		key, ok := strings.CutPrefix(r.URL.Path, "/bucket/repos/v1/")
		data, err := os.ReadFile(filepath.Join(dir, key))
		if !ok || err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		http.ServeContent(w, r, key, time.Time{}, strings.NewReader(string(data)))
	}))
	defer srv.Close()
	st := httpstore.NewS3(storage.S3Config{
		Bucket: "bucket", Prefix: "repos/v1", Endpoint: srv.URL, Region: "auto",
		AccessKeyID: "AK", SecretAccessKey: "SK",
	}, nil)
	repo, err := icechunk.Open(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	tags, err := repo.ListTags(ctx)
	if err != nil || !slices.Equal(tags, []string{"it also works!", "it works!"}) {
		t.Errorf("tags = %q, %v", tags, err)
	}
	if _, err := repo.LookupTag(ctx, "deleted"); !errors.Is(err, icechunk.ErrRefNotFound) {
		t.Errorf("deleted tag: %v", err)
	}
}

// Servers that ignore conditional headers must not let modified objects
// through: the response's ETag / Last-Modified are checked too.
func TestHTTPStoreVerifiesChecksumsItself(t *testing.T) {
	ctx := context.Background()
	modified := time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)
	withETag := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if withETag {
			w.Header().Set("ETag", `"abc"`)
		}
		w.Header().Set("Last-Modified", modified.Format(http.TimeFormat))
		w.Write([]byte("0123456789")) // ignores If-Match and Range
	}))
	defer srv.Close()
	st := httpstore.New(srv.URL, nil)
	get := func(etag string, since time.Time) error {
		opts := storage.RangeOf(0, 2)
		opts.IfMatch, opts.IfUnmodifiedSince = etag, since
		_, err := st.Get(ctx, "x", opts)
		return err
	}
	if err := get("abc", time.Time{}); err != nil {
		t.Errorf("matching etag: %v", err)
	}
	if err := get("other", time.Time{}); !errors.Is(err, storage.ErrPreconditionFailed) {
		t.Errorf("mismatched etag: %v", err)
	}
	if err := get("", modified); err != nil {
		t.Errorf("not modified since: %v", err)
	}
	if err := get("", modified.Add(-time.Hour)); !errors.Is(err, storage.ErrPreconditionFailed) {
		t.Errorf("modified since: %v", err)
	}
	withETag = false
	if err := get("abc", time.Time{}); !errors.Is(err, storage.ErrPreconditionFailed) {
		t.Errorf("missing etag: %v", err)
	}
}

// fakeS3 is an in-memory S3 endpoint with ETags and conditional PUTs.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func (f *fakeS3) etag(b []byte) string { return fmt.Sprintf(`"%x"`, md5.Sum(b)) }

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential=AK/") {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/bucket/")
	if r.URL.Query().Get("list-type") == "2" {
		prefix := r.URL.Query().Get("prefix")
		var keys []string
		for k := range f.objects {
			if strings.HasPrefix(k, prefix) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		fmt.Fprint(w, "<ListBucketResult>")
		for _, k := range keys {
			fmt.Fprintf(w, "<Contents><Key>%s</Key></Contents>", k)
		}
		fmt.Fprint(w, "<IsTruncated>false</IsTruncated></ListBucketResult>")
		return
	}
	cur, exists := f.objects[key]
	switch r.Method {
	case "GET":
		if !exists {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("ETag", f.etag(cur))
		http.ServeContent(w, r, key, time.Time{}, bytes.NewReader(cur))
	case "PUT":
		body, _ := io.ReadAll(r.Body)
		sum := sha256.Sum256(body)
		if r.Header.Get("X-Amz-Content-Sha256") != hex.EncodeToString(sum[:]) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if m := r.Header.Get("If-Match"); m != "" && (!exists || m != f.etag(cur)) {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		if r.Header.Get("If-None-Match") == "*" && exists {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		f.objects[key] = body
		w.Header().Set("ETag", f.etag(body))
	case "DELETE":
		delete(f.objects, key)
		w.WriteHeader(http.StatusNoContent)
	}
}

// The S3 store can host a repository written by the Go writer.
func TestS3StoreWrites(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(&fakeS3{objects: map[string][]byte{}})
	defer srv.Close()
	st := httpstore.NewS3(storage.S3Config{Bucket: "bucket", Prefix: "repo", Endpoint: srv.URL, AccessKeyID: "AK", SecretAccessKey: "SK"}, nil)

	v1, err := st.Put(ctx, "x", []byte("one"), &storage.PutOptions{IfNotExists: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Put(ctx, "x", []byte("two"), &storage.PutOptions{IfNotExists: true}); !errors.Is(err, storage.ErrPreconditionFailed) {
		t.Errorf("create over existing: %v", err)
	}
	data, v, err := st.GetVersion(ctx, "x")
	if err != nil || string(data) != "one" || v != v1 {
		t.Errorf("GetVersion = %q %q %v (put returned %q)", data, v, err, v1)
	}
	if _, err := st.Put(ctx, "x", []byte("three"), &storage.PutOptions{IfMatch: "stale"}); !errors.Is(err, storage.ErrPreconditionFailed) {
		t.Errorf("stale If-Match: %v", err)
	}
	if _, err := st.Put(ctx, "x", []byte("three"), &storage.PutOptions{IfMatch: v}); err != nil {
		t.Errorf("fresh If-Match: %v", err)
	}
	if err := st.Delete(ctx, "x"); err != nil {
		t.Fatal(err)
	}

	repo, err := icechunk.Create(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := repo.WritableSession(ctx, "main")
	if err := s.CreateGroup(ctx, "/", map[string]any{"on": "s3"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Commit(ctx, "via s3", nil); err != nil {
		t.Fatal(err)
	}
	main, _ := repo.LookupBranch(ctx, "main")
	if err := repo.CreateTag(ctx, "t", main); err != nil {
		t.Fatal(err)
	}
	r2, _ := icechunk.Open(ctx, st, nil)
	rs, err := r2.ReadonlySession(ctx, icechunk.AtTag("t"))
	if err != nil {
		t.Fatal(err)
	}
	if attrs, _ := rs.Attributes("/"); attrs["on"] != "s3" {
		t.Errorf("attributes = %v", attrs)
	}
}
