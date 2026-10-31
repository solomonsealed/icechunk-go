package httpstore_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	icechunk "github.com/solomonsealed/icechunk-go"
	"github.com/solomonsealed/icechunk-go/storage"
	"github.com/solomonsealed/icechunk-go/storage/httpstore"
	zarr "github.com/solomonsealed/zarr-go"
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
// fakeS3 is an S3 endpoint for the bucket "bucket" and the credentials
// AK/SK: it verifies SigV4 signatures, refusing unsigned x-amz-* headers as
// S3 does, and keeps each object's storage class and user metadata.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	attrs   map[string]http.Header // x-amz-storage-class and x-amz-meta-* of each object
	puts    []fakePut
}

// fakePut records a PUT: its key, the storage class and user metadata it
// stored, and for a CopyObject, the key copied.
type fakePut struct {
	key, copyOf string
	attrs       http.Header
}

func (f *fakeS3) etag(b []byte) string { return fmt.Sprintf(`"%x"`, md5.Sum(b)) }

func (f *fakeS3) takePuts() []fakePut {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.puts
	f.puts = nil
	return out
}

func hmacSum(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

// verifySigV4 checks a request's AWS Signature Version 4 for the secret key
// "SK", and that every x-amz-* header is signed.
func verifySigV4(r *http.Request) error {
	const prefix = "AWS4-HMAC-SHA256 Credential="
	auth, ok := strings.CutPrefix(r.Header.Get("Authorization"), prefix)
	parts := strings.Split(auth, ", ")
	if !ok || len(parts) != 3 {
		return fmt.Errorf("not a SigV4 authorization: %q", r.Header.Get("Authorization"))
	}
	cred := strings.Split(parts[0], "/")
	signedList, _ := strings.CutPrefix(parts[1], "SignedHeaders=")
	if len(cred) != 5 || cred[0] != "AK" {
		return fmt.Errorf("bad credential %q", parts[0])
	}
	signed := strings.Split(signedList, ";")
	for k := range r.Header {
		if k := strings.ToLower(k); strings.HasPrefix(k, "x-amz-") && !slices.Contains(signed, k) {
			return fmt.Errorf("header %s is not signed", k)
		}
	}
	var canon strings.Builder
	for _, h := range signed {
		v := r.Host
		if h != "host" {
			v = strings.Join(strings.Fields(r.Header.Get(h)), " ")
		}
		canon.WriteString(h + ":" + v + "\n")
	}
	canonical := strings.Join([]string{r.Method, r.URL.EscapedPath(), r.URL.RawQuery, canon.String(), signedList, r.Header.Get("X-Amz-Content-Sha256")}, "\n")
	sum := sha256.Sum256([]byte(canonical))
	toSign := "AWS4-HMAC-SHA256\n" + r.Header.Get("X-Amz-Date") + "\n" + strings.Join(cred[1:], "/") + "\n" + hex.EncodeToString(sum[:])
	key := hmacSum([]byte("AWS4SK"), cred[1])
	for _, s := range cred[2:] {
		key = hmacSum(key, s)
	}
	if want := "Signature=" + hex.EncodeToString(hmacSum(key, toSign)); parts[2] != want {
		return fmt.Errorf("signature mismatch")
	}
	return nil
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := verifySigV4(r); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
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
	case "GET", "HEAD":
		if !exists {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("ETag", f.etag(cur))
		for k, v := range f.attrs[key] {
			w.Header()[k] = v
		}
		http.ServeContent(w, r, key, time.Time{}, bytes.NewReader(cur))
	case "PUT":
		if src := r.Header.Get("X-Amz-Copy-Source"); src != "" {
			// CopyObject keeps the source's metadata; the storage class is
			// the request's.
			srcKey, _ := url.PathUnescape(strings.TrimPrefix(src, "/bucket/"))
			data, ok := f.objects[srcKey]
			switch {
			case !ok:
				w.WriteHeader(http.StatusNotFound)
				return
			case r.Header.Get("X-Amz-Copy-Source-If-Match") != "" && r.Header.Get("X-Amz-Copy-Source-If-Match") != f.etag(data):
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			attrs := http.Header{}
			for k, v := range f.attrs[srcKey] {
				if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") {
					attrs[k] = v
				}
			}
			if c := r.Header.Get("X-Amz-Storage-Class"); c != "" {
				attrs.Set("X-Amz-Storage-Class", c)
			}
			f.objects[key] = slices.Clone(data)
			if f.attrs == nil {
				f.attrs = map[string]http.Header{}
			}
			f.attrs[key] = attrs
			f.puts = append(f.puts, fakePut{key: strings.TrimPrefix(key, "repo/"), copyOf: strings.TrimPrefix(srcKey, "repo/"), attrs: attrs})
			fmt.Fprintf(w, "<CopyObjectResult><ETag>%s</ETag></CopyObjectResult>", html.EscapeString(f.etag(data)))
			return
		}
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
		attrs := http.Header{}
		for k, v := range r.Header {
			if lk := strings.ToLower(k); lk == "x-amz-storage-class" || strings.HasPrefix(lk, "x-amz-meta-") {
				attrs[k] = v
			}
		}
		if f.attrs == nil {
			f.attrs = map[string]http.Header{}
		}
		f.attrs[key] = attrs
		f.puts = append(f.puts, fakePut{key: strings.TrimPrefix(key, "repo/"), attrs: attrs})
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

// recordingS3 is a fakeS3 that records each request's key and User-Agent.
type recordingS3 struct {
	fakeS3
	mu       sync.Mutex
	requests [][2]string // key, User-Agent
}

func (f *recordingS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests = append(f.requests, [2]string{strings.TrimPrefix(r.URL.Path, "/bucket/repo/"), r.Header.Get("User-Agent")})
	f.mu.Unlock()
	f.fakeS3.ServeHTTP(w, r)
}

func (f *recordingS3) take() [][2]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.requests
	f.requests = nil
	return out
}

// Every request a repository makes carries its attribution in the
// User-Agent header, as upstream's do (since icechunk 2.3.0): chunk requests
// (virtual ones included) name the array and chunk, manifest requests the
// array, and the others neither.
func TestAttributionUserAgents(t *testing.T) {
	ctx := context.Background()
	srv := &recordingS3{fakeS3: fakeS3{objects: map[string][]byte{}}}
	hs := httptest.NewServer(srv)
	defer hs.Close()
	var virtualUA []string
	var vmu sync.Mutex
	vs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vmu.Lock()
		virtualUA = append(virtualUA, r.Header.Get("User-Agent"))
		vmu.Unlock()
		http.ServeContent(w, r, "f", time.Time{}, bytes.NewReader(bytes.Repeat([]byte{7}, 4096)))
	}))
	defer vs.Close()
	st := httpstore.NewS3(storage.S3Config{Bucket: "bucket", Prefix: "repo", Endpoint: hs.URL, AccessKeyID: "AK", SecretAccessKey: "SK"},
		&httpstore.Options{Header: http.Header{"User-Agent": {"my-app/1"}}})
	opts := &icechunk.Options{
		Attribution:            icechunk.Attribution{Client: "weatherlib/0.9", Workload: "nightly-ingest", Principal: "u_123"},
		VirtualChunkContainers: map[string]storage.Storage{"s3://src/": httpstore.New(vs.URL, nil)},
	}
	const labels = "my-app/1 weatherlib/0.9 icechunk/2.3.1 (workload=nightly-ingest; principal=u_123"
	check := func(what string, reqs [][2]string, chunks map[string]bool) {
		t.Helper()
		if len(reqs) == 0 {
			t.Errorf("%s: no requests", what)
		}
		for _, r := range reqs {
			key, ua := r[0], r[1]
			switch {
			case strings.HasPrefix(key, "chunks/"):
				c, ok := strings.CutPrefix(ua, labels+"; array=g/temperature; chunk=")
				if !ok || !strings.HasSuffix(c, ")") || !chunks[strings.TrimSuffix(c, ")")] {
					t.Errorf("%s: %s sent %q", what, key, ua)
				}
			case strings.HasPrefix(key, "manifests/"):
				if ua != labels+"; array=g/temperature)" {
					t.Errorf("%s: %s sent %q", what, key, ua)
				}
			default:
				if ua != labels+")" {
					t.Errorf("%s: %s sent %q", what, key, ua)
				}
			}
		}
	}

	repo, err := icechunk.Create(ctx, st, opts)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := repo.WritableSession(ctx, "main")
	arr, err := s.CreateArray(ctx, "/g/temperature", zarr.ArraySpec{Shape: []uint64{300}, ChunkShape: []uint64{100}, DataType: "float64",
		Codecs: []zarr.CodecSpec{{Name: "bytes", Configuration: []byte(`{"endian": "little"}`)}}})
	if err != nil {
		t.Fatal(err)
	}
	vals := make([]float64, 200)
	for i := range vals {
		vals[i] = float64(i) + 0.5
	}
	nd, _ := zarr.FromSlice([]uint64{200}, vals)
	if err := arr.Write(ctx, []uint64{0}, nd); err != nil {
		t.Fatal(err)
	}
	if err := s.SetVirtualRef(ctx, "/g/temperature", []uint32{2}, icechunk.VirtualRef{Location: "s3://src/f", Offset: 0, Length: 800}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Commit(ctx, "attributed", nil); err != nil {
		t.Fatal(err)
	}
	check("create and commit", srv.take(), map[string]bool{"0": true, "1": true})

	r2, err := icechunk.Open(ctx, st, opts)
	if err != nil {
		t.Fatal(err)
	}
	rs, err := r2.ReadonlySession(ctx, icechunk.AtBranch("main"))
	if err != nil {
		t.Fatal(err)
	}
	ra, _ := rs.OpenArray(ctx, "/g/temperature")
	if _, err := ra.ReadAll(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := rs.Store().Get(ctx, "g/temperature/c/1"); err != nil {
		t.Fatal(err)
	}
	check("read", srv.take(), map[string]bool{"0": true, "1": true})
	vmu.Lock()
	if len(virtualUA) == 0 || virtualUA[0] != "weatherlib/0.9 icechunk/2.3.1 (workload=nightly-ingest; principal=u_123; array=g/temperature; chunk=2)" {
		t.Errorf("virtual chunk requests sent %q", virtualUA)
	}
	vmu.Unlock()

	// Unattributed repositories identify the module only.
	r3, err := icechunk.Open(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r3.LookupBranch(ctx, "main"); err != nil {
		t.Fatal(err)
	}
	for _, r := range srv.take() {
		if r[1] != "my-app/1 icechunk/2.3.1" {
			t.Errorf("unattributed %s sent %q", r[0], r[1])
		}
	}
	if _, err := icechunk.Open(ctx, st, &icechunk.Options{Attribution: icechunk.Attribution{Workload: "a;b"}}); err == nil {
		t.Error("invalid attribution accepted")
	}
}

// faultyS3 is a fakeS3 whose next PUT fails as set by fault: "lost" stores
// the object and drops the connection before answering, "drop" drops it
// without storing, "replay" stores the object and answers 412 (as when a
// retried write trips its own condition), and "headfail" drops it and
// answers the next HEAD with 500.
type faultyS3 struct {
	*fakeS3
	mu            sync.Mutex
	fault         string
	failHead      bool
	heads         int
	headsAfterPut int
}

func (f *faultyS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	fault := ""
	if r.Method == "PUT" {
		fault, f.fault = f.fault, ""
		if fault == "headfail" {
			f.failHead = true
		}
	}
	failHead := r.Method == "HEAD" && f.failHead
	if r.Method == "HEAD" {
		f.heads++
		f.failHead = false
	}
	f.mu.Unlock()
	drop := func() {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
	}
	switch {
	case failHead:
		w.WriteHeader(http.StatusInternalServerError)
	case fault == "lost":
		f.fakeS3.ServeHTTP(httptest.NewRecorder(), r)
		drop()
	case fault == "drop", fault == "headfail":
		io.ReadAll(r.Body)
		drop()
	case fault == "replay":
		f.fakeS3.ServeHTTP(httptest.NewRecorder(), r)
		w.WriteHeader(http.StatusPreconditionFailed)
	default:
		f.fakeS3.ServeHTTP(w, r)
	}
}

// A conditional write stamped with a write id recovers from a lost
// response by reading the object back, as upstream's do.
func TestS3PutReadsBackConditionalWrites(t *testing.T) {
	ctx := context.Background()
	f := &faultyS3{fakeS3: &fakeS3{objects: map[string][]byte{}}}
	srv := httptest.NewServer(f)
	defer srv.Close()
	st := httpstore.NewS3(storage.S3Config{Bucket: "bucket", Endpoint: srv.URL, AccessKeyID: "AK", SecretAccessKey: "SK"}, nil)
	withID := func(id string, o storage.PutOptions) *storage.PutOptions {
		o.Metadata = map[string]string{storage.WriteIDMetadataKey: id, "icfiletype": "repo-info"}
		return &o
	}
	put := func(fault, key, data string, opts *storage.PutOptions) (string, error) {
		f.mu.Lock()
		f.fault, f.heads = fault, 0
		f.mu.Unlock()
		return st.Put(ctx, key, []byte(data), opts)
	}
	heads := func() int {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.heads
	}

	// The response of a write that landed is lost: the read-back finds it.
	v1, err := put("lost", "k", "one", withID("w1", storage.PutOptions{IfNotExists: true}))
	if err != nil || v1 == "" {
		t.Fatalf("lost response: %q, %v", v1, err)
	}
	if _, v, _ := st.GetVersion(ctx, "k"); v != v1 {
		t.Errorf("version %q, stored %q", v1, v)
	}
	// A write whose (retried) request trips its own condition succeeded.
	v2, err := put("replay", "k", "two", withID("w2", storage.PutOptions{IfMatch: v1}))
	if err != nil || v2 == v1 {
		t.Fatalf("replayed write: %q, %v", v2, err)
	}
	// A genuine conflict: the stored object is another write.
	if _, err := put("", "k", "three", withID("w3", storage.PutOptions{IfMatch: v1})); !errors.Is(err, storage.ErrPreconditionFailed) {
		t.Errorf("conflict: %v", err)
	}
	if heads() != 1 {
		t.Errorf("conflict: %d read-backs", heads())
	}
	// A write that did not land returns its own error.
	if _, err := put("drop", "new", "x", withID("w4", storage.PutOptions{IfNotExists: true})); err == nil || errors.Is(err, storage.ErrPreconditionFailed) {
		t.Errorf("lost write: %v", err)
	}
	// A failed read-back is reported, not guessed from.
	if _, err := put("headfail", "new", "x", withID("w5", storage.PutOptions{IfNotExists: true})); err == nil || !strings.Contains(err.Error(), "reading back") {
		t.Errorf("failed read-back: %v", err)
	}
	// Unconditional writes, and conditional ones without a write id, are
	// not read back.
	if _, err := put("drop", "new", "x", withID("w6", storage.PutOptions{})); err == nil || heads() != 0 {
		t.Errorf("unconditional: %v, %d read-backs", err, heads())
	}
	if _, err := put("", "k", "x", &storage.PutOptions{IfMatch: v1}); !errors.Is(err, storage.ErrPreconditionFailed) || heads() != 0 {
		t.Errorf("without a write id: %v, %d read-backs", err, heads())
	}
}

// Writes apply the storage settings: the storage classes and the object
// metadata (icspecver, icclient, icfiletype, iccompalg, and a write id on
// conditional writes of the repo info file) upstream writes.
func TestStorageSettings(t *testing.T) {
	ctx := context.Background()
	f := &fakeS3{objects: map[string][]byte{}}
	srv := httptest.NewServer(f)
	defer srv.Close()
	st := httpstore.NewS3(storage.S3Config{Bucket: "bucket", Prefix: "repo", Endpoint: srv.URL, AccessKeyID: "AK", SecretAccessKey: "SK"}, nil)
	uuid := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	fileTypes := map[string]string{"snapshots/": "snapshot", "manifests/": "manifest", "transactions/": "transaction-log", "repo": "repo-info", "overwritten/": "repo-info"}
	// check verifies the PUTs of puts given the storage class of each kind
	// of object ("chunks/", "metadata" for snapshots, manifests and
	// transaction logs, and "repo" for the repo info file and its backups).
	check := func(what string, puts []fakePut, classes map[string]string, metadata bool) {
		t.Helper()
		seen := map[string]bool{}
		repoMeta := http.Header{} // of the repo info version last written
		for _, p := range puts {
			kind := "metadata"
			switch {
			case strings.HasPrefix(p.key, "chunks/"):
				kind = "chunks/"
			case p.key == "repo", strings.HasPrefix(p.key, "overwritten/"):
				kind = "repo"
			}
			seen[kind] = true
			if got := p.attrs.Get("X-Amz-Storage-Class"); got != classes[kind] {
				t.Errorf("%s: %s stored in class %q, want %q", what, p.key, got, classes[kind])
			}
			var fileType string
			for prefix, ft := range fileTypes {
				if strings.HasPrefix(p.key, prefix) {
					fileType = ft
				}
			}
			meta := map[string]string{}
			metaHeader := http.Header{}
			for k := range p.attrs {
				if name, ok := strings.CutPrefix(strings.ToLower(k), "x-amz-meta-"); ok {
					meta[name] = p.attrs.Get(k)
					metaHeader[k] = p.attrs[k]
				}
			}
			if strings.HasPrefix(p.key, "overwritten/") {
				// Backups are server-side copies of the repo info version
				// read, as upstream's: they keep its metadata.
				if p.copyOf != "repo" || !reflect.DeepEqual(metaHeader, repoMeta) {
					t.Errorf("%s: backup %s copies %q with metadata %v, want a copy of repo with %v", what, p.key, p.copyOf, metaHeader, repoMeta)
				}
				continue
			}
			if p.key == "repo" {
				repoMeta = metaHeader
			}
			want := map[string]string{}
			if metadata && fileType != "" {
				want = map[string]string{"icspecver": "2", "icclient": "ic-2.3.1", "icfiletype": fileType, "iccompalg": "zstd"}
				if p.key == "repo" {
					want[storage.WriteIDMetadataKey] = meta[storage.WriteIDMetadataKey]
					if !uuid.MatchString(meta[storage.WriteIDMetadataKey]) {
						t.Errorf("%s: repo info write id %q", what, meta[storage.WriteIDMetadataKey])
					}
				}
			}
			if !reflect.DeepEqual(meta, want) {
				t.Errorf("%s: %s has metadata %v, want %v", what, p.key, meta, want)
			}
		}
		for _, kind := range []string{"chunks/", "metadata", "repo"} {
			if !seen[kind] {
				t.Errorf("%s: no %s written", what, kind)
			}
		}
	}
	commit := func(repo *icechunk.Repository, path string) {
		t.Helper()
		s, err := repo.WritableSession(ctx, "main")
		if err != nil {
			t.Fatal(err)
		}
		arr, err := s.CreateArray(ctx, path, zarr.ArraySpec{Shape: []uint64{200}, ChunkShape: []uint64{100}, DataType: "float64",
			Codecs: []zarr.CodecSpec{{Name: "bytes", Configuration: []byte(`{"endian": "little"}`)}}})
		if err != nil {
			t.Fatal(err)
		}
		nd, _ := zarr.FromSlice([]uint64{200}, make([]float64, 200))
		for i := range 200 {
			nd.Data[8*i] = byte(i + 1)
		}
		if err := arr.Write(ctx, []uint64{0}, nd); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Commit(ctx, "write "+path, nil); err != nil {
			t.Fatal(err)
		}
	}

	// A repository created with settings (Go does not persist them).
	opts := &icechunk.Options{StorageSettings: icechunk.StorageSettings{StorageClass: "STANDARD_IA", ChunksStorageClass: "INTELLIGENT_TIERING"}}
	repo, err := icechunk.Create(ctx, st, opts)
	if err != nil {
		t.Fatal(err)
	}
	commit(repo, "/a")
	classes := map[string]string{"chunks/": "INTELLIGENT_TIERING", "metadata": "STANDARD_IA", "repo": "STANDARD_IA"}
	check("created", f.takePuts(), classes, true)

	// Settings persisted by icechunk-python, which also persists that its
	// local repositories keep no metadata.
	f2 := &fakeS3{objects: map[string][]byte{}}
	srv2 := httptest.NewServer(f2)
	defer srv2.Close()
	dir := "../../testdata/writer/initial/config"
	err = filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		rel, _ := filepath.Rel(dir, p)
		f2.objects["repo/"+filepath.ToSlash(rel)] = b
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	st2 := httpstore.NewS3(storage.S3Config{Bucket: "bucket", Prefix: "repo", Endpoint: srv2.URL, AccessKeyID: "AK", SecretAccessKey: "SK"}, nil)
	repo, err = icechunk.Open(ctx, st2, nil)
	if err != nil {
		t.Fatal(err)
	}
	commit(repo, "/b")
	persisted := map[string]string{"chunks/": "INTELLIGENT_TIERING", "metadata": "GLACIER_IR", "repo": "STANDARD_IA"}
	check("persisted", f2.takePuts(), persisted, false)

	// Settings given to Open override the persisted ones field by field.
	on := true
	repo, err = icechunk.Open(ctx, st2, &icechunk.Options{StorageSettings: icechunk.StorageSettings{StorageClass: "ONEZONE_IA", UnsafeUseMetadata: &on}})
	if err != nil {
		t.Fatal(err)
	}
	commit(repo, "/c")
	check("overridden", f2.takePuts(), map[string]string{"chunks/": "INTELLIGENT_TIERING", "metadata": "GLACIER_IR", "repo": "ONEZONE_IA"}, true)
}
