package conformance

// Consistency tests against icechunk-python.
//
// testdata/oracle/oracle.py records what icechunk-python (with zarr-python)
// reads from every fixture repository: refs, ancestry, repo info, snapshot
// and manifest contents, the Zarr store view (keys, bytes, byte ranges,
// listings, missing and malformed keys), chunk references and array values.
// These tests read the same repositories with the Go reader and require the
// same answers. Differences the Go reader makes on purpose are listed in
// acceptedDivergence. Differences these tests found that are not fixed yet
// are listed in knownInconsistencies: they are logged, and fail with
// ICECHUNK_STRICT=1. Anything else fails. Manifest contents are compared by
// TestPythonManifests in package icechunk, which needs its internals.
//
// The recorded answers live in testdata/oracle/*.json.gz. To compare against
// a live icechunk-python instead, point ICECHUNK_PYTHON at an interpreter
// that has icechunk, zarr and numpy installed:
//
//	ICECHUNK_PYTHON=.venv/bin/python go test -run Python ./internal/conformance
//
// Virtual chunks are served on both sides by a local, anonymous, path-style
// S3 endpoint with the same ETag and Last-Modified semantics (objectServer
// here, ObjectServer in oracle.py), so checksum preconditions are exercised
// through the real HTTP clients.

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	icechunk "github.com/solomonsealed/icechunk-go"
	"github.com/solomonsealed/icechunk-go/storage"
	"github.com/solomonsealed/icechunk-go/storage/httpstore"
	zarr "github.com/solomonsealed/zarr-go"
)

// ---------------------------------------------------------------------------
// Oracle files (see the docstring of testdata/oracle/oracle.py)

type oracleFile struct {
	Meta struct {
		Fixture  string `json:"fixture"`
		Path     string `json:"path"`
		Icechunk string `json:"icechunk"`
		Zarr     string `json:"zarr"`
	} `json:"_meta"`
	Virtual   map[string]virtualSource   `json:"virtual"`
	Repo      oracleRepo                 `json:"repo"`
	Manifests map[string]json.RawMessage `json:"manifests"`
	Snapshots map[string]*oracleSnapshot `json:"snapshots"`
	Arrays    map[string]*oracleArray    `json:"arrays"`
}

type virtualSource struct {
	Dir     string            `json:"dir"`
	Objects map[string]string `json:"objects"` // base64
	MTime   int64             `json:"mtime"`
}

type oracleRepo struct {
	SpecVersion   int                        `json:"spec_version"`
	Branches      map[string]string          `json:"branches"` // snapshot id or "error:..."
	Tags          map[string]string          `json:"tags"`
	Lookups       map[string]json.RawMessage `json:"lookups"`
	Ancestry      map[string][]string        `json:"ancestry"`
	SnapshotInfos map[string]json.RawMessage `json:"snapshot_infos"`
	Info          oracleInfo                 `json:"info"`
}

type oracleInfo struct {
	Error             string          `json:"error"`
	WrittenBy         string          `json:"written_by"`
	SpecVersion       string          `json:"spec_version"`
	Branches          json.RawMessage `json:"branches"`
	Tags              json.RawMessage `json:"tags"`
	DeletedTags       json.RawMessage `json:"deleted_tags"`
	Snapshots         json.RawMessage `json:"snapshots"`
	LatestUpdates     []updateEntry   `json:"latest_updates"`
	RepoBeforeUpdates *string         `json:"repo_before_updates"`
	OpsLog            json.RawMessage `json:"ops_log"`
	Metadata          json.RawMessage `json:"metadata"`
	Status            json.RawMessage `json:"status"`
	FeatureFlags      json.RawMessage `json:"feature_flags"`
	Config            json.RawMessage `json:"config"`
}

type oracleSnapshot struct {
	Header struct {
		WrittenBy   string `json:"written_by"`
		SpecVersion string `json:"spec_version"`
	} `json:"header"`
	ID               string                     `json:"id"`
	FlushedAt        string                     `json:"flushed_at"`
	Message          string                     `json:"message"`
	Metadata         json.RawMessage            `json:"metadata"`
	Manifests        json.RawMessage            `json:"manifests"`
	Nodes            []nodeEntry                `json:"nodes"`
	VirtualLocations json.RawMessage            `json:"virtual_locations"`
	Store            oracleStore                `json:"store"`
	GroupAttributes  map[string]json.RawMessage `json:"group_attributes"`
}

type nodeEntry struct {
	Path           string             `json:"path"`
	ID             string             `json:"id"`
	Type           string             `json:"type"`
	ZarrJSON       string             `json:"zarr_json"`
	Content        string             `json:"content,omitempty"` // oracle only: key into oracleFile.Arrays
	Shape          [][2]uint64        `json:"shape,omitempty"`
	DimensionNames []string           `json:"dimension_names"`
	ManifestRefs   []manifestRefEntry `json:"manifest_refs,omitempty"`
}

type manifestRefEntry struct {
	ID      string      `json:"id"`
	Extents [][2]uint32 `json:"extents"`
}

type oracleStore struct {
	List         []string                   `json:"list"`
	MetadataKeys map[string]keyInfo         `json:"metadata_keys"`
	Probes       map[string]probeInfo       `json:"probes"`
	ListDir      map[string]json.RawMessage `json:"list_dir"`    // listing or "error:..."
	ListPrefix   map[string]json.RawMessage `json:"list_prefix"` // listing or "error:..."
}

type keyInfo struct {
	Get    string            `json:"get"`  // digest, "absent" or "error:..."
	Size   json.RawMessage   `json:"size"` // int or "error:..."
	Ranges map[string]string `json:"ranges"`
}

type probeInfo struct {
	Get    string          `json:"get"`
	Exists json.RawMessage `json:"exists"` // bool or "error:..."
	Size   json.RawMessage `json:"size"`
}

type listing struct {
	Count  int      `json:"count"`
	Digest string   `json:"digest"`
	Keys   []string `json:"keys,omitempty"`
}

type oracleArray struct {
	Path            string             `json:"path"`
	NodeID          string             `json:"node_id"`
	Refs            []json.RawMessage  `json:"refs"` // [coords, kind, location, offset, length, inline digest]
	ChunkTypeProbes map[string]string  `json:"chunk_type_probes"`
	ChunkKeys       map[string]keyInfo `json:"chunk_keys"`
	Zarr            struct {
		Error          string          `json:"error"`
		Shape          []uint64        `json:"shape"`
		DataType       json.RawMessage `json:"data_type"`
		ChunkGridShape []uint64        `json:"chunk_grid_shape"`
		DimensionNames []*string       `json:"dimension_names"` // as in zarr.json: null, or a name or null per dimension
		Attributes     json.RawMessage `json:"attributes"`
		Reads          []oracleRead    `json:"reads"`
		ChunkReads     []oracleRead    `json:"chunk_reads"`
	} `json:"zarr"`
}

type oracleRead struct {
	Start  []uint64        `json:"start"`
	Count  []uint64        `json:"count"`
	Coords []uint64        `json:"coords"`
	Result string          `json:"result"` // digest or "error:..."
	Values json.RawMessage `json:"values"`
}

type updateEntry struct {
	Kind               string  `json:"kind"`
	Name               *string `json:"name"`
	SnapshotID         *string `json:"snapshot_id"`
	PreviousSnapshotID *string `json:"previous_snapshot_id"`
	UpdatedAt          string  `json:"updated_at"`
	BackupPath         *string `json:"backup_path"`
}

// ---------------------------------------------------------------------------
// Fixtures: an oracle file plus the same repository opened by the Go reader

type fixture struct {
	name string
	o    *oracleFile
	repo *icechunk.Repository
	// arrayAt locates one snapshot and path holding each array version.
	arrayAt map[string]arrayLoc
}

type arrayLoc struct {
	snapshot string
	path     string
}

var (
	fixturesOnce sync.Once
	fixturesList []*fixture
	fixturesErr  error
	fixturesSkip string
)

// pythonFixtures loads every oracle file (recording them first when
// ICECHUNK_PYTHON is set) and opens the matching repositories.
func pythonFixtures(t *testing.T) []*fixture {
	t.Helper()
	fixturesOnce.Do(func() { fixturesList, fixturesSkip, fixturesErr = loadPythonFixtures() })
	if fixturesErr != nil {
		t.Fatal(fixturesErr)
	}
	if fixturesSkip != "" {
		t.Skip(fixturesSkip)
	}
	return fixturesList
}

func loadPythonFixtures() ([]*fixture, string, error) {
	dir := testdata + "/oracle"
	if py := os.Getenv("ICECHUNK_PYTHON"); py != "" {
		tmp, err := os.MkdirTemp("", "icechunk-oracle-")
		if err != nil {
			return nil, "", err
		}
		cmd := exec.Command(py, "testdata/oracle/oracle.py", "--out", tmp)
		cmd.Dir = repoRoot // so a relative ICECHUNK_PYTHON resolves from the module root
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			return nil, "", fmt.Errorf("recording with %s: %v\n%s", py, err, tail(stderr.String(), 4000))
		}
		dir = tmp
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.json.gz"))
	if len(files) == 0 {
		return nil, "no oracle files in " + dir + " (run testdata/oracle/oracle.py)", nil
	}
	sort.Strings(files)
	var out []*fixture
	for _, f := range files {
		o, err := readOracle(f)
		if err != nil {
			return nil, "", fmt.Errorf("%s: %w", f, err)
		}
		fx := &fixture{name: o.Meta.Fixture, o: o, arrayAt: map[string]arrayLoc{}}
		containers := map[string]storage.Storage{}
		if len(o.Virtual) > 0 {
			srv := httptest.NewServer(objectServer(o.Virtual))
			// Reads fetch up to 16 chunks at once; http.DefaultClient keeps 2
			// idle connections per host, and closing the rest leaves enough
			// sockets in TIME_WAIT to exhaust local ports over a few runs.
			client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 64}}
			for prefix := range o.Virtual {
				u, err := url.Parse(prefix)
				if err != nil {
					return nil, "", err
				}
				containers[prefix] = httpstore.NewS3(storage.S3Config{
					Bucket: u.Host, Prefix: strings.Trim(u.Path, "/"), Endpoint: srv.URL, Anonymous: true,
				}, &httpstore.Options{Client: client})
			}
		}
		fx.repo, err = icechunk.Open(context.Background(), storage.NewLocal(filepath.Join(repoRoot, o.Meta.Path)), &icechunk.Options{
			VirtualChunkContainers: containers,
		})
		if err != nil {
			return nil, "", fmt.Errorf("opening %s: %w", o.Meta.Path, err)
		}
		for _, sid := range sortedKeys(o.Snapshots) {
			for _, n := range o.Snapshots[sid].Nodes {
				if _, ok := fx.arrayAt[n.Content]; n.Content != "" && !ok {
					fx.arrayAt[n.Content] = arrayLoc{sid, n.Path}
				}
			}
		}
		out = append(out, fx)
	}
	return out, "", nil
}

func readOracle(path string) (*oracleFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	var o oracleFile
	if err := json.NewDecoder(zr).Decode(&o); err != nil {
		return nil, err
	}
	return &o, nil
}

// forEachFixture runs fn in a parallel subtest per fixture.
func forEachFixture(t *testing.T, fn func(t *testing.T, fx *fixture)) {
	for _, fx := range pythonFixtures(t) {
		t.Run(fx.name, func(t *testing.T) {
			t.Parallel()
			fn(t, fx)
		})
	}
}

func (fx *fixture) session(t *testing.T, sid string) *icechunk.Session {
	t.Helper()
	id, err := icechunk.ParseSnapshotID(sid)
	if err != nil {
		t.Fatal(err)
	}
	s, err := fx.repo.ReadonlySession(context.Background(), icechunk.AtSnapshot(id))
	if err != nil {
		t.Fatalf("session at %s: %v", sid, err)
	}
	return s
}

// objectServer serves virtual chunk objects like ObjectServer in oracle.py:
// GET /<bucket>/<key> with Range, If-Match (against the quoted MD5 ETag) and
// If-Unmodified-Since (against the file's mtime, or the recorded mtime for
// objects without a file); failed preconditions answer 412.
func objectServer(sources map[string]virtualSource) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bucket, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
		var data []byte
		var mtime int64
		found := false
		for prefix, src := range sources {
			u, _ := url.Parse(prefix)
			base := strings.TrimPrefix(u.Path, "/")
			rest, ok := strings.CutPrefix(key, base)
			if u.Host != bucket || !ok {
				continue
			}
			if src.Dir != "" {
				p := filepath.Join(repoRoot, src.Dir, filepath.FromSlash(rest))
				if st, err := os.Stat(p); err == nil && !st.IsDir() {
					data, _ = os.ReadFile(p)
					mtime, found = st.ModTime().Unix(), true
				}
			} else if b64, ok := src.Objects[rest]; ok {
				data, _ = base64.StdEncoding.DecodeString(b64)
				mtime, found = src.MTime, true
			}
		}
		if !found {
			http.NotFound(w, r)
			return
		}
		sum := md5.Sum(data)
		etag := hex.EncodeToString(sum[:])
		if im := r.Header.Get("If-Match"); im != "" && strings.Trim(strings.TrimSpace(im), `"`) != etag {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		if ius := r.Header.Get("If-Unmodified-Since"); ius != "" {
			if t, err := http.ParseTime(ius); err == nil && mtime > t.Unix() {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
		}
		w.Header().Set("ETag", `"`+etag+`"`)
		w.Header().Set("Last-Modified", time.Unix(mtime, 0).UTC().Format(http.TimeFormat))
		status, body := http.StatusOK, data
		if rng := r.Header.Get("Range"); rng != "" {
			a, b, _ := strings.Cut(strings.TrimPrefix(rng, "bytes="), "-")
			start, _ := strconv.Atoi(a)
			end := len(data) - 1
			if b != "" {
				end, _ = strconv.Atoi(b)
			}
			end = min(end, len(data)-1)
			body, status = data[start:end+1], http.StatusPartialContent
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		}
		w.WriteHeader(status)
		w.Write(body)
	})
}

// ---------------------------------------------------------------------------
// Digests and outcomes, encoded as oracle.py encodes them

func digestBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

// digestND hashes fixed-size elements as their little-endian bytes, and
// variable-length ones as an 8-byte length followed by the bytes.
func digestND(nd *zarr.NDArray) string {
	if !nd.DataType.Variable() {
		return digestBytes(nd.Data)
	}
	h := sha256.New()
	var n [8]byte
	for _, s := range nd.Strings {
		binary.LittleEndian.PutUint64(n[:], uint64(len(s)))
		h.Write(n[:])
		h.Write([]byte(s))
	}
	return hex.EncodeToString(h.Sum(nil)[:8])
}

func digestListing(keys []string) listing {
	keys = slices.Clone(keys)
	sort.Strings(keys)
	l := listing{Count: len(keys), Digest: digestBytes([]byte(strings.Join(keys, "\n")))}
	if len(keys) <= 32 {
		l.Keys = keys
	}
	return l
}

func isError(s string) bool { return strings.HasPrefix(s, "error:") }

// getOutcome classifies a Go read like oracle.py does: a digest, "absent"
// (ErrKeyNotFound) or "error".
func getOutcome(data []byte, err error) string {
	switch {
	case errors.Is(err, icechunk.ErrKeyNotFound):
		return "absent"
	case err != nil:
		return "error"
	}
	return digestBytes(data)
}

// acceptedDivergence reports whether a Go outcome that differs from
// icechunk-python's is one of the documented, deliberate differences:
//
//  1. Malformed keys and keys that cannot exist ("a/c/x", "a/c/-1",
//     "a/zarr.json/x", chunk keys of groups or beyond uint32): upstream
//     raises InvalidInputError (or NodeNotFoundError); Go's Store treats
//     them as missing (ErrKeyNotFound, Exists false).
//  2. Store.Size of a missing chunk: upstream returns 0; Go returns
//     ErrKeyNotFound, as Get does.
//  3. (none: Store.ListPrefix now refuses prefixes naming no node, as
//     upstream does)
//  4. A zero-length byte range ending at the end of a chunk: upstream
//     raises; Go returns no bytes.
//  5. Keys and prefixes with a leading or doubled slash ("//zarr.json",
//     list_dir("/group")): upstream finds nothing; Go normalizes slashes.
func acceptedDivergence(key, py, got string) bool {
	switch py {
	case "error:InvalidInputError", "error:NodeNotFoundError":
		return got == "absent" || got == "false" || got == "error"
	case "absent", "false":
		return nonCanonical(key)
	}
	return isError(py) && got == "error"
}

func nonCanonical(key string) bool {
	return strings.HasPrefix(key, "/") || strings.Contains(key, "//")
}

// knownInconsistencies are differences from icechunk-python that these tests
// found in the Go reader or writer and that are not fixed yet, by id with an
// explanation. Matching failures are logged; set ICECHUNK_STRICT=1 to make
// them fail. Delete an entry once fixed. (All found so far are fixed.)
var knownInconsistencies = map[string]string{}

func knownIssue(t *testing.T, id, format string, args ...any) {
	t.Helper()
	if _, ok := knownInconsistencies[id]; !ok {
		t.Fatalf("unregistered inconsistency %q", id)
	}
	msg := fmt.Sprintf(format, args...)
	if os.Getenv("ICECHUNK_STRICT") != "" {
		t.Errorf("[%s] %s", id, msg)
		return
	}
	t.Logf("known inconsistency [%s]: %s", id, msg)
}

// ---------------------------------------------------------------------------
// Structural comparison of JSON-like values

// jsonValue round-trips v through JSON (numbers as json.Number), so Go values
// and oracle values compare as the same kinds.
func jsonValue(t testing.TB, v any) any {
	t.Helper()
	b, err := json.Marshal(sanitize(v))
	if err != nil {
		t.Fatalf("encoding %T: %v", v, err)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var out any
	if err := dec.Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

// sanitize encodes what JSON cannot hold the way oracle.py does: non-finite
// floats as Python's repr, byte strings as {"bytes": hex}.
func sanitize(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = sanitize(e)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = sanitize(e)
		}
		return out
	case []byte:
		return map[string]any{"bytes": hex.EncodeToString(x)}
	case float32:
		return sanitize(float64(x))
	case float64:
		switch {
		case math.IsNaN(x):
			return "nan"
		case math.IsInf(x, 1):
			return "inf"
		case math.IsInf(x, -1):
			return "-inf"
		}
	}
	return v
}

// jsonDiff is one difference found by diffJSON; issue names the known
// inconsistency it is an instance of, if any.
type jsonDiff struct {
	msg   string
	issue string
}

// diffJSON lists where got differs from want (paths in JSON pointer style).
func diffJSON(path string, want, got any, out *[]jsonDiff) {
	if len(*out) >= 20 {
		return
	}
	add := func(format string, args ...any) { *out = append(*out, jsonDiff{msg: fmt.Sprintf(format, args...)}) }
	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok {
			add("%s: python %s, go %s", path, short(want), short(got))
			return
		}
		keys := map[string]bool{}
		for k := range w {
			keys[k] = true
		}
		for k := range g {
			keys[k] = true
		}
		for _, k := range sortedKeys(keys) {
			wv, wok := w[k]
			gv, gok := g[k]
			switch {
			case !gok:
				add("%s/%s: missing in go (python %s)", path, k, short(wv))
			case !wok:
				add("%s/%s: only in go (%s)", path, k, short(gv))
			default:
				diffJSON(path+"/"+k, wv, gv, out)
			}
		}
	case []any:
		g, ok := got.([]any)
		if !ok {
			add("%s: python %s, go %s", path, short(want), short(got))
			return
		}
		for i := 0; i < min(len(w), len(g)); i++ {
			diffJSON(fmt.Sprintf("%s/%d", path, i), w[i], g[i], out)
		}
		if len(w) != len(g) {
			add("%s: python has %d entries, go %d (python %s, go %s)", path, len(w), len(g), short(want), short(got))
		}
	case json.Number:
		g, ok := got.(json.Number)
		if !ok || !sameNumber(w, g) {
			add("%s: python %s, go %s", path, short(want), short(got))
		}
	default:
		if !reflect.DeepEqual(want, got) {
			add("%s: python %s, go %s", path, short(want), short(got))
		}
	}
}

func sameNumber(a, b json.Number) bool {
	if a == b {
		return true
	}
	x, ok1 := new(big.Rat).SetString(string(a))
	y, ok2 := new(big.Rat).SetString(string(b))
	return ok1 && ok2 && x.Cmp(y) == 0
}

func short(v any) string {
	b, _ := json.Marshal(v)
	return tail(string(b), 300)
}

func tail(s string, n int) string {
	if len(s) > n {
		return s[:n/2] + " … " + s[len(s)-n/2:]
	}
	return s
}

// expectSame fails the test if want (from the oracle) and got (from Go)
// differ as JSON values, unless every difference is one known inconsistency.
func expectSame(t *testing.T, what string, want, got any) bool {
	t.Helper()
	var diffs []jsonDiff
	diffJSON("", jsonValue(t, want), jsonValue(t, got), &diffs)
	if len(diffs) == 0 {
		return true
	}
	msgs := make([]string, len(diffs))
	issue := diffs[0].issue
	for i, d := range diffs {
		msgs[i] = d.msg
		if d.issue != issue {
			issue = ""
		}
	}
	if issue != "" {
		knownIssue(t, issue, "%s: %s", what, strings.Join(msgs, "; "))
		return false
	}
	t.Errorf("%s differs from icechunk-python:\n  %s", what, strings.Join(msgs, "\n  "))
	return false
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func ts(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000000Z") }

func optString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func specMajor(s string) int {
	n, _ := strconv.Atoi(strings.SplitN(s, ".", 2)[0])
	return n
}

// keyPrefix is the store key prefix of a node path: "" for the root.
func keyPrefix(path string) string {
	if p := strings.Trim(path, "/"); p != "" {
		return p + "/"
	}
	return ""
}

// ---------------------------------------------------------------------------
// Refs and history

func versionOf(t *testing.T, label string) icechunk.Version {
	kind, name, _ := strings.Cut(label, ":")
	switch kind {
	case "branch":
		return icechunk.AtBranch(name)
	case "tag":
		return icechunk.AtTag(name)
	}
	id, err := icechunk.ParseSnapshotID(name)
	if err != nil {
		t.Fatalf("bad version label %q", label)
	}
	return icechunk.AtSnapshot(id)
}

func infoEntry(si icechunk.SnapshotInfo) map[string]any {
	var parent any
	if si.ParentID != nil {
		parent = si.ParentID.String()
	}
	md := si.Metadata
	if md == nil {
		md = map[string]any{}
	}
	return map[string]any{"parent_id": parent, "written_at": ts(si.FlushedAt), "message": si.Message, "metadata": md}
}

func ancestry(ctx context.Context, repo *icechunk.Repository, v icechunk.Version) ([]icechunk.SnapshotInfo, error) {
	var out []icechunk.SnapshotInfo
	for si, err := range repo.Ancestry(ctx, v) {
		if err != nil {
			return out, err
		}
		out = append(out, si)
	}
	return out, nil
}

// checkLookup compares a ref lookup with the oracle's snapshot id or error.
func checkLookup(t *testing.T, fx *fixture, what, name, want string, got icechunk.SnapshotID, err error) {
	t.Helper()
	var msg string
	switch {
	case isError(want) && err == nil:
		msg = fmt.Sprintf("%s: icechunk-python fails (%s), go finds %s", what, want, got)
	case isError(want) && want == "error:RefNotFoundError" && !errors.Is(err, icechunk.ErrRefNotFound):
		msg = fmt.Sprintf("%s: err = %v, want ErrRefNotFound (python %s)", what, err, want)
	case !isError(want) && err != nil:
		msg = fmt.Sprintf("%s: icechunk-python finds %s, go fails: %v", what, want, err)
	case !isError(want) && got.String() != want:
		msg = fmt.Sprintf("%s = %s, icechunk-python %s", what, got, want)
	}
	if msg != "" {
		t.Error(msg)
	}
}

// TestPythonRefs: spec version, branch and tag listings and lookups, lookups
// that must fail, and the ancestry of every branch, tag and snapshot.
func TestPythonRefs(t *testing.T) {
	forEachFixture(t, func(t *testing.T, fx *fixture) {
		ctx := context.Background()
		r := fx.o.Repo
		if got := fx.repo.SpecVersion(); got != r.SpecVersion {
			t.Errorf("spec version %d, icechunk-python %d", got, r.SpecVersion)
		}
		branches, err := fx.repo.ListBranches(ctx)
		if err != nil {
			t.Fatal(err)
		}
		expectSame(t, "branch list", sortedKeys(r.Branches), append([]string{}, branches...))
		tags, err := fx.repo.ListTags(ctx)
		if err != nil {
			t.Fatal(err)
		}
		expectSame(t, "tag list", sortedKeys(r.Tags), append([]string{}, tags...))
		for name, want := range r.Branches {
			id, err := fx.repo.LookupBranch(ctx, name)
			checkLookup(t, fx, fmt.Sprintf("listed branch %q", name), name, want, id, err)
		}
		for name, want := range r.Tags {
			id, err := fx.repo.LookupTag(ctx, name)
			checkLookup(t, fx, fmt.Sprintf("listed tag %q", name), name, want, id, err)
		}

		for _, label := range sortedKeys(r.Lookups) {
			raw := r.Lookups[label]
			kind, name, _ := strings.Cut(label, ":")
			var want string
			if json.Unmarshal(raw, &want) != nil {
				want = "" // an ancestry list
			}
			switch kind {
			case "branch":
				id, err := fx.repo.LookupBranch(ctx, name)
				checkLookup(t, fx, fmt.Sprintf("branch %q", name), name, want, id, err)
			case "tag":
				id, err := fx.repo.LookupTag(ctx, name)
				checkLookup(t, fx, fmt.Sprintf("tag %q", name), name, want, id, err)
			case "snapshot":
				id, _ := icechunk.ParseSnapshotID(name)
				_, err := fx.repo.Snapshot(ctx, id)
				if isError(want) && !errors.Is(err, icechunk.ErrSnapshotNotFound) {
					t.Errorf("snapshot %s: err = %v, want ErrSnapshotNotFound (python %s)", name, err, want)
				}
			case "ancestry":
				id, _ := icechunk.ParseSnapshotID(name)
				_, err := ancestry(ctx, fx.repo, icechunk.AtSnapshot(id))
				if isError(want) && err == nil {
					t.Errorf("ancestry of %s: icechunk-python fails (%s), go does not", name, want)
				}
			}
		}

		for _, label := range sortedKeys(r.Ancestry) {
			want := r.Ancestry[label]
			got, err := ancestry(ctx, fx.repo, versionOf(t, label))
			if err != nil {
				t.Errorf("ancestry of %s: %v", label, err)
				continue
			}
			ids := make([]string, len(got))
			for i, si := range got {
				ids[i] = si.ID.String()
				if i < len(want) && ids[i] == want[i] {
					expectSame(t, fmt.Sprintf("ancestry of %s: snapshot %s", label, ids[i]), r.SnapshotInfos[ids[i]], infoEntry(si))
				}
			}
			expectSame(t, "ancestry of "+label, want, ids)
		}
	})
}

// TestPythonRepoInfo: the spec v2 repo info file (refs, snapshot graph,
// status, metadata, feature flags, ops log, stored config); spec v1
// repositories have none, on both sides.
func TestPythonRepoInfo(t *testing.T) {
	forEachFixture(t, func(t *testing.T, fx *fixture) {
		ctx := context.Background()
		want := fx.o.Repo.Info
		info, err := fx.repo.RepoInfo(ctx)
		if want.Error != "" {
			if err == nil {
				t.Errorf("icechunk-python has no repo info (%s), go reads one", want.Error)
			}
			if _, err := fx.repo.Config(ctx); err == nil {
				t.Errorf("icechunk-python has no repo info (%s), go reads a config", want.Error)
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if info.Implementation != want.WrittenBy {
			t.Errorf("written by %q, icechunk-python %q", info.Implementation, want.WrittenBy)
		}
		if info.SpecVersion != specMajor(want.SpecVersion) {
			t.Errorf("repo info spec version %d, icechunk-python %s", info.SpecVersion, want.SpecVersion)
		}
		ids := func(m map[string]icechunk.SnapshotID) map[string]string {
			out := map[string]string{}
			for k, v := range m {
				out[k] = v.String()
			}
			return out
		}
		expectSame(t, "repo info branches", want.Branches, ids(info.Branches))
		expectSame(t, "repo info tags", want.Tags, ids(info.Tags))
		expectSame(t, "deleted tags", want.DeletedTags, append([]string{}, info.DeletedTags...))

		var snaps []map[string]any
		for _, si := range info.Snapshots {
			e := infoEntry(si)
			e["id"] = si.ID.String()
			e["flushed_at"] = e["written_at"]
			delete(e, "written_at")
			snaps = append(snaps, e)
		}
		expectSame(t, "repo info snapshots", want.Snapshots, snaps)

		updates := make([]updateEntry, len(info.Updates))
		for i, u := range info.Updates {
			updates[i] = goUpdate(u)
		}
		expectSame(t, "latest updates", want.LatestUpdates, updates)
		var before any
		if want.RepoBeforeUpdates != nil {
			before = *want.RepoBeforeUpdates
		}
		expectSame(t, "repo_before_updates", before, optString(info.RepoBeforeUpdates))
		// The whole ops log, following repo_before_updates into older repo
		// info files.
		var ops []updateEntry
		if err := json.Unmarshal(want.OpsLog, &ops); err != nil {
			t.Fatal(err)
		}
		all := []updateEntry{}
		for u, err := range fx.repo.OpsLog(ctx) {
			if err != nil {
				t.Fatalf("ops log: %v", err)
			}
			all = append(all, goUpdate(u))
		}
		expectSame(t, "ops log", ops, all)

		expectSame(t, "repo metadata", want.Metadata, info.Metadata)
		expectSame(t, "repo status", want.Status, map[string]any{
			"availability": info.Status.Availability,
			"set_at":       ts(info.Status.SetAt),
			"reason":       optString(info.Status.Reason),
		})
		flags := func(f []uint16) []int {
			out := []int{}
			for _, x := range f {
				out = append(out, int(x))
			}
			sort.Ints(out)
			return out
		}
		expectSame(t, "feature flags", want.FeatureFlags, map[string]any{
			"enabled": flags(info.EnabledFeatureFlags), "disabled": flags(info.DisabledFeatureFlags),
		})
		cfg, err := fx.repo.Config(ctx)
		if err != nil {
			t.Fatal(err)
		}
		expectSame(t, "stored config", want.Config, configEntry(cfg))
	})
}

// namedUpdates are the update kinds that name a branch or tag (possibly
// the empty name, which upstream allows).
var namedUpdates = map[string]bool{
	"TagCreated": true, "TagDeleted": true, "BranchCreated": true, "BranchDeleted": true,
	"BranchReset": true, "NewCommit": true, "CommitAmended": true,
}

func updateName(u icechunk.Update) any {
	if namedUpdates[u.Kind] {
		return u.Name
	}
	return nil
}

func goUpdate(u icechunk.Update) updateEntry {
	opt := func(s string) *string {
		if s == "" {
			return nil
		}
		return &s
	}
	var name *string
	if namedUpdates[u.Kind] {
		name = &u.Name
	}
	optid := func(id icechunk.SnapshotID) *string {
		if id.IsZero() {
			return nil
		}
		return opt(id.String())
	}
	return updateEntry{
		Kind:               u.Kind,
		Name:               name,
		SnapshotID:         optid(u.SnapshotID),
		PreviousSnapshotID: optid(u.PreviousSnapshotID),
		UpdatedAt:          ts(u.UpdatedAt),
		BackupPath:         opt(u.BackupPath),
	}
}

// configEntry picks the fields oracle.py records from the stored config
// (Repository.Config is the decoded serde document).
func configEntry(cfg any) any {
	if cfg == nil {
		return nil
	}
	at := func(path string) any {
		var v any = cfg
		for _, k := range strings.Split(path, ".") {
			m, ok := v.(map[string]any)
			if !ok {
				return nil
			}
			v = m[k]
		}
		return v
	}
	containers := map[string]any{}
	if m, ok := at("virtual_chunk_containers").(map[string]any); ok {
		for prefix, c := range m {
			c, _ := c.(map[string]any)
			kind := ""
			if st, ok := c["store"].(map[string]any); ok {
				for k := range st {
					kind = k
				}
			}
			containers[prefix] = map[string]any{"url_prefix": c["url_prefix"], "name": c["name"], "store": kind}
		}
	}
	return map[string]any{
		"inline_chunk_threshold_bytes":                               at("inline_chunk_threshold_bytes"),
		"get_partial_values_concurrency":                             at("get_partial_values_concurrency"),
		"num_updates_per_repo_info_file":                             at("num_updates_per_repo_info_file"),
		"compression.level":                                          at("compression.level"),
		"manifest.virtual_chunk_location_compression.min_num_chunks": at("manifest.virtual_chunk_location_compression.min_num_chunks"),
		"manifest.splitting":                                         at("manifest.splitting") != nil,
		"virtual_chunk_containers":                                   containers,
	}
}

// ---------------------------------------------------------------------------
// Snapshots, nodes and manifests

func goNodeEntry(n *icechunk.Node) nodeEntry {
	e := nodeEntry{Path: n.Path, ID: n.ID.String(), Type: n.Type.String(), ZarrJSON: digestBytes(n.ZarrMetadata)}
	if a := n.Array; a != nil {
		for _, d := range a.Shape {
			e.Shape = append(e.Shape, [2]uint64{d.ArrayLength, uint64(d.NumChunks)})
		}
		if slices.ContainsFunc(a.DimensionNames, func(s string) bool { return s != "" }) {
			e.DimensionNames = a.DimensionNames
		}
		for _, m := range a.Manifests {
			r := manifestRefEntry{ID: m.ID.String(), Extents: [][2]uint32{}}
			for _, x := range m.Extents {
				r.Extents = append(r.Extents, [2]uint32{x.From, x.To})
			}
			e.ManifestRefs = append(e.ManifestRefs, r)
		}
	}
	return e
}

// TestPythonSnapshots: every snapshot file (header, commit info, manifest
// files, nodes in file order), node lookup by path, and group attributes.
func TestPythonSnapshots(t *testing.T) {
	forEachFixture(t, func(t *testing.T, fx *fixture) {
		ctx := context.Background()
		for _, sid := range sortedKeys(fx.o.Snapshots) {
			want := fx.o.Snapshots[sid]
			id, _ := icechunk.ParseSnapshotID(sid)
			snap, err := fx.repo.Snapshot(ctx, id)
			if err != nil {
				t.Errorf("snapshot %s: %v", sid, err)
				continue
			}
			what := "snapshot " + sid
			if snap.ID().String() != want.ID || snap.Implementation() != want.Header.WrittenBy || snap.SpecVersion() != specMajor(want.Header.SpecVersion) {
				t.Errorf("%s: id %s, written by %q, spec %d; icechunk-python %s, %q, %s", what,
					snap.ID(), snap.Implementation(), snap.SpecVersion(), want.ID, want.Header.WrittenBy, want.Header.SpecVersion)
			}
			if ts(snap.FlushedAt()) != want.FlushedAt || snap.Message() != want.Message {
				t.Errorf("%s: flushed %s %q, icechunk-python %s %q", what, ts(snap.FlushedAt()), snap.Message(), want.FlushedAt, want.Message)
			}
			md, err := snap.Metadata()
			if err != nil {
				t.Errorf("%s metadata: %v", what, err)
			}
			expectSame(t, what+" metadata", want.Metadata, md)
			files, err := snap.ManifestFiles()
			if err != nil {
				t.Errorf("%s manifest files: %v", what, err)
			}
			mf := []map[string]any{}
			for _, f := range files {
				mf = append(mf, map[string]any{"id": f.ID.String(), "size_bytes": f.SizeBytes, "num_chunk_refs": f.NumChunkRefs})
			}
			expectSame(t, what+" manifest files", want.Manifests, mf)

			nodes, err := snap.Nodes()
			if err != nil {
				t.Errorf("%s nodes: %v", what, err)
				continue
			}
			wantNodes := slices.Clone(want.Nodes)
			for i := range wantNodes {
				wantNodes[i].Content = ""
			}
			gotNodes := make([]nodeEntry, len(nodes))
			for i, n := range nodes {
				gotNodes[i] = goNodeEntry(n)
			}
			expectSame(t, what+" nodes", wantNodes, gotNodes)

			sess := fx.session(t, sid)
			for _, w := range wantNodes {
				for _, p := range []string{w.Path, strings.TrimPrefix(w.Path, "/")} {
					n, err := sess.Node(p)
					if err != nil {
						t.Errorf("%s: Node(%q): %v", what, p, err)
						continue
					}
					expectSame(t, fmt.Sprintf("%s: Node(%q)", what, p), w, goNodeEntry(n))
				}
			}
			for _, path := range sortedKeys(want.GroupAttributes) {
				raw := want.GroupAttributes[path]
				attrs, err := sess.Attributes(path)
				var e string
				if json.Unmarshal(raw, &e) == nil && isError(e) {
					if err == nil {
						t.Errorf("%s: attributes of %s: icechunk-python fails (%s), go does not", what, path, e)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s: attributes of %s: %v", what, path, err)
					continue
				}
				expectSame(t, fmt.Sprintf("%s: attributes of group %s", what, path), raw, attrs)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// The Zarr store view

// rangeRead performs an oracle byte range request: "r:a:b" = [a, b),
// "o:a" = from a to the end, "s:n" = the last n bytes.
func rangeRead(ctx context.Context, st *icechunk.Store, key, spec string) ([]byte, error) {
	parts := strings.Split(spec, ":")
	num := func(s string) int64 { n, _ := strconv.ParseInt(s, 10, 64); return n }
	switch parts[0] {
	case "r":
		return st.GetRange(ctx, key, num(parts[1]), num(parts[2])-num(parts[1]))
	case "o":
		return st.GetRange(ctx, key, num(parts[1]), -1)
	case "s":
		size, err := st.Size(ctx, key)
		if err != nil {
			return nil, err
		}
		n := min(num(parts[1]), size)
		return st.GetRange(ctx, key, size-n, n)
	}
	return nil, fmt.Errorf("bad range spec %q", spec)
}

func emptyRange(spec string) bool {
	p := strings.Split(spec, ":")
	return p[0] == "r" && p[1] == p[2]
}

func checkKey(t *testing.T, what string, st *icechunk.Store, key string, want keyInfo) {
	t.Helper()
	ctx := context.Background()
	data, err := st.Get(ctx, key)
	if got := getOutcome(data, err); got != want.Get && !acceptedDivergence(key, want.Get, got) {
		t.Errorf("%s: Get(%q) = %s (err %v), icechunk-python %s", what, key, got, err, want.Get)
	}
	size, err := st.Size(ctx, key)
	var wantSize int64
	if json.Unmarshal(want.Size, &wantSize) == nil {
		switch {
		case err == nil && size != wantSize:
			t.Errorf("%s: Size(%q) = %d, icechunk-python %d", what, key, size, wantSize)
		case err != nil && !(wantSize == 0 && errors.Is(err, icechunk.ErrKeyNotFound) && want.Get == "absent"):
			t.Errorf("%s: Size(%q): %v, icechunk-python %d", what, key, err, wantSize)
		}
	} else if err == nil {
		t.Errorf("%s: Size(%q) = %d, icechunk-python %s", what, key, size, want.Size)
	}
	for _, spec := range sortedKeys(want.Ranges) {
		w := want.Ranges[spec]
		data, err := rangeRead(ctx, st, key, spec)
		got := getOutcome(data, err)
		if got == w || acceptedDivergence(key, w, got) || (isError(w) && emptyRange(spec) && err == nil && len(data) == 0) {
			continue
		}
		t.Errorf("%s: %q range %s = %s (err %v), icechunk-python %s", what, key, spec, got, err, w)
	}
}

// listingMismatch compares a ListDir / ListPrefix result with the oracle's,
// returning "" when they agree.
func listingMismatch(t *testing.T, what, prefix string, want json.RawMessage, got []string, err error) string {
	t.Helper()
	var e string
	if json.Unmarshal(want, &e) == nil {
		if !isError(e) {
			t.Fatalf("%s: unexpected oracle value %s", what, want)
		}
		if err != nil {
			return "" // both refuse
		}
		return fmt.Sprintf("%s = %d entries, icechunk-python refuses (%s)", what, len(got), e)
	}
	if err != nil {
		return fmt.Sprintf("%s: %v, icechunk-python %s", what, err, tail(string(want), 300))
	}
	var w listing
	if err := json.Unmarshal(want, &w); err != nil {
		t.Fatal(err)
	}
	g := digestListing(got)
	if g.Count == w.Count && g.Digest == w.Digest || (w.Count == 0 && nonCanonical(prefix)) { // divergence 5
		return ""
	}
	if g.Keys == nil && len(got) > 32 {
		g.Keys = got[:32]
	}
	return fmt.Sprintf("%s = %d entries %q, icechunk-python %d entries %q", what, g.Count, g.Keys, w.Count, w.Keys)
}

// TestPythonStore: the Zarr key/value view of every snapshot: the full key
// list, every key's bytes and size, byte ranges, missing and malformed keys,
// list_dir and list_prefix.
func TestPythonStore(t *testing.T) {
	forEachFixture(t, func(t *testing.T, fx *fixture) {
		ctx := context.Background()
		for _, sid := range sortedKeys(fx.o.Snapshots) {
			want := fx.o.Snapshots[sid]
			what := "snapshot " + sid
			st := fx.session(t, sid).Store()

			keys, err := st.ListPrefix(ctx, "")
			if err != nil {
				t.Errorf("%s: ListPrefix: %v", what, err)
			}
			expectSame(t, what+" key list", want.Store.List, append([]string{}, keys...))

			for _, key := range sortedKeys(want.Store.MetadataKeys) {
				checkKey(t, what, st, key, want.Store.MetadataKeys[key])
			}
			for _, n := range want.Nodes {
				if n.Content == "" {
					continue
				}
				arr := fx.o.Arrays[n.Content]
				for _, rel := range sortedKeys(arr.ChunkKeys) {
					checkKey(t, what, st, keyPrefix(n.Path)+rel, arr.ChunkKeys[rel])
				}
			}

			for _, key := range sortedKeys(want.Store.Probes) {
				p := want.Store.Probes[key]
				data, err := st.Get(ctx, key)
				if got := getOutcome(data, err); got != p.Get && !acceptedDivergence(key, p.Get, got) {
					t.Errorf("%s: Get(%q) = %s (err %v), icechunk-python %s", what, key, got, err, p.Get)
				}
				ok, err := st.Exists(ctx, key)
				got := strconv.FormatBool(ok)
				if err != nil {
					got = "error"
				}
				if want := strings.Trim(string(p.Exists), `"`); got != want && !acceptedDivergence(key, want, got) {
					t.Errorf("%s: Exists(%q) = %s (err %v), icechunk-python %s", what, key, got, err, want)
				}
				size, err := st.Size(ctx, key)
				var wantSize int64
				switch {
				case json.Unmarshal(p.Size, &wantSize) == nil:
					if !(err == nil && size == wantSize) && !(wantSize == 0 && p.Get == "absent" && errors.Is(err, icechunk.ErrKeyNotFound)) {
						t.Errorf("%s: Size(%q) = %d (err %v), icechunk-python %d", what, key, size, err, wantSize)
					}
				case err == nil && !(nonCanonical(key) && string(p.Size) == `"error:NodeNotFoundError"`):
					t.Errorf("%s: Size(%q) = %d, icechunk-python %s", what, key, size, p.Size)
				}
			}

			for _, d := range sortedKeys(want.Store.ListDir) {
				got, err := st.ListDir(ctx, d)
				if msg := listingMismatch(t, fmt.Sprintf("%s: ListDir(%q)", what, d), d, want.Store.ListDir[d], got, err); msg != "" {
					t.Error(msg)
				}
			}
			for _, p := range sortedKeys(want.Store.ListPrefix) {
				got, err := st.ListPrefix(ctx, p)
				if msg := listingMismatch(t, fmt.Sprintf("%s: ListPrefix(%q)", what, p), p, want.Store.ListPrefix[p], got, err); msg != "" {
					t.Error(msg)
				}
			}
		}
	})
}

// ---------------------------------------------------------------------------
// Chunk references

func refEntry(coords []uint32, r *icechunk.ChunkRef) []any {
	c := append([]uint32{}, coords...)
	switch r.Kind {
	case icechunk.InlineChunk:
		return []any{c, "inline", nil, 0, len(r.Inline), digestBytes(r.Inline)}
	case icechunk.NativeChunk:
		return []any{c, "native", r.ID.String(), r.Offset, r.Length, nil}
	case icechunk.VirtualChunk:
		return []any{c, "virtual", r.Location, r.Offset, r.Length, nil}
	}
	return []any{c, r.Kind.String()}
}

func parseCoords(s string) []uint32 {
	out := []uint32{}
	if s == "" {
		return out
	}
	for _, p := range strings.Split(s, ",") {
		n, _ := strconv.ParseUint(p, 10, 32)
		out = append(out, uint32(n))
	}
	return out
}

// TestPythonChunkRefs: every array version's chunk refs, both iterated
// (ChunkRefs) and looked up one by one (ChunkRef); chunk_type of unwritten,
// out-of-grid and wrong-arity coordinates; and each snapshot's virtual
// chunk locations.
func TestPythonChunkRefs(t *testing.T) {
	forEachFixture(t, func(t *testing.T, fx *fixture) {
		ctx := context.Background()
		for _, content := range sortedKeys(fx.o.Arrays) {
			want := fx.o.Arrays[content]
			loc := fx.arrayAt[content]
			what := fmt.Sprintf("array %s at %s", loc.path, loc.snapshot)
			sess := fx.session(t, loc.snapshot)
			var got [][]any
			for e, err := range sess.ChunkRefs(ctx, loc.path) {
				if err != nil {
					t.Errorf("%s: ChunkRefs: %v", what, err)
					break
				}
				got = append(got, refEntry(e.Coords, e.Ref))
			}
			sort.Slice(got, func(i, j int) bool {
				return slices.Compare(got[i][0].([]uint32), got[j][0].([]uint32)) < 0
			})
			if got == nil {
				got = [][]any{}
			}
			expectSame(t, what+" chunk refs", want.Refs, got)

			for _, raw := range want.Refs {
				var entry []json.RawMessage
				var coords []uint32
				json.Unmarshal(raw, &entry)
				json.Unmarshal(entry[0], &coords)
				ref, err := sess.ChunkRef(ctx, loc.path, coords)
				if err != nil || ref == nil {
					t.Errorf("%s: ChunkRef(%v) = %v, %v; icechunk-python %s", what, coords, ref, err, raw)
					continue
				}
				expectSame(t, fmt.Sprintf("%s: ChunkRef(%v)", what, coords), raw, refEntry(coords, ref))
			}
			for _, key := range sortedKeys(want.ChunkTypeProbes) {
				w := want.ChunkTypeProbes[key]
				ref, err := sess.ChunkRef(ctx, loc.path, parseCoords(key))
				got := "uninitialized"
				switch {
				case err != nil:
					got = "error"
				case ref != nil:
					got = ref.Kind.String()
				}
				if got != w && !(isError(w) && got == "error") {
					t.Errorf("%s: chunk type at [%s] = %s (err %v), icechunk-python %s", what, key, got, err, w)
				}
			}
		}

		for _, sid := range sortedKeys(fx.o.Snapshots) {
			want := fx.o.Snapshots[sid]
			sess := fx.session(t, sid)
			locs := map[string]bool{}
			for _, n := range want.Nodes {
				if n.Type != "array" {
					continue
				}
				for e, err := range sess.ChunkRefs(ctx, n.Path) {
					if err != nil {
						t.Errorf("snapshot %s: ChunkRefs(%s): %v", sid, n.Path, err)
						break
					}
					if e.Ref.Kind == icechunk.VirtualChunk {
						locs[e.Ref.Location] = true
					}
				}
			}
			expectSame(t, "snapshot "+sid+" virtual chunk locations", want.VirtualLocations, sortedKeys(locs))
		}
	})
}

// ---------------------------------------------------------------------------
// Arrays

// TestPythonArrays: every array version's Zarr metadata as decoded (shape,
// data type, chunk grid, dimension names, attributes), region reads against
// zarr-python's, and each chunk (or shard) decoded alone.
func TestPythonArrays(t *testing.T) {
	forEachFixture(t, func(t *testing.T, fx *fixture) {
		ctx := context.Background()
		for _, content := range sortedKeys(fx.o.Arrays) {
			want := fx.o.Arrays[content]
			loc := fx.arrayAt[content]
			what := fmt.Sprintf("array %s at %s", loc.path, loc.snapshot)
			arr, err := fx.session(t, loc.snapshot).OpenArray(ctx, loc.path)
			if want.Zarr.Error != "" {
				t.Logf("%s: zarr-python cannot open it (%s); go: %v", what, want.Zarr.Error, err)
				continue
			}
			if err != nil {
				t.Errorf("%s: %v", what, err)
				continue
			}
			dt := arr.DataType()
			wantDims := make([]string, len(want.Zarr.Shape)) // Go reports "" for unnamed dimensions
			for i, n := range want.Zarr.DimensionNames {
				if n != nil && i < len(wantDims) {
					wantDims[i] = *n
				}
			}
			var dtConfig any // zarr-python writes no configuration for types without one
			if len(dt.Configuration) > 0 {
				dtConfig = dt.Configuration
			}
			expectSame(t, what+" metadata", map[string]any{
				"shape": want.Zarr.Shape, "data_type": want.Zarr.DataType, "chunk_grid_shape": want.Zarr.ChunkGridShape,
				"dimension_names": wantDims, "attributes": want.Zarr.Attributes,
			}, map[string]any{
				"shape": append([]uint64{}, arr.Shape()...), "data_type": map[string]any{"name": dt.Name, "configuration": dtConfig},
				"chunk_grid_shape": arr.ChunkGridShape(), "dimension_names": arr.DimensionNames(), "attributes": arr.Attributes(),
			})
			for _, r := range want.Zarr.Reads {
				nd, err := arr.Read(ctx, r.Start, r.Count)
				checkRead(t, fmt.Sprintf("%s: Read(%v, %v)", what, r.Start, r.Count), r, nd, err)
			}
			for _, r := range want.Zarr.ChunkReads {
				nd, err := arr.ReadChunk(ctx, r.Coords)
				if err == nil {
					// Go decodes whole chunks, edge chunks included; zarr-python
					// read the part inside the array.
					nd = slice(nd, make([]uint64, len(r.Count)), r.Count)
				}
				checkRead(t, fmt.Sprintf("%s: ReadChunk(%v)", what, r.Coords), r, nd, err)
			}
		}
	})
}

func checkRead(t *testing.T, what string, want oracleRead, nd *zarr.NDArray, err error) {
	t.Helper()
	switch {
	case isError(want.Result) && err == nil:
		t.Errorf("%s: zarr-python fails (%s), go reads %s", what, want.Result, digestND(nd))
	case !isError(want.Result) && err != nil:
		t.Errorf("%s: %v; zarr-python reads %s", what, err, tail(string(want.Values), 200))
	case err == nil && digestND(nd) != want.Result:
		vals, _ := nd.Values()
		t.Errorf("%s = %s, zarr-python %s", what, tail(fmt.Sprint(vals), 300), tail(string(want.Values), 300))
	}
}
