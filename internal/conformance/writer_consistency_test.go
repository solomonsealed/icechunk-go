package conformance

// Writer consistency: the Go writer against icechunk-python.
//
// testdata/writer/scenarios.json lists scenarios: sequences of steps such as
// opening sessions, creating groups and arrays, writing regions, deleting
// chunks and nodes, rewriting zarr.json, setting virtual refs, committing
// (with and without rebase) and changing branches and tags. Both writers run
// the same steps; each run is reduced to a canonical description that leaves
// out what legitimately differs between writers (object ids, timestamps,
// compressor output) and must match, as must every step's outcome.
//
// TestWriterScenarios runs the Go writer and compares with the descriptions
// recorded from icechunk-python (testdata/writer/expected, written by
// `python testdata/writer/scenario.py record`), read back with the Go
// reader. With ICECHUNK_PYTHON set, TestWriterScenariosLive also has
// icechunk-python read every Go-written repository (including the
// transaction logs, as diffs), and interleaves the two writers on one
// repository, session by session, so each commits on top of, and rebases
// over, the other's files.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	icechunk "github.com/solomonsealed/icechunk-go"
	"github.com/solomonsealed/icechunk-go/serve"
	"github.com/solomonsealed/icechunk-go/storage"
	"github.com/solomonsealed/icechunk-go/zarr"
)

const writerTestdata = testdata + "/writer"

// ---------------------------------------------------------------------------
// Scenarios

type scenario struct {
	Description string `json:"description"`
	// HTTP marks scenarios whose every step after an open is committed by
	// itself, so they can also run through the HTTP write endpoints.
	HTTP bool `json:"http"`
	// Config, when set, is the repository config icechunk-python creates
	// the repository with (the Go writer cannot set config); the empty
	// repository is kept in testdata/writer/initial/<name>.
	Config map[string]any    `json:"config"`
	Steps  []json.RawMessage `json:"steps"`
}

type step struct {
	Op           string          `json:"op"`
	Session      string          `json:"session"`
	Branch       string          `json:"branch"`
	Path         string          `json:"path"`
	Name         string          `json:"name"`
	At           string          `json:"at"`
	Message      string          `json:"message"`
	Metadata     map[string]any  `json:"metadata"`
	Rebase       bool            `json:"rebase"`
	AllowEmpty   bool            `json:"allow_empty"`
	Attributes   map[string]any  `json:"attributes"`
	Spec         *arraySpecJSON  `json:"spec"`
	ZarrJSON     json.RawMessage `json:"zarr_json"`
	Start        []uint64        `json:"start"`
	Shape        []uint64        `json:"shape"`
	Seed         int             `json:"seed"`
	Fill         bool            `json:"fill"`
	NaN          bool            `json:"nan"`
	Coords       []uint32        `json:"coords"`
	Size         int             `json:"size"`
	Location     string          `json:"location"`
	Offset       uint64          `json:"offset"`
	Length       uint64          `json:"length"`
	ETag         string          `json:"etag"`
	LastModified string          `json:"last_modified"`
}

type arraySpecJSON struct {
	Shape          []uint64         `json:"shape"`
	Chunks         []uint64         `json:"chunks"`
	Shards         []uint64         `json:"shards"`
	DataType       string           `json:"data_type"`
	FillValue      any              `json:"fill_value"`
	Codecs         []zarr.CodecSpec `json:"codecs"`
	DimensionNames []*string        `json:"dimension_names"`
	Attributes     map[string]any   `json:"attributes"`
}

// decodeJSON decodes with numbers as json.Number, so commit metadata and
// attributes keep their integer or float form, as they do in Python.
func decodeJSON(raw []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	return dec.Decode(v)
}

func loadScenarios(t *testing.T) (map[string]scenario, []string) {
	t.Helper()
	raw, err := os.ReadFile(writerTestdata + "/scenarios.json")
	if err != nil {
		t.Fatal(err)
	}
	var sc map[string]scenario
	if err := json.Unmarshal(raw, &sc); err != nil {
		t.Fatal(err)
	}
	return sc, sortedKeys(sc)
}

func parseSteps(t *testing.T, sc scenario) []step {
	t.Helper()
	out := make([]step, len(sc.Steps))
	for i, raw := range sc.Steps {
		if err := decodeJSON(raw, &out[i]); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
	}
	return out
}

// expectedRun is what icechunk-python produced for a scenario.
type expectedRun struct {
	Outcomes  []string       `json:"outcomes"`
	Canonical map[string]any `json:"canonical"`
}

func loadRecordedRun(t *testing.T, name string) expectedRun {
	t.Helper()
	raw, err := os.ReadFile(fmt.Sprintf("%s/expected/%s.json", writerTestdata, name))
	if err != nil {
		t.Skipf("no recorded run for %s (run testdata/writer/scenario.py record): %v", name, err)
	}
	var e expectedRun
	if err := decodeJSON(raw, &e); err != nil {
		t.Fatal(err)
	}
	return e
}

// ---------------------------------------------------------------------------
// Running steps with the Go writer

// missingSnapshot stands for snapshot labels no commit has (as in scenario.py).
var missingSnapshot, _ = icechunk.ParseSnapshotID("ZZZZZZZZZZZZZZZZZZZ0")

type goRunner struct {
	repo     *icechunk.Repository
	sessions map[string]*icechunk.Session
}

func newGoRunner(t *testing.T, dir string) *goRunner {
	t.Helper()
	repo, err := icechunk.Open(context.Background(), storage.NewLocal(dir), nil)
	if err != nil {
		t.Fatal(err)
	}
	return &goRunner{repo: repo, sessions: map[string]*icechunk.Session{}}
}

// category classifies errors like scenario.py classifies upstream's.
func category(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, icechunk.ErrConflict):
		return "error:conflict"
	case errors.Is(err, icechunk.ErrNoChanges), errors.Is(err, icechunk.ErrReadOnlySession),
		errors.Is(err, icechunk.ErrCannotDeleteMain):
		return "error:session"
	case errors.Is(err, icechunk.ErrAlreadyExists):
		return "error:exists"
	case errors.Is(err, icechunk.ErrRefNotFound), errors.Is(err, icechunk.ErrSnapshotNotFound),
		errors.Is(err, icechunk.ErrNodeNotFound), errors.Is(err, icechunk.ErrNotAnArray):
		return "error:not_found"
	case errors.Is(err, icechunk.ErrInvalidChunkCoords), errors.Is(err, icechunk.ErrNoVirtualContainer):
		return "error:invalid"
	}
	return "error:other"
}

// snapshot finds a commit by message (unique within a scenario).
func (r *goRunner) snapshot(ctx context.Context, label string) (icechunk.SnapshotID, error) {
	r.repo.Refresh()
	info, err := r.repo.RepoInfo(ctx)
	if err != nil {
		return icechunk.SnapshotID{}, err
	}
	for _, si := range info.Snapshots {
		if si.Message == label {
			return si.ID, nil
		}
	}
	return missingSnapshot, nil
}

func (r *goRunner) apply(ctx context.Context, st step) (outcome string, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic: %v", p)
			outcome = "error:panic"
		}
	}()
	err = r.do(ctx, st)
	return category(err), err
}

func (r *goRunner) do(ctx context.Context, st step) error {
	at := func() (icechunk.SnapshotID, error) { return r.snapshot(ctx, st.At) }
	switch st.Op {
	case "open":
		s, err := r.repo.WritableSession(ctx, st.Branch)
		if err == nil {
			r.sessions[st.Session] = s
		}
		return err
	case "create_branch", "reset_branch", "create_tag":
		id, err := at()
		if err != nil {
			return err
		}
		switch st.Op {
		case "create_branch":
			return r.repo.CreateBranch(ctx, st.Name, id)
		case "reset_branch":
			return r.repo.ResetBranch(ctx, st.Name, id)
		}
		return r.repo.CreateTag(ctx, st.Name, id)
	case "delete_branch":
		return r.repo.DeleteBranch(ctx, st.Name)
	case "delete_tag":
		return r.repo.DeleteTag(ctx, st.Name)
	}

	s := r.sessions[st.Session]
	if s == nil {
		return fmt.Errorf("scenario error: no session %q", st.Session)
	}
	path := st.Path
	if path == "" {
		path = "/"
	}
	switch st.Op {
	case "commit":
		_, err := s.Commit(ctx, st.Message, &icechunk.CommitOptions{Metadata: st.Metadata, Rebase: st.Rebase, AllowEmpty: st.AllowEmpty})
		return err
	case "create_group":
		return s.CreateGroup(ctx, path, st.Attributes)
	case "create_array":
		spec, err := goArraySpec(st.Spec)
		if err != nil {
			return err
		}
		_, err = s.CreateArray(ctx, path, spec)
		return err
	case "set_metadata":
		return s.SetMetadata(ctx, path, st.ZarrJSON)
	case "update_attributes", "set_shape":
		n, err := s.Node(path)
		if err != nil {
			return err
		}
		var doc map[string]any
		if err := decodeJSON(n.ZarrMetadata, &doc); err != nil {
			return err
		}
		if st.Op == "update_attributes" {
			doc["attributes"] = st.Attributes
		} else {
			doc["shape"] = st.Shape
		}
		raw, err := json.Marshal(doc)
		if err != nil {
			return err
		}
		return s.SetMetadata(ctx, path, raw)
	case "write":
		arr, err := s.OpenArray(ctx, path)
		if err != nil {
			return err
		}
		var nd *zarr.NDArray
		switch {
		case st.Fill:
			n, err := s.Node(path)
			if err != nil {
				return err
			}
			nd, err = fillND(ctx, n.ZarrMetadata, st.Start, st.Shape)
			if err != nil {
				return err
			}
		case st.NaN:
			nd, err = nanND(arr.DataType(), st.Shape)
		default:
			nd, err = genND(arr.DataType(), st.Shape, st.Seed)
		}
		if err != nil {
			return err
		}
		return arr.Write(ctx, st.Start, nd)
	case "delete_chunk":
		return s.DeleteChunk(ctx, path, st.Coords)
	case "set_chunk":
		return s.SetChunk(ctx, path, st.Coords, rawBytes(st.Seed, st.Size))
	case "set_virtual_ref":
		v := icechunk.VirtualRef{Location: st.Location, Offset: st.Offset, Length: st.Length, ETag: st.ETag}
		if st.LastModified != "" {
			t, err := time.Parse(time.RFC3339, st.LastModified)
			if err != nil {
				return err
			}
			v.LastModified = t
		}
		return s.SetVirtualRef(ctx, path, st.Coords, v)
	case "delete_node":
		return s.DeleteNode(ctx, path)
	}
	return fmt.Errorf("scenario error: unknown op %q", st.Op)
}

// goArraySpec converts a scenario array spec, whose fill value is JSON as
// in zarr.json, to a zarr.ArraySpec.
func goArraySpec(j *arraySpecJSON) (zarr.ArraySpec, error) {
	spec := zarr.ArraySpec{
		Shape: j.Shape, ChunkShape: j.Chunks, ShardShape: j.Shards, DataType: j.DataType,
		Codecs: j.Codecs, Attributes: j.Attributes,
	}
	if j.DimensionNames != nil {
		spec.DimensionNames = make([]string, len(j.DimensionNames))
		for i, n := range j.DimensionNames {
			if n != nil {
				spec.DimensionNames[i] = *n
			}
		}
	}
	dt, err := zarr.ParseDataType(j.DataType)
	if err != nil {
		return spec, err
	}
	num := func(v any) json.Number { n, _ := v.(json.Number); return n }
	switch v := j.FillValue.(type) {
	case nil, bool, string:
		spec.FillValue = v
		if s, ok := v.(string); ok && dt.Kind == zarr.KindFloat {
			spec.FillValue = map[string]float64{"NaN": math.NaN(), "Infinity": math.Inf(1), "-Infinity": math.Inf(-1)}[s]
		}
	case []any:
		re, _ := num(v[0]).Float64()
		im, _ := num(v[1]).Float64()
		spec.FillValue = complex(re, im)
	case json.Number:
		switch dt.Kind {
		case zarr.KindUint:
			var u uint64
			_, err = fmt.Sscan(string(v), &u)
			spec.FillValue = u
		case zarr.KindInt:
			spec.FillValue, err = v.Int64()
		default:
			spec.FillValue, err = v.Float64()
		}
	}
	return spec, err
}

// ---------------------------------------------------------------------------
// Running steps through the HTTP write endpoints

// httpRunner runs a scenario through serve.Service: each create_array or
// write step becomes the request its session's commit sends (POST
// /arrays/<path> or PUT /array/<path>), committed with the step's message.
type httpRunner struct {
	repo    *icechunk.Repository
	svc     *serve.Service
	branch  map[string]string
	pending map[string]*httpRequest
}

type httpRequest struct {
	method, path string
	body         any
}

func newHTTPRunner(t *testing.T, dir string) *httpRunner {
	t.Helper()
	repo, err := icechunk.Open(context.Background(), storage.NewLocal(dir), nil)
	if err != nil {
		t.Fatal(err)
	}
	return &httpRunner{repo: repo, svc: &serve.Service{Repo: repo, WriteToken: "scenario"},
		branch: map[string]string{}, pending: map[string]*httpRequest{}}
}

func (h *httpRunner) apply(ctx context.Context, st step) (string, error) {
	path := strings.Trim(st.Path, "/")
	switch st.Op {
	case "open":
		h.branch[st.Session] = st.Branch
		delete(h.pending, st.Session)
		return "ok", nil
	case "create_array":
		j := st.Spec
		h.pending[st.Session] = &httpRequest{"POST", "/arrays/" + escapePath(path), map[string]any{
			"shape": j.Shape, "chunk_shape": j.Chunks, "shard_shape": j.Shards, "data_type": j.DataType,
			"fill_value": j.FillValue, "codecs": j.Codecs, "dimension_names": j.DimensionNames, "attributes": j.Attributes,
		}}
		return "ok", nil
	case "write":
		sess, err := h.repo.ReadonlySession(ctx, icechunk.AtBranch(h.branch[st.Session]))
		if err != nil {
			return category(err), err
		}
		arr, err := sess.OpenArray(ctx, path)
		if err != nil {
			return category(err), err
		}
		var nd *zarr.NDArray
		switch {
		case st.Fill:
			n, _ := sess.Node(path)
			nd, err = fillND(ctx, n.ZarrMetadata, st.Start, st.Shape)
		case st.NaN:
			nd, err = nanND(arr.DataType(), st.Shape)
		default:
			nd, err = genND(arr.DataType(), st.Shape, st.Seed)
		}
		if err != nil {
			return "error:other", err
		}
		data, err := valuesJSON(nd)
		if err != nil {
			return "error:other", err
		}
		h.pending[st.Session] = &httpRequest{"PUT", "/array/" + escapePath(path),
			map[string]any{"start": st.Start, "shape": st.Shape, "data": data}}
		return "ok", nil
	case "commit":
		req := h.pending[st.Session]
		if req == nil {
			return "error:session", errors.New("nothing to send")
		}
		delete(h.pending, st.Session)
		body, err := json.Marshal(req.body)
		if err != nil {
			return "error:other", err
		}
		u, _ := url.Parse(req.path + "?branch=" + url.QueryEscape(h.branch[st.Session]) + "&message=" + url.QueryEscape(st.Message))
		resp := h.svc.Handle(ctx, &serve.Request{Method: req.method, URL: u, Body: body,
			Header: map[string]string{"authorization": "Bearer scenario"}})
		if resp.Status != 201 {
			return fmt.Sprintf("error:http-%d", resp.Status), fmt.Errorf("%s %s: %d %s", req.method, req.path, resp.Status, resp.Body)
		}
		return "ok", nil
	}
	return "error:unsupported", fmt.Errorf("op %s has no HTTP endpoint", st.Op)
}

// valuesJSON encodes elements as the PUT /array body takes them: numbers
// ("NaN" and "Infinity" as strings), booleans, strings, [re, im] pairs.
func valuesJSON(nd *zarr.NDArray) ([]any, error) {
	vals, err := nd.Values()
	if err != nil {
		return nil, err
	}
	num := func(f float64) any {
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return map[bool]string{true: "NaN", false: map[bool]string{true: "Infinity", false: "-Infinity"}[f > 0]}[math.IsNaN(f)]
		}
		return f
	}
	v := reflect.ValueOf(vals)
	out := make([]any, v.Len())
	for i := range out {
		switch x := v.Index(i).Interface().(type) {
		case float32:
			out[i] = num(float64(x))
		case float64:
			out[i] = num(x)
		case complex64:
			out[i] = []any{num(float64(real(x))), num(float64(imag(x)))}
		case complex128:
			out[i] = []any{num(real(x)), num(imag(x))}
		default:
			out[i] = x
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Generated data, as scenario.py generates it

func genFloat(i, seed int) float64 { return float64((i*37+seed*11)%2001-1000) / 8 }

// halfBits encodes a float exactly representable as an IEEE half (normal or zero).
func halfBits(f float64) uint16 {
	b := math.Float32bits(float32(f))
	sign := uint16(b>>16) & 0x8000
	if b&0x7fffffff == 0 {
		return sign
	}
	exp := int((b>>23)&0xff) - 127 + 15
	return sign | uint16(exp)<<10 | uint16((b>>13)&0x3ff)
}

func putFloat(buf []byte, size int, f float64) []byte {
	le := binary.LittleEndian
	switch size {
	case 2:
		return le.AppendUint16(buf, halfBits(f))
	case 4:
		return le.AppendUint32(buf, math.Float32bits(float32(f)))
	}
	return le.AppendUint64(buf, math.Float64bits(f))
}

func numElements(shape []uint64) int {
	n := 1
	for _, s := range shape {
		n *= int(s)
	}
	return n
}

func genND(dt zarr.DataType, shape []uint64, seed int) (*zarr.NDArray, error) {
	n := numElements(shape)
	nd := &zarr.NDArray{Shape: shape, DataType: dt}
	le := binary.LittleEndian
	for i := 0; i < n; i++ {
		switch dt.Kind {
		case zarr.KindBool:
			b := byte(0)
			if (i*7+seed)%3 == 0 {
				b = 1
			}
			nd.Data = append(nd.Data, b)
		case zarr.KindInt, zarr.KindUint:
			v := uint64(i)*2654435761 + uint64(seed)*40503
			nd.Data = le.AppendUint64(nd.Data, v)[:len(nd.Data)+dt.Size]
		case zarr.KindFloat:
			nd.Data = putFloat(nd.Data, dt.Size, genFloat(i, seed))
		case zarr.KindComplex:
			nd.Data = putFloat(nd.Data, dt.Size/2, genFloat(i, seed))
			nd.Data = putFloat(nd.Data, dt.Size/2, genFloat(i+1000, seed))
		case zarr.KindDatetime, zarr.KindTimedelta:
			nd.Data = le.AppendUint64(nd.Data, uint64(int64(i*86400+seed*3600)))
		case zarr.KindString:
			nd.Strings = append(nd.Strings, fmt.Sprintf("s%d-%d%c", seed, i, rune(0x3b1+i%20)))
		case zarr.KindBytes:
			nd.Strings = append(nd.Strings, fmt.Sprintf("b%d-%d", seed, i))
		case zarr.KindFixedString:
			s := []rune(fmt.Sprintf("%d%dü", seed, i))
			elem := make([]byte, dt.Size)
			for k := 0; k < len(s) && 4*k < dt.Size; k++ {
				le.PutUint32(elem[4*k:], uint32(s[k]))
			}
			nd.Data = append(nd.Data, elem...)
		case zarr.KindFixedBytes:
			elem := make([]byte, dt.Size)
			if dt.Name == "null_terminated_bytes" {
				copy(elem, fmt.Sprintf("%d:%d", seed, i))
			} else {
				for k := range elem {
					elem[k] = byte((i*31 + seed + k) % 256)
				}
			}
			nd.Data = append(nd.Data, elem...)
		default:
			return nil, fmt.Errorf("scenario error: no generator for %s", dt.Name)
		}
	}
	return nd, nil
}

func nanND(dt zarr.DataType, shape []uint64) (*zarr.NDArray, error) {
	if dt.Kind != zarr.KindFloat {
		return nil, fmt.Errorf("scenario error: NaN write to %s", dt.Name)
	}
	// numpy's NaN bit patterns (Go's math.NaN() sets a payload bit).
	nan := map[int][]byte{2: {0x00, 0x7e}, 4: {0x00, 0x00, 0xc0, 0x7f}, 8: {0, 0, 0, 0, 0, 0, 0xf8, 0x7f}}[dt.Size]
	return &zarr.NDArray{Shape: shape, DataType: dt, Data: bytes.Repeat(nan, numElements(shape))}, nil
}

// emptySource has no chunks, so reads through it give the fill value.
type emptySource struct{}

func (emptySource) GetChunk(context.Context, []uint32, int64, int64) ([]byte, bool, error) {
	return nil, false, nil
}
func (emptySource) ChunkSize(context.Context, []uint32) (int64, bool, error) { return 0, false, nil }

func fillND(ctx context.Context, zarrJSON []byte, start, shape []uint64) (*zarr.NDArray, error) {
	arr, err := zarr.OpenArray(zarrJSON, emptySource{}, nil)
	if err != nil {
		return nil, err
	}
	return arr.Read(ctx, start, shape)
}

func rawBytes(seed, size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte((i*13 + seed) % 256)
	}
	return b
}

// ---------------------------------------------------------------------------
// Canonical description, as the Go reader reads a repository

// deterministicCodecs produce the same bytes in every implementation, so
// chunks of arrays using only these are compared byte for byte.
var deterministicCodecs = map[string]bool{
	"bytes": true, "transpose": true, "crc32c": true, "vlen-utf8": true, "vlen-bytes": true,
	"numcodecs.crc32": true, "numcodecs.crc32c": true, "numcodecs.adler32": true, "numcodecs.fletcher32": true,
	"numcodecs.shuffle": true,
}

func canonicalOf(ctx context.Context, dir string) (map[string]any, error) {
	repo, err := icechunk.Open(ctx, storage.NewLocal(dir), nil)
	if err != nil {
		return nil, err
	}
	info, err := repo.RepoInfo(ctx)
	if err != nil {
		return nil, err
	}
	msgOf := map[icechunk.SnapshotID]string{}
	for _, si := range info.Snapshots {
		msgOf[si.ID] = si.Message
	}
	msg := func(id icechunk.SnapshotID) any {
		if id.IsZero() {
			return nil
		}
		if m, ok := msgOf[id]; ok {
			return m
		}
		return "unknown:" + id.String()
	}
	refs := func(m map[string]icechunk.SnapshotID) map[string]any {
		out := map[string]any{}
		for k, v := range m {
			out[k] = msg(v)
		}
		return out
	}
	commits := map[string]any{}
	for _, si := range info.Snapshots {
		var parent any
		if si.ParentID != nil {
			parent = msg(*si.ParentID)
		}
		md := si.Metadata
		if md == nil {
			md = map[string]any{}
		}
		commits[si.Message] = map[string]any{"parent": parent, "metadata": md}
	}
	update := func(u icechunk.Update) []any {
		return []any{u.Kind, updateName(u), msg(u.SnapshotID), msg(u.PreviousSnapshotID)}
	}
	updates := []any{}
	for _, u := range info.Updates {
		updates = append(updates, update(u))
	}
	opsLog := []any{}
	for u, err := range repo.OpsLog(ctx) {
		if err != nil {
			return nil, err
		}
		opsLog = append(opsLog, update(u))
	}
	ancestryOf := map[string]any{}
	for kind, m := range map[string]map[string]icechunk.SnapshotID{"branch": info.Branches, "tag": info.Tags} {
		for name := range m {
			v := icechunk.AtBranch(name)
			if kind == "tag" {
				v = icechunk.AtTag(name)
			}
			msgs := []string{}
			for si, err := range repo.Ancestry(ctx, v) {
				if err != nil {
					return nil, err
				}
				msgs = append(msgs, si.Message)
			}
			ancestryOf[kind+":"+name] = msgs
		}
	}
	cfg, err := repo.Config(ctx)
	if err != nil {
		return nil, err
	}
	deleted := append([]string{}, info.DeletedTags...)
	sort.Strings(deleted)
	snapshots := map[string]any{}
	manifestIDs := map[icechunk.SnapshotID]map[string][]icechunk.ManifestID{}
	for _, si := range info.Snapshots {
		d, ids, err := describeSnapshotGo(ctx, repo, si.ID)
		if err != nil {
			return nil, fmt.Errorf("snapshot %q: %w", si.Message, err)
		}
		snapshots[si.Message], manifestIDs[si.ID] = d, ids
	}
	// How many of each array's manifests a commit kept from its parent.
	for _, si := range info.Snapshots {
		var parent map[string][]icechunk.ManifestID
		if si.ParentID != nil {
			parent = manifestIDs[*si.ParentID]
		}
		nodes := snapshots[si.Message].(map[string]any)["nodes"].(map[string]any)
		for path, ids := range manifestIDs[si.ID] {
			kept := 0
			for _, id := range ids {
				if slices.Contains(parent[path], id) {
					kept++
				}
			}
			nodes[path].(map[string]any)["manifests_kept"] = kept
		}
	}
	return map[string]any{
		"branches":     refs(info.Branches),
		"tags":         refs(info.Tags),
		"deleted_tags": deleted,
		"commits":      commits,
		"updates":      updates,
		"ops_log":      opsLog,
		"ancestry":     ancestryOf,
		"status":       info.Status.Availability,
		"config":       configEntry(cfg),
		"snapshots":    snapshots,
	}, nil
}

func describeSnapshotGo(ctx context.Context, repo *icechunk.Repository, id icechunk.SnapshotID) (map[string]any, map[string][]icechunk.ManifestID, error) {
	sess, err := repo.ReadonlySession(ctx, icechunk.AtSnapshot(id))
	if err != nil {
		return nil, nil, err
	}
	keys, err := sess.Store().ListPrefix(ctx, "")
	if err != nil {
		return nil, nil, err
	}
	all, err := sess.Nodes()
	if err != nil {
		return nil, nil, err
	}
	manifestIDs := map[string][]icechunk.ManifestID{}
	nodes := map[string]any{}
	for _, n := range all {
		var doc map[string]any
		if err := decodeJSON(n.ZarrMetadata, &doc); err != nil {
			return nil, nil, fmt.Errorf("%s: zarr.json: %w", n.Path, err)
		}
		normalizeZarrJSON(doc)
		e := map[string]any{"type": n.Type.String(), "zarr_json": doc}
		if n.Type == icechunk.ArrayNode {
			extents := [][][2]uint32{}
			for _, m := range n.Array.Manifests {
				x := [][2]uint32{}
				for _, r := range m.Extents {
					x = append(x, [2]uint32{r.From, r.To})
				}
				extents = append(extents, x)
			}
			sort.Slice(extents, func(i, j int) bool { return compareExtents(extents[i], extents[j]) < 0 })
			e["manifest_extents"] = extents
			manifestIDs[n.Path] = []icechunk.ManifestID{}
			for _, m := range n.Array.Manifests {
				manifestIDs[n.Path] = append(manifestIDs[n.Path], m.ID)
			}
			e["values"] = "error:other"
			if arr, err := sess.OpenArray(ctx, n.Path); err != nil {
				e["values"] = category(err)
			} else if nd, err := arr.ReadAll(ctx); err != nil {
				e["values"] = category(err)
			} else {
				e["values"] = digestND(nd)
			}
			exact := true
			for _, c := range codecNames(doc) {
				exact = exact && deterministicCodecs[c]
			}
			chunks := map[string]any{}
			for ce, err := range sess.ChunkRefs(ctx, n.Path) {
				if err != nil {
					return nil, nil, err
				}
				parts := make([]string, len(ce.Coords))
				for i, c := range ce.Coords {
					parts[i] = fmt.Sprint(c)
				}
				key := strings.Join(parts, ",")
				switch {
				case ce.Ref.Kind == icechunk.VirtualChunk:
					chunks[key] = fmt.Sprintf("virtual:%s:%d:%d", ce.Ref.Location, ce.Ref.Offset, ce.Ref.Length)
				case exact:
					data, _, err := sess.GetChunk(ctx, n.Path, ce.Coords)
					if err != nil {
						return nil, nil, err
					}
					chunks[key] = ce.Ref.Kind.String() + ":" + digestBytes(data)
				default:
					chunks[key] = "stored"
				}
			}
			e["chunks"] = chunks
		}
		nodes[n.Path] = e
	}
	return map[string]any{"keys": append([]string{}, keys...), "nodes": nodes}, manifestIDs, nil
}

// endianless data types have single-byte elements: the bytes codec's endian
// has no effect on them, and zarr-python omits it.
var endianless = map[string]bool{"bool": true, "int8": true, "uint8": true, "null_terminated_bytes": true, "raw_bytes": true}

// normalizeZarrJSON drops zarr.json differences that mean nothing, as
// scenario.py's zarr_doc does: consolidated_metadata null (same as absent),
// empty codec configurations, and the bytes codec's endian for single-byte
// data types.
func normalizeZarrJSON(doc map[string]any) {
	if v, ok := doc["consolidated_metadata"]; ok && v == nil {
		delete(doc, "consolidated_metadata")
	}
	name, _ := doc["data_type"].(string)
	if m, ok := doc["data_type"].(map[string]any); ok {
		name, _ = m["name"].(string)
	}
	codecs, _ := doc["codecs"].([]any)
	normalizeCodecs(codecs, endianless[name])
}

func normalizeCodecs(codecs []any, endianless bool) {
	for _, c := range codecs {
		m, _ := c.(map[string]any)
		cfg, _ := m["configuration"].(map[string]any)
		if cfg == nil {
			continue
		}
		if m["name"] == "bytes" && endianless {
			delete(cfg, "endian")
		}
		if m["name"] == "sharding_indexed" {
			inner, _ := cfg["codecs"].([]any)
			normalizeCodecs(inner, endianless)
			index, _ := cfg["index_codecs"].([]any)
			normalizeCodecs(index, false)
		}
		if len(cfg) == 0 {
			delete(m, "configuration")
		}
	}
}

func codecNames(doc map[string]any) []string {
	var out []string
	codecs, _ := doc["codecs"].([]any)
	for _, c := range codecs {
		if m, ok := c.(map[string]any); ok {
			name, _ := m["name"].(string)
			out = append(out, name)
		}
	}
	return out
}

// compareExtents orders extents as Python orders nested lists.
func compareExtents(a, b [][2]uint32) int {
	for i := 0; i < min(len(a), len(b)); i++ {
		if c := slices.Compare(a[i][:], b[i][:]); c != 0 {
			return c
		}
	}
	return len(a) - len(b)
}

// withoutDiffs drops the transaction log diffs, which only icechunk-python
// reads (the Go reader does not expose transaction logs).
func withoutDiffs(c map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range c {
		if k != "diffs" {
			out[k] = v
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Comparing runs

// expectSameRepo compares canonical descriptions of repositories.
func expectSameRepo(t *testing.T, what string, want, got map[string]any) {
	t.Helper()
	expectSame(t, what, want, got)
}

// compareOutcomes compares each step's outcome with icechunk-python's
// recorded one; writers names who ran each step.
func compareOutcomes(t *testing.T, steps []step, writers, want, got []string, errs []error) {
	t.Helper()
	if len(want) != len(steps) {
		t.Fatalf("recorded %d outcomes for %d steps (re-record with scenario.py)", len(want), len(steps))
	}
	for i, st := range steps {
		if got[i] == want[i] {
			continue
		}
		what := fmt.Sprintf("step %d (%s %s%s%s) run by %s", i, st.Op, st.Path, st.Name, st.Message, writers[i])
		t.Errorf("%s: %s, recorded from icechunk-python: %s (%v)", what, got[i], want[i], errs[i])
	}
}

// runner applies one step with one writer.
type runner interface {
	apply(ctx context.Context, st step) (string, error)
}

// runGo runs every step with the Go library writer, or through the HTTP
// write endpoints (writer "http"), in a new repository.
func runGo(t *testing.T, name string, sc scenario, dir string, steps []step, writer string) ([]string, []error) {
	t.Helper()
	ctx := context.Background()
	createRepo(t, name, sc, dir)
	var r runner = newGoRunner(t, dir)
	if writer == "http" {
		r = newHTTPRunner(t, dir)
	}
	outcomes, errs := make([]string, len(steps)), make([]error, len(steps))
	for i, st := range steps {
		outcomes[i], errs[i] = r.apply(ctx, st)
	}
	return outcomes, errs
}

// createRepo creates a scenario's repository with the Go writer, or, for a
// scenario with a config, copies the empty repository icechunk-python
// created with it.
func createRepo(t *testing.T, name string, sc scenario, dir string) {
	t.Helper()
	if sc.Config != nil {
		if err := os.CopyFS(dir, os.DirFS(writerTestdata+"/initial/"+name)); err != nil {
			t.Fatalf("copying the initial repository (run scenario.py record): %v", err)
		}
		return
	}
	if _, err := icechunk.Create(context.Background(), storage.NewLocal(dir), nil); err != nil {
		t.Fatal(err)
	}
}

func repeat(s string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = s
	}
	return out
}

// TestWriterScenarios runs every scenario with the Go writer (and the
// HTTP-compatible ones through the write endpoints too) and compares step
// outcomes and the repository the Go reader reads back with what
// icechunk-python produced from the same steps.
func TestWriterScenarios(t *testing.T) {
	scenarios, names := loadScenarios(t)
	for _, name := range names {
		writers := []string{"go"}
		if scenarios[name].HTTP {
			writers = append(writers, "http")
		}
		for _, writer := range writers {
			t.Run(name+"/"+writer, func(t *testing.T) {
				t.Parallel()
				want := loadRecordedRun(t, name)
				steps := parseSteps(t, scenarios[name])
				dir := filepath.Join(t.TempDir(), "repo")
				outcomes, errs := runGo(t, name, scenarios[name], dir, steps, writer)
				compareOutcomes(t, steps, repeat(writer, len(steps)), want.Outcomes, outcomes, errs)
				got, err := canonicalOf(context.Background(), dir)
				if err != nil {
					t.Fatalf("reading the Go-written repository: %v", err)
				}
				expectSameRepo(t, writer+"-written repository read by Go", withoutDiffs(want.Canonical), got)
			})
		}
	}
}

// ---------------------------------------------------------------------------
// Live: icechunk-python reads and writes alongside the Go writer

// pyDriver talks to `scenario.py serve`.
type pyDriver struct {
	mu   sync.Mutex
	cmd  *exec.Cmd
	in   io.WriteCloser
	out  *bufio.Reader
	errb bytes.Buffer
}

func startPyDriver(t *testing.T) *pyDriver {
	t.Helper()
	py := os.Getenv("ICECHUNK_PYTHON")
	if py == "" {
		t.Skip("set ICECHUNK_PYTHON to an interpreter with icechunk, zarr and numpy to run")
	}
	d := &pyDriver{cmd: exec.Command(py, writerTestdata+"/scenario.py", "serve")}
	d.cmd.Stderr = &d.errb
	var err error
	if d.in, err = d.cmd.StdinPipe(); err != nil {
		t.Fatal(err)
	}
	stdout, err := d.cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	d.out = bufio.NewReaderSize(stdout, 1<<20)
	if err := d.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.in.Close(); d.cmd.Wait() })
	return d
}

func (d *pyDriver) call(t *testing.T, req map[string]any) map[string]json.RawMessage {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	line, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.in.Write(append(line, '\n')); err != nil {
		t.Fatalf("python driver: %v\n%s", err, tail(d.errb.String(), 2000))
	}
	resp, err := d.out.ReadBytes('\n')
	if err != nil {
		t.Fatalf("python driver: %v\n%s", err, tail(d.errb.String(), 2000))
	}
	var out map[string]json.RawMessage
	if err := json.Unmarshal(resp, &out); err != nil {
		t.Fatalf("python driver reply %q: %v", tail(string(resp), 300), err)
	}
	if e, ok := out["driver_error"]; ok {
		var msg string
		json.Unmarshal(e, &msg)
		t.Fatalf("python driver failed on %s:\n%s", tail(string(line), 300), msg)
	}
	return out
}

func (d *pyDriver) canonical(t *testing.T, dir string) map[string]any {
	t.Helper()
	var c map[string]any
	if err := decodeJSON(d.call(t, map[string]any{"cmd": "canonical", "repo": dir})["canonical"], &c); err != nil {
		t.Fatal(err)
	}
	return c
}

// writerModes assigns each step to a writer: "go" (the library), "python"
// or "http" (the Go write endpoints). In the mixed modes, writable sessions
// alternate between two writers in the order they are opened, as do branch
// and tag operations, starting with the named writer (which also creates
// the repository; the Go library creates it for "http").
var writerModes = []string{"go", "python", "mixed-go-first", "mixed-python-first"}

// httpWriterModes are added for scenarios marked "http".
var httpWriterModes = []string{"http", "mixed-http-first", "mixed-python-http"}

func assignWriters(steps []step, mode string) (creator string, writers []string) {
	langs := [2]string{"go", "python"}
	switch mode {
	case "go", "python", "http":
		creator = mode
		if mode == "http" {
			creator = "go"
		}
		return creator, repeat(mode, len(steps))
	case "mixed-python-first":
		langs = [2]string{"python", "go"}
	case "mixed-http-first":
		langs = [2]string{"http", "python"}
	case "mixed-python-http":
		langs = [2]string{"python", "http"}
	}
	session := map[string]string{}
	opens, refOps := 0, 0
	for _, st := range steps {
		switch {
		case st.Op == "open":
			session[st.Session] = langs[opens%2]
			opens++
			writers = append(writers, session[st.Session])
		case st.Session != "":
			writers = append(writers, session[st.Session])
		default:
			writers = append(writers, langs[refOps%2])
			refOps++
		}
	}
	creator = langs[0]
	if creator == "http" {
		creator = "go"
	}
	return creator, writers
}

// TestWriterScenariosLive runs every scenario with the Go writer, with
// icechunk-python, and with both interleaved on one repository; every run
// must produce the recorded outcomes and a repository that both readers
// read as icechunk-python's own (transaction logs included).
func TestWriterScenariosLive(t *testing.T) {
	d := startPyDriver(t)
	scenarios, names := loadScenarios(t)
	for _, name := range names {
		modes := writerModes
		if scenarios[name].HTTP {
			modes = append(slices.Clone(writerModes), httpWriterModes...)
		}
		for _, mode := range modes {
			t.Run(name+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				want := loadRecordedRun(t, name)
				steps := parseSteps(t, scenarios[name])
				dir := filepath.Join(t.TempDir(), "repo")
				creator, writers := assignWriters(steps, mode)
				if creator == "go" {
					createRepo(t, name, scenarios[name], dir)
				} else {
					d.call(t, map[string]any{"cmd": "create", "repo": dir, "config": scenarios[name].Config})
				}
				runners := map[string]runner{"go": newGoRunner(t, dir), "http": newHTTPRunner(t, dir)}
				outcomes, errs := make([]string, len(steps)), make([]error, len(steps))
				for i, st := range steps {
					if r := runners[writers[i]]; r != nil {
						outcomes[i], errs[i] = r.apply(ctx, st)
						continue
					}
					var raw map[string]any
					decodeJSON(scenarios[name].Steps[i], &raw)
					json.Unmarshal(d.call(t, map[string]any{"cmd": "step", "repo": dir, "step": raw})["outcome"], &outcomes[i])
					errs[i] = errors.New("(python)")
				}
				compareOutcomes(t, steps, writers, want.Outcomes, outcomes, errs)
				expectSameRepo(t, mode+" repository read by icechunk-python", want.Canonical, d.canonical(t, dir))
				got, err := canonicalOf(ctx, dir)
				if err != nil {
					t.Fatalf("reading the repository with Go: %v", err)
				}
				expectSameRepo(t, mode+" repository read by Go", withoutDiffs(want.Canonical), got)
			})
		}
	}
}
