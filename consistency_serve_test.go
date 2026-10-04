package icechunk_test

// The HTTP service (package serve, as run by `icechunk-go serve` and the
// Cloudflare Worker) checked against icechunk-python, using the same oracle
// files as consistency_test.go.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/solomonsealed/icechunk-go/serve"
)

func serveGet(t *testing.T, svc *serve.Service, target string, header map[string]string) *serve.Response {
	t.Helper()
	u, err := url.Parse(target)
	if err != nil {
		t.Fatalf("bad target %q: %v", target, err)
	}
	if header == nil {
		header = map[string]string{}
	}
	return svc.Handle(context.Background(), &serve.Request{Method: "GET", URL: u, Header: header})
}

// escapePath escapes each segment of a slash-separated path for a URL.
func escapePath(p string) string {
	segs := strings.Split(p, "/")
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	return strings.Join(segs, "/")
}

// rangeHeader turns an oracle range spec into an HTTP Range header, or ""
// for requests HTTP cannot express (empty ranges) or answers differently by
// design (ranges past the end are clamped, RFC 9110).
func rangeHeader(spec string, size int64) string {
	p := strings.Split(spec, ":")
	var a, b int64
	fmt.Sscan(p[1], &a)
	switch p[0] {
	case "r":
		fmt.Sscan(p[2], &b)
		if b <= a || b > size {
			return ""
		}
		return fmt.Sprintf("bytes=%d-%d", a, b-1)
	case "o":
		if a >= size {
			return ""
		}
		return fmt.Sprintf("bytes=%d-", a)
	case "s":
		return fmt.Sprintf("bytes=-%d", a)
	}
	return ""
}

// TestPythonServe: the repository summary, /log of every branch and tag,
// and for every snapshot /nodes, /chunks, every /zarr key (whole and with
// Range requests, by snapshot id and by ref name) and /array region reads
// with format=binary.
func TestPythonServe(t *testing.T) {
	forEachFixture(t, func(t *testing.T, fx *fixture) {
		svc := &serve.Service{Repo: fx.repo}
		r := fx.o.Repo
		v1 := fx.repo.SpecVersion() == 1

		// Summary: refs as icechunk-python resolves them.
		resp := serveGet(t, svc, "/", nil)
		var sum struct {
			Spec     int               `json:"spec_version"`
			Branches map[string]string `json:"branches"`
			Tags     map[string]string `json:"tags"`
		}
		if resp.Status != http.StatusOK || json.Unmarshal(resp.Body, &sum) != nil {
			t.Fatalf("GET /: %d %s", resp.Status, resp.Body)
		}
		if sum.Spec != r.SpecVersion {
			t.Errorf("GET /: spec version %d, icechunk-python %d", sum.Spec, r.SpecVersion)
		}
		for _, refs := range []struct {
			kind      string
			want, got map[string]string
		}{{"branches", r.Branches, sum.Branches}, {"tags", r.Tags, sum.Tags}} {
			want := map[string]string{}
			for name, id := range refs.want {
				if !isError(id) {
					want[name] = id
				}
			}
			got := map[string]string{}
			for name, id := range refs.got {
				if _, ok := want[name]; !ok && v1 && percentEncoded(name) {
					knownIssue(t, "v1-ref-encoding", "GET /: %s %q = %s, which icechunk-python cannot look up", refs.kind, name, id)
					continue
				}
				got[name] = id
			}
			expectSame(t, "GET / "+refs.kind, want, got)
		}

		// /log of every ref, and /zarr/<ref>/zarr.json.
		for _, label := range sortedKeys(r.Ancestry) {
			kind, name, _ := strings.Cut(label, ":")
			if kind == "snapshot" {
				continue
			}
			var want []map[string]any
			for _, id := range r.Ancestry[label] {
				var info map[string]any
				json.Unmarshal(r.SnapshotInfos[id], &info)
				e := map[string]any{"id": id, "flushed_at": info["written_at"], "message": info["message"]}
				if info["parent_id"] != nil {
					e["parent"] = info["parent_id"]
				}
				if md, _ := info["metadata"].(map[string]any); len(md) > 0 {
					e["metadata"] = md
				}
				want = append(want, e)
			}
			resp := serveGet(t, svc, "/log?limit=100000&ref="+url.QueryEscape(name), nil)
			var got []map[string]any
			if resp.Status != http.StatusOK || json.Unmarshal(resp.Body, &got) != nil {
				t.Errorf("GET /log for %s: %d %s", label, resp.Status, tail(string(resp.Body), 300))
				continue
			}
			expectSame(t, "GET /log for "+label, want, got)

			wantRoot := "absent" // the initial snapshot has no root group
			if root, ok := fx.o.Snapshots[r.Ancestry[label][0]].Store.MetadataKeys["zarr.json"]; ok {
				wantRoot = root.Get
			}
			resp = serveGet(t, svc, "/zarr/"+url.PathEscape(name)+"/zarr.json", nil)
			checkServed(t, fmt.Sprintf("GET /zarr/%s/zarr.json", name), resp, http.StatusOK, wantRoot)
		}

		for _, sid := range sortedKeys(fx.o.Snapshots) {
			snap := fx.o.Snapshots[sid]
			what := "snapshot " + sid

			// /nodes
			wantNodes := []map[string]any{}
			for _, n := range snap.Nodes {
				e := map[string]any{"path": n.Path, "type": n.Type}
				var attrs map[string]any
				if n.Type == "array" {
					z := fx.o.Arrays[n.Content].Zarr
					if len(z.Shape) > 0 {
						e["shape"] = z.Shape
					}
					var dt struct {
						Name string `json:"name"`
					}
					json.Unmarshal(z.DataType, &dt)
					e["dtype"] = dt.Name
					if z.DimensionNames != nil {
						names := make([]string, len(z.DimensionNames))
						for i, n := range z.DimensionNames {
							if n != nil {
								names[i] = *n
							}
						}
						e["dimension_names"] = names
					}
					json.Unmarshal(z.Attributes, &attrs)
				} else {
					json.Unmarshal(snap.GroupAttributes[n.Path], &attrs)
				}
				if len(attrs) > 0 {
					e["attributes"] = attrs
				}
				wantNodes = append(wantNodes, e)
			}
			resp := serveGet(t, svc, "/nodes?ref="+sid, nil)
			var gotNodes []map[string]any
			if resp.Status != http.StatusOK || json.Unmarshal(resp.Body, &gotNodes) != nil {
				t.Errorf("%s: GET /nodes: %d %s", what, resp.Status, tail(string(resp.Body), 300))
			} else {
				for _, n := range gotNodes {
					delete(n, "chunk_shape") // zarr-python has no single equivalent for every grid
				}
				expectSame(t, what+": GET /nodes", wantNodes, gotNodes)
				if resp.Header["x-snapshot-id"] != sid {
					t.Errorf("%s: GET /nodes x-snapshot-id = %q", what, resp.Header["x-snapshot-id"])
				}
			}

			// /zarr/<id>/<key>, whole and ranged.
			for _, key := range snap.Store.List {
				ki, ok := snap.Store.MetadataKeys[key]
				if !ok {
					for _, n := range snap.Nodes {
						if rel, found := strings.CutPrefix(key, keyPrefix(n.Path)); found && n.Content != "" {
							if k, found := fx.o.Arrays[n.Content].ChunkKeys[rel]; found {
								ki, ok = k, true
								break
							}
						}
					}
				}
				if !ok {
					t.Errorf("%s: no oracle entry for key %q", what, key)
					continue
				}
				target := "/zarr/" + sid + "/" + escapePath(key)
				checkServed(t, fmt.Sprintf("%s: GET %s", what, key), serveGet(t, svc, target, nil), http.StatusOK, ki.Get)
				var size int64
				json.Unmarshal(ki.Size, &size)
				for _, spec := range sortedKeys(ki.Ranges) {
					h := rangeHeader(spec, size)
					if h == "" {
						continue
					}
					resp := serveGet(t, svc, target, map[string]string{"range": h})
					checkServed(t, fmt.Sprintf("%s: GET %s (%s)", what, key, h), resp, http.StatusPartialContent, ki.Ranges[spec])
				}
			}

			// /chunks/<path> and /array/<path>?format=binary
			for _, n := range snap.Nodes {
				if n.Content == "" {
					continue
				}
				arr := fx.o.Arrays[n.Content]
				var want []map[string]any
				for _, raw := range arr.Refs {
					var ref []any
					json.Unmarshal(raw, &ref)
					kind, loc := ref[1].(string), ref[2]
					e := map[string]any{"coords": ref[0], "kind": kind, "size": ref[4]}
					switch kind {
					case "native":
						e["chunk_id"] = loc
					case "virtual":
						e["location"] = loc
					}
					if off := ref[3].(float64); off != 0 && kind != "inline" {
						e["offset"] = off
					}
					want = append(want, e)
				}
				resp := serveGet(t, svc, "/chunks/"+escapePath(strings.TrimPrefix(n.Path, "/"))+"?limit=100000000&ref="+sid, nil)
				var got []map[string]any
				if resp.Status != http.StatusOK || json.Unmarshal(resp.Body, &got) != nil {
					t.Errorf("%s: GET /chunks%s: %d %s", what, n.Path, resp.Status, tail(string(resp.Body), 300))
				} else {
					sort.Slice(got, func(i, j int) bool { return lessCoords(got[i]["coords"], got[j]["coords"]) })
					if want == nil {
						want = []map[string]any{}
					}
					expectSame(t, fmt.Sprintf("%s: GET /chunks%s", what, n.Path), want, got)
				}

				if loc := fx.arrayAt[n.Content]; loc.snapshot != sid || loc.path != n.Path {
					continue // region reads of each array version once
				}
				var dt struct {
					Name string `json:"name"`
				}
				json.Unmarshal(arr.Zarr.DataType, &dt)
				if arr.Zarr.Error != "" || slices.Contains([]string{"string", "bytes", "variable_length_bytes"}, dt.Name) {
					continue // format=binary is for fixed-size types
				}
				for _, rd := range arr.Zarr.Reads {
					if slices.Contains(rd.Count, 0) {
						continue
					}
					parts := make([]string, len(rd.Start))
					for d := range rd.Start {
						parts[d] = fmt.Sprintf("%d:%d", rd.Start[d], rd.Start[d]+rd.Count[d])
					}
					target := "/array/" + escapePath(strings.TrimPrefix(n.Path, "/")) + "?format=binary&ref=" + sid +
						"&slice=" + url.QueryEscape(strings.Join(parts, ","))
					checkServed(t, fmt.Sprintf("%s: GET /array%s[%s]", what, n.Path, strings.Join(parts, ",")),
						serveGet(t, svc, target, nil), http.StatusOK, rd.Result)
				}
			}
		}
	})
}

// checkServed compares a response with an oracle outcome: a digest of the
// expected body (with status ok), or an error (any status but 2xx).
func checkServed(t *testing.T, what string, resp *serve.Response, ok int, want string) {
	t.Helper()
	switch {
	case isError(want) || want == "absent":
		if resp.Status < 300 {
			t.Errorf("%s: status %d, icechunk-python %s", what, resp.Status, want)
		}
	case resp.Status != ok:
		t.Errorf("%s: status %d (%s), icechunk-python %s", what, resp.Status, tail(string(resp.Body), 200), want)
	case digestBytes(resp.Body) != want:
		t.Errorf("%s: body %s, icechunk-python %s", what, digestBytes(resp.Body), want)
	}
}

func lessCoords(a, b any) bool {
	x, _ := a.([]any)
	y, _ := b.([]any)
	for i := 0; i < min(len(x), len(y)); i++ {
		if p, q := x[i].(float64), y[i].(float64); p != q {
			return p < q
		}
	}
	return len(x) < len(y)
}
