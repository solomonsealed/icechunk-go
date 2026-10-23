//go:build js && wasm

// Run with Node:
//
//	GOOS=js GOARCH=wasm go test -exec="$(go env GOROOT)/lib/wasm/go_js_wasm_exec" ./cfworker

package cfworker

import (
	"context"
	"errors"
	"strings"
	"syscall/js"
	"testing"

	"github.com/solomonsealed/icechunk-go/storage"
)

// fakeR2 is an R2 bucket binding: put stores objects with their custom
// metadata and records the options it gets; fault makes the next put fail:
// "null" answers null without storing (a failed condition), "store-null"
// stores and answers null (a retried write tripping its own condition),
// "store-throw" stores and rejects (a lost response), "throw" rejects.
type fakeR2 struct {
	objects map[string]js.Value // key -> {etag, customMetadata}
	lastPut js.Value            // the options of the last put
	fault   string
	n       int
	binding js.Value
}

func newFakeR2() *fakeR2 {
	f := &fakeR2{objects: map[string]js.Value{}}
	resolve := func(v any) js.Value { return jsPromise.Call("resolve", v) }
	f.binding = jsObject.New()
	f.binding.Set("get", js.FuncOf(func(js.Value, []js.Value) any { return resolve(js.Null()) }))
	f.binding.Set("head", js.FuncOf(func(_ js.Value, args []js.Value) any {
		if o, ok := f.objects[args[0].String()]; ok {
			return resolve(o)
		}
		return resolve(js.Null())
	}))
	f.binding.Set("put", js.FuncOf(func(_ js.Value, args []js.Value) any {
		key, opts := args[0].String(), args[2]
		f.lastPut = opts
		fault := f.fault
		f.fault = ""
		store := func() js.Value {
			f.n++
			o := jsObject.New()
			o.Set("etag", "e"+string(rune('0'+f.n)))
			meta := jsObject.New()
			if m := opts.Get("customMetadata"); m.Type() == js.TypeObject {
				meta = jsObject.Call("assign", meta, m)
			}
			o.Set("customMetadata", meta)
			f.objects[key] = o
			return o
		}
		switch fault {
		case "null":
			return resolve(js.Null())
		case "store-null":
			store()
			return resolve(js.Null())
		case "store-throw":
			store()
			return jsPromise.Call("reject", js.Global().Get("Error").New("network connection lost"))
		case "throw":
			return jsPromise.Call("reject", js.Global().Get("Error").New("network connection lost"))
		}
		return resolve(store())
	}))
	return f
}

func TestR2PutStorageClassMetadataAndReadBack(t *testing.T) {
	ctx := context.Background()
	f := newFakeR2()
	r := NewR2(f.binding, "repo")
	opts := func(id string, o storage.PutOptions) *storage.PutOptions {
		o.StorageClass = "InfrequentAccess"
		o.Metadata = map[string]string{"icfiletype": "repo-info", storage.WriteIDMetadataKey: id}
		return &o
	}

	v, err := r.Put(ctx, "repo", []byte("one"), opts("w1", storage.PutOptions{IfNotExists: true}))
	if err != nil || v != "e1" {
		t.Fatalf("put: %q, %v", v, err)
	}
	if c := f.lastPut.Get("storageClass"); c.Type() != js.TypeString || c.String() != "InfrequentAccess" {
		t.Errorf("storageClass = %v", c)
	}
	if m := f.lastPut.Get("customMetadata"); m.Get("icfiletype").String() != "repo-info" || m.Get(storage.WriteIDMetadataKey).String() != "w1" {
		t.Errorf("customMetadata = %v", js.Global().Get("JSON").Call("stringify", m))
	}
	if h := f.lastPut.Get("onlyIf"); h.Call("get", "if-none-match").String() != "*" {
		t.Errorf("onlyIf = %v", h)
	}

	f.fault = "store-throw"
	if v, err := r.Put(ctx, "repo", []byte("two"), opts("w2", storage.PutOptions{IfMatch: "e1"})); err != nil || v != "e2" {
		t.Errorf("lost response: %q, %v", v, err)
	}
	f.fault = "store-null"
	if v, err := r.Put(ctx, "repo", []byte("three"), opts("w3", storage.PutOptions{IfMatch: "e2"})); err != nil || v != "e3" {
		t.Errorf("replayed write: %q, %v", v, err)
	}
	f.fault = "null"
	if _, err := r.Put(ctx, "repo", []byte("four"), opts("w4", storage.PutOptions{IfMatch: "e1"})); !errors.Is(err, storage.ErrPreconditionFailed) {
		t.Errorf("conflict: %v", err)
	}
	f.fault = "throw"
	if _, err := r.Put(ctx, "repo", []byte("five"), opts("w5", storage.PutOptions{IfMatch: "e3"})); err == nil || errors.Is(err, storage.ErrPreconditionFailed) {
		t.Errorf("lost write: %v", err)
	}
	// Plain writes set no options they do not need.
	if _, err := r.Put(ctx, "chunks/x", []byte("c"), nil); err != nil {
		t.Fatal(err)
	}
	if f.lastPut.Get("storageClass").Type() != js.TypeUndefined || f.lastPut.Get("customMetadata").Type() != js.TypeUndefined {
		t.Errorf("plain put options: %v", js.Global().Get("JSON").Call("stringify", f.lastPut))
	}
}

// fakeFetch replaces the global fetch: it records each request's method
// and headers, and answers with respond(method).
func fakeFetch(t *testing.T, respond func(method string) (status int, headers map[string]any)) *[]map[string]string {
	var seen []map[string]string
	old := js.Global().Get("fetch")
	fn := js.FuncOf(func(_ js.Value, args []js.Value) any {
		init := args[1]
		h := map[string]string{"method": init.Get("method").String()}
		hs := init.Get("headers")
		keys := jsObject.Call("keys", hs)
		for i := 0; i < keys.Length(); i++ {
			k := keys.Index(i).String()
			h[strings.ToLower(k)] = hs.Get(k).String()
		}
		seen = append(seen, h)
		status, headers := respond(h["method"])
		ri := jsObject.New()
		ri.Set("status", status)
		ri.Set("headers", js.ValueOf(headers))
		body := js.Null()
		if status != 204 && h["method"] != "HEAD" && status != 412 {
			body = js.ValueOf("")
		}
		return jsPromise.Call("resolve", js.Global().Get("Response").New(body, ri))
	})
	js.Global().Set("fetch", fn)
	t.Cleanup(func() { js.Global().Set("fetch", old); fn.Release() })
	return &seen
}

func TestS3PutHeadersAndReadBack(t *testing.T) {
	ctx := storage.WithUserAgent(context.Background(), "icechunk/2.3.0")
	s := NewS3(storage.S3Config{Bucket: "b", Endpoint: "https://s3.example", AccessKeyID: "AK", SecretAccessKey: "SK"})
	var stored string // the write id the fake store holds
	seen := fakeFetch(t, func(method string) (int, map[string]any) {
		switch method {
		case "PUT":
			return 412, map[string]any{}
		case "HEAD":
			return 200, map[string]any{"etag": `"e9"`, "x-amz-meta-icechunkwriteid": stored}
		}
		return 500, map[string]any{}
	})
	opts := &storage.PutOptions{IfMatch: "e1", StorageClass: "STANDARD_IA",
		Metadata: map[string]string{"icfiletype": "repo-info", storage.WriteIDMetadataKey: "w1"}}

	// The PUT trips a condition, but the stored object holds our write id.
	stored = "w1"
	v, err := s.Put(ctx, "repo", []byte("x"), opts)
	if err != nil || v != "e9" {
		t.Fatalf("replayed write: %q, %v", v, err)
	}
	if len(*seen) != 2 || (*seen)[1]["method"] != "HEAD" {
		t.Fatalf("requests: %v", *seen)
	}
	put := (*seen)[0]
	if put["user-agent"] != "icechunk/2.3.0" || put["x-amz-storage-class"] != "STANDARD_IA" || put["x-amz-meta-icfiletype"] != "repo-info" ||
		put["x-amz-meta-icechunkwriteid"] != "w1" || !strings.Contains(put["authorization"], "x-amz-meta-icfiletype;") {
		t.Errorf("PUT headers: %v", put)
	}
	if (*seen)[1]["user-agent"] != "icechunk/2.3.0" {
		t.Errorf("HEAD headers: %v", (*seen)[1])
	}
	// Another write holds the object: a genuine conflict.
	stored = "other"
	if _, err := s.Put(ctx, "repo", []byte("x"), opts); !errors.Is(err, storage.ErrPreconditionFailed) {
		t.Errorf("conflict: %v", err)
	}
}
