//go:build js && wasm

package cfworker

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"syscall/js"

	"github.com/solomonsealed/icechunk-go/storage"
)

// GlobalHandlerName is the global function Serve registers; the JavaScript
// entry point forwards fetch(request, env, ctx) to it.
const GlobalHandlerName = "__goWorkerFetch"

// Request is an incoming Worker request.
type Request struct {
	Method string
	URL    *url.URL
	// Header holds request headers with lower-case names.
	Header map[string]string
	Env    Env
	// JS is the original Request object.
	JS      js.Value
	execCtx js.Value
}

// Body reads the request body.
func (r *Request) Body(ctx context.Context) ([]byte, error) {
	buf, err := Await(ctx, r.JS.Call("arrayBuffer"))
	if err != nil {
		return nil, err
	}
	return bytesOf(buf), nil
}

// WaitUntil keeps the Worker alive until promise settles (ctx.waitUntil).
func (r *Request) WaitUntil(promise js.Value) {
	if r.execCtx.Truthy() {
		r.execCtx.Call("waitUntil", promise)
	}
}

// Env gives access to the Worker's bindings, variables and secrets.
type Env struct{ v js.Value }

// Binding returns a binding such as an R2 bucket (env[name]).
func (e Env) Binding(name string) js.Value { return e.v.Get(name) }

// Var returns a string variable or secret, or "" if unset.
func (e Env) Var(name string) string {
	v := e.v.Get(name)
	if v.Type() != js.TypeString {
		return ""
	}
	return v.String()
}

// Response is returned to the Worker runtime.
type Response struct {
	Status int
	Header map[string]string
	Body   []byte
}

// Handler serves one request. A returned error becomes a 500 response.
type Handler func(ctx context.Context, r *Request) (*Response, error)

// Serve registers h as the Worker's fetch handler and blocks forever,
// keeping the Go runtime alive between requests (so caches persist for
// the lifetime of the isolate). Call it at the end of main.
//
// Workers forbid one request from awaiting I/O started by another, and in
// Go's WebAssembly runtime a goroutine woken by another request's event
// runs in that request's I/O context. So handlers must not block on
// anything that a different request releases while it performs I/O (a
// mutex held across I/O, a channel fed by another request, ...). The ctx
// passed to h carries a per-request storage.Scope, which keeps the
// repository's shared caches from coalescing loads across requests.
func Serve(h Handler) {
	js.Global().Set(GlobalHandlerName, js.FuncOf(func(_ js.Value, args []js.Value) any {
		jsReq, env, execCtx := arg(args, 0), arg(args, 1), arg(args, 2)
		return newPromise(func() (any, error) {
			req, err := convertRequest(jsReq, env, execCtx)
			if err != nil {
				return toJSResponse(&Response{Status: 400, Body: []byte(err.Error())}), nil
			}
			// A scope per request: see storage.WithScope.
			resp, err := h(storage.WithScope(context.Background(), req), req)
			if err != nil {
				resp = &Response{Status: 500, Header: map[string]string{"content-type": "text/plain"}, Body: []byte(err.Error())}
			}
			return toJSResponse(resp), nil
		})
	}))
	select {}
}

func convertRequest(jsReq, env, execCtx js.Value) (*Request, error) {
	u, err := url.Parse(jsReq.Get("url").String())
	if err != nil {
		return nil, fmt.Errorf("invalid request URL: %w", err)
	}
	header := map[string]string{}
	cb := js.FuncOf(func(_ js.Value, args []js.Value) any {
		header[strings.ToLower(arg(args, 1).String())] = arg(args, 0).String()
		return nil
	})
	jsReq.Get("headers").Call("forEach", cb)
	cb.Release()
	return &Request{
		Method:  jsReq.Get("method").String(),
		URL:     u,
		Header:  header,
		Env:     Env{env},
		JS:      jsReq,
		execCtx: execCtx,
	}, nil
}

func toJSResponse(r *Response) js.Value {
	if r == nil {
		r = &Response{Status: 204}
	}
	if r.Status == 0 {
		r.Status = 200
	}
	init := jsObject.New()
	init.Set("status", r.Status)
	h := jsObject.New()
	for k, v := range r.Header {
		h.Set(k, v)
	}
	init.Set("headers", h)
	var body any = nil
	// Null-body statuses must not carry a body.
	if len(r.Body) > 0 && r.Status != 204 && r.Status != 304 {
		body = toUint8Array(r.Body)
	}
	return js.Global().Get("Response").New(body, init)
}
