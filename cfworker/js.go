//go:build js && wasm

package cfworker

import (
	"context"
	"errors"
	"fmt"
	"syscall/js"
)

var (
	jsObject     = js.Global().Get("Object")
	jsUint8Array = js.Global().Get("Uint8Array")
	jsPromise    = js.Global().Get("Promise")
	jsDate       = js.Global().Get("Date")
)

// Await blocks the calling goroutine until promise settles, returning its
// value or its rejection as an error. It must not be called from inside a
// js.FuncOf callback; call it from a goroutine.
func Await(ctx context.Context, promise js.Value) (js.Value, error) {
	type result struct {
		v   js.Value
		err error
	}
	ch := make(chan result, 1) // buffered: JS callbacks must never block
	var onOK, onErr js.Func
	onOK = js.FuncOf(func(_ js.Value, args []js.Value) any {
		ch <- result{v: arg(args, 0)}
		return nil
	})
	onErr = js.FuncOf(func(_ js.Value, args []js.Value) any {
		ch <- result{err: jsError(arg(args, 0))}
		return nil
	})
	promise.Call("then", onOK, onErr)
	release := func() { onOK.Release(); onErr.Release() }
	select {
	case r := <-ch:
		release()
		return r.v, r.err
	case <-ctx.Done():
		go func() { <-ch; release() }()
		return js.Undefined(), ctx.Err()
	}
}

func arg(args []js.Value, i int) js.Value {
	if i < len(args) {
		return args[i]
	}
	return js.Undefined()
}

// jsError converts a JS exception or rejection value into a Go error.
func jsError(v js.Value) error {
	if v.Type() == js.TypeObject {
		if msg := v.Get("message"); msg.Type() == js.TypeString {
			name := v.Get("name")
			if name.Type() == js.TypeString {
				return fmt.Errorf("%s: %s", name.String(), msg.String())
			}
			return errors.New(msg.String())
		}
	}
	return fmt.Errorf("javascript error: %s", js.Global().Get("String").Invoke(v).String())
}

// bytesOf copies an ArrayBuffer or typed array into Go memory.
func bytesOf(v js.Value) []byte {
	u8 := v
	if !v.InstanceOf(jsUint8Array) {
		u8 = jsUint8Array.New(v)
	}
	out := make([]byte, u8.Get("byteLength").Int())
	js.CopyBytesToGo(out, u8)
	return out
}

// toUint8Array copies Go bytes into a new JS Uint8Array.
func toUint8Array(b []byte) js.Value {
	u8 := jsUint8Array.New(len(b))
	js.CopyBytesToJS(u8, b)
	return u8
}

// newPromise returns a JS Promise settled by fn, which runs on a new
// goroutine so it may block on Await.
func newPromise(fn func() (any, error)) js.Value {
	var executor js.Func
	executor = js.FuncOf(func(_ js.Value, args []js.Value) any {
		resolve, reject := args[0], args[1]
		go func() {
			defer executor.Release()
			defer func() {
				if r := recover(); r != nil {
					reject.Invoke(js.Global().Get("Error").New(fmt.Sprint("go panic: ", r)))
				}
			}()
			v, err := fn()
			if err != nil {
				reject.Invoke(js.Global().Get("Error").New(err.Error()))
				return
			}
			resolve.Invoke(v)
		}()
		return nil
	})
	return jsPromise.New(executor)
}
