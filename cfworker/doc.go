// Package cfworker runs the Icechunk reader inside Cloudflare Workers
// (GOOS=js GOARCH=wasm). It provides:
//
//   - NewR2: a storage.Storage backed by an R2 bucket binding, with ranged
//     and conditional reads;
//   - NewHTTP and NewS3: storages that use the Workers fetch API (plain
//     HTTP, or S3-compatible APIs signed with SigV4) instead of net/http,
//     which would roughly triple the size of the WebAssembly module;
//   - Serve: a minimal bridge from the Worker's fetch handler to Go.
//
// The JavaScript side (see examples/worker/worker.mjs) instantiates the Go
// module on the first request and forwards every request to the function
// Serve registers.
//
// The storages only need a js.Value for the binding, so they also work with
// other Go-on-Workers frameworks, e.g. syumai/workers'
// cloudflare.GetBinding(ctx, "BUCKET").
//
// Outside js/wasm builds this package is empty.
package cfworker
