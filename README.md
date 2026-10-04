# icechunk-go

A pure-Go, read-only implementation of [Icechunk](https://icechunk.io), with a
Zarr v3 decoder. It runs natively, and in Cloudflare Workers (`GOOS=js
GOARCH=wasm`, ~1.8 MB gzipped).

It follows the split recommended for porting a storage format whose writer is
the hard part:

```
 icechunk-python / Rust  ──commits──▶  object storage  ◀──reads──  Go (this module)
 (writes, conflict                      (R2, S3, GCS,              native programs,
  resolution, GC, expiry)               HTTP, local disk)          Cloudflare Workers
```

Writing (optimistic commits, rebase, manifest splitting, garbage collection,
expiration) stays with the official library; see
[`examples/python-writer`](examples/python-writer). The Go side reads
everything those writers produce.

## Features

- **Spec versions 1, 2 and 2.1**, including repositories migrated from v1 to
  v2, manifest splitting, expired repositories, and the v1 `refs/` layout.
- **Inline, native and virtual chunks.** This includes dictionary-compressed
  virtual chunk locations, and ETag / last-modified checksums enforced as
  conditional reads.
- **Branches, tags (deleted tags hidden), snapshot ids, ancestry, commit and
  repository metadata** (MessagePack in v1, FlexBuffers in v2), plus repo
  config, status and the ops log.
- **Zarr v3 arrays**: region reads into N-d results, typed accessors and a
  Zarr key/value view (`Store`) using upstream's key layout.
  - Data types: `bool`, `int8`–`int64`, `uint8`–`uint64`, `float16/32/64`,
    `complex64/128`, `string` (vlen-utf8), `bytes`, `numpy.datetime64/timedelta64`,
    `rN`, fixed-length UTF-32 and bytes.
  - Codecs: `bytes` (both endians), `transpose`, `sharding_indexed` (index at
    start or end, with ranged reads of individual inner chunks), `zstd`,
    `gzip`, `blosc` (blosclz, lz4, lz4hc, zlib, zstd; shuffle and bitshuffle),
    `crc32c`, `vlen-utf8`, `vlen-bytes`, and the `numcodecs.*` zlib, gzip, bz2,
    lz4, zstd, blosc, shuffle, crc32, crc32c, adler32, fletcher32 and bitround.
    `zarr.RegisterBytesCodec` adds more.
  - Chunk grids: regular and rectilinear.
- **Storage backends**: local files and memory (`storage`); plain HTTP(S) and
  S3-compatible APIs with SigV4 for AWS, R2, MinIO, GCS interop and Tigris
  (`storage/httpstore`); R2 bindings and the Workers `fetch` API
  (`cfworker`).
- **An HTTP service** (`serve`): JSON API plus a read-only Zarr store, so
  zarr-python, zarrita.js and similar clients can read a repository through a
  Worker.

## Library usage

```go
import (
	icechunk "github.com/solomonsealed/icechunk-go"
	"github.com/solomonsealed/icechunk-go/storage"
	"github.com/solomonsealed/icechunk-go/storage/httpstore"
	"github.com/solomonsealed/icechunk-go/zarr"
)

st := httpstore.NewS3(storage.S3Config{
	Bucket: "my-bucket", Prefix: "repos/weather", Region: "us-east-1",
	AccessKeyID: os.Getenv("AWS_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
}, nil)
repo, err := icechunk.Open(ctx, st, &icechunk.Options{
	// Virtual chunks are only read from explicitly mapped locations.
	VirtualChunkContainers: map[string]storage.Storage{
		"s3://source-bucket/": httpstore.NewS3(storage.S3Config{Bucket: "source-bucket", Anonymous: true}, nil),
	},
})
session, err := repo.ReadonlySession(ctx, icechunk.AtBranch("main")) // or AtTag, AtSnapshot

arr, err := session.OpenArray(ctx, "temperature")
nd, err := arr.Read(ctx, []uint64{0, 0, 0}, []uint64{1, 90, 180}) // start, count
temps, err := zarr.Convert[float32](nd)                          // or nd.Values(), nd.Float64s()

for si, err := range repo.Ancestry(ctx, icechunk.AtBranch("main")) { /* commits */ }
raw, err := session.Store().Get(ctx, "temperature/c/0/0/0")      // Zarr key/value view
```

`Repository` is safe for concurrent use and meant to be long-lived. It caches
decoded snapshots and manifests (256 MiB by default). Manifests are searched
in place in their flatbuffer, never expanded into per-ref Go structs, which
avoids the memory blowup that per-ref objects cause on large manifests.

## Cloudflare Workers

[`examples/worker`](examples/worker) is a complete Worker in Go that serves a
repository from an R2 bucket. It has been run under `wrangler dev` (workerd
plus local R2), where zarr-python, reading every fixture array through it,
matched icechunk-python reading the same repository directly. Warm requests
take a few milliseconds. To use the reader in your own Worker:

```go
cfworker.Serve(func(ctx context.Context, r *cfworker.Request) (*cfworker.Response, error) {
	st := cfworker.NewR2(r.Env.Binding("REPO_BUCKET"), "my-repo") // or NewHTTP, NewS3 (fetch-based)
	...
})
```

Read the [Worker README](examples/worker/README.md) for the one rule Go
handlers in Workers must follow: never wait on I/O another request started.

## CLI

```sh
go install github.com/solomonsealed/icechunk-go/cmd/icechunk-go@latest
icechunk-go info  s3://bucket/prefix
icechunk-go log   -ref main ./repo
icechunk-go ls    ./repo
icechunk-go read  -slice "0, 10:20, :" ./repo temperature
icechunk-go cat   ./repo temperature/zarr.json
icechunk-go serve -addr :8080 ./repo        # same API as the Worker
```

## Limitations

- Read-only by design. Writes go through icechunk-python or the Rust crate.
- Transaction logs (diffs, conflict detection) are not read; they are not
  needed for reading data.
- Ops-log entries beyond the current repo info file (`repo_before_updates`)
  are not followed.
- GCS and Azure are reachable only through S3-compatible (HMAC) or plain
  HTTP access, not their native auth.
- Not decoded: blosc snappy, `numcodecs.lzma`, numcodecs array filters other
  than bitround (delta, fixedscaleoffset, …), Zarr v2 metadata, and storage
  transformers.
- The JSON API (`serve`) encodes NaN/±Inf as `null`. The `/zarr/` endpoint
  and `format=binary` return exact bytes.

Upstream (Earthmover) recommends binding to the Rust library over
reimplementing Icechunk. This module is an independent reader, kept honest by
the tests below. Their README invites people implementing Icechunk support to
open an issue, which is worth doing for anything long-lived.

## How it is built and tested

- `internal/fbs` is generated by `flatc` 25.12.19 (the version upstream uses)
  from verbatim copies of upstream's `.fbs` schemas. flatc's Go backend cannot
  emit fixed-size arrays, so `gen.sh` rewrites the two object-id structs into a
  layout-identical form first.
- `testdata/upstream`: upstream's own on-disk compatibility fixtures. The tests
  assert the same facts as upstream's `test_can_read_old.py`.
- `testdata/generated`: repositories written by icechunk-python 2.2.2 via
  `testdata/generate.py` (every data type and codec above, sharding,
  rectilinear grids, 2000 dictionary-compressed virtual chunks, checksums,
  spec v1 and v2). Go must reproduce the SHA-256 of every array's bytes.
- Corrupted flatbuffers (thousands of random mutations) must produce errors,
  never panics or runaway allocations.
- `testdata/check_http.py` compares zarr-python reading through the HTTP
  service (native or the Worker) with icechunk-python reading the repository
  directly.

```sh
go test ./...
python testdata/generate.py                    # regenerate fixtures (icechunk, zarr, numpy)
python testdata/check_http.py testdata/generated/codecs-v2 http://localhost:8787
```
