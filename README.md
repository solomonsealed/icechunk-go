# icechunk-go

A pure-Go implementation of [Icechunk](https://icechunk.io), reader and
writer, with a Zarr v3 codec layer. It runs natively, and in Cloudflare
Workers (`GOOS=js GOARCH=wasm`, ~2.3 MB gzipped).

Go and icechunk-python/Rust can work on the same repositories: the Go writer
produces files upstream reads and verifies, and it commits on top of
upstream-written history (both directions are tested). Repository
maintenance (garbage collection, expiration, configuration, migrations from
spec v1) stays with the official library; see
[`examples/python-writer`](examples/python-writer).

## Features

- **Spec versions 1, 2 and 2.1**, including repositories migrated from v1 to
  v2, manifest splitting, expired repositories, and the v1 `refs/` layout.
- **Inline, native and virtual chunks.** This includes dictionary-compressed
  virtual chunk locations, and ETag / last-modified checksums enforced as
  conditional reads.
- **Branches, tags (deleted tags hidden), snapshot ids, ancestry, commit and
  repository metadata** (MessagePack in v1, FlexBuffers in v2), plus repo
  config, status and the whole ops log (`Repository.OpsLog` follows it into
  older repo info files). Spec v1 refs are found under the percent-encoded
  keys upstream stores them at, and `zarr.json` attributes keep integers
  exact, as in Python.
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
  Worker, and optional token-protected endpoints that create and write arrays.

### Writing (spec v2)

- `Create` a repository, open a `WritableSession` on a branch, create groups
  and arrays (from a `zarr.ArraySpec` or a raw `zarr.json`), write chunks,
  virtual references and deletions, then `Commit` with metadata. Reads in a
  writable session see its uncommitted changes.
- Commits follow upstream's protocol: chunks, manifests, transaction log and
  snapshot are written first, then the repo info file is replaced with a
  conditional write (after backing it up to `overwritten/`), retrying when
  another writer got there first. With `CommitOptions{Rebase: true}` a
  commit whose branch moved is re-applied on the new tip unless the
  intervening transaction logs touched the same nodes or chunks. Like
  upstream, such commits record the rebases they took in their metadata
  (`"__icechunk": {"rebase_attempts": n}`).
- Commit metadata is stored as JSON values (upstream parses it as JSON):
  structs become objects and `[]byte` a base64 string. Writes are refused
  when the local clock is more than 10 minutes off the object store's, as
  upstream does, since future timestamps would block other writers.
- Branches and tags: create, reset and delete, honouring the repository's
  status and the `create_tag` / `delete_tag` feature flags. The ops log and
  everything else in the repo info file are carried over byte-for-byte.
- Manifests follow the repository's `manifest.splitting` config as upstream
  writes them: one manifest per split holding chunks, with the bounding box
  of its chunks as extents, and a commit rewrites only the splits it
  touched.
- `zarr.Array.Write` writes regions (partly covered chunks are
  read-modified-written; chunks holding only the fill value are deleted, as
  zarr-python does), with encoders for every codec above except bz2, lzma
  and blosclz/snappy (blosc frames for those are written with lz4).
- Writable storage (`storage.Writer`): memory, local files (atomic renames,
  conditional writes serialized with a file lock), S3-compatible APIs
  (`If-Match` / `If-None-Match`), R2 bindings and fetch-based S3 in Workers.

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

Writing:

```go
repo, err := icechunk.Create(ctx, storage.NewLocal("./weather"), nil) // or Open an existing one
s, err := repo.WritableSession(ctx, "main")
arr, err := s.CreateArray(ctx, "/temperature", zarr.ArraySpec{
	Shape: []uint64{365, 90, 180}, ChunkShape: []uint64{1, 90, 180},
	DataType: "float32", FillValue: math.NaN(),
	DimensionNames: []string{"time", "lat", "lon"},
	Attributes: map[string]any{"units": "degC"},
})                                                                   // default codecs: bytes + zstd
day, err := zarr.FromSlice([]uint64{1, 90, 180}, values)             // values []float32
err = arr.Write(ctx, []uint64{0, 0, 0}, day)
id, err := s.Commit(ctx, "add day 0", &icechunk.CommitOptions{Rebase: true})
err = repo.CreateTag(ctx, "v1", id)
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

The same Worker can also write (`WRITE_TOKEN`, `CREATE_IF_MISSING`): in
`wrangler dev` it created a repository in R2 and committed concurrent writes
from parallel requests, and icechunk-python read the result. Read the
[Worker README](examples/worker/README.md) for the one rule Go handlers in
Workers must follow: never wait on I/O another request started.

## CLI

```sh
go install github.com/solomonsealed/icechunk-go/cmd/icechunk-go@latest
icechunk-go info  s3://bucket/prefix
icechunk-go log   -ref main ./repo
icechunk-go ls    ./repo
icechunk-go read  -slice "0, 10:20, :" ./repo temperature
icechunk-go cat   ./repo temperature/zarr.json
icechunk-go serve -addr :8080 ./repo        # same API as the Worker (-write-token T enables writes)
icechunk-go create ./new-repo
icechunk-go branch ./repo dev main          # -delete to delete
icechunk-go tag    ./repo v1 main
```

## Limitations

- The writer handles spec v2 only (upgrade v1 repositories with
  icechunk-python). It does not compress virtual chunk locations, amend
  commits, move nodes, change configuration, or run garbage collection and
  expiration. Virtual references are not checked against the
  repository's configured virtual chunk containers.
- Rebase conflict detection is coarse: any commit that touched the same
  chunk, or changed or deleted the same node, conflicts.
- Diffs between snapshots are not exposed.
- GCS and Azure are reachable only through S3-compatible (HMAC) or plain
  HTTP access, not their native auth.
- Not decoded: blosc snappy, `numcodecs.lzma`, numcodecs array filters other
  than bitround (delta, fixedscaleoffset, …), Zarr v2 metadata, and storage
  transformers.
- The JSON API (`serve`) encodes NaN/±Inf as `null`. The `/zarr/` endpoint
  and `format=binary` return exact bytes.

Upstream (Earthmover) recommends binding to the Rust library over
reimplementing Icechunk. This module is an independent implementation, kept
honest by the tests below. Their README invites people implementing Icechunk support to
open an issue, which is worth doing for anything long-lived.

## How it is built and tested

- `internal/fbs` is generated by `flatc` 25.12.19 (the version upstream uses)
  from verbatim copies of upstream's `.fbs` schemas. flatc's Go backend cannot
  emit fixed-size arrays, so `gen.sh` rewrites the two object-id structs into a
  layout-identical form first.
- The tests that compare Go with upstream and icechunk-python, described
  below, live in `internal/conformance`; the rest sit next to the code they
  test.
- `testdata/upstream`: upstream's own on-disk compatibility fixtures. The tests
  assert the same facts as upstream's `test_can_read_old.py`.
- `testdata/generated`: repositories written by icechunk-python 2.2.2 via
  `testdata/generate.py` (every data type and codec above, sharding,
  rectilinear grids, 2000 dictionary-compressed virtual chunks, checksums,
  spec v1 and v2, and in `features-*` deleted, reset, amended, detached and
  unicode/slash-named refs, moves, resizes, deletions, repo metadata, status,
  flags and a multi-file ops log). Go must reproduce the SHA-256 of every
  array's bytes.
- Consistency with icechunk-python: `testdata/oracle/oracle.py` records what
  icechunk-python reads from every fixture above (refs, ancestry, repo info,
  snapshots, manifests, every store key and byte range, listings, malformed
  keys, chunk refs, region and per-chunk reads, and virtual chunks with ETag
  and Last-Modified checks against a local S3 endpoint). The `TestPython*`
  tests require the Go reader and the HTTP service to read the same.
  Deliberate differences are listed in `acceptedDivergence`, and ones not yet
  fixed in `knownInconsistencies` (these fail with `ICECHUNK_STRICT=1`).
- Corrupted flatbuffers (thousands of random mutations) must produce errors,
  never panics or runaway allocations.
- `testdata/check_http.py` compares zarr-python reading through the HTTP
  service (native or the Worker) with icechunk-python reading the repository
  directly.
- Writer: every repo info file in the fixtures survives a parse/encode round
  trip unchanged; concurrent writers (memory, local files, fake S3, R2 in
  `wrangler dev`) lose no commits; `testdata/check_go_writer.py` has
  icechunk-python verify a Go-written repository (history, metadata, refs,
  diffs from Go transaction logs, arrays in 16 codec configurations) and
  commit on top of it; `testdata/check_go_on_python.py` verifies Go commits
  on Python-written repositories (split manifests, expired history,
  sharding).
- Writer consistency with icechunk-python: `testdata/writer/scenarios.json`
  lists scenarios (arrays of every writable type and codec, partial and
  fill-value writes, zarr.json rewrites, deletions, virtual refs, refs and
  their refusals, concurrent sessions with and without rebase, a
  non-default repository config, tricky node names, and single-change
  commits for the HTTP write endpoints). `TestWriterScenarios` runs each
  with the Go writer and compares every step's outcome and a canonical
  description of the result (values, chunk bytes for deterministic codecs,
  inline/native placement, keys, manifest extents, history, refs, ops log)
  with icechunk-python's run of the same steps (`testdata/writer/expected`).
  With `ICECHUNK_PYTHON` set, `TestWriterScenariosLive` also has
  icechunk-python read every Go-written repository, transaction logs and
  the whole ops log included, and interleaves the Go library, the HTTP
  endpoints and icechunk-python on one repository, session by session.

```sh
go test ./...
python testdata/generate.py                    # regenerate fixtures (icechunk, zarr, numpy)
python testdata/oracle/oracle.py               # re-record icechunk-python's answers for TestPython*
ICECHUNK_PYTHON=.venv/bin/python go test -run Python ./internal/conformance   # compare with a live icechunk-python
python testdata/writer/scenario.py record      # re-record icechunk-python's writer scenario runs
ICECHUNK_PYTHON=.venv/bin/python go test -run WriterScenarios ./internal/conformance   # interleave with a live icechunk-python
python testdata/check_http.py testdata/generated/codecs-v2 http://localhost:8787
ICECHUNK_GO_WRITE_DIR=/tmp/gw go test -run 'TestWriteForPython|TestWriteOnPythonRepos' ./internal/conformance
python testdata/check_go_writer.py /tmp/gw && python testdata/check_go_on_python.py /tmp/gw
```
