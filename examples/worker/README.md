# Icechunk in a Cloudflare Worker, in Go

This Worker serves an Icechunk repository stored in R2 (or behind any HTTP
endpoint) using [`serve`](../../serve): a JSON API and a read-only Zarr v3
store that any Zarr client can read.

| Route | Returns |
| --- | --- |
| `GET /` | spec version, branches and tags |
| `GET /log?ref=main&limit=50` | commit history with metadata |
| `GET /nodes?ref=main` | arrays and groups: shape, dtype, chunking, attributes |
| `GET /array/<path>?ref=main&slice=0:10,5` | values as JSON (`null` for NaN/Inf); `&format=binary` for raw little-endian bytes with `x-shape`/`x-dtype` headers |
| `GET /chunks/<path>?ref=main` | chunk references (inline / native / virtual) |
| `GET /zarr/<ref>/<key>` | Zarr v3 key/value store with `Range` support; `<ref>` is a branch, tag or snapshot id |

```python
import zarr
temperature = zarr.open_array("https://<worker>/zarr/main/temperature", mode="r")
```

## Build and run locally

Needs Go ≥ 1.25 and Node (for `npx wrangler`).

```sh
./build.sh                                   # → build/app.wasm (~1.8 MB gzipped) + build/wasm_exec.js
./seed-r2.sh ../../testdata/upstream/test-repo-v2 test-repo-v2   # fill wrangler's local R2
npx wrangler dev                             # http://localhost:8787
curl 'localhost:8787/array/group1/small_chunks?ref=my-branch'
```

`npx wrangler dev --env http` reads the repository over HTTP instead
(`REPO_URL` in `wrangler.toml`).

## Deploy

```sh
npx wrangler r2 bucket create icechunk-repos
# write a repository with icechunk-python straight into R2:
python ../python-writer/write_repo.py --r2-bucket icechunk-repos --prefix demo
# set REPO_PREFIX = "demo" in wrangler.toml, then
./build.sh && npx wrangler deploy
```

## Configuration

| Variable / binding | Meaning |
| --- | --- |
| `REPO_BUCKET` (R2 binding) | bucket holding the repository |
| `REPO_PREFIX` | key prefix of the repository in the bucket (`/` for legacy bucket-root repositories) |
| `REPO_URL` | read over HTTP(S) instead of an R2 binding; or `s3://bucket/prefix` for S3-compatible APIs, signed with the `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` secrets (`wrangler secret put`), with optional `S3_ENDPOINT` / `S3_REGION` |
| `DEFAULT_REF` | ref used when a request names none (default `main`) |
| `REPO_INFO_TTL` | how long branch/tag lookups are cached per isolate (default `10s`) |
| `CACHE_MB` | decoded snapshot/manifest cache per isolate, in MiB (default `32`; isolates have 128 MB) |
| `VIRTUAL_CONTAINERS` | JSON object mapping virtual chunk URL prefixes to `https://…` base URLs or `r2:<BINDING>[/prefix]` |

## How it works

- `worker.mjs` instantiates the Go module on the first request (the Go
  runtime needs randomness and timers, which Workers only allow while handling
  a request) and forwards every request to the function `cfworker.Serve`
  registered. The Go runtime then lives as long as the isolate, so the
  repository handle and its cache of decoded snapshots and manifests are
  reused across requests.
- Storage goes through the R2 binding (`bucket.get` with `range` and
  `onlyIf`) or `fetch`. `net/http` is not linked, which keeps the module at
  ~1.8 MB gzipped (with `net/http` it would be ~3 MB+, over the free-plan
  limit).
- Workers forbid a request from awaiting I/O started by another request, and
  in Go's WebAssembly runtime a goroutine woken by another request's event runs
  in that request's context. `cfworker.Serve` therefore gives every request its
  own `storage.Scope`, so concurrent requests never wait on each other's
  downloads, and the example opens the repository without a shared lock. Keep
  that rule in your own handlers: never block on something another request
  releases while doing I/O.

## Sizing

Manifests are kept decoded but not expanded: a manifest holding 500,000
virtual chunk refs (4 MB on disk) costs about 43 MB of Go heap, with ~2 µs
lookups. Manifests larger than `CACHE_MB` are not cached and are fetched
again by every request that needs them. For large arrays served from a
Worker, use Icechunk's manifest splitting (`ManifestSplittingConfig` in
icechunk-python) so each request only loads the manifests covering the
chunks it reads.
