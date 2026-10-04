"""Record what icechunk-python reads from the fixture repositories.

consistency_test.go reads the same repositories with the Go reader and
requires the same answers. Run with icechunk, zarr and numpy installed (the
versions recorded in each file's "_meta" reproduce the checked-in files):

    python testdata/oracle/oracle.py                     # every fixture
    python testdata/oracle/oracle.py codecs-v2 virtual-v1
    python testdata/oracle/oracle.py --out DIR           # write elsewhere

Each fixture gets testdata/oracle/<fixture>.json.gz (compact JSON; pass
--pretty for indented, uncompressed .json files to read) with:

  repo       spec version, branches, tags, ref lookups that fail, the
             ancestry of every branch, tag and snapshot, every snapshot's
             commit info, and (spec v2) the repo info file: status, metadata,
             feature flags, ops log, stored config.
  manifests  per manifest file, chunk ref counts by kind and array.
  snapshots  per snapshot: file header, manifest files, nodes (ids, shapes,
             dimension names, manifest extents) and the Zarr store view:
             every key, every metadata key's bytes and byte ranges, probes
             for missing and malformed keys, list_dir and list_prefix.
  arrays     per array version (node id + zarr.json + manifest refs, so an
             array unchanged across snapshots appears once): Zarr metadata,
             chunk refs, chunk_type probes, every chunk key's bytes (and some
             byte ranges), region reads and per-chunk reads.

Bytes and array values are recorded as digests: the first 16 hex digits of a
sha256. Arrays of fixed-size types hash their little-endian C-order bytes;
variable-length strings and bytes hash each element as an 8-byte
little-endian length followed by its bytes. Failures are recorded as
"error:<exception class>", missing keys as "absent".

Virtual chunks are served by a local, anonymous, path-style S3 endpoint
(ObjectServer), so that checksum semantics (ETag and Last-Modified
preconditions) are exercised for real. The Go test runs an equivalent server.
"""

from __future__ import annotations

import argparse
import base64
import email.utils
import gzip
import hashlib
import itertools
import json
import math
import platform
import random
import sys
import threading
from datetime import timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit

import numpy as np

import icechunk as ic
import zarr
from zarr.abc.store import OffsetByteRequest, RangeByteRequest, SuffixByteRequest
from zarr.core.buffer import default_buffer_prototype
from zarr.core.sync import sync

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "testdata" / "oracle"

zarr.config.set({"array.rectilinear_chunks": True})

# Upstream's compatibility repositories point one virtual chunk at
# s3://testbucket/can_read_old/chunk-1, which upstream's own tests fill with
# a copy of a native chunk of the same array.
UPSTREAM_VIRTUAL = {"s3://testbucket/": {"copy": {"can_read_old/chunk-1": ("main", "group1/big_chunks/c/0/1")}}}
GENERATED_VIRTUAL = {"s3://icechunk-go-fixtures/virtual/": {"dir": "testdata/generated/virtual-data"}}

FIXTURES = {
    "test-repo-v1": ("testdata/upstream/test-repo-v1", UPSTREAM_VIRTUAL),
    "test-repo-v2": ("testdata/upstream/test-repo-v2", UPSTREAM_VIRTUAL),
    "test-repo-v2-migrated": ("testdata/upstream/test-repo-v2-migrated", UPSTREAM_VIRTUAL),
    "split-repo-v1": ("testdata/upstream/split-repo-v1", {}),
    "split-repo-v2": ("testdata/upstream/split-repo-v2", {}),
    "split-repo-v2-migrated": ("testdata/upstream/split-repo-v2-migrated", {}),
    "expire-repo-v1-by-2.0.5": ("testdata/upstream/expire-repo-v1-by-2.0.5", {}),
    "expire-repo-v2-by-working-copy": ("testdata/upstream/expire-repo-v2-by-working-copy", {}),
    "codecs-v1": ("testdata/generated/codecs-v1", {}),
    "codecs-v2": ("testdata/generated/codecs-v2", {}),
    "virtual-v1": ("testdata/generated/virtual-v1", GENERATED_VIRTUAL),
    "virtual-v2": ("testdata/generated/virtual-v2", GENERATED_VIRTUAL),
    "features-v1": ("testdata/generated/features-v1", {}),
    "features-v2": ("testdata/generated/features-v2", {}),
}

# Objects served without a backing file report this modification time.
OBJECT_MTIME = 1704067200  # 2024-01-01T00:00:00Z
# A well-formed snapshot id that no repository contains.
MISSING_SNAPSHOT = "ZZZZZZZZZZZZZZZZZZZ0"
CHUNK_TYPES = {0: "uninitialized", 1: "native", 2: "virtual", 3: "inline"}
MAX_CHUNK_READS = 24
MAX_RANGED_CHUNK_KEYS = 4
VALUES_LIMIT = 16
LISTING_LIMIT = 32

PROTO = default_buffer_prototype()


# ---------------------------------------------------------------------------
# Encoding helpers


def digest(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()[:16]


def err(e: BaseException) -> str:
    return f"error:{type(e).__name__}"


def iso(dt) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def jsonable(v):
    """Commit and repo metadata, attributes: plain JSON values."""
    if isinstance(v, dict):
        return {str(k): jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [jsonable(x) for x in v]
    if isinstance(v, float) and not math.isfinite(v):
        return repr(v)
    if isinstance(v, (bytes, bytearray)):
        return {"bytes": bytes(v).hex()}
    if hasattr(v, "isoformat"):
        return iso(v)
    return v


def is_variable(dtype: np.dtype) -> bool:
    return dtype.kind in "OT"


def digest_array(a: np.ndarray) -> str:
    a = np.asarray(a)
    if is_variable(a.dtype):
        h = hashlib.sha256()
        for x in a.reshape(-1):
            b = x.encode("utf-8") if isinstance(x, str) else bytes(x)
            h.update(len(b).to_bytes(8, "little"))
            h.update(b)
        return h.hexdigest()[:16]
    if a.dtype.byteorder == ">" or (a.dtype.byteorder == "=" and sys.byteorder == "big"):
        a = a.astype(a.dtype.newbyteorder("<"))
    return digest(np.ascontiguousarray(a).tobytes())


def json_values(a: np.ndarray) -> list:
    """Human-readable values, only for diagnosing digest mismatches."""
    out = []
    for x in np.asarray(a).reshape(-1):
        if isinstance(x, (bytes, np.bytes_, np.void)):
            out.append("0x" + bytes(x).hex())
        elif isinstance(x, (str, np.str_)):
            out.append(str(x))
        elif np.asarray(x).dtype.kind in "mM":
            out.append(int(np.asarray(x).view("i8")))
        elif isinstance(x, (complex, np.complexfloating)):
            out.append([repr(float(x.real)), repr(float(x.imag))])
        elif isinstance(x, (float, np.floating)):
            out.append(repr(float(x)))
        elif isinstance(x, (bool, np.bool_)):
            out.append(bool(x))
        else:
            out.append(int(x))
    return out


def collect(aiter):
    async def go():
        return [x async for x in aiter]
    return sync(go())


def outcome(fn):
    """The value fn() returns, or "error:<class>"."""
    try:
        return fn()
    except BaseException as e:  # icechunk turns Rust panics into PanicException (a BaseException)
        if isinstance(e, KeyboardInterrupt):
            raise
        return err(e)


# ---------------------------------------------------------------------------
# Virtual chunk objects


class ObjectServer:
    """A minimal path-style, anonymous S3 endpoint for virtual chunks: GET
    /<bucket>/<key> with Range, If-Match and If-Unmodified-Since. ETags are
    quoted MD5s of the object; Last-Modified is the backing file's mtime
    (OBJECT_MTIME for objects without a file). Precondition failures answer
    412. consistency_test.go implements the same server in Go."""

    def __init__(self, containers: dict):
        # prefix "s3://bucket/sub/" -> (bucket, "sub/", source)
        self.routes = []
        for prefix, source in containers.items():
            u = urlsplit(prefix)
            self.routes.append((u.netloc, u.path.lstrip("/"), source))
        server = self

        class Handler(BaseHTTPRequestHandler):
            # Keep-alive: tens of thousands of reads must not exhaust ports.
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def do_GET(self):
                server.handle(self)

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.httpd.daemon_threads = True
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.httpd.server_port}"

    def close(self):
        self.httpd.shutdown()

    def lookup(self, bucket: str, key: str):
        for b, base, source in self.routes:
            if b != bucket or not key.startswith(base):
                continue
            rest = key[len(base):]
            if "dir" in source:
                p = ROOT / source["dir"] / rest
                if p.is_file():
                    return p.read_bytes(), int(p.stat().st_mtime)
            elif rest in source.get("objects", {}):
                return source["objects"][rest], OBJECT_MTIME
        return None, None

    def handle(self, h: BaseHTTPRequestHandler):
        path = unquote(urlsplit(h.path).path)
        bucket, _, key = path.lstrip("/").partition("/")
        data, mtime = self.lookup(bucket, key)
        if data is None:
            h.send_response(404)
            h.send_header("Content-Length", "0")
            h.end_headers()
            return
        etag = '"' + hashlib.md5(data).hexdigest() + '"'
        im = h.headers.get("If-Match")
        ius = h.headers.get("If-Unmodified-Since")
        if (im is not None and im.strip().strip('"') != etag.strip('"')) or \
                (ius is not None and mtime > email.utils.parsedate_to_datetime(ius).timestamp()):
            h.send_response(412)
            h.send_header("Content-Length", "0")
            h.end_headers()
            return
        status, body = 200, data
        rng = h.headers.get("Range")
        if rng:
            a, _, b = rng.removeprefix("bytes=").partition("-")
            start, end = int(a), (int(b) if b else len(data) - 1)
            body, status = data[start:end + 1], 206
        h.send_response(status)
        h.send_header("ETag", etag)
        h.send_header("Last-Modified", email.utils.formatdate(mtime, usegmt=True))
        h.send_header("Content-Length", str(len(body)))
        if status == 206:
            h.send_header("Content-Range", f"bytes {start}-{start + len(body) - 1}/{len(data)}")
        h.end_headers()
        h.wfile.write(body)


def resolve_virtual(spec: dict, rel: str) -> dict:
    """Turn the fixture's virtual spec into served sources ("dir" or "objects")."""
    out = {}
    for prefix, source in spec.items():
        if "dir" in source:
            out[prefix] = {"dir": source["dir"]}
            continue
        repo = ic.Repository.open(ic.local_filesystem_storage(str(ROOT / rel)))
        objects = {}
        for key, (branch, store_key) in source["copy"].items():
            store = repo.readonly_session(branch=branch).store
            objects[key] = sync(store.get(store_key, PROTO)).to_bytes()
        out[prefix] = {"objects": objects}
    return out


# ---------------------------------------------------------------------------
# Repository level


def snapshot_info(si) -> dict:
    return {
        "parent_id": si.parent_id,
        "written_at": iso(si.written_at),
        "message": si.message,
        "metadata": jsonable(si.metadata),
    }


def normalize_update(u: dict) -> dict:
    """An inspect_repo_info() update, in the shape of the Go Update struct."""
    t = dict(u["update_type"])
    kind = t.pop("type")
    kind = kind.removesuffix("Update")
    name = t.pop("name", None)
    branch = t.pop("branch", None)
    return {
        "kind": kind,
        "name": name if name is not None else branch,
        "snapshot_id": t.pop("new_snap_id", None),
        "previous_snapshot_id": t.pop("previous_snap_id", None),
        "updated_at": u["updated_at"],
        "backup_path": u.get("backup_path"),
        # Fields the Go Update struct does not carry (flag ids, statuses, ...).
        "other": jsonable(t),
    }


def normalize_op(u) -> dict:
    """A repo.ops_log() entry, in the same shape as normalize_update."""
    k = u.kind
    name = getattr(k, "name", None)
    if name is None:
        name = getattr(k, "branch", None)
    return {
        "kind": type(k).__name__,
        "name": name,
        "snapshot_id": getattr(k, "new_snap_id", None),
        "previous_snapshot_id": getattr(k, "previous_snap_id", None),
        "updated_at": iso(u.updated_at),
        "backup_path": u.backup_path,
    }


def store_kind(store) -> str:
    """ObjectStoreConfig variant name in the stored (serde) form, e.g. "s3_compatible"."""
    name = type(store).__name__
    out = ""
    for i, ch in enumerate(name):
        if ch.isupper() and i > 0 and not name[i - 1].isupper():
            out += "_"
        out += ch.lower()
    return out


def describe_config(cfg) -> dict | None:
    """The stored config fields the Go reader exposes (Repository.Config), by
    their serde paths; None when unset."""
    if cfg is None:
        return None
    m = cfg.manifest
    vclc = m.virtual_chunk_location_compression if m is not None else None
    comp = cfg.compression
    return {
        "inline_chunk_threshold_bytes": cfg.inline_chunk_threshold_bytes,
        "get_partial_values_concurrency": cfg.get_partial_values_concurrency,
        "num_updates_per_repo_info_file": cfg.num_updates_per_repo_info_file,
        "compression.level": comp.level if comp is not None else None,
        "manifest.virtual_chunk_location_compression.min_num_chunks": vclc.min_num_chunks if vclc is not None else None,
        "manifest.splitting": m is not None and m.splitting is not None,
        "virtual_chunk_containers": {
            prefix: {"url_prefix": c.url_prefix, "name": c.name, "store": store_kind(c.store)}
            for prefix, c in sorted((cfg.virtual_chunk_containers or {}).items())
        },
    }


def describe_repo(repo, spec: int, stored_config) -> tuple[dict, list[str]]:
    out: dict = {"spec_version": spec}
    # Listed refs map to their snapshot, or to the error looking them up
    # raises: in spec v1, upstream lists the storage key form of names with
    # characters it percent-encodes (e.g. "%C3%BC" for "ü"), which it then
    # cannot look up.
    out["branches"] = {b: outcome(lambda: repo.lookup_branch(b)) for b in sorted(repo.list_branches())}
    out["tags"] = {t: outcome(lambda: repo.lookup_tag(t)) for t in sorted(repo.list_tags())}

    lookups = {}
    for name in ["does-not-exist", "deleted", "to-delete", "temp", "with/slash", "main", "", "ünïcode", "with space",
                 "feature/nested/x", "rel/1.0", "%C3%BCn%C3%AFcode"]:
        lookups[f"branch:{name}"] = outcome(lambda: repo.lookup_branch(name))
        lookups[f"tag:{name}"] = outcome(lambda: repo.lookup_tag(name))
    lookups[f"snapshot:{MISSING_SNAPSHOT}"] = outcome(lambda: repo.lookup_snapshot(MISSING_SNAPSHOT).id)
    lookups[f"ancestry:{MISSING_SNAPSHOT}"] = outcome(lambda: [s.id for s in repo.ancestry(snapshot_id=MISSING_SNAPSHOT)])
    out["lookups"] = lookups

    infos: dict = {}
    ancestry: dict = {}

    def walk(label, **kw):
        ids = []
        for si in repo.ancestry(**kw):
            ids.append(si.id)
            infos[si.id] = snapshot_info(si)
        ancestry[label] = ids

    for b, sid in out["branches"].items():
        if not sid.startswith("error:"):
            walk(f"branch:{b}", branch=b)
    for t, sid in out["tags"].items():
        if not sid.startswith("error:"):
            walk(f"tag:{t}", tag=t)

    info = outcome(repo.inspect_repo_info)
    if isinstance(info, dict):
        snapshot_ids = [s["id"] for s in info["snapshots"]]
    else:
        snapshot_ids = sorted(infos)  # spec v1: everything reachable from a ref
    for sid in snapshot_ids:
        walk(f"snapshot:{sid}", snapshot_id=sid)
    out["ancestry"] = ancestry
    out["snapshot_infos"] = {sid: snapshot_info(repo.lookup_snapshot(sid)) for sid in sorted(set(snapshot_ids) | set(infos))}

    if isinstance(info, dict):
        status = repo.get_status()
        ri = {
            "written_by": info["header"]["written_by"],
            "spec_version": info["spec_version"],
            "branches": info["branches"],
            "tags": info["tags"],
            "deleted_tags": info.get("deleted_tags", []),
            "snapshots": [
                {"id": s["id"], "parent_id": s.get("parent_id"), "flushed_at": s["flushed_at"],
                 "message": s["message"], "metadata": jsonable(s.get("metadata", {}))}
                for s in info["snapshots"]
            ],
            "latest_updates": [normalize_update(u) for u in info.get("latest_updates", [])],
            "repo_before_updates": info.get("repo_before_updates"),
            "ops_log": [normalize_op(u) for u in repo.ops_log()],
            "metadata": jsonable(repo.get_metadata()),
            "status": {
                "availability": str(status.availability).rsplit(".", 1)[-1].replace("_", "-"),
                "set_at": iso(status.set_at),
                "reason": status.limited_availability_reason,
            },
            "feature_flags": {
                "enabled": sorted(f.id for f in repo.feature_flags() if f.setting is True),
                "disabled": sorted(f.id for f in repo.feature_flags() if f.setting is False),
            },
            "config": describe_config(stored_config),
        }
    else:
        # Spec v1 repositories have no repo info file.
        ri = {"error": info}
        for fn in ["get_status", "get_metadata", "feature_flags"]:
            ri[fn] = outcome(getattr(repo, fn))
        ri["ops_log"] = outcome(lambda: list(repo.ops_log()))
    out["info"] = ri
    return out, snapshot_ids


# ---------------------------------------------------------------------------
# Snapshot level


def key_prefix(path: str) -> str:
    """Store key prefix of a node path: "" for the root, "a/b/" for /a/b."""
    p = path.strip("/")
    return p + "/" if p else ""


def get_bytes(store, key, byte_range=None):
    v = sync(store.get(key, PROTO, byte_range))
    return "absent" if v is None else digest(v.to_bytes())


def ranges_for(size: int, chunk: bool) -> dict:
    """Byte range requests: "r:a:b" = [a, b), "o:a" = from a, "s:n" = last n.
    Suffix and out-of-bounds requests only for chunks: upstream answers
    suffix requests for metadata keys with the wrong bytes, and panics on
    out-of-bounds metadata ranges."""
    third = size // 3
    reqs = {
        f"r:0:{min(1, size)}": RangeByteRequest(0, min(1, size)),
        f"r:{third}:{min(size, third + max(1, third))}": RangeByteRequest(third, min(size, third + max(1, third))),
        f"r:{size}:{size}": RangeByteRequest(size, size),
        f"o:{size // 2}": OffsetByteRequest(size // 2),
        "o:0": OffsetByteRequest(0),
    }
    if chunk and size > 0:
        reqs[f"s:{min(3, size)}"] = SuffixByteRequest(min(3, size))
        reqs[f"s:{size}"] = SuffixByteRequest(size)
        reqs[f"r:0:{size + 1}"] = RangeByteRequest(0, size + 1)
    return reqs


def key_info(store, key: str, chunk: bool, with_ranges: bool) -> dict:
    out = {"get": outcome(lambda: get_bytes(store, key)), "size": outcome(lambda: sync(store.getsize(key)))}
    if with_ranges and isinstance(out["size"], int):
        out["ranges"] = {
            spec: outcome(lambda: get_bytes(store, key, br))
            for spec, br in ranges_for(out["size"], chunk).items()
        }
    return out


def listing(keys) -> dict:
    """A list_dir / list_prefix result: its size and digest (of the sorted
    entries joined by newlines), plus the entries themselves when few."""
    keys = sorted(keys)
    out = {"count": len(keys), "digest": digest("\n".join(keys).encode())}
    if len(keys) <= LISTING_LIMIT:
        out["keys"] = keys
    return out


def probe(store, key: str) -> dict:
    return {
        "get": outcome(lambda: get_bytes(store, key)),
        "exists": outcome(lambda: sync(store.exists(key))),
        "size": outcome(lambda: sync(store.getsize(key))),
    }


def node_entry(n: dict, zarr_json: bytes) -> dict:
    e = {"path": n["path"], "id": n["id"], "type": n["node_type"], "zarr_json": digest(zarr_json)}
    if n["node_type"] == "array":
        e["shape"] = [[d["array_length"], d["num_chunks"]] for d in n["shape"]]
        names = [d.get("name") or "" for d in n["shape"]]
        e["dimension_names"] = names if any(names) else None
        e["manifest_refs"] = [{"id": m["id"], "extents": m["extents"]} for m in n.get("manifest_refs", [])]
    return e


def describe_snapshot(repo, sid: str, arrays: dict, manifests: dict) -> dict:
    insp = repo.inspect_snapshot(sid)
    session = repo.readonly_session(snapshot_id=sid)
    store = session.store
    out = {
        "header": {"written_by": insp["header"]["written_by"], "spec_version": insp["header"]["spec_version"]},
        "id": insp["id"],
        "flushed_at": insp["flushed_at"],
        "message": insp["commit_message"],
        "metadata": jsonable(insp.get("metadata", {})),
        "manifests": [{"id": m["id"], "size_bytes": m["size_bytes"], "num_chunk_refs": m["num_chunk_refs"]}
                      for m in insp["manifests"]],
    }
    for m in insp["manifests"]:
        if m["id"] not in manifests:
            manifests[m["id"]] = describe_manifest(repo, m["id"])

    nodes = []
    for n in insp["nodes"]:
        meta = sync(store.get(key_prefix(n["path"]) + "zarr.json", PROTO)).to_bytes()
        e = node_entry(n, meta)
        if n["node_type"] == "array":
            content = digest(json.dumps([n["id"], e["zarr_json"], e["manifest_refs"]]).encode())
            e["content"] = content
            if content not in arrays:
                arrays[content] = describe_array(session, n, e, meta)
        nodes.append(e)
    out["nodes"] = nodes
    out["virtual_locations"] = sorted(set(outcome(session.all_virtual_chunk_locations)))
    out["store"] = describe_store(store, nodes, arrays)
    return out


def describe_manifest(repo, mid: str) -> dict:
    m = repo.inspect_manifest(mid)
    return {
        "arrays": [
            {"node_id": a["node_id"], "num_chunk_refs": a["num_chunk_refs"], "num_inline": a["num_inline"],
             "num_native": a["num_native"], "num_virtual": a["num_virtual"]}
            for a in m["arrays"]
        ],
        "total_chunk_refs": m["total_chunk_refs"],
        "uses_location_compression": m["compression"]["uses_location_compression"],
        "num_compressed_refs": m["compression"]["num_compressed_refs"],
    }


def describe_store(store, nodes: list, arrays: dict) -> dict:
    keys = sorted(collect(store.list()))
    out = {"list": keys}
    out["metadata_keys"] = {
        k: key_info(store, k, chunk=False, with_ranges=True) for k in keys if k == "zarr.json" or k.endswith("/zarr.json")
    }

    probes = {"nope/zarr.json", "zarr.json/x", "c/0", "c", "/zarr.json", "//zarr.json", "nope/c/0", ""}
    dirs = {"", "nope", "nope/deeper"}
    prefixes = {"", "nope", "zarr.json"}
    groups = [n for n in nodes if n["type"] == "group"]
    for n in nodes:
        kp = key_prefix(n["path"])
        if kp:
            dirs.update({kp.rstrip("/"), kp})
            prefixes.update({kp.rstrip("/"), kp, kp[:max(1, len(kp) // 2)]})
        if n["type"] == "group":
            probes.update({kp + "c/0", kp + "nope/zarr.json", kp + "zarr.json/zarr.json"})
            continue
        a = arrays[n["content"]]
        grid = [d[1] for d in n["shape"]]
        nd = len(n["shape"])
        refs = {tuple(r[0]) for r in a["refs"]}
        probes.update({kp + "c/x", kp + "c/-1", kp + "zarr.json/x", kp + "c/" + "/".join(["0"] * (nd + 1))})
        if nd:
            probes.add(kp + "c")
            probes.add(kp + "c/" + "/".join(str(g) for g in grid))  # just outside the grid
            probes.add(kp + "c/" + "/".join(["4294967296"] * nd))  # beyond uint32
            missing = [c for c in itertools.product(*(range(min(g, 4)) for g in grid)) if c not in refs][:2]
            probes.update(kp + "c/" + "/".join(map(str, c)) for c in missing)
        dirs.update({kp + "c", kp + "c/", kp + "c/999999"})
        prefixes.update({kp + "c", kp + "c/"})
        for coords in sorted(refs)[:8]:
            for i in range(1, len(coords)):
                dirs.add(kp + "c/" + "/".join(map(str, coords[:i])))
    if groups and len(groups) > 1:
        g = key_prefix(groups[1]["path"])
        dirs.update({"/" + g.rstrip("/"), g + "/"})
    out["probes"] = {k: probe(store, k) for k in sorted(probes - set(keys))}
    out["list_dir"] = {d: outcome(lambda: listing(collect(store.list_dir(d)))) for d in sorted(dirs)}
    out["list_prefix"] = {p: outcome(lambda: listing(collect(store.list_prefix(p)))) for p in sorted(prefixes)}
    return out


# ---------------------------------------------------------------------------
# Array level


def chunk_layout(meta: dict) -> tuple[list, list]:
    """Per dimension: the outer chunk sizes (one per grid chunk) and every
    chunk edge (outer and, for sharding, inner chunk boundaries)."""
    shape = meta["shape"]
    grid = meta["chunk_grid"]
    cfg = grid.get("configuration", {})
    outer = []
    for d, n in enumerate(shape):
        if grid["name"] == "regular":
            c = cfg["chunk_shape"][d]
            outer.append([min(c, n - s) for s in range(0, n, c)] if c else [])
        else:
            sizes = []
            for spec in cfg["chunk_shapes"][d] if "chunk_shapes" in cfg else cfg["kind"]:
                if isinstance(spec, list):
                    sizes.extend([spec[0]] * spec[1])
                else:
                    sizes.append(spec)
            outer.append(sizes)
    inner = None
    codecs = meta.get("codecs", [])
    if codecs and codecs[0]["name"] == "sharding_indexed":
        inner = codecs[0]["configuration"]["chunk_shape"]
    edges = []
    for d, n in enumerate(shape):
        e = set(itertools.accumulate(outer[d][:-1]))
        if inner and inner[d]:
            e.update(range(inner[d], n, inner[d]))
        edges.append(sorted(x for x in e if 0 < x < n))
    return outer, edges


def regions(shape: list, edges: list, rng: random.Random) -> list:
    nd = len(shape)
    if nd == 0:
        return [([], [])]
    if 0 in shape:
        return [([0] * nd, list(shape))]
    out = [
        ([0] * nd, list(shape)),
        ([0] * nd, [1] * nd),
        ([n - 1 for n in shape], [1] * nd),
        ([n // 3 for n in shape], [min(max(1, n // 2), n - n // 3) for n in shape]),
    ]
    for d in range(nd):
        if edges[d]:
            b = min(edges[d], key=lambda x: abs(x - shape[d] / 2))
            start = [0] * nd
            count = list(shape)
            start[d], count[d] = b - 1, min(2, shape[d] - b + 1)
            out.append((start, count))
    if all(edges):
        out.append(([e[0] - 1 for e in edges], [min(2, n - e[0] + 1) for n, e in zip(shape, edges)]))
    out.append(([shape[0] // 2] + [0] * (nd - 1), [0] + list(shape[1:])))
    for _ in range(3):
        start = [rng.randrange(n) for n in shape]
        out.append((start, [rng.randrange(1, n - s + 1) for n, s in zip(shape, start)]))
    seen, uniq = set(), []
    for r in out:
        k = (tuple(r[0]), tuple(r[1]))
        if k not in seen:
            seen.add(k)
            uniq.append(r)
    return uniq


def read_region(arr, start, count) -> dict:
    try:
        data = np.asarray(arr[tuple(slice(s, s + c) for s, c in zip(start, count))] if start else arr[()])
    except Exception as e:
        return {"start": start, "count": count, "result": err(e)}
    r = {"start": start, "count": count, "result": digest_array(data)}
    if data.size <= VALUES_LIMIT:
        r["values"] = json_values(data)
    return r


def describe_array(session, n: dict, entry: dict, meta_bytes: bytes) -> dict:
    meta = json.loads(meta_bytes)
    store = session.store
    path = n["path"]
    kp = key_prefix(path)
    out: dict = {"path": path, "node_id": n["id"]}

    # Chunk references, as the store iterates them.
    refs = []
    for coords, types, locs, offsets, lengths, inline in collect(store.array_chunk_iterator(path)):
        for i in range(len(types)):
            refs.append([
                [int(c) for c in coords[i]], CHUNK_TYPES[int(types[i])], locs[i] or None,
                int(offsets[i]), int(lengths[i]), digest(inline[i]) if i in inline else None,
            ])
    refs.sort(key=lambda r: r[0])
    out["refs"] = refs

    nd = len(n["shape"])
    grid = [d["num_chunks"] for d in n["shape"]]
    have = {tuple(r[0]) for r in refs}
    probes = [list(c) for c in itertools.product(*(range(min(g, 3)) for g in grid)) if c not in have][:3]
    probes += [list(grid), [0] * (nd + 1), [0] * max(nd - 1, 0)]
    if nd:
        probes.append([2**31] * nd)
    out["chunk_type_probes"] = {
        ",".join(map(str, c)): outcome(lambda: str(session.chunk_type(path, c)).rsplit(".", 1)[-1]) for c in probes
    }

    # Every chunk key's bytes; byte ranges for a few.
    chunk_keys = sorted(k for k in collect(store.list_prefix(kp.rstrip("/"))) if k == kp + "c" or k.startswith(kp + "c/"))
    rng = random.Random(path)
    ranged = set(chunk_keys[:2] + chunk_keys[-1:] + rng.sample(chunk_keys, min(len(chunk_keys), MAX_RANGED_CHUNK_KEYS - 3)))
    out["chunk_keys"] = {
        k[len(kp):]: key_info(store, k, chunk=True, with_ranges=k in ranged) for k in chunk_keys
    }

    # Zarr-level view.
    z: dict = {}
    try:
        arr = zarr.open_array(store, path=path.lstrip("/"), mode="r")
    except Exception as e:
        out["zarr"] = {"error": err(e)}
        return out
    outer, edges = chunk_layout(meta)
    dt = arr.metadata.data_type.to_json(zarr_format=3)
    z["shape"] = list(arr.shape)
    z["data_type"] = {"name": dt, "configuration": None} if isinstance(dt, str) else {"name": dt["name"], "configuration": dt.get("configuration")}
    z["chunk_grid_shape"] = [len(o) for o in outer]
    # As in zarr.json: null, or one name (or null) per dimension.
    names = arr.metadata.dimension_names
    z["dimension_names"] = list(names) if names is not None else None
    z["attributes"] = jsonable(arr.attrs.asdict())
    z["reads"] = [read_region(arr, s, c) for s, c in regions(list(arr.shape), edges, random.Random(path + "/regions"))]

    coords = list(itertools.product(*(range(len(o)) for o in outer)))
    if len(coords) > MAX_CHUNK_READS:
        crng = random.Random(path + "/chunks")
        coords = sorted(set(coords[:8] + coords[-8:] + crng.sample(coords, MAX_CHUNK_READS - 16)))
    reads = []
    for c in coords:
        start = [sum(outer[d][:i]) for d, i in enumerate(c)]
        count = [outer[d][i] for d, i in enumerate(c)]
        r = read_region(arr, start, count)
        r["coords"] = list(c)
        reads.append(r)
    z["chunk_reads"] = reads
    out["zarr"] = z
    return out


def describe_group_attrs(store, nodes: list) -> dict:
    out = {}
    for n in nodes:
        if n["type"] == "group":
            out[n["path"]] = outcome(lambda: jsonable(zarr.open_group(store, path=n["path"].strip("/"), mode="r").attrs.asdict()))
    return out


# ---------------------------------------------------------------------------


def describe(name: str) -> dict:
    rel, vspec = FIXTURES[name]
    storage = ic.local_filesystem_storage(str(ROOT / rel))
    spec = int(ic.Repository.fetch_spec_version(storage))
    stored_config = ic.Repository.fetch_config(storage)
    virtual = resolve_virtual(vspec, rel)

    server = ObjectServer(virtual) if virtual else None
    try:
        config, creds = None, None
        if server:
            config = ic.RepositoryConfig.default()
            for prefix in virtual:
                config.set_virtual_chunk_container(ic.VirtualChunkContainer(prefix, ic.s3_store(
                    region="us-east-1", endpoint_url=server.url, allow_http=True, force_path_style=True, anonymous=True)))
            creds = ic.containers_credentials({p: ic.s3_anonymous_credentials() for p in virtual})
        repo = ic.Repository.open(storage, config=config, authorize_virtual_chunk_access=creds)

        repo_desc, snapshot_ids = describe_repo(repo, spec, stored_config)
        arrays: dict = {}
        manifests: dict = {}
        snapshots = {}
        for sid in snapshot_ids:
            snapshots[sid] = describe_snapshot(repo, sid, arrays, manifests)
            nodes = snapshots[sid]["nodes"]
            snapshots[sid]["group_attributes"] = describe_group_attrs(repo.readonly_session(snapshot_id=sid).store, nodes)
    finally:
        if server:
            server.close()

    served = {}
    for prefix, source in virtual.items():
        if "dir" in source:
            served[prefix] = {"dir": source["dir"]}
        else:
            served[prefix] = {"objects": {k: base64.b64encode(v).decode() for k, v in source["objects"].items()},
                              "mtime": OBJECT_MTIME}
    return {
        "_meta": {
            "fixture": name,
            "path": rel,
            "icechunk": ic.__version__,
            "zarr": zarr.__version__,
            "numpy": np.__version__,
            "python": platform.python_version(),
        },
        "virtual": served,
        "repo": repo_desc,
        "manifests": manifests,
        "snapshots": snapshots,
        "arrays": arrays,
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("fixtures", nargs="*", help=f"default: all of {', '.join(FIXTURES)}")
    p.add_argument("--out", type=Path, default=OUT)
    p.add_argument("--pretty", action="store_true", help="write indented .json instead of .json.gz")
    args = p.parse_args()
    unknown = set(args.fixtures) - set(FIXTURES)
    if unknown:
        sys.exit(f"unknown fixtures {sorted(unknown)}; choose from {list(FIXTURES)}")
    args.out.mkdir(parents=True, exist_ok=True)
    for name in args.fixtures or FIXTURES:
        doc = describe(name)
        if args.pretty:
            path = args.out / f"{name}.json"
            path.write_text(json.dumps(doc, indent=1, sort_keys=True, ensure_ascii=False) + "\n")
        else:
            path = args.out / f"{name}.json.gz"
            raw = json.dumps(doc, separators=(",", ":"), sort_keys=True, ensure_ascii=False).encode()
            path.write_bytes(gzip.compress(raw, mtime=0))
        print(f"wrote {path}", file=sys.stderr)


if __name__ == "__main__":
    main()
