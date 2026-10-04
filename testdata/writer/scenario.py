"""Run writer scenarios with icechunk-python, for the Go writer's
consistency tests (internal/conformance/writer_consistency_test.go).

A scenario (testdata/writer/scenarios.json) is a list of steps: open a
writable session, create groups and arrays, write regions, delete chunks and
nodes, rewrite zarr.json, set virtual refs, commit (optionally rebasing), and
create, reset or delete branches and tags. The Go test runs the same steps
with the Go writer. Both runs are reduced to a canonical description that
leaves out what legitimately differs between writers (object ids, timestamps,
compressed bytes), and must match.

    python testdata/writer/scenario.py record [name ...]   # write testdata/writer/expected/
    python testdata/writer/scenario.py serve                # JSON lines, for live Go tests

A scenario with a "config" starts from a repository icechunk-python creates
with that config; `record` keeps a copy of it, empty, in
testdata/writer/initial/<name> for the Go test (the Go writer cannot set
repository config).

`serve` reads one command per line and answers one JSON object per line:

    {"cmd": "create", "repo": DIR, "config": {...}} create a spec v2 repository
    {"cmd": "step", "repo": DIR, "step": {...}}     apply one step: {"outcome": ...}
    {"cmd": "canonical", "repo": DIR}               {"canonical": ...} as read by Python

so the Go test can interleave Python and Go writers on one repository.

Step outcomes are "ok" or "error:<category>" with categories conflict,
session, exists, not_found, invalid and other. Data written by "write" steps
is generated from (seed, flat index) by formulas the Go test reproduces.
"""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
import traceback
from pathlib import Path

import numpy as np

import icechunk as ic
import zarr
from zarr.core.buffer import default_buffer_prototype
from zarr.core.sync import sync
from zarr.registry import get_codec_class

HERE = Path(__file__).resolve().parent
sys.dont_write_bytecode = True  # no __pycache__ in testdata/oracle
sys.path.insert(0, str(HERE.parent / "oracle"))
import oracle  # noqa: E402  (digests and normalization shared with the reader oracle)

PROTO = default_buffer_prototype()
SCENARIOS = HERE / "scenarios.json"
EXPECTED = HERE / "expected"
INITIAL = HERE / "initial"
MISSING_SNAPSHOT = "ZZZZZZZZZZZZZZZZZZZ0"

# Codecs whose output is fully determined by their input, so Go and Python
# must produce the same chunk bytes (compressors, and sharding's inner chunk
# order, may legitimately differ).
DETERMINISTIC_CODECS = {
    "bytes", "transpose", "crc32c", "vlen-utf8", "vlen-bytes",
    "numcodecs.crc32", "numcodecs.crc32c", "numcodecs.adler32", "numcodecs.fletcher32", "numcodecs.shuffle",
}

CATEGORIES = {
    "ConflictError": "conflict",
    "RebaseFailedError": "conflict",
    "SessionStateError": "session",
    "AlreadyExistsError": "exists",
    "ContainsGroupError": "exists",
    "ContainsArrayError": "exists",
    "RefNotFoundError": "not_found",
    "SnapshotNotFoundError": "not_found",
    "NodeNotFoundError": "not_found",
    "NotFoundError": "not_found",
    "InvalidInputError": "invalid",
    # zarr-python's, when a step opens a missing array or a group as an array
    "ArrayNotFoundError": "not_found",
    "GroupNotFoundError": "not_found",
    "NodeTypeValidationError": "not_found",
}


def category(e: BaseException) -> str:
    return CATEGORIES.get(type(e).__name__, "other")


# ---------------------------------------------------------------------------
# Generated data: the Go test implements the same formulas.


def data_type(meta: dict) -> tuple[str, dict]:
    dt = meta["data_type"]
    return (dt, {}) if isinstance(dt, str) else (dt["name"], dt.get("configuration") or {})


def gen_values(meta: dict, shape: list[int], seed: int) -> np.ndarray:
    """Element i (C order) of a write with this seed."""
    name, cfg = data_type(meta)
    n = int(np.prod(shape)) if shape else 1
    idx = range(n)

    def flt(i):
        return ((i * 37 + seed * 11) % 2001 - 1000) / 8.0

    if name == "bool":
        out = np.array([(i * 7 + seed) % 3 == 0 for i in idx], dtype=bool)
    elif name.startswith(("int", "uint")):
        size = int(name.lstrip("uint")) // 8
        raw = b"".join(((i * 2654435761 + seed * 40503) % (1 << (8 * size))).to_bytes(size, "little") for i in idx)
        out = np.frombuffer(raw, dtype=f"<{'u' if name.startswith('u') else 'i'}{size}").copy()
    elif name.startswith("float"):
        out = np.array([flt(i) for i in idx], dtype=name)
    elif name.startswith("complex"):
        out = np.array([complex(flt(i), flt(i + 1000)) for i in idx], dtype=name)
    elif name in ("numpy.datetime64", "numpy.timedelta64"):
        kind = "datetime64" if name.endswith("datetime64") else "timedelta64"
        out = np.array([i * 86400 + seed * 3600 for i in idx], dtype="i8").view(f"{kind}[{cfg['unit']}]")
    elif name == "string":
        out = np.array([f"s{seed}-{i}{chr(0x3B1 + i % 20)}" for i in idx], dtype=object)
    elif name in ("bytes", "variable_length_bytes"):
        out = np.array([f"b{seed}-{i}".encode() for i in idx], dtype=object)
    elif name == "fixed_length_utf32":
        chars = cfg["length_bytes"] // 4
        out = np.array([f"{seed}{i}ü"[:chars] for i in idx], dtype=f"<U{chars}")
    elif name == "null_terminated_bytes":
        size = cfg["length_bytes"]
        out = np.array([f"{seed}:{i}".encode()[:size] for i in idx], dtype=f"S{size}")
    elif name == "raw_bytes":
        size = cfg["length_bytes"]
        raw = b"".join(bytes((i * 31 + seed + k) % 256 for k in range(size)) for i in idx)
        out = np.frombuffer(raw, dtype=f"V{size}").copy()
    else:
        raise ValueError(f"no generator for data type {name}")
    return out.reshape(shape)


def make_config(c: dict | None):
    """A scenario's repository config: inline_chunk_threshold_bytes,
    num_updates_per_repo_info_file, and manifest_split {path, axis0, any}."""
    if not c:
        return None
    cfg = ic.RepositoryConfig.default()
    if "inline_chunk_threshold_bytes" in c:
        cfg.inline_chunk_threshold_bytes = c["inline_chunk_threshold_bytes"]
    if "num_updates_per_repo_info_file" in c:
        cfg.num_updates_per_repo_info_file = c["num_updates_per_repo_info_file"]
    if "manifest_split" in c:
        sp = c["manifest_split"]
        cfg.manifest = ic.ManifestConfig(splitting=ic.ManifestSplittingConfig.from_dict({
            ic.ManifestSplitCondition.path_matches(sp["path"]): {
                ic.ManifestSplitDimCondition.Axis(0): sp["axis0"],
                ic.ManifestSplitDimCondition.Any(): sp["any"],
            },
        }))
    return cfg


def create_repo(path: str, config: dict | None) -> None:
    ic.Repository.create(ic.local_filesystem_storage(path), config=make_config(config), spec_version=2)


def raw_bytes(seed: int, size: int) -> bytes:
    return bytes((i * 13 + seed) % 256 for i in range(size))


# ---------------------------------------------------------------------------
# Applying steps


def split_codecs(codecs: list[dict]):
    """A zarr.json codec list as create_array's filters, serializer and compressors."""
    filters, serializer, compressors = [], None, []
    for c in codecs:
        codec = get_codec_class(c["name"]).from_dict(c)
        if serializer is None and c["name"] in ("bytes", "vlen-utf8", "vlen-bytes", "sharding_indexed"):
            serializer = codec
        elif serializer is None:
            filters.append(codec)
        else:
            compressors.append(codec)
    return filters, serializer, compressors


def fill_value(spec: dict):
    v = spec.get("fill_value")
    if isinstance(v, list):
        return complex(float(v[0]), float(v[1]))
    if v in ("NaN", "Infinity", "-Infinity"):
        return float(v.replace("Infinity", "inf"))
    return v


def key_of(path: str, suffix: str) -> str:
    p = path.strip("/")
    return f"{p}/{suffix}" if p else suffix


class Runner:
    """Applies steps to one repository with icechunk-python."""

    def __init__(self, path: str):
        self.repo = ic.Repository.open(ic.local_filesystem_storage(path))
        self.sessions: dict[str, ic.Session] = {}

    def snapshot(self, label: str) -> str:
        """A snapshot by commit message (messages are unique in scenarios)."""
        for s in self.repo.inspect_repo_info()["snapshots"]:
            if s["message"] == label:
                return s["id"]
        return MISSING_SNAPSHOT

    def apply(self, step: dict) -> str:
        try:
            self._apply(step)
            return "ok"
        except BaseException as e:  # icechunk raises Rust panics as BaseException
            if isinstance(e, KeyboardInterrupt):
                raise
            return f"error:{category(e)}"

    def _apply(self, step: dict) -> None:
        op = step["op"]
        repo = self.repo
        if op == "open":
            self.sessions[step["session"]] = repo.writable_session(step["branch"])
            return
        if op == "create_branch":
            return repo.create_branch(step["name"], self.snapshot(step["at"]))
        if op == "reset_branch":
            return repo.reset_branch(step["name"], self.snapshot(step["at"]))
        if op == "delete_branch":
            return repo.delete_branch(step["name"])
        if op == "create_tag":
            return repo.create_tag(step["name"], self.snapshot(step["at"]))
        if op == "delete_tag":
            return repo.delete_tag(step["name"])

        session = self.sessions[step["session"]]
        store = session.store
        path = step.get("path", "/")
        if op == "commit":
            session.commit(
                step["message"],
                metadata=step.get("metadata"),
                rebase_with=ic.ConflictDetector() if step.get("rebase") else None,
                allow_empty=step.get("allow_empty", False),
            )
        elif op == "create_group":
            zarr.create_group(store=store, path=path.strip("/") or None, attributes=step.get("attributes") or {})
        elif op == "create_array":
            spec = step["spec"]
            kw = {}
            if "codecs" in spec:
                filters, serializer, compressors = split_codecs(spec["codecs"])
                kw = {"filters": filters, "serializer": serializer or "auto", "compressors": compressors}
            dims = spec.get("dimension_names")
            zarr.create_array(
                store=store, name=path.strip("/"), shape=tuple(spec["shape"]),
                chunks=tuple(spec.get("chunks", spec["shape"])),
                shards=tuple(spec["shards"]) if "shards" in spec else None,
                dtype=str if spec["data_type"] == "string" else spec["data_type"],
                fill_value=fill_value(spec), dimension_names=tuple(dims) if dims else None,
                attributes=spec.get("attributes") or {}, **kw,
            )
        elif op == "set_metadata":
            doc = json.dumps(step["zarr_json"]).encode()
            sync(store.set(key_of(path, "zarr.json"), PROTO.buffer.from_bytes(doc)))
        elif op in ("update_attributes", "set_shape"):
            doc = json.loads(sync(store.get(key_of(path, "zarr.json"), PROTO)).to_bytes())
            if op == "update_attributes":
                doc["attributes"] = step["attributes"]
            else:
                doc["shape"] = step["shape"]
            sync(store.set(key_of(path, "zarr.json"), PROTO.buffer.from_bytes(json.dumps(doc).encode())))
        elif op == "write":
            arr = zarr.open_array(store=store, path=path.strip("/"), mode="r+")
            meta = json.loads(sync(store.get(key_of(path, "zarr.json"), PROTO)).to_bytes())
            shape = step["shape"]
            if step.get("fill"):
                data = np.full(shape, arr.fill_value, dtype=arr.dtype)
            elif step.get("nan"):
                data = np.full(shape, np.nan, dtype=arr.dtype)
            else:
                data = gen_values(meta, shape, step["seed"])
            region = tuple(slice(s, s + c) for s, c in zip(step["start"], shape))
            arr[region if region else ()] = data
        elif op == "delete_chunk":
            sync(store.delete(key_of(path, "c/" + "/".join(map(str, step["coords"])))))
        elif op == "set_chunk":
            key = key_of(path, "c/" + "/".join(map(str, step["coords"])))
            sync(store.set(key, PROTO.buffer.from_bytes(raw_bytes(step["seed"], step["size"]))))
        elif op == "set_virtual_ref":
            checksum = step.get("etag")
            if step.get("last_modified"):
                from datetime import datetime
                checksum = datetime.fromisoformat(step["last_modified"].replace("Z", "+00:00"))
            store.set_virtual_ref(key_of(path, "c/" + "/".join(map(str, step["coords"]))), step["location"],
                                  offset=step["offset"], length=step["length"], checksum=checksum,
                                  validate_container=False)
        elif op == "delete_node":
            sync(store.delete_dir(path.strip("/")))
        else:
            raise ValueError(f"unknown op {op}")


# ---------------------------------------------------------------------------
# Canonical description of a repository, as icechunk-python reads it


def canonical(path: str) -> dict:
    storage = ic.local_filesystem_storage(path)
    repo = ic.Repository.open(storage)
    info = repo.inspect_repo_info()
    by_id = {s["id"]: s for s in info["snapshots"]}

    def msg(sid):
        return by_id[sid]["message"] if sid in by_id else (sid and f"unknown:{sid}")

    out = {
        "branches": {b: msg(i) for b, i in sorted(info["branches"].items())},
        "tags": {t: msg(i) for t, i in sorted(info["tags"].items())},
        "deleted_tags": sorted(info.get("deleted_tags", [])),
        "commits": {
            s["message"]: {"parent": msg(s.get("parent_id")), "metadata": oracle.jsonable(s.get("metadata", {}))}
            for s in info["snapshots"]
        },
        "updates": [],
        "ancestry": {},
        # The whole ops log, following repo_before_updates into older repo
        # info files (the Go reader reads only the latest file's updates).
        "ops_log": [],
        "status": str(repo.get_status().availability).rsplit(".", 1)[-1].replace("_", "-"),
        "config": oracle.describe_config(ic.Repository.fetch_config(storage)),
        "snapshots": {},
        "diffs": {},
    }
    for u in info.get("latest_updates", []):
        n = oracle.normalize_update(u)
        out["updates"].append([n["kind"], n["name"], msg(n["snapshot_id"]), msg(n["previous_snapshot_id"])])
    for u in repo.ops_log():
        n = oracle.normalize_op(u)
        out["ops_log"].append([n["kind"], n["name"], msg(n["snapshot_id"]), msg(n["previous_snapshot_id"])])
    for b in info["branches"]:
        out["ancestry"][f"branch:{b}"] = [s.message for s in repo.ancestry(branch=b)]
    for t in info["tags"]:
        out["ancestry"][f"tag:{t}"] = [s.message for s in repo.ancestry(tag=t)]
    manifest_ids = {}
    for s in info["snapshots"]:
        out["snapshots"][s["message"]], manifest_ids[s["id"]] = describe_snapshot(repo, s["id"])
    # How many of each array's manifests a commit kept from its parent (the
    # rest it wrote anew): upstream rewrites only the manifests it must.
    for s in info["snapshots"]:
        parent = manifest_ids.get(s.get("parent_id"), {})
        for path, node in out["snapshots"][s["message"]]["nodes"].items():
            if node["type"] == "array":
                node["manifests_kept"] = len(set(manifest_ids[s["id"]][path]) & set(parent.get(path, [])))
    for s in info["snapshots"]:
        if s.get("parent_id"):
            out["diffs"][s["message"]] = describe_diff(repo.diff(from_snapshot_id=s["parent_id"], to_snapshot_id=s["id"]))
    return out


def describe_diff(d) -> dict:
    return {
        "new_groups": sorted(d.new_groups),
        "new_arrays": sorted(d.new_arrays),
        "deleted_groups": sorted(d.deleted_groups),
        "deleted_arrays": sorted(d.deleted_arrays),
        "updated_groups": sorted(d.updated_groups),
        "updated_arrays": sorted(d.updated_arrays),
        "updated_chunks": {p: sorted(list(c) for c in cs) for p, cs in sorted(d.updated_chunks.items())},
    }


# Data types whose elements are single bytes, so the bytes codec's endian
# has no effect (zarr-python omits it).
ENDIANLESS = {"bool", "int8", "uint8", "null_terminated_bytes", "raw_bytes"}


def normalize_codecs(codecs: list, endianless: bool) -> None:
    for c in codecs:
        cfg = c.get("configuration")
        if c["name"] == "bytes" and endianless and isinstance(cfg, dict):
            cfg.pop("endian", None)
        if c["name"] == "sharding_indexed" and isinstance(cfg, dict):
            normalize_codecs(cfg.get("codecs", []), endianless)
            normalize_codecs(cfg.get("index_codecs", []), False)
        if cfg == {}:
            del c["configuration"]


def zarr_doc(raw: bytes) -> dict:
    """zarr.json without differences that mean nothing: consolidated_metadata
    null (same as absent), empty codec configurations, and the bytes codec's
    endian for single-byte data types. consistency tests normalize Go's the
    same way."""
    doc = json.loads(raw)
    if doc.get("consolidated_metadata", 0) is None:
        del doc["consolidated_metadata"]
    if "codecs" in doc:
        name = doc["data_type"] if isinstance(doc["data_type"], str) else doc["data_type"]["name"]
        normalize_codecs(doc["codecs"], name in ENDIANLESS)
    return doc


def deterministic(doc: dict) -> bool:
    return all(c["name"] in DETERMINISTIC_CODECS for c in doc.get("codecs", []))


def describe_snapshot(repo, sid: str) -> dict:
    session = repo.readonly_session(snapshot_id=sid)
    store = session.store
    nodes = {}
    manifest_ids = {}
    for n in repo.inspect_snapshot(sid)["nodes"]:
        path = n["path"]
        raw = sync(store.get(key_of(path, "zarr.json"), PROTO)).to_bytes()
        doc = zarr_doc(raw)
        e = {"type": n["node_type"], "zarr_json": oracle.jsonable(doc)}
        if n["node_type"] == "array":
            e["manifest_extents"] = sorted(m["extents"] for m in n.get("manifest_refs", []))
            manifest_ids[path] = [m["id"] for m in n.get("manifest_refs", [])]
            try:
                arr = zarr.open_array(store=store, path=path.strip("/"), mode="r")
                e["values"] = oracle.digest_array(np.asarray(arr[...]))
            except Exception as err:
                e["values"] = f"error:{category(err)}"
            exact = deterministic(doc)
            chunks = {}
            for coords, types, locs, offsets, lengths, _inline in oracle.collect(store.array_chunk_iterator(path)):
                for i in range(len(types)):
                    c = ",".join(str(int(x)) for x in coords[i])
                    kind = oracle.CHUNK_TYPES[int(types[i])]
                    if kind == "virtual":
                        chunks[c] = f"virtual:{locs[i]}:{int(offsets[i])}:{int(lengths[i])}"
                    elif exact:
                        data = sync(store.get(key_of(path, "c/" + c.replace(",", "/") if c else "c"), PROTO)).to_bytes()
                        chunks[c] = f"{kind}:{oracle.digest(data)}"
                    else:
                        chunks[c] = "stored"
            e["chunks"] = dict(sorted(chunks.items()))
        nodes[path] = e
    return {"keys": sorted(oracle.collect(store.list())), "nodes": dict(sorted(nodes.items()))}, manifest_ids


# ---------------------------------------------------------------------------


def load_scenarios() -> dict:
    return json.loads(SCENARIOS.read_text())


def record(names: list[str]) -> None:
    scenarios = load_scenarios()
    EXPECTED.mkdir(exist_ok=True)
    for name in names or sorted(scenarios):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "repo")
            config = scenarios[name].get("config")
            create_repo(path, config)
            if config:
                shutil.rmtree(INITIAL / name, ignore_errors=True)
                shutil.copytree(path, INITIAL / name)
            runner = Runner(path)
            outcomes = [runner.apply(step) for step in scenarios[name]["steps"]]
            doc = {
                "_meta": {"icechunk": ic.__version__, "zarr": zarr.__version__, "numpy": np.__version__},
                "outcomes": outcomes,
                "canonical": canonical(path),
            }
        (EXPECTED / f"{name}.json").write_text(json.dumps(doc, indent=1, sort_keys=True, ensure_ascii=False) + "\n")
        print(f"wrote {EXPECTED / name}.json", file=sys.stderr)


def serve() -> None:
    runners: dict[str, Runner] = {}
    for line in sys.stdin:
        if not line.strip():
            continue
        try:
            cmd = json.loads(line)
            repo = cmd.get("repo")
            if cmd["cmd"] == "create":
                shutil.rmtree(repo, ignore_errors=True)
                create_repo(repo, cmd.get("config"))
                runners.pop(repo, None)
                reply = {}
            elif cmd["cmd"] == "step":
                if repo not in runners:
                    runners[repo] = Runner(repo)
                reply = {"outcome": runners[repo].apply(cmd["step"])}
            elif cmd["cmd"] == "canonical":
                reply = {"canonical": canonical(repo)}
            else:
                raise ValueError(f"unknown command {cmd['cmd']}")
        except Exception:
            reply = {"driver_error": traceback.format_exc()}
        sys.stdout.write(json.dumps(reply, ensure_ascii=False) + "\n")
        sys.stdout.flush()


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] not in ("record", "serve"):
        sys.exit(__doc__)
    if sys.argv[1] == "record":
        record(sys.argv[2:])
    else:
        serve()


if __name__ == "__main__":
    main()
