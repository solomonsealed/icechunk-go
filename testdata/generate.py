"""Generate Icechunk repositories that exercise the Go reader.

Run with an environment that has icechunk, zarr and numpy installed:

    python testdata/generate.py              # regenerate everything
    python testdata/generate.py virtual-v2   # only some repositories

Writes testdata/generated/<repo>/ plus testdata/generated/expected.json,
which records, for every array, its dtype, shape and either its values or
the sha256 of its C-order little-endian bytes.
"""

import hashlib
import json
import math
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

import icechunk as ic
import zarr
from zarr.codecs import (
    BloscCodec,
    BytesCodec,
    Crc32cCodec,
    GzipCodec,
    ShardingCodec,
    TransposeCodec,
    ZstdCodec,
)
import zarr.codecs.numcodecs as ncz

HERE = Path(__file__).resolve().parent
OUT = HERE / "generated"
VIRTUAL_PREFIX = "s3://icechunk-go-fixtures/virtual/"

COMMIT_PROPERTIES = {
    "author": "icechunk-go generator",
    "count": 42,
    "negative": -7,
    "ratio": 0.25,
    "flag": True,
    "nothing": None,
    "nested": {"list": [1, "two", 3.5, False], "empty": {}},
}

expected: dict = {}


def to_json_values(arr: np.ndarray) -> list:
    flat = arr.reshape(-1)
    if arr.dtype.kind in "US" or arr.dtype == object or arr.dtype.kind == "T":
        return [str(x) for x in flat]
    if arr.dtype.kind == "c":
        return [[encode_float(x.real), encode_float(x.imag)] for x in flat]
    if arr.dtype.kind == "f":
        return [encode_float(float(x)) for x in flat]
    if arr.dtype.kind == "b":
        return [bool(x) for x in flat]
    if arr.dtype.kind == "M" or arr.dtype.kind == "m":
        return [int(x) for x in flat.view("i8")]
    return [int(x) for x in flat]


def encode_float(x: float):
    if math.isnan(x):
        return "NaN"
    if math.isinf(x):
        return "Infinity" if x > 0 else "-Infinity"
    return float(x)


def record(repo: str, branch: str, path: str, arr: zarr.Array, values: bool = True) -> None:
    data = np.asarray(arr[...])
    entry = {
        "dtype": str(arr.metadata.data_type.to_json(zarr_format=3) if hasattr(arr.metadata.data_type, "to_json") else arr.dtype),
        "shape": list(arr.shape),
    }
    if data.dtype.kind not in "OTUS":
        le = data.astype(data.dtype.newbyteorder("<"), copy=False)
        entry["sha256"] = hashlib.sha256(np.ascontiguousarray(le).tobytes()).hexdigest()
    if values or data.dtype.kind in "OTUS":
        entry["values"] = to_json_values(data)
    expected.setdefault(repo, {}).setdefault(branch, {})[path] = entry


def rng_data(shape, dtype, seed):
    rng = np.random.default_rng(seed)
    if np.dtype(dtype).kind == "f":
        # Smooth-ish data compresses like real fields.
        x = np.cumsum(rng.normal(size=int(np.prod(shape)))).reshape(shape)
        return x.astype(dtype)
    info = np.iinfo(dtype)
    return rng.integers(max(info.min, -1000), min(info.max, 1000), size=shape, dtype=dtype)


def write_codecs_repo(name: str, spec_version: int) -> None:
    path = OUT / name
    repo = ic.Repository.create(ic.local_filesystem_storage(str(path)), spec_version=spec_version)
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, attributes={"title": "codec matrix", "spec": spec_version})
    arrays = {}

    # --- every core data type, default codecs, partial writes (fill values)
    dtypes = {
        "bool": (np.bool_, True),
        "int8": (np.int8, -3),
        "int16": (np.int16, 7),
        "int32": (np.int32, -1),
        "int64": (np.int64, 123456789012),
        "uint8": (np.uint8, 255),
        "uint16": (np.uint16, 9),
        "uint32": (np.uint32, 4000000000),
        "uint64": (np.uint64, 2**63 + 5),
        "float16": (np.float16, 1.5),
        "float32": (np.float32, float("nan")),
        "float64": (np.float64, float("-inf")),
        "complex64": (np.complex64, complex(1, -2)),
        "complex128": (np.complex128, complex(float("nan"), 0.5)),
    }
    types = root.create_group("types", attributes={"about": "one array per dtype"})
    for i, (dname, (dt, fill)) in enumerate(dtypes.items()):
        a = types.create_array(dname, shape=(7, 5), chunks=(3, 2), dtype=dt, fill_value=fill)
        if np.dtype(dt).kind == "b":
            data = (np.arange(35).reshape(7, 5) % 3 == 0)
        elif np.dtype(dt).kind == "c":
            data = (np.arange(35) + 1j * np.arange(35)[::-1]).reshape(7, 5).astype(dt)
        elif np.dtype(dt).kind == "f":
            data = (np.arange(35).reshape(7, 5) / 4 - 3).astype(dt)
        else:
            data = (np.arange(35).reshape(7, 5) * (i + 1)).astype(dt)
        # Leave the bottom-right region unwritten so the fill value shows.
        a[:4, :] = data[:4, :]
        a[4:, :2] = data[4:, :2]
        arrays[f"types/{dname}"] = a

    # --- codec matrix
    codecs = root.create_group("codecs")
    shape, chunks = (13, 11), (4, 3)
    variants = {
        "zstd": dict(),
        "nocompress": dict(compressors=None),
        "gzip": dict(compressors=GzipCodec(level=5)),
        "crc32c": dict(compressors=[ZstdCodec(level=3), Crc32cCodec()]),
        "transpose": dict(filters=[TransposeCodec(order=(1, 0))]),
        "bigendian": dict(serializer=BytesCodec(endian="big")),
        "blosc_lz4_shuffle": dict(compressors=BloscCodec(cname="lz4", shuffle="shuffle")),
        "blosc_lz4hc_bitshuffle": dict(compressors=BloscCodec(cname="lz4hc", shuffle="bitshuffle")),
        "blosc_zstd_bitshuffle": dict(compressors=BloscCodec(cname="zstd", shuffle="bitshuffle")),
        "blosc_blosclz_noshuffle": dict(compressors=BloscCodec(cname="blosclz", shuffle="noshuffle")),
        "blosc_zlib_shuffle": dict(compressors=BloscCodec(cname="zlib", shuffle="shuffle")),
        "numcodecs_zlib": dict(compressors=ncz.Zlib(level=4)),
        "numcodecs_lz4": dict(compressors=ncz.LZ4()),
        "numcodecs_shuffle_zlib": dict(compressors=[ncz.Shuffle(elementsize=8), ncz.Zlib(level=1)]),
        "numcodecs_crc32": dict(compressors=[ncz.Zstd(level=1), ncz.CRC32()]),
        "numcodecs_crc32c": dict(compressors=[ncz.Zlib(level=1), ncz.CRC32C()]),
        "numcodecs_adler32": dict(compressors=[ncz.Adler32()]),
        "numcodecs_fletcher32": dict(compressors=[ncz.Fletcher32()]),
        "numcodecs_bz2": dict(compressors=ncz.BZ2(level=9)),
        "numcodecs_bitround": dict(filters=[ncz.BitRound(keepbits=10)], compressors=ZstdCodec()),
        "sharded": dict(shards=(8, 6)),
        "sharded_index_start": dict(
            serializer=ShardingCodec(chunk_shape=chunks, index_location="start"),
            compressors=None,
            chunks=(8, 6),
        ),
    }
    for vname, kw in variants.items():
        kw = dict(kw)
        c = kw.pop("chunks", chunks)
        a = codecs.create_array(vname, shape=shape, chunks=c, dtype="float64", fill_value=-1.0, **kw)
        data = rng_data(shape, "float64", seed=len(vname))
        if vname.startswith("sharded"):
            # Leave some inner chunks (and one whole shard) unwritten.
            a[:8, :] = data[:8, :]
            a[8:12, :6] = data[8:12, :6]
        else:
            a[...] = data
        arrays[f"codecs/{vname}"] = a

    # --- bigger arrays: several blosc blocks, splits and leftovers
    big = root.create_group("big")
    for vname, comp in {
        "blosc_lz4": BloscCodec(cname="lz4", clevel=5, shuffle="shuffle"),
        "blosc_blosclz_bitshuffle": BloscCodec(cname="blosclz", clevel=9, shuffle="bitshuffle"),
        "blosc_zstd_noshuffle": BloscCodec(cname="zstd", clevel=1, shuffle="noshuffle"),
    }.items():
        a = big.create_array(vname, shape=(150, 230), chunks=(100, 150), dtype="float32", compressors=comp)
        a[...] = rng_data((150, 230), "float32", seed=7)
        arrays[f"big/{vname}"] = a
    a = big.create_array("sharded_int16", shape=(64, 64), chunks=(8, 8), shards=(32, 32), dtype="int16",
                         compressors=BloscCodec(cname="lz4", shuffle="bitshuffle"))
    a[...] = rng_data((64, 64), "int16", seed=3)
    arrays["big/sharded_int16"] = a

    # --- special shapes and types
    misc = root.create_group("misc")
    s = misc.create_array("scalar", shape=(), dtype="float64", fill_value=0.0)
    s[()] = 3.5
    arrays["misc/scalar"] = s
    st = misc.create_array("strings", shape=(6,), chunks=(4,), dtype=str, fill_value="")
    st[:5] = np.array(["alpha", "βeta", "", "a much longer string value", "🧊"])
    arrays["misc/strings"] = st
    dtm = misc.create_array("datetime", shape=(5,), chunks=(2,), dtype="datetime64[s]")
    dtm[...] = np.array(["2024-01-01T00:00:00", "1970-01-01T00:00:01", "2000-02-29T12:00:00", "NaT", "2262-04-11T23:47:16"], dtype="datetime64[s]")
    arrays["misc/datetime"] = dtm
    e = misc.create_array("empty", shape=(0, 4), chunks=(2, 2), dtype="int32")
    arrays["misc/empty"] = e
    # 3-d with transpose + uneven edges
    t3 = misc.create_array("cube", shape=(5, 6, 7), chunks=(2, 4, 3), dtype="int32", fill_value=0,
                           filters=[TransposeCodec(order=(2, 0, 1))], dimension_names=("t", "y", "x"))
    t3[...] = np.arange(5 * 6 * 7, dtype="int32").reshape(5, 6, 7)
    arrays["misc/cube"] = t3
    if spec_version >= 2:
        with zarr.config.set({"array.rectilinear_chunks": True}):
            r = misc.create_array("rectilinear", shape=(10, 7), chunks=([3, 3, 4], [2, 5]), dtype="int16", fill_value=-9)
            r[:6, :] = np.arange(42, dtype="int16").reshape(6, 7)
            arrays["misc/rectilinear"] = r

    root.create_group("nested/deeper/deepest", attributes={"depth": 3})
    session.commit("codec matrix", metadata=COMMIT_PROPERTIES)

    # A second commit on another branch, to test version selection.
    repo.create_branch("edits", repo.lookup_branch("main"))
    session = repo.writable_session("edits")
    a = zarr.open_array(session.store, path="types/int32", mode="a")
    a[0, 0] = 999
    session.commit("edit int32")
    repo.create_tag("v1.0", repo.lookup_branch("main"))
    if spec_version >= 2:
        # Spec v2 allows "/" in ref names (v1 stores refs as storage keys).
        repo.create_branch("feature/x", repo.lookup_branch("edits"))
        repo.create_tag("v1/rc", repo.lookup_branch("main"))

    for p, a in arrays.items():
        small = int(np.prod(a.shape)) <= 500
        record(name, "main", p, a, values=small)
    session = repo.readonly_session("edits")
    record(name, "edits", "types/int32", zarr.open_array(session.store, path="types/int32", mode="r"))


def write_virtual_repo(name: str, spec_version: int) -> None:
    """2000 virtual chunks spread over 40 files: enough for dictionary
    compression of locations (with the threshold lowered to 100 chunks)."""
    path = OUT / name
    data_dir = OUT / "virtual-data"
    data_dir.mkdir(parents=True, exist_ok=True)
    values = (np.arange(2000, dtype="<i4") * 7 - 3000)
    nfiles = 40
    per_file = 2000 // nfiles
    for f in range(nfiles):
        (data_dir / f"part-{f:02d}.bin").write_bytes(b"HEADER" + values[f * per_file:(f + 1) * per_file].tobytes())
    (data_dir / "my file.bin").write_bytes(np.arange(4, dtype="<i4").tobytes())

    config = ic.RepositoryConfig.default()
    config.set_virtual_chunk_container(ic.VirtualChunkContainer(VIRTUAL_PREFIX, ic.s3_store(region="us-east-1")))
    config.manifest = ic.ManifestConfig(
        virtual_chunk_location_compression=ic.ManifestVirtualChunkLocationCompressionConfig(min_num_chunks=100)
    )
    repo = ic.Repository.create(ic.local_filesystem_storage(str(path)), config=config, spec_version=spec_version)
    session = repo.writable_session("main")
    root = zarr.group(store=session.store)
    root.create_array("virtual", shape=(2000,), chunks=(1,), dtype="<i4", compressors=None, fill_value=0)
    specs = []
    for i in range(2000):
        f, k = divmod(i, per_file)
        specs.append(ic.VirtualChunkSpec(
            index=[i],
            location=f"{VIRTUAL_PREFIX}part-{f:02d}.bin",
            offset=6 + 4 * k,
            length=4,
        ))
    session.store.set_virtual_refs("virtual", specs)

    # Checksummed refs: chunks 0 and 1 no longer match the object (a wrong
    # ETag, and a modification date older than the file); chunks 2 and 3 do
    # (the file's MD5, which is the ETag of single-part S3/R2 uploads, and a
    # date in the future).
    root.create_array("checksummed", shape=(4,), chunks=(1,), dtype="<i4", compressors=None, fill_value=0)
    part0 = f"{VIRTUAL_PREFIX}part-00.bin"
    md5 = hashlib.md5((data_dir / "part-00.bin").read_bytes()).hexdigest()
    session.store.set_virtual_ref("checksummed/c/0", part0, offset=6, length=4, checksum="etag-123")
    session.store.set_virtual_ref("checksummed/c/1", part0, offset=10, length=4,
                                  checksum=datetime(2020, 1, 2, 3, 4, 5, tzinfo=timezone.utc))
    session.store.set_virtual_ref("checksummed/c/2", part0, offset=14, length=4, checksum=md5)
    session.store.set_virtual_ref("checksummed/c/3", part0, offset=18, length=4,
                                  checksum=datetime(2100, 1, 1, tzinfo=timezone.utc))

    # Locations are URLs: upstream percent-decodes the path and drops any
    # query or fragment before reading the object.
    root.create_array("encoded", shape=(2,), chunks=(1,), dtype="<i4", compressors=None, fill_value=0)
    session.store.set_virtual_ref("encoded/c/0", f"{VIRTUAL_PREFIX}my%20file.bin", offset=8, length=4)
    session.store.set_virtual_ref("encoded/c/1", f"{VIRTUAL_PREFIX}part-00.bin?versionId=7#frag", offset=6, length=4)
    session.commit("virtual chunks")
    expected.setdefault(name, {})["main"] = {
        "virtual": {"dtype": "int32", "shape": [2000], "values": [int(v) for v in values]},
        "encoded": {"dtype": "int32", "shape": [2], "values": [2, int(values[0])]},
        "_virtual_prefix": VIRTUAL_PREFIX,
    }


REPOS = {
    "codecs-v2": lambda: write_codecs_repo("codecs-v2", spec_version=2),
    "codecs-v1": lambda: write_codecs_repo("codecs-v1", spec_version=1),
    "virtual-v2": lambda: write_virtual_repo("virtual-v2", spec_version=2),
    "virtual-v1": lambda: write_virtual_repo("virtual-v1", spec_version=1),
}


def main() -> None:
    """Regenerate all repositories, or only those named on the command line."""
    names = sys.argv[1:] or list(REPOS)
    unknown = set(names) - set(REPOS)
    if unknown:
        sys.exit(f"unknown repositories: {sorted(unknown)}; choose from {sorted(REPOS)}")
    previous = {}
    if names != list(REPOS) and (OUT / "expected.json").exists():
        previous = json.loads((OUT / "expected.json").read_text())
        previous.pop("_meta", None)
    elif OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True, exist_ok=True)
    for name in names:
        if (OUT / name).exists():
            shutil.rmtree(OUT / name)
        previous.pop(name, None)
        REPOS[name]()
    expected.update({k: v for k, v in previous.items() if k not in expected})
    meta = {
        "icechunk": ic.__version__,
        "zarr": zarr.__version__,
        "numpy": np.__version__,
        "commit_properties": COMMIT_PROPERTIES,
    }
    (OUT / "expected.json").write_text(json.dumps({"_meta": meta, **expected}, indent=1))
    print("wrote", OUT, file=sys.stderr)


if __name__ == "__main__":
    main()
