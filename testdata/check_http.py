"""Cross-check the Go server against icechunk-python.

Reads every array of a local repository with icechunk-python, then reads the
same arrays through the Go service's read-only Zarr endpoint
(<base-url>/zarr/<ref>/...) with zarr-python, plus the JSON /array endpoint,
and compares the values.

    python testdata/check_http.py <local-repo-dir> <base-url> [ref]

Works against `icechunk-go serve` and against the Cloudflare Worker
(`wrangler dev`). Needs icechunk, zarr, numpy, fsspec, aiohttp, requests.
"""

import json
import sys

import numpy as np
import requests

import icechunk as ic
import zarr

zarr.config.set({"array.rectilinear_chunks": True})


def arrays(group, prefix=""):
    for name, item in group.members():
        path = f"{prefix}{name}"
        if isinstance(item, zarr.Array):
            yield path, item
        else:
            yield from arrays(item, path + "/")


def same(a: np.ndarray, b: np.ndarray) -> bool:
    if a.shape != b.shape or a.dtype != b.dtype:
        return False
    if a.dtype.kind in "Mm":  # NaT != NaT, compare the int64 counts
        return bool(np.array_equal(a.view("i8"), b.view("i8")))
    if a.dtype.kind in "fc":
        return bool(np.array_equal(a, b, equal_nan=True))
    return bool(np.array_equal(a, b))


def main() -> None:
    local, base = sys.argv[1], sys.argv[2].rstrip("/")
    ref = sys.argv[3] if len(sys.argv) > 3 else "main"
    repo = ic.Repository.open(ic.local_filesystem_storage(local))
    session = repo.readonly_session(branch=ref)
    root = zarr.open_group(session.store, mode="r")
    failures, checked = [], 0
    for path, arr in sorted(arrays(root)):
        want = arr[...]
        remote = zarr.open_array(f"{base}/zarr/{ref}/{path}", mode="r")
        got = remote[...]
        if not same(np.asarray(want), np.asarray(got)):
            failures.append(f"zarr {path}: values differ")
        # A sub-region exercises partial (ranged) shard reads.
        if arr.ndim >= 1 and arr.size > 0:
            region = tuple(slice(n // 3, n // 3 + max(1, n // 2)) for n in arr.shape)
            if not same(np.asarray(arr[region]), np.asarray(remote[region])):
                failures.append(f"zarr {path}{region}: region differs")
        # The JSON endpoint, for numeric arrays.
        if want.dtype.kind in "biuf" and want.size <= 50000:
            r = requests.get(f"{base}/array/{path}", params={"ref": ref}, timeout=60)
            if r.status_code != 200:
                failures.append(f"json {path}: HTTP {r.status_code} {r.text[:200]}")
            else:
                body = r.json()
                # JSON has no NaN/Infinity: the service encodes them as null.
                expect = [
                    None if isinstance(v, float) and not np.isfinite(v) else v
                    for v in np.asarray(want).reshape(-1).tolist()
                ]
                if body["shape"] != list(want.shape) or body["data"] != expect:
                    failures.append(f"json {path}: values differ")
        checked += 1
    print(json.dumps({"checked_arrays": checked, "failures": failures}, indent=1))
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
