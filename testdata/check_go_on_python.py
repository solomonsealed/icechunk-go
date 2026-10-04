"""Verify, with icechunk-python, Python-written repositories that the Go
writer committed on top of (see TestWriteOnPythonRepos).

    ICECHUNK_GO_WRITE_DIR=/tmp/gw go test -run TestWriteOnPythonRepos ./internal/conformance
    python testdata/check_go_on_python.py /tmp/gw
"""

import json
import sys
from pathlib import Path

import numpy as np

import icechunk as ic
import zarr

HERE = Path(__file__).resolve().parent
zarr.config.set({"array.rectilinear_chunks": True})

CASES = {
    "split-repo-v2": ("upstream/split-repo-v2", "group1/split", (slice(2, 6), slice(2, 7)), 0.0),
    "expire-repo-v2-by-working-copy": ("upstream/expire-repo-v2-by-working-copy", "group1/data", (slice(3, 4), slice(0, 1)), -42),
    "codecs-v2": ("generated/codecs-v2", "codecs/sharded", (slice(5, 7), slice(4, 7)), np.array([[1, 2, 3], [4, 5, 6]])),
}


def arrays(group, prefix=""):
    for name, item in group.members():
        path = f"{prefix}{name}"
        if isinstance(item, zarr.Array):
            yield path, item
        else:
            yield from arrays(item, path + "/")


def same(a, b):
    if a.dtype.kind in "Mm":
        return np.array_equal(a.view("i8"), b.view("i8"))
    if a.dtype.kind in "fc":
        return np.array_equal(a, b, equal_nan=True)
    return np.array_equal(a, b)


def main() -> None:
    base = Path(sys.argv[1]) / "onpython"
    failures = []
    for name, (fixture, changed, region, value) in CASES.items():
        repo = ic.Repository.open(ic.local_filesystem_storage(str(base / name)))
        orig = ic.Repository.open(ic.local_filesystem_storage(str(HERE / fixture)))
        hist = list(repo.ancestry(branch="main"))
        if hist[0].message != "go commit on python repo" or hist[0].metadata != {"by": "go"}:
            failures.append(f"{name}: tip {hist[0].message!r} {hist[0].metadata}")
        if [h.message for h in hist[1:]] != [h.message for h in orig.ancestry(branch="main")]:
            failures.append(f"{name}: older history changed")
        new_root = zarr.open_group(repo.readonly_session(branch="main").store, mode="r")
        old_root = zarr.open_group(orig.readonly_session(branch="main").store, mode="r")
        for path, old in arrays(old_root):
            new = new_root[path][...]
            old = old[...]
            if path == changed:
                expected = old.copy()
                expected[region] = value
                if not same(new, expected):
                    failures.append(f"{name}: {path} does not contain the Go patch")
            elif not same(new, old):
                failures.append(f"{name}: {path} changed")
        diff = repo.diff(from_snapshot_id=hist[1].id, to_snapshot_id=hist[0].id)
        if set(diff.updated_chunks) != {"/" + changed}:
            failures.append(f"{name}: diff {diff.updated_chunks}")
        # And Python can keep committing.
        ws = repo.writable_session("main")
        zarr.open_group(ws.store, mode="r+").attrs["after_go"] = True
        ws.commit("python after go")
    print(json.dumps({"checked": list(CASES), "failures": failures}, indent=1))
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
