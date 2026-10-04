"""Verify a repository written by the Go writer with icechunk-python, then
extend it with a Python commit for the Go side to read back.

    ICECHUNK_GO_WRITE_DIR=/tmp/gw go test -run TestWriteForPython ./internal/conformance
    python testdata/check_go_writer.py /tmp/gw
"""

import json
import sys

import numpy as np

import icechunk as ic
import zarr


def main() -> None:
    d = sys.argv[1]
    exp = json.load(open(f"{d}/expected.json"))
    repo = ic.Repository.open(ic.local_filesystem_storage(f"{d}/repo"))
    failures = []

    def check(cond, msg):
        if not cond:
            failures.append(msg)

    hist = list(repo.ancestry(branch="main"))
    check([h.message for h in hist] == exp["history"], f"history {[h.message for h in hist]}")
    c1 = [h for h in hist if h.id == exp["snapshots"]["c1"]][0]
    check(c1.metadata == exp["commit_meta"], f"commit metadata {c1.metadata}")
    check(sorted(repo.list_branches()) == ["feature/go", "main"], f"branches {repo.list_branches()}")
    check(sorted(repo.list_tags()) == ["v1"], f"tags {repo.list_tags()}")
    check(repo.lookup_tag("v1") == exp["snapshots"]["c2"], "tag v1")
    check(repo.lookup_branch("feature/go") == exp["snapshots"]["c1"], "branch feature/go")
    try:
        repo.create_tag("gone", exp["snapshots"]["c1"])
        failures.append("recreating a deleted tag succeeded")
    except ic.IcechunkError:
        pass

    s = repo.readonly_session(branch="main")
    root = zarr.open_group(s.store, mode="r")
    check(dict(root.attrs) == {"title": "written by go", "n": 7}, f"root attrs {dict(root.attrs)}")
    check(sorted(root.group_keys()) == ["codecs", "group"], f"groups {sorted(root.group_keys())}")  # codecs: parent created by CreateArray
    small = root["group/small"][...]
    check(small.tolist() == exp["small"], f"small {small.tolist()}")
    big = root["group/big"][...]
    check(big.shape == (450,) and big.tolist() == exp["big_values"], f"big {big.shape}")

    # Arrays encoded by the Go zarr package, decoded by zarr-python.
    import hashlib
    for path, e in exp["arrays"].items():
        try:
            arr = root[path]
            data = arr[...]
        except Exception as err:
            failures.append(f"{path}: cannot read: {err}")
            continue
        if arr.shape != (13, 11) or dict(arr.attrs) != {"codec": path.split("/")[-1]}:
            failures.append(f"{path}: shape {arr.shape} attrs {dict(arr.attrs)}")
        if list(arr.metadata.dimension_names or []) != ["y", None]:
            failures.append(f"{path}: dimension names {arr.metadata.dimension_names}")
        if "strings" in e:
            if [str(x) for x in data.reshape(-1)] != e["strings"]:
                failures.append(f"{path}: strings differ")
        else:
            le = np.ascontiguousarray(data.astype(data.dtype.newbyteorder("<")))
            if hashlib.sha256(le.tobytes()).hexdigest() != e["sha256"]:
                failures.append(f"{path}: values differ")

    # Transaction logs: diffs between Go commits.
    diff = repo.diff(from_snapshot_id=exp["snapshots"]["c1"], to_snapshot_id=exp["snapshots"]["c2"])
    check(diff.updated_arrays == {"/group/big"}, f"diff updated arrays {diff.updated_arrays}")
    check(diff.new_groups == {"/doomed"}, f"diff new groups {diff.new_groups}")
    check(sorted(diff.updated_chunks["/group/small"]) == [[0], [2]], f"diff chunks {diff.updated_chunks}")
    diff = repo.diff(from_snapshot_id=exp["snapshots"]["c2"], to_snapshot_id=exp["snapshots"]["c3"])
    check(diff.deleted_groups == {"/doomed"}, f"diff deleted {diff.deleted_groups}")

    ops = [type(u).__name__ for u in repo.ops_log()] if hasattr(repo, "ops_log") else []
    print("ops log:", ops)

    # Python commits on top of the Go history.
    ws = repo.writable_session("main")
    g = zarr.open_group(ws.store, mode="r+")
    a = g.create_array("from_python", shape=(4,), chunks=(2,), dtype="float64", fill_value=0.0)
    a[...] = np.array([0.5, 1.5, 2.5, 3.5])
    g["group/small"][0:2] = np.array([10, 20], dtype="int32")
    snap = ws.commit("python commit on go history", metadata={"by": "python"})
    repo.create_tag("from-python", snap)
    print(json.dumps({"failures": failures, "python_commit": snap}, indent=1))
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
