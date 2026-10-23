"""Compare the objects the Go writer and icechunk-python store on S3: their
storage classes and user metadata, with the same storage settings. Each
writer then commits on top of the other's repository.

    python testdata/check_s3_objects.py <endpoint> <bucket>      # writes <bucket>/py
    ICECHUNK_S3_ENDPOINT=<endpoint> ICECHUNK_S3_BUCKET=<bucket> \\
        go test -run TestWriteToS3 ./internal/conformance        # writes <bucket>/go, commits on py
    python testdata/check_s3_objects.py <endpoint> <bucket>      # compares, commits on go

Any S3 endpoint works; a local one is `moto_server -p 5555` (pip install
"moto[server]" boto3). Credentials come from AWS_ACCESS_KEY_ID and
AWS_SECRET_ACCESS_KEY. Needs icechunk, zarr, numpy and boto3.
"""

import json
import os
import re
import sys

import boto3
import numpy as np

import icechunk as ic
import zarr

UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
# The storage settings of TestWriteToS3.
SETTINGS = dict(storage_class="STANDARD_IA", chunks_storage_class="INTELLIGENT_TIERING")


def main() -> None:
    endpoint, bucket = sys.argv[1], sys.argv[2]
    key, secret = os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"]
    s3 = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=key, aws_secret_access_key=secret, region_name="us-east-1")
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)

    def storage(prefix):
        return ic.s3_storage(bucket=bucket, prefix=prefix, endpoint_url=endpoint, allow_http=True, force_path_style=True,
                             access_key_id=key, secret_access_key=secret, region="us-east-1")

    def config():
        cfg = ic.RepositoryConfig.default()
        cfg.storage = ic.StorageSettings(**SETTINGS)
        return cfg

    def write(repo, path, message):
        s = repo.writable_session("main")
        a = zarr.create_array(s.store, name=path, shape=(200,), chunks=(100,), dtype="f8", fill_value=0.0, compressors=None)
        a[:] = np.arange(200) + 1.0
        s.commit(message)

    def exists(prefix):
        return s3.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}/repo").get("KeyCount", 0) > 0

    if not exists("py"):
        repo = ic.Repository.create(storage("py"), config=config())
        write(repo, "a", "write")
        repo.create_tag("t", repo.lookup_branch("main"))
        print(f"wrote {bucket}/py; now run TestWriteToS3, then this script again", file=sys.stderr)
        if not exists("go"):
            return

    failures = []

    def objects(prefix):
        out = {}
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}/"):
            for o in page.get("Contents", []):
                h = s3.head_object(Bucket=bucket, Key=o["Key"])
                out[o["Key"][len(prefix) + 1:]] = (h.get("StorageClass") or "STANDARD", h.get("Metadata", {}))
        return out

    def kind(k):
        return k if k == "repo" else k.split("/")[0]

    def describe(objs):
        """Per kind of object, the set of (storage class, metadata) its objects
        carry, with the write ids generalized."""
        out = {}
        for k, (cls, meta) in objs.items():
            m = dict(meta)
            if "icechunkwriteid" in m:
                if not UUID.match(m["icechunkwriteid"]):
                    failures.append(f"{k}: write id {m['icechunkwriteid']!r} is not a UUID")
                m["icechunkwriteid"] = "<uuid>"
            out.setdefault(kind(k), set()).add(json.dumps([cls, m], sort_keys=True))
        return {k: sorted(v) for k, v in sorted(out.items())}

    go_objs, py_objs = objects("go"), objects("py")
    go_desc, py_desc = describe(go_objs), describe(py_objs)
    # Go writes upstream's client name, at the release it is aligned with.
    clients = {m.get("icclient") for _, m in go_objs.values()} - {None}
    if clients != {f"ic-{ic.__version__}"}:
        failures.append(f"go icclient {clients}, python ic-{ic.__version__}")

    # py also holds the objects of Go's commit on top of it, which must
    # look the same.
    for k in sorted(set(go_desc) | set(py_desc)):
        if go_desc.get(k) != py_desc.get(k):
            failures.append(f"{k}: go {go_desc.get(k)} != python {py_desc.get(k)}")

    # icechunk-python reads the Go repository, and Go's commit on its own.
    go_repo = ic.Repository.open(storage("go"), config=config())
    a = zarr.open_array(go_repo.readonly_session(tag="t").store, path="a")
    if not np.array_equal(a[:], np.arange(200) + 1.0):
        failures.append("go repository: wrong values")
    py_repo = ic.Repository.open(storage("py"), config=config())
    hist = [s.message for s in py_repo.ancestry(branch="main")]
    if hist[:2] != ["go commit", "write"]:
        failures.append(f"py repository history {hist}")
    else:
        g = zarr.open_array(py_repo.readonly_session(branch="main").store, path="go")
        if not np.array_equal(g[:], np.arange(200) + 1.0):
            failures.append("go commit on py: wrong values")
    # And commits on top of the Go repository.
    if [s.message for s in go_repo.ancestry(branch="main")][0] != "python commit":
        write(go_repo, "py", "python commit")
    hist = [s.message for s in go_repo.ancestry(branch="main")]
    if hist[:2] != ["python commit", "write"]:
        failures.append(f"go repository history after the python commit {hist}")

    print(json.dumps({"go": go_desc, "python": py_desc, "failures": failures}, indent=1))
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
