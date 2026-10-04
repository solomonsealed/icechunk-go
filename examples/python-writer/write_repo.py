"""Write an Icechunk repository with icechunk-python for Go programs to read.

This is the write half of the setup: Python (or Rust) owns commits,
conflict handling and maintenance; Go reads, e.g. from a Cloudflare Worker.

    pip install icechunk zarr numpy

    # into Cloudflare R2 (the bucket the Worker binds as REPO_BUCKET):
    export R2_ACCOUNT_ID=... R2_ACCESS_KEY_ID=... R2_SECRET_ACCESS_KEY=...
    python write_repo.py --r2-bucket icechunk-repos --prefix demo

    # or into a local directory (then e.g. ../worker/seed-r2.sh it):
    python write_repo.py --local ./demo-repo

Each run appends one day of synthetic data as a new commit on main.
"""

import argparse
import os
from datetime import datetime, timezone

import numpy as np

import icechunk as ic
import zarr


def open_storage(args: argparse.Namespace) -> ic.Storage:
    if args.local:
        return ic.local_filesystem_storage(args.local)
    return ic.r2_storage(
        bucket=args.r2_bucket,
        prefix=args.prefix,
        account_id=os.environ["R2_ACCOUNT_ID"],
        access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    )


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    where = p.add_mutually_exclusive_group(required=True)
    where.add_argument("--local", help="local directory for the repository")
    where.add_argument("--r2-bucket", help="R2 bucket name")
    p.add_argument("--prefix", default="demo", help="key prefix inside the R2 bucket")
    args = p.parse_args()

    repo = ic.Repository.open_or_create(open_storage(args))
    session = repo.writable_session("main")
    root = zarr.open_group(session.store, mode="a", attributes={"title": "icechunk-go demo"})

    ny, nx = 90, 180
    if "temperature" not in root:
        lat = root.create_array("lat", shape=(ny,), chunks=(ny,), dtype="float32", dimension_names=("lat",))
        lat[:] = np.linspace(-89, 89, ny, dtype="float32")
        lon = root.create_array("lon", shape=(nx,), chunks=(nx,), dtype="float32", dimension_names=("lon",))
        lon[:] = np.linspace(-179, 179, nx, dtype="float32")
        root.create_array(
            "temperature",
            shape=(0, ny, nx),
            chunks=(1, ny, nx),
            dtype="float32",
            fill_value=float("nan"),
            dimension_names=("time", "lat", "lon"),
            attributes={"units": "degC"},
        )
    temp = root["temperature"]
    day = temp.shape[0]
    temp.resize((day + 1, ny, nx))
    lat = np.linspace(-89, 89, ny)[:, None]
    lon = np.linspace(-179, 179, nx)[None, :]
    temp[day] = (25 * np.cos(np.radians(lat)) - 5 + 3 * np.sin(np.radians(lon + 10 * day))).astype("float32")

    snapshot = session.commit(
        f"add day {day}",
        metadata={"written_at": datetime.now(timezone.utc).isoformat(), "day": day},
    )
    print(f"committed day {day} as snapshot {snapshot}")


if __name__ == "__main__":
    main()
