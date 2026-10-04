#!/usr/bin/env bash
# Uploads an Icechunk repository directory into an R2 bucket under a prefix.
#
#   ./seed-r2.sh <repo-dir> <prefix> [--local|--remote]
#
# --local (default) fills the bucket simulated by `wrangler dev`; --remote
# uploads to the real bucket configured in wrangler.toml. For large
# repositories prefer rclone or `aws s3 sync` against R2's S3 API.
# Note: `wrangler r2 object put` percent-encodes keys with characters such
# as spaces (a file "a b" is stored as "a%20b"); Icechunk's own keys never
# contain them, but data files for virtual chunks might.
set -euo pipefail
cd "$(dirname "$0")"
src="$(cd "$1" && pwd)"
prefix="${2%/}"
mode="${3:---local}"
bucket="$(sed -n 's/^bucket_name *= *"\(.*\)"/\1/p' wrangler.toml | head -1)"
export bucket prefix mode src
# Local mode writes to a SQLite-backed simulation that does not like many
# concurrent writers, so uploads are retried.
upload() {
  for attempt in 1 2 3 4 5; do
    out=$(npx --yes wrangler@4 r2 object put "$bucket/$prefix/$1" --file "$src/$1" $mode 2>&1) && return 0
    sleep "$attempt"
  done
  echo "failed: $1: $out" >&2
  return 255
}
export -f upload
(cd "$src" && find . -type f -print0) | xargs -0 -P "${JOBS:-2}" -n 1 bash -c 'upload "${0#./}"'
echo "uploaded $src to $bucket/$prefix ($mode)"
