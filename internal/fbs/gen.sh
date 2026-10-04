#!/usr/bin/env bash
# Regenerates the Go flatbuffers bindings from the upstream Icechunk schemas.
#
# schema/*.fbs are verbatim copies of icechunk-format/flatbuffers/*.fbs.
#
# flatc's Go backend does not support fixed-size arrays inside structs, which
# Icechunk uses for object ids (`bytes:[uint8:12]` / `bytes:[uint8:8]`). We
# rewrite those two structs into an equivalent layout (N single-byte fields:
# same size, alignment 1, same offsets) before running flatc, then expose the
# raw id bytes through the hand-written helpers in ids.go.
#
# Usage: FLATC=/path/to/flatc ./gen.sh   (flatc 25.12.19, same as upstream)
set -euo pipefail
cd "$(dirname "$0")"
FLATC="${FLATC:-flatc}"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
cp schema/*.fbs "$tmp/"
python3 - "$tmp/common.fbs" <<'PY'
import sys
p = sys.argv[1]
s = open(p).read()
def fields(n):
    return "\n".join(f"  b{i}:uint8;" for i in range(n))
s = s.replace("struct ObjectId12 {\n  bytes:[uint8:12];\n}", "struct ObjectId12 {\n" + fields(12) + "\n}")
s = s.replace("struct ObjectId8 {\n  bytes:[uint8:8];\n}", "struct ObjectId8 {\n" + fields(8) + "\n}")
assert "[uint8:" not in s, "object id structs were not rewritten"
open(p, "w").write(s)
PY
rm -f ./*_generated.go
"$FLATC" --go --go-namespace fbs --gen-all --gen-onefile -o "$tmp/out" "$tmp/all.fbs"
mv "$tmp"/out/*.go ./all_generated.go
gofmt -w all_generated.go
