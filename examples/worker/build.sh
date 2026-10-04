#!/usr/bin/env bash
# Builds the Go WebAssembly module and copies Go's JS support file next to it.
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p build
GOOS=js GOARCH=wasm go build -trimpath -ldflags="-s -w" -o build/app.wasm .
goroot="$(go env GOROOT)"
if [ -f "$goroot/lib/wasm/wasm_exec.js" ]; then
  cp "$goroot/lib/wasm/wasm_exec.js" build/
else
  cp "$goroot/misc/wasm/wasm_exec.js" build/ # Go < 1.24
fi
raw=$(wc -c < build/app.wasm | tr -d ' ')
gz=$(gzip -9 -c build/app.wasm | wc -c | tr -d ' ')
echo "build/app.wasm: $raw bytes ($gz bytes gzipped)"
