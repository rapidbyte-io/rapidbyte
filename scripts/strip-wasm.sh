#!/usr/bin/env bash
# Post-build step: strip custom sections from WASI P2 component .wasm files.
# Usage: strip-wasm.sh <input.wasm> [output.wasm]
#
# If output is omitted, strips in-place.
# Requires: wasm-tools (https://github.com/bytecodealliance/wasm-tools)
set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: strip-wasm.sh <input.wasm> [output.wasm]" >&2
    exit 1
fi

INPUT="$1"
OUTPUT="${2:-$1}"

if ! command -v wasm-tools &>/dev/null; then
    echo "[WARN] wasm-tools not found; skipping strip (install: cargo install wasm-tools)" >&2
    if [[ "$INPUT" != "$OUTPUT" ]]; then
        cp "$INPUT" "$OUTPUT"
    fi
    exit 0
fi

if [[ "$INPUT" == "$OUTPUT" ]]; then
    TMP="${INPUT}.strip.tmp"
    wasm-tools strip --all "$INPUT" -o "$TMP"
    mv "$TMP" "$OUTPUT"
else
    wasm-tools strip --all "$INPUT" -o "$OUTPUT"
fi
