#!/usr/bin/env bash
# Post-build step: strip custom sections from WASI P2 component .wasm files,
# then re-embed the connector manifest if provided.
#
# Usage: strip-wasm.sh <input.wasm> [output.wasm] [manifest.json]
#
# If output is omitted, strips in-place.
# Requires: wasm-tools (https://github.com/bytecodealliance/wasm-tools)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [[ $# -lt 1 ]]; then
    echo "Usage: strip-wasm.sh <input.wasm> [output.wasm] [manifest.json]" >&2
    exit 1
fi

INPUT="$1"
OUTPUT="${2:-$1}"
MANIFEST="${3:-}"

if ! command -v wasm-tools &>/dev/null; then
    echo "[WARN] wasm-tools not found; skipping strip (install: cargo install wasm-tools)" >&2
    if [[ "$INPUT" != "$OUTPUT" ]]; then
        cp "$INPUT" "$OUTPUT"
    fi
    exit 0
fi

if [[ "$INPUT" == "$OUTPUT" ]]; then
    TMP="${INPUT}.strip.tmp"
    wasm-tools strip "$INPUT" -o "$TMP"
    mv "$TMP" "$OUTPUT"
else
    wasm-tools strip "$INPUT" -o "$OUTPUT"
fi

# Re-embed manifest custom section after stripping
if [[ -n "$MANIFEST" && -f "$MANIFEST" ]]; then
    python3 "$SCRIPT_DIR/embed-wasm-section.py" "$OUTPUT" rapidbyte_manifest_v1 "$MANIFEST"
fi
