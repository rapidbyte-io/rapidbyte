#!/usr/bin/env bash
# Post-build step: strip debug custom sections from WASI P2 component .wasm
# files while preserving rapidbyte_* application sections.
#
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

# Delete only known debug/build-metadata sections, preserving rapidbyte_*
# application sections (manifest, config schema).
if [[ "$INPUT" == "$OUTPUT" ]]; then
    TMP="${INPUT}.strip.tmp"
    wasm-tools strip "$INPUT" \
        -d '^\.(debug_|reloc\.)' \
        -d '^(target_features|producers|linking)$' \
        -o "$TMP"
    mv "$TMP" "$OUTPUT"
else
    wasm-tools strip "$INPUT" \
        -d '^\.(debug_|reloc\.)' \
        -d '^(target_features|producers|linking)$' \
        -o "$OUTPUT"
fi
