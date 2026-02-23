#!/usr/bin/env bash
# Post-build step: strip custom sections from WASI P2 component .wasm files,
# then re-embed any rapidbyte_* custom sections.
#
# Usage: strip-wasm.sh <input.wasm> [output.wasm]
#
# If output is omitted, strips in-place.
# Requires: wasm-tools (https://github.com/bytecodealliance/wasm-tools)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

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

# Sections to preserve across stripping
SECTIONS=(rapidbyte_manifest_v1 rapidbyte_config_schema_v1)
TMPFILES=()

# Extract sections before stripping
for section in "${SECTIONS[@]}"; do
    tmp="${INPUT}.${section}.tmp"
    if python3 "$SCRIPT_DIR/extract-wasm-section.py" "$INPUT" "$section" "$tmp" 2>/dev/null; then
        TMPFILES+=("$section:$tmp")
    fi
done

# Strip all custom sections
if [[ "$INPUT" == "$OUTPUT" ]]; then
    TMP="${INPUT}.strip.tmp"
    wasm-tools strip "$INPUT" -o "$TMP"
    mv "$TMP" "$OUTPUT"
else
    wasm-tools strip "$INPUT" -o "$OUTPUT"
fi

# Re-embed preserved sections
for entry in "${TMPFILES[@]}"; do
    section="${entry%%:*}"
    tmp="${entry##*:}"
    python3 "$SCRIPT_DIR/embed-wasm-section.py" "$OUTPUT" "$section" "$tmp"
    rm -f "$tmp"
done
