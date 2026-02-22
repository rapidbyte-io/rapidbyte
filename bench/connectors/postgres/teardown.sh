#!/usr/bin/env bash
# Postgres connector benchmark teardown.
# Docker is stopped by the main orchestrator's EXIT trap, not here.
# This handles connector-specific cleanup only.
set -euo pipefail

source "$(cd "$(dirname "$0")/../../lib" && pwd)/helpers.sh"

# Clean state DB
rm -f /tmp/rapidbyte_bench_state.db 2>/dev/null || \
    sudo rm -f /tmp/rapidbyte_bench_state.db 2>/dev/null || true
