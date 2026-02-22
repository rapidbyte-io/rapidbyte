#!/usr/bin/env bash
# Postgres E2E teardown: stop Docker, clean state files.
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/lib.sh"

info "Stopping Docker Compose..."
docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true

# Clean all E2E state files
rm -f /tmp/rapidbyte_e2e_*.db 2>/dev/null || true
