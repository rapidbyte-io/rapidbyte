#!/usr/bin/env bash
# Postgres E2E test helpers. Source this from scenarios.
# Layers on top of tests/lib/helpers.sh with PG-specific functions.

_PG_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$_PG_LIB_DIR/../../lib/helpers.sh"

# Set COMPOSE_FILE for postgres (used by pg_exec/pg_cmd in shared helpers)
COMPOSE_FILE="$_PG_LIB_DIR/docker-compose.yml"

# PG fixture paths
PG_FIXTURES="$_PG_LIB_DIR/fixtures"
PG_PIPELINES="$PG_FIXTURES/pipelines"
