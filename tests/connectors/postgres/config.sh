#!/usr/bin/env bash
# Postgres connector E2E test configuration.
# Sourced by e2e.sh orchestrator.

# Connectors to build: directory_name:wasm_output_name
CONNECTOR_BUILDS=(
    "source-postgres:source_postgres"
    "dest-postgres:dest_postgres"
    "transform-sql:transform_sql"
)

# Docker Compose file for this connector's infrastructure
CONNECTOR_COMPOSE_FILE="$CONNECTOR_DIR_PATH/docker-compose.yml"
