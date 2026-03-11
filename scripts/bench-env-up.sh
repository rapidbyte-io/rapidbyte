#!/usr/bin/env bash

set -euo pipefail

git_common_dir="$(git rev-parse --path-format=absolute --git-common-dir)"
repo_root="$(dirname "$git_common_dir")"
compose_project="rapidbyte-bench"

cd "$repo_root"
docker compose -f benchmarks/docker-compose.yml -p "$compose_project" up -d --wait

docker exec "${compose_project}-postgres-1" \
  psql -U postgres -d rapidbyte_test -c "ALTER USER postgres WITH PASSWORD 'postgres';" \
  >/dev/null
