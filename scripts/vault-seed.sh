#!/usr/bin/env bash
set -euo pipefail

echo "Seeding Vault test secrets..."

# Run vault CLI inside the container — no host vault binary required.
docker compose exec -T vault vault kv put secret/postgres \
  host=localhost \
  port=5433 \
  user=postgres \
  password=postgres \
  database=rapidbyte_test

echo "Vault secrets seeded."
