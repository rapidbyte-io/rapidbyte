#!/usr/bin/env bash
set -euo pipefail

VAULT_ADDR="${VAULT_ADDR:-http://127.0.0.1:8200}"
VAULT_TOKEN="${VAULT_TOKEN:-rapidbyte-dev-vault-token}"

export VAULT_ADDR VAULT_TOKEN

echo "Seeding Vault test secrets..."

# Write test postgres credentials (matches docker-compose postgres service)
vault kv put secret/postgres \
  host=localhost \
  port=5433 \
  user=postgres \
  password=postgres \
  database=rapidbyte_test

echo "Vault secrets seeded."
