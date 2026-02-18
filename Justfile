default:
    @just --list

build:
    cargo build

build-connectors:
    cd connectors/source-postgres && cargo build
    cd connectors/dest-postgres && cargo build
    mkdir -p target/connectors
    cp connectors/source-postgres/target/wasm32-wasip1/debug/source_postgres.wasm target/connectors/
    cp connectors/dest-postgres/target/wasm32-wasip1/debug/dest_postgres.wasm target/connectors/
    cp connectors/source-postgres/manifest.json target/connectors/source_postgres.manifest.json
    cp connectors/dest-postgres/manifest.json target/connectors/dest_postgres.manifest.json

test:
    cargo test

check:
    cargo check --all-targets

fmt:
    cargo fmt --all

lint:
    cargo clippy --all-targets -- -D warnings

e2e:
    ./tests/e2e.sh

# Benchmark postgres connectors: INSERT vs COPY comparison (10K rows default)
bench-connector-postgres rows="10000":
    ./tests/bench.sh --rows {{rows}} --iters 3

clean:
    cargo clean
