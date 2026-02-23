default:
    @just --list

build:
    cargo build

build-no-sccache:
    NO_SCCACHE=1 cargo build

build-connectors:
    cd connectors/source-postgres && cargo build
    cd connectors/dest-postgres && cargo build
    mkdir -p target/connectors
    cp connectors/source-postgres/target/wasm32-wasip2/debug/source_postgres.wasm target/connectors/
    cp connectors/dest-postgres/target/wasm32-wasip2/debug/dest_postgres.wasm target/connectors/

release:
    cargo build --release

release-connectors:
    cd connectors/source-postgres && cargo build --release
    cd connectors/dest-postgres && cargo build --release
    mkdir -p target/connectors
    cp connectors/source-postgres/target/wasm32-wasip2/release/source_postgres.wasm target/connectors/
    cp connectors/dest-postgres/target/wasm32-wasip2/release/dest_postgres.wasm target/connectors/
    ./scripts/strip-wasm.sh target/connectors/source_postgres.wasm target/connectors/source_postgres.wasm
    ./scripts/strip-wasm.sh target/connectors/dest_postgres.wasm target/connectors/dest_postgres.wasm

test:
    cargo test

check:
    cargo check --all-targets

check-no-sccache:
    NO_SCCACHE=1 cargo check --all-targets

fmt:
    cargo fmt --all

lint:
    cargo clippy --all-targets -- -D warnings

e2e *args="":
    ./tests/e2e.sh {{args}}

# Run a single E2E scenario (e.g. just e2e-scenario postgres/scenarios/01_full_refresh)
e2e-scenario path:
    RAPIDBYTE_CONNECTOR_DIR=target/connectors bash tests/connectors/{{path}}.sh

# Run benchmarks: bench [CONNECTOR] [ROWS] [--iters N] [--profile]
bench *args="":
    ./bench/bench.sh {{args}}

# Compare benchmarks between two git refs
bench-compare ref1 ref2 *args="":
    ./bench/compare.sh {{ref1}} {{ref2}} {{args}}

# Scaffold a new connector project
scaffold name output=("connectors/" + name):
    cargo run -- scaffold {{name}} --output {{output}}

clean:
    cargo clean

sccache-stats:
    sccache --show-stats
