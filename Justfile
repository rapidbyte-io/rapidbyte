default:
    @just --list

build:
    cargo build

build-no-sccache:
    NO_SCCACHE=1 cargo build

build-connectors:
    cd connectors/source-postgres && cargo build
    cd connectors/dest-postgres && cargo build
    cd connectors/transform-sql && cargo build
    mkdir -p target/connectors
    cp connectors/source-postgres/target/wasm32-wasip2/debug/source_postgres.wasm target/connectors/
    cp connectors/dest-postgres/target/wasm32-wasip2/debug/dest_postgres.wasm target/connectors/
    cp connectors/transform-sql/target/wasm32-wasip2/debug/transform_sql.wasm target/connectors/

release:
    cargo build --release

release-connectors:
    cd connectors/source-postgres && cargo build --release
    cd connectors/dest-postgres && cargo build --release
    cd connectors/transform-sql && cargo build --release
    mkdir -p target/connectors
    cp connectors/source-postgres/target/wasm32-wasip2/release/source_postgres.wasm target/connectors/
    cp connectors/dest-postgres/target/wasm32-wasip2/release/dest_postgres.wasm target/connectors/
    cp connectors/transform-sql/target/wasm32-wasip2/release/transform_sql.wasm target/connectors/
    ./scripts/strip-wasm.sh target/connectors/source_postgres.wasm target/connectors/source_postgres.wasm
    ./scripts/strip-wasm.sh target/connectors/dest_postgres.wasm target/connectors/dest_postgres.wasm
    ./scripts/strip-wasm.sh target/connectors/transform_sql.wasm target/connectors/transform_sql.wasm

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
    cargo test --manifest-path tests/e2e/Cargo.toml {{args}}

# Run benchmarks: bench [CONNECTOR] [ROWS] --profile PROFILE [--iters N] [--cpu-profile]
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
