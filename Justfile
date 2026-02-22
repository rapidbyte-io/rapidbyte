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
    cp connectors/source-postgres/manifest.json target/connectors/source_postgres.manifest.json
    cp connectors/dest-postgres/manifest.json target/connectors/dest_postgres.manifest.json

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

e2e:
    ./tests/e2e.sh

# Run a single E2E scenario by name (e.g. just e2e-scenario 01_full_refresh)
e2e-scenario name:
    RAPIDBYTE_CONNECTOR_DIR=target/connectors bash tests/e2e/{{name}}.sh

# Benchmark postgres connectors: INSERT vs COPY comparison (10K rows default)
bench-connector-postgres rows="10000":
    ./tests/bench.sh --rows {{rows}} --iters 3

# Scaffold a new connector project
scaffold name output=("connectors/" + name):
    cargo run -- scaffold {{name}} --output {{output}}

clean:
    cargo clean

# Profile a benchmark run and generate flamegraph
bench-profile rows="10000" mode="insert":
    ./tests/bench_profile.sh {{rows}} {{mode}}

# Benchmark with flamegraph profiling at the end
bench-connector-postgres-profile rows="10000":
    ./tests/bench.sh --rows {{rows}} --iters 3 --profile

# Run benchmark matrix across row counts
bench-matrix *args="":
    ./tests/bench_matrix.sh {{args}}

# Compare benchmarks between two git refs
bench-compare ref1 ref2 *args="":
    ./tests/bench_compare.sh {{ref1}} {{ref2}} {{args}}

# View/compare past benchmark results
bench-results *args="":
    python3 tests/bench_compare.py {{args}}

sccache-stats:
    sccache --show-stats
