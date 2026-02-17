default:
    @just --list

build:
    cargo build

build-connectors:
    cd connectors/source-postgres && cargo build
    cd connectors/dest-postgres && cargo build

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
