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

# Quick benchmark: 1K rows, 3 iterations
bench:
    ./tests/bench.sh --rows 1000 --iters 3

# Full benchmark: multiple data sizes
bench-full:
    @echo "=== 1K rows ==="
    ./tests/bench.sh --rows 1000 --iters 3
    @echo ""
    @echo "=== 10K rows ==="
    ./tests/bench.sh --rows 10000 --iters 3
    @echo ""
    @echo "=== 100K rows ==="
    ./tests/bench.sh --rows 100000 --iters 3

# Benchmark with COPY mode
bench-copy:
    ./tests/bench.sh --rows 1000 --iters 3 --mode copy

# Compare INSERT vs COPY at multiple data sizes
bench-compare:
    @echo "=== INSERT mode (1K rows) ==="
    ./tests/bench.sh --rows 1000 --iters 3 --mode insert
    @echo ""
    @echo "=== COPY mode (1K rows) ==="
    ./tests/bench.sh --rows 1000 --iters 3 --mode copy
    @echo ""
    @echo "=== INSERT mode (10K rows) ==="
    ./tests/bench.sh --rows 10000 --iters 3 --mode insert
    @echo ""
    @echo "=== COPY mode (10K rows) ==="
    ./tests/bench.sh --rows 10000 --iters 3 --mode copy

# Compare AOT vs non-AOT module load times
bench-aot:
    @echo "=== With AOT (10K rows) ==="
    ./tests/bench.sh --rows 10000 --iters 3 --mode insert
    @echo ""
    @echo "=== Without AOT (10K rows) ==="
    ./tests/bench.sh --rows 10000 --iters 3 --mode insert --no-aot

clean:
    cargo clean
