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

clean:
    cargo clean
