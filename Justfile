default:
    @just --list

build:
    cargo build --release

build-connectors:
    cd connectors/source-postgres && cargo build --release
    cd connectors/dest-postgres && cargo build --release

test:
    cargo test

check:
    cargo check --all-targets

fmt:
    cargo fmt --all

lint:
    cargo clippy --all-targets -- -D warnings

clean:
    cargo clean
