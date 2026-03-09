# README Badges Design

## Problem

The README now has a real CI workflow and clearer contributor checks, but the header still lacks quick trust signals. Adding a compact badge row makes the project status legible at a glance for external evaluators and contributors.

## Design

Add a single badge row directly under `# Rapidbyte` in `README.md` with four badges:

- CI status badge pointing to `.github/workflows/ci.yml`
- Apache-2.0 license badge pointing to `LICENSE`
- Rust `1.75+` badge matching the documented minimum toolchain
- Docs/Contributing badge pointing to `CONTRIBUTING.md`

## Constraints

- Keep the row compact; no release/crates.io/docs.rs badges.
- Prefer stable badge URLs that do not depend on unpublished artifacts.
- Do not change the rest of the README structure.
