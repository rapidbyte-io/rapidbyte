# Documentation & Contributor Onboarding Design

**Goal:** Make Rapidbyte ready for external contributors across three audiences: OSS community contributors, hired/contracted developers, and connector authors.

**Approach:** B+ — layered docs (README + CONTRIBUTING + Connector Guide) plus high-value GitHub scaffolding (PR template, LICENSE file).

## 1. README.md Refresh

Rewrite as the project's front door. Someone should understand what Rapidbyte is in 30 seconds.

**Structure:**
1. Hero — one-liner, what it replaces, key differentiators
2. Features — implemented only, no planned items
3. Quick Start — `just dev-up` / `just run` flow
4. CLI — command table + verbosity flags (`-v`, `-vv`, `--quiet`)
5. Pipeline Configuration — minimal YAML example
6. Connectors — table of implemented connectors + link to connector dev guide
7. Architecture — short paragraph + ASCII diagram (implemented layers only)
8. Development — Justfile commands table
9. Contributing — pointer to CONTRIBUTING.md
10. License — Apache-2.0

**Key changes:** Remove detailed pipeline config (keep minimal), update dev commands to new Justfile, add verbosity flags, link to new docs.

## 2. CONTRIBUTING.md

Clone-to-first-PR in one doc, with three paths for three audiences.

**Structure:**
1. Welcome — 2 sentences
2. Getting Started — prerequisites (Rust 1.75+, just, Docker), `just dev-up`, verify
3. Three contribution paths: bug fixes/features, new connectors (link to guide), docs/tests
4. Code standards — pointer to CODING_STYLE.md, key rules highlighted
5. PR process — branch naming, PR description, link to template
6. Testing expectations — `just fmt`, `just lint`, `just test`, `just e2e`
7. Architecture overview — crate dependency graph, links to PROTOCOL.md and CODING_STYLE.md

## 3. Connector Developer Guide (`docs/CONNECTOR_DEV.md`)

The most important doc. Build a working connector from zero without reading engine code.

**Structure:**
1. Overview — what a connector is, three roles
2. Scaffold — `rapidbyte scaffold my-source`
3. Project structure — standard module layout
4. Manifest (`build.rs`) — ManifestBuilder API, examples per role, permissions, config schema
5. Config type — derive macros, schema annotations, validate(), secrets
6. Implementing the trait — lifecycle methods with real code examples (Source, Destination, Transform)
7. Networking — HostTcpStream, why no raw sockets, usage with async clients
8. Data flow — Arrow IPC, FrameWriter, next_batch, frame lifecycle
9. Error handling — ConnectorError factories, categories, retry semantics
10. Testing — build, test pipeline YAML, `just run --dry-run --limit`
11. Publishing — release build, wasm placement, connector resolution

Every section has concrete code examples.

## 4. GitHub Scaffolding

**PR template (`.github/pull_request_template.md`):**
- Summary (what/why)
- Checklist from CODING_STYLE.md S18
- Verification section
- Fits on one screen

**LICENSE file:**
- Root Apache-2.0 full text (README claims it but file is missing)

**Not included (add later when volume justifies):** Issue templates, code of conduct, CODEOWNERS, CI workflows.
