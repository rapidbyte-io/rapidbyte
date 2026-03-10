# Benchmark MiB Label And Pre-Push Lint Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Rename the benchmark summary bandwidth label to `MiB/sec` and make `pre-push` run `just lint` so clippy failures are blocked locally.

**Architecture:** Keep the benchmark artifact schema unchanged while updating only the rendered summary label and related docs. Extend the existing repo-managed `pre-push` hook by preserving its formatting behavior and adding a `just lint` gate after formatting passes cleanly.

**Tech Stack:** Rust, Bash, Just, Markdown docs, Cargo clippy.

---

### Task 1: Rename the rendered bandwidth label to `MiB/sec`

**Files:**
- Modify: `benchmarks/src/summary.rs`
- Test: `benchmarks/src/summary.rs`

**Step 1: Write the failing test**

Update or add a summary rendering test that asserts the rendered report contains
`MiB/sec` and does not contain `MB/sec`.

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p rapidbyte-benchmarks summary::tests::render_summary_report_uses_mib_label
```

Expected:
- FAIL because the summary currently renders `MB/sec`

**Step 3: Write minimal implementation**

Update `benchmarks/src/summary.rs` to render `MiB/sec` in the bandwidth line.
Do not rename `mb_per_sec` in artifacts or code structure beyond presentation
and test names.

**Step 4: Run test to verify it passes**

Run:

```bash
cargo test -p rapidbyte-benchmarks summary::tests::render_summary_report_uses_mib_label
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add benchmarks/src/summary.rs
git commit -m "fix(bench): label summary bandwidth in mib per second"
```

### Task 2: Make `pre-push` enforce `just lint`

**Files:**
- Modify: `.githooks/pre-push`

**Step 1: Write the failing expectation**

Re-read the hook and confirm it currently stops after formatting validation and
does not invoke `just lint`.

**Step 2: Verify the current gap**

Run:

```bash
sed -n '1,220p' .githooks/pre-push
```

Expected:
- no `just lint` invocation

**Step 3: Write minimal implementation**

Update `.githooks/pre-push` so that:

- formatting behavior stays unchanged
- if formatting changed files, the hook still aborts before lint
- if formatting is clean, the hook runs `just lint`

**Step 4: Syntax check the hook**

Run:

```bash
bash -n .githooks/pre-push
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add .githooks/pre-push
git commit -m "chore(hooks): run lint during pre-push"
```

### Task 3: Update docs for `MiB/sec` and the stricter push gate

**Files:**
- Modify: `docs/BENCHMARKING.md`
- Modify: `README.md`
- Modify: `CONTRIBUTING.md`

**Step 1: Write the failing expectation**

Identify stale user-facing wording:

- docs still say `MB/sec` for rendered summaries
- hook docs still imply `pre-push` is formatting-only

**Step 2: Re-read current files to confirm the gap**

Run:

```bash
rg -n "MB/sec|pre-push" docs/BENCHMARKING.md README.md CONTRIBUTING.md
```

Expected:
- stale `MB/sec` and formatting-only push-hook wording remains

**Step 3: Write minimal implementation**

Update docs to:

- refer to rendered summary bandwidth as `MiB/sec`
- explain that `pre-push` now runs lint after formatting succeeds
- keep `just ci` documented as the full pre-PR gate

**Step 4: Re-read docs**

Run:

```bash
rg -n "MiB/sec|pre-push|just lint" docs/BENCHMARKING.md README.md CONTRIBUTING.md
```

Expected:
- docs reflect the new label and push-hook behavior

**Step 5: Commit**

```bash
git add docs/BENCHMARKING.md README.md CONTRIBUTING.md
git commit -m "docs: update benchmark units and pre-push lint guidance"
```

### Task 4: Verify the combined change

**Files:**
- Verify in-place against:
  - `benchmarks/src/summary.rs`
  - `.githooks/pre-push`
  - `docs/BENCHMARKING.md`
  - `README.md`
  - `CONTRIBUTING.md`

**Step 1: Check malformed diffs**

Run:

```bash
git diff --check
```

Expected:
- no output

**Step 2: Run targeted benchmark tests**

Run:

```bash
cargo test -p rapidbyte-benchmarks summary::
```

Expected:
- PASS

**Step 3: Run lint**

Run:

```bash
just lint
```

Expected:
- PASS

**Step 4: Verify the hook script**

Run:

```bash
bash -n .githooks/pre-push
```

Expected:
- PASS

**Step 5: Smoke-check rendered output**

Run:

```bash
just bench-summary target/benchmarks/lab/pg_dest_copy_release.jsonl
```

Expected:
- rendered output contains `MiB/sec`

**Step 6: Commit**

If any verification-driven adjustments were required:

```bash
git add benchmarks/src/summary.rs .githooks/pre-push docs/BENCHMARKING.md README.md CONTRIBUTING.md
git commit -m "chore: finalize benchmark unit and pre-push lint verification"
```
