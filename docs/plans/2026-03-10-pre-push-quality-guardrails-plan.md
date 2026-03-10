# Pre-Push Quality Guardrails Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a lightweight repo-managed `pre-push` hook that auto-formats Rust
changes where safe, stages those fixes, and aborts the push so contributors can
review and recommit before retrying.

**Architecture:** Extend the existing `.githooks` workflow with a dedicated
`pre-push` hook that mirrors the current `pre-commit` formatting strategy while
keeping push-time validation cheap. Documentation should describe the updated
two-hook model and the install flow remains `just install-hooks`.

**Tech Stack:** Bash, Git hooks, Rust formatting (`cargo fmt`), Justfile docs
and developer workflow commands.

---

### Task 1: Add the pre-push hook script

**Files:**
- Create: `.githooks/pre-push`
- Reference: `.githooks/pre-commit`

**Step 1: Write the hook script**

Implement a Bash hook that:
- resolves `repo_root`
- detects whether local tracked Rust-relevant files changed
- exits early when no Rust-relevant changes are present
- runs `cargo fmt --all`
- re-stages modified tracked files
- aborts with a clear message if formatting changed files
- otherwise runs `cargo fmt --all -- --check`

**Step 2: Syntax check the hook**

Run: `bash -n .githooks/pre-push`
Expected: exit code `0`

**Step 3: Commit**

```bash
git add .githooks/pre-push
git commit -m "feat: add pre-push formatting guardrail"
```

### Task 2: Update installation and docs

**Files:**
- Modify: `scripts/install-git-hooks.sh`
- Modify: `README.md`
- Modify: `CONTRIBUTING.md`

**Step 1: Update installer messaging if needed**

Keep `core.hooksPath=.githooks`, but clarify that both `pre-commit` and
`pre-push` are now repo-managed.

**Step 2: Document the new workflow**

Describe:
- `just install-hooks`
- what `pre-commit` does
- what `pre-push` does
- why a push may stop after auto-formatting
- that the user should review, commit, and push again

**Step 3: Commit**

```bash
git add scripts/install-git-hooks.sh README.md CONTRIBUTING.md
git commit -m "docs: document pre-push hook workflow"
```

### Task 3: Verify hook behavior end to end

**Files:**
- Test in-place against the worktree

**Step 1: Reinstall hooks**

Run: `just install-hooks`
Expected: `core.hooksPath=.githooks`

**Step 2: Verify shell syntax**

Run:
- `bash -n .githooks/pre-commit`
- `bash -n .githooks/pre-push`
- `bash -n scripts/install-git-hooks.sh`

Expected: all exit code `0`

**Step 3: Verify auto-fix behavior**

Create a temporary unformatted change in a Rust file, then run:
`./.githooks/pre-push`

Expected:
- hook runs `cargo fmt --all`
- affected file becomes staged
- hook exits non-zero
- printed message instructs review/commit/push again

**Step 4: Verify clean-path behavior**

After recommitting or restoring formatting, run:
`./.githooks/pre-push`

Expected: exit code `0`

**Step 5: Run repo verification**

Run:
- `cargo fmt --all -- --check`
- `cargo test -p rapidbyte-benchmarks`

Expected: both pass

**Step 6: Commit**

If any verification-driven adjustments were needed:

```bash
git add .githooks/pre-push scripts/install-git-hooks.sh README.md CONTRIBUTING.md
git commit -m "chore: finalize pre-push hook verification"
```
