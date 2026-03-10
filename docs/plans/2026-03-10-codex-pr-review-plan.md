# Codex PR Review Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add an advisory GitHub Actions workflow that runs `openai/codex-action`
on every non-draft pull request from branches in this repository and posts a
concise review summary back to the PR.

**Architecture:** Keep Codex review isolated from deterministic CI by adding a
dedicated workflow at `.github/workflows/codex-review.yml`. Gate execution to
same-repo non-draft pull requests, run Codex in a read-only sandbox with
`drop-sudo`, then use a separate follow-up job to publish the result. Document
the advisory behavior in the PR template and contributor docs.

**Tech Stack:** GitHub Actions YAML, `openai/codex-action`, repository docs,
GitHub pull request template Markdown.

---

### Task 1: Add the Codex review workflow

**Files:**
- Create: `.github/workflows/codex-review.yml`
- Reference: `.github/workflows/ci.yml`
- Reference: `.github/workflows/bench-pr.yml`

**Step 1: Write the failing workflow draft**

Create `.github/workflows/codex-review.yml` with:
- `name: Codex Review`
- `on.pull_request.types` set to `opened`, `synchronize`, and `ready_for_review`
- a job-level `if:` guard that skips drafts and forked PRs
- explicit minimal `permissions`
- checkout plus explicit fetch of base and head SHAs
- a `codex-review` job that runs `openai/codex-action@v1`
- `sandbox: read-only`
- `safety-strategy: drop-sudo`
- `OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}`
- a review prompt focused on bugs, regressions, security issues, and missing
  tests in the PR diff only
- a second `post-review` job that posts the saved review output back to the PR

**Step 2: Verify the initial workflow is incomplete on purpose**

Run:

```bash
sed -n '1,240p' .github/workflows/codex-review.yml
```

Expected:
- the skeleton exists
- any missing wiring for result handoff or comment posting is visible before
  refinement

**Step 3: Implement the minimal complete workflow**

Refine the workflow so that:
- the first job produces durable output for the second job
- the second job uses PR-scoped permissions only for posting
- the workflow remains advisory and separate from `.github/workflows/ci.yml`

**Step 4: Re-read the final workflow**

Run:

```bash
sed -n '1,260p' .github/workflows/codex-review.yml
```

Expected:
- trigger, guards, permissions, checkout/fetch, Codex action config, and
  result-posting job all appear in one coherent workflow

**Step 5: Commit**

```bash
git add .github/workflows/codex-review.yml
git commit -m "ci: add codex PR review workflow"
```

### Task 2: Document the advisory review in PR-facing files

**Files:**
- Modify: `.github/pull_request_template.md`
- Modify: `CONTRIBUTING.md`

**Step 1: Update the PR template**

Add a brief note that same-repo non-draft PRs receive an advisory Codex review
in GitHub Actions, and that authors should treat it as supplemental review
signal rather than a merge gate.

**Step 2: Update contributor/maintainer docs**

Add a short note to `CONTRIBUTING.md` covering:
- the workflow name
- that it runs only for same-repo PRs
- that maintainers must configure the `OPENAI_API_KEY` Actions secret
- that the workflow is advisory-only for now

**Step 3: Re-read the doc changes**

Run:

```bash
sed -n '1,220p' .github/pull_request_template.md
sed -n '1,260p' CONTRIBUTING.md
```

Expected:
- the PR template note is concise and contributor-facing
- the contributor doc note includes the secret/setup detail and advisory scope

**Step 4: Commit**

```bash
git add .github/pull_request_template.md CONTRIBUTING.md
git commit -m "docs: describe codex PR review"
```

### Task 3: Verify the workflow and docs end to end

**Files:**
- Verify in-place against:
  - `.github/workflows/codex-review.yml`
  - `.github/pull_request_template.md`
  - `CONTRIBUTING.md`

**Step 1: Check for malformed diffs**

Run:

```bash
git diff --check
```

Expected: no output

**Step 2: Re-read the workflow with focus on safety**

Run:

```bash
sed -n '1,260p' .github/workflows/codex-review.yml
```

Expected:
- `pull_request`, not `pull_request_target`
- same-repo and non-draft guard present
- Codex job uses `read-only` sandbox and `drop-sudo`
- result posting is isolated to a follow-up job

**Step 3: Re-read the user-facing documentation**

Run:

```bash
sed -n '1,220p' .github/pull_request_template.md
sed -n '1,260p' CONTRIBUTING.md
```

Expected:
- docs match the implemented workflow behavior
- advisory-only wording is consistent

**Step 4: Capture the final change set**

Run:

```bash
git status --short
git log --oneline -n 3
```

Expected:
- only the intended files are modified or committed
- the recent commits reflect workflow and doc changes clearly

**Step 5: Commit**

If verification required any cleanup:

```bash
git add .github/workflows/codex-review.yml .github/pull_request_template.md CONTRIBUTING.md
git commit -m "chore: finalize codex PR review integration"
```
