# Codex PR Review Design

## Summary

Add an advisory GitHub Actions workflow that runs OpenAI's `codex-action` on
every non-draft pull request opened from branches in this repository. The
workflow should review the PR diff, highlight likely bugs and test gaps, and
post the result back to the pull request without becoming a required merge
gate.

## Goals

- Add Codex-powered review coverage to same-repo pull requests.
- Keep the integration isolated from deterministic CI jobs.
- Limit trust and secret exposure by excluding forked PRs.
- Make the output visible in the PR while keeping it advisory only.

## Non-Goals

- Blocking merges on Codex output in the first rollout.
- Running Codex on forked pull requests.
- Allowing the workflow to write code or mutate the repository.
- Replacing human review or existing CI checks in `.github/workflows/ci.yml`.

## Alternatives Considered

### 1. Recommended: dedicated advisory workflow

Create a separate `.github/workflows/codex-review.yml` workflow triggered by
`pull_request` events and scoped to same-repo PRs.

Pros:
- Keeps AI review separate from required CI.
- Easier to tune, disable, or promote later.
- Matches `codex-action` guidance around isolating result-posting behavior.

Cons:
- Adds another workflow file and another PR check entry.

### 2. Add a Codex job to the existing CI workflow

Extend `.github/workflows/ci.yml` with an extra advisory review job.

Pros:
- Fewer workflow files.

Cons:
- Mixes advisory AI review with deterministic validation.
- Makes branch-protection and failure semantics harder to reason about.

### 3. Label-gated or manual workflow

Run Codex only on demand via label or `workflow_dispatch`.

Pros:
- Lower cost and lower operational noise.

Cons:
- Does not satisfy the requirement to review every repository PR.

## Recommended Design

Add a new GitHub Actions workflow with two jobs:

1. A `codex-review` job that checks out the pull request, fetches the exact
   base and head commits needed for diffing, and runs `openai/codex-action`
   with a tightly scoped review prompt.
2. A follow-up `post-review` job that reads the Codex output artifact or step
   output and posts a PR comment or job summary.

The workflow should trigger on:
- `pull_request` with `opened`
- `pull_request` with `synchronize`
- `pull_request` with `ready_for_review`

The workflow should skip when:
- the pull request is a draft
- `github.event.pull_request.head.repo.full_name != github.repository`

Security and execution rules:
- use `pull_request`, not `pull_request_target`
- provide `OPENAI_API_KEY` from GitHub Actions secrets
- run Codex with `sandbox: read-only`
- set `safety-strategy: drop-sudo`
- keep workflow permissions minimal
- separate result posting into a second job rather than letting the Codex step
  both analyze code and post content directly

Prompt and output rules:
- review only the diff from base SHA to head SHA
- prioritize bugs, regressions, security issues, and missing tests
- avoid style-only feedback unless it masks correctness risk
- keep findings concise and actionable
- treat the result as advisory only

## Change Scope

### New file

- `.github/workflows/codex-review.yml`

### Modified files

- `.github/pull_request_template.md`
  - mention that same-repo PRs receive advisory Codex review
- `README.md` or `CONTRIBUTING.md`
  - add brief maintainer setup notes for the required Actions secret if the
    repository documents CI configuration there

## Error Handling

- If the workflow runs on a forked PR, the job should skip cleanly before
  secrets are needed.
- If the Codex action fails due to API or infrastructure issues, the workflow
  should surface the failure in Actions but remain non-required for merges.
- If posting the summary fails, the underlying Codex run output should still be
  visible in the job summary or logs.

## Verification Strategy

- YAML validation by reading the rendered workflow for trigger, permission, and
  job wiring correctness.
- Dry-run review of the `if:` guard to confirm only same-repo non-draft PRs
  execute.
- Confirm docs mention the advisory nature of the review and the required
  `OPENAI_API_KEY` secret.
- If available, run a local YAML linter or the repository's workflow validation
  command.
