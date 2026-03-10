# Pre-Push Quality Guardrails Design

## Summary

Add a repo-managed `pre-push` hook to complement the existing lightweight
`pre-commit` hook. The new hook should stay narrowly focused on safe formatting
fixes for Rust changes, stage any formatter edits, and abort the push so the
user can review, commit, and push again. This catches the exact class of
failure that recently escaped to CI while avoiding a heavy local gate that
contributors will bypass.

## Goals

- Prevent obvious formatting-only CI failures from reaching `origin`.
- Reuse the existing repo-managed hook installation flow in `.githooks/`.
- Apply safe auto-fixes where possible, but preserve a review/commit boundary
  before the push proceeds.
- Keep push-time checks fast and predictable.

## Non-Goals

- Running `clippy`, tests, benchmarks, or any networked command in `pre-push`.
- Replacing `just ci` or GitHub Actions.
- Adding third-party hook managers such as Python `pre-commit` or `lefthook`.
- Mutating files unrelated to Rust formatting.

## Alternatives Considered

### 1. Recommended: auto-fix formatting in `pre-push`, then abort

Run `cargo fmt --all`, re-stage any touched tracked files, and abort the push if
formatting changed something. If no changes were needed, run
`cargo fmt --all -- --check` as the final validation step.

Pros:
- Fixes the exact common failure mode automatically.
- Preserves an explicit review/commit step after mutation.
- Stays cheap enough to keep enabled.

Cons:
- Pushes can be interrupted by local edits that now require a follow-up commit.

### 2. Validation-only `pre-push`

Run `cargo fmt --all -- --check` and fail without changing anything.

Pros:
- No mutation during push.

Cons:
- Makes the user do a separate fix step for a deterministic problem that can be
  solved automatically.

### 3. Heavy `pre-push`

Run `clippy`, tests, or `just ci` before push.

Pros:
- Stronger local assurance.

Cons:
- Too slow and too intrusive for an always-on push hook.
- High risk that contributors disable or bypass it.

## Recommended Design

Add `.githooks/pre-push` with behavior aligned to the existing `pre-commit`
hook:

1. Resolve the repository root and operate there.
2. Detect whether the pending push is Rust-relevant.
3. If not Rust-relevant, exit successfully.
4. Run `cargo fmt --all`.
5. Detect formatter-modified files.
6. Re-stage only modified tracked files.
7. If formatter changes occurred, abort the push with a clear message asking the
   user to review, commit, and push again.
8. If nothing changed, run `cargo fmt --all -- --check` and succeed only if the
   repo is still formatted.

This keeps the local quality model consistent:

- `pre-commit`: first auto-fix layer during commit.
- `pre-push`: last cheap auto-fix and validation layer before networked CI.
- `just ci`: strict local validation.
- GitHub Actions: remote enforcement.

## Change Scope

### New file

- `.githooks/pre-push`

### Modified files

- `scripts/install-git-hooks.sh`
  - only if installation messaging needs to mention both hooks
- `Justfile`
  - only if a helper target is needed to surface hook setup or shared fix logic
- `README.md`
- `CONTRIBUTING.md`

## Error Handling

- If there are no Rust-relevant local changes for the push, the hook exits `0`.
- If `cargo fmt --all` fails, the hook exits non-zero and surfaces the tool
  failure.
- If formatting changed files, the hook re-stages them and exits non-zero with a
  short message explaining the next step.
- If the repository remains formatted, the hook exits `0`.

## Verification Strategy

- Shell syntax check:
  - `bash -n .githooks/pre-commit`
  - `bash -n .githooks/pre-push`
  - `bash -n scripts/install-git-hooks.sh`
- Hook installation check:
  - `just install-hooks`
  - `git config --get core.hooksPath`
- Formatting guardrail check:
  - create a temporary unformatted Rust change
  - run `.githooks/pre-push`
  - confirm it stages formatting changes and exits non-zero
- Clean-path check:
  - run `.githooks/pre-push` with no formatter changes and confirm success
- Repo verification:
  - `cargo fmt --all -- --check`
  - one representative test command such as `cargo test -p rapidbyte-benchmarks`
