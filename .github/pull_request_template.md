## Summary

<!-- What does this PR do and why? -->

<!-- Same-repo non-draft PRs receive an advisory Codex review in GitHub Actions. Treat it as supplemental review signal, not a merge gate. -->

## Changes

-

## Coding Style Compliance

- [ ] Correctness semantics unchanged or documented
- [ ] Error categories preserved at boundaries
- [ ] No unbounded buffering introduced
- [ ] Hot-path allocation impact reviewed
- [ ] Metrics/logging impact reviewed
- [ ] Serde roundtrip tests for new/changed types

## Verification

- [ ] `just fmt`
- [ ] `just lint`
- [ ] `just test`
- [ ] E2E tests (if engine/plugin changed): `just e2e`
- [ ] Benchmark evidence (if hot path touched)
