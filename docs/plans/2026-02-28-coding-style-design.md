# Rapidbyte Coding Style Design

**Date:** 2026-02-28

## Goal

Create a maintainer-grade `CODING_STYLE.md` that acts as an operational blueprint for writing and refactoring high-performance, correctness-critical Rust across Rapidbyte.

## Scope Decisions

- Audience: core maintainers (strict standards)
- Content: standards + enforcement model + per-crate refactor priority rubric
- Performance guidance: principle-based verification and measurement protocol, without fixed numeric budgets

## Design Summary

The style guide is designed as a policy-plus-playbook document rather than an aesthetic formatter guide. It defines explicit rule levels (`MUST`, `MUST NOT`, `SHOULD`, `MAY`), ties each mandatory rule to failure modes relevant to ingestion systems, and prescribes verification pathways (lint, tests, e2e, benchmark evidence).

The document also codifies DRY/SOLID/YAGNI in a performance-aware way so abstraction decisions do not damage hot-path throughput or hide boundary semantics. It emphasizes correctness surfaces specific to Rapidbyte: policy-driven row handling, typed error categories, decimal/numeric edge behavior, checkpoint/commit coordination, and observability labels.

To make the guide actionable for ongoing cleanup, it introduces a weighted refactor rubric:

```text
Priority = 2*Correctness + 2*Performance + BlastRadius + Maintainability + Observability
```

This allows maintainers to rank candidate refactors consistently instead of relying on ad hoc prioritization.

## Section Map in `CODING_STYLE.md`

1. Purpose and non-goals
2. Rule semantics
3. Core principles (DRY/SOLID/YAGNI)
4. Correctness standards
5. Performance standards
6. Code structure and API conventions
7. Testing and verification standards
8. Enforcement matrix
9. Refactor priority rubric
10. Good/bad implementation patterns
11. PR template snippet

## Why This Fits Rapidbyte

- Aligns with connector/runtime boundaries and policy-driven pipeline semantics.
- Reinforces observed production-sensitive concerns (state/checkpoint correctness, transform policy handling, metrics quality, hot-loop allocations).
- Provides a direct framework for future refactors in engine/runtime/connectors without scope creep.

## Implementation Artifact

- Created: `CODING_STYLE.md`
