# README Badges Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a compact badge row to the README header for CI, license, Rust version, and contributing docs.

**Architecture:** This is a single-file documentation change. Add markdown badges directly under the top-level heading using the GitHub Actions workflow badge plus stable static Shields badges.

**Tech Stack:** Markdown, GitHub Actions badge URL, Shields.io badges

**Design doc:** `docs/plans/2026-03-09-readme-badges-design.md`

---

### Task 1: Add the README badge row

**Files:**
- Modify: `README.md`

**Step 1: Write the failing verification target**

Run: `sed -n '1,12p' README.md`
Expected: no badge row under `# Rapidbyte`.

**Step 2: Write minimal implementation**

Add one markdown line under the heading with badges for:

- CI
- Apache-2.0
- Rust 1.75+
- Docs: Contributing

**Step 3: Verify the result**

Run: `sed -n '1,12p' README.md`
Expected: the badge row appears directly under the heading with valid markdown links.

**Step 4: Commit**

```bash
git add README.md
git commit -m "docs: add README status badges"
```
