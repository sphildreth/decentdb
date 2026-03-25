---
agent: agent
name: Review Rust Change
description: "Use when reviewing a Rust change in DecentDB for bugs, regressions, unsafe assumptions, ownership issues, testing gaps, ABI risk, or performance-risk tradeoffs."
argument-hint: "Describe the Rust change, PR, file, or concern to review"
---

# Review Rust Change

Review the requested Rust change with a code-review mindset.

Use the repository's Rust workflow and references from [the Rust code generation skill](../skills/rust-code-generation/SKILL.md).

Load deeper references as needed:

- [Error handling patterns](../skills/rust-code-generation/references/errors.md)
- [Async and concurrency patterns](../skills/rust-code-generation/references/async.md)
- [FFI and layout safety patterns](../skills/rust-code-generation/references/ffi.md)
- [Performance and allocation patterns](../skills/rust-code-generation/references/performance.md)
- [Testing and validation patterns](../skills/rust-code-generation/references/testing.md)

## Review priorities

Focus on findings first, ordered by severity.

Look for:

- correctness bugs
- behavioral regressions
- ownership, lifetime, and borrowing issues
- concurrency hazards and lock-scope mistakes
- `unsafe` assumptions or undocumented invariants
- ABI, layout, WAL, or on-disk compatibility risks
- error-handling weaknesses
- unnecessary allocation or performance regressions in hot paths
- missing or insufficient tests and validation

## Required workflow

1. Read the affected code and nearby tests before judging the change.
2. Check whether the change touches public Rust APIs, the C ABI, bindings, on-disk format, WAL semantics, or concurrency behavior.
3. Evaluate whether the validation run matches the risk of the change.
4. Produce findings first, with concrete file references.
5. Keep summaries brief and secondary to the findings.

## Output expectations

If you find issues, report:

1. Findings, ordered by severity
2. Open questions or assumptions
3. Brief change summary

If you find no issues, say so explicitly and note any residual testing or validation gaps.