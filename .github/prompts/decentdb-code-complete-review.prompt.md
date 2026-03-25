---
description: "Validate a claimed code-complete DecentDB change by reviewing implementation quality, tests, validation, and repo-specific completion criteria"
name: "DecentDB Code Complete Review"
argument-hint: "Optional PR, diff, feature area, or files to focus on"
agent: "agent"
model: "GPT-5 (copilot)"
---
Review the requested work as a strict code-complete validation pass.

If the user supplied a PR, diff, feature area, or file set, use that as the review scope. Otherwise, review the current uncommitted or otherwise provided change set.

Start by reading the repository standards and relevant touched code:
- [AGENTS.md](../../AGENTS.md)
- [.github/copilot-instructions.md](../copilot-instructions.md)
- any design docs, ADRs, public API definitions, tests, and binding surfaces relevant to the change

Primary objective:
- Determine whether the claimed implementation is actually code complete for this repository. Do not trust the claim. Verify it.

Core review responsibilities:
- Evaluate the real implementation, not the stated intent.
- Validate correctness, regression risk, test coverage, and follow-through quality.
- Identify anything missing that should block calling the work done.
- Explicitly decide whether the change is code complete.

Repository-specific standards to enforce:
- Prioritize durable correctness over cleverness.
- Keep changes scoped and incremental.
- Avoid unnecessary dependencies.
- Avoid panics in library code.
- Avoid `unwrap()` and `expect()` unless narrowly justified.
- Preserve stable C ABI expectations where relevant.
- Keep Rust API, CLI, C ABI, and bindings aligned when behavior changes affect them.
- Treat warnings as errors.
- Require relevant tests and documentation updates when behavior changed.

Mandatory review process:
1. Determine the actual scope of the change from the diff and touched files.
2. Read the surrounding code paths, not just the edited lines.
3. Check whether the implementation solves the root problem or only patches symptoms.
4. Look for:
   - behavioral regressions,
   - edge case failures,
   - incomplete error handling,
   - ownership or lifetime mistakes,
   - concurrency risks,
   - ABI drift,
   - binding drift,
   - stale or missing documentation,
   - validation that is claimed but not evidenced.
5. Verify whether tests exist at the right level:
   - unit tests for local logic,
   - integration tests for cross-module behavior,
   - binding or ABI validation if native interfaces or bindings were affected.
6. Verify whether validation is sufficient for the touched surface:
   - `cargo check`,
   - `cargo clippy`,
   - targeted tests,
   - broader tests when risk justifies them,
   - binding-specific smoke coverage if applicable.
7. Check whether public or user-visible behavior changes required updates to docs, examples, changelog notes, rustdoc, or plan/status files.
8. Decide whether the work is truly complete.

Decision rules:
- Mark the work `Not code complete` if there are missing tests for changed behavior, incomplete error handling, uncovered regression paths, missing ABI or binding follow-through, missing docs for user-visible behavior changes, or validation gaps that materially reduce confidence.
- Mark the work `Code complete` only if the implementation, tests, validation, and required follow-through are all present and coherent.
- If there are no findings, still call out residual risks or unverified areas.

Review mindset:
- Be skeptical and specific.
- Prefer concrete defects over style commentary.
- Do not suggest unrelated refactors.
- Do not accept "probably fine" as evidence.

Output requirements:
- Start with findings only. Do not lead with a summary.
- Order findings by severity.
- For each finding include:
  - severity: `blocker`, `major`, or `minor`
  - why it matters
  - exact file references
  - what is missing, broken, or risky
  - what would be needed to consider it complete
- After findings, include these sections in order:
  1. `Completion verdict`
  2. `Missing validation`
  3. `Open assumptions`
  4. `Brief summary`

Verdict format:
- `Code complete`
- `Not code complete`

When there are no findings, say that explicitly under `Findings`, then provide the verdict and any remaining validation or confidence limits.