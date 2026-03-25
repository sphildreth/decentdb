---
description: "Work through the Rust missing-feature slices iteratively until the plan is completed, keeping docs, tests, and slice status aligned"
name: "DecentDB Rust Feature Slices"
argument-hint: "Optional slice number, feature family, or priority override"
agent: "agent"
model: "GPT-5 (copilot)"
---
Work through the Rust missing-feature implementation plan iteratively until every slice in `design/RUST_MISSING_FEATURE_PLAN.md` is completed, or until you hit a real blocker that cannot be resolved safely within the current repository constraints.

If the user supplied a slice number, feature family, or priority override, treat that as the starting point. Otherwise, start from the first slice in `design/RUST_MISSING_FEATURE_PLAN.md` whose status is not `Completed`.

Start every session by reading:
- [design/RUST_MISSING_FEATURE_PLAN.md](../../design/RUST_MISSING_FEATURE_PLAN.md)
- [design/PRD.md](../../design/PRD.md)
- [design/SPEC.md](../../design/SPEC.md)
- [design/TESTING_STRATEGY.md](../../design/TESTING_STRATEGY.md)
- [AGENTS.md](../../AGENTS.md)
- [.github/copilot-instructions.md](../copilot-instructions.md)

Primary objective:
- Close the implementation gap between the documented SQL feature matrix and the Rust engine by completing the slices in `design/RUST_MISSING_FEATURE_PLAN.md` with real end-to-end behavior, focused tests, and documentation alignment.

Global rules:
- Treat `docs/user-guide/sql-feature-matrix.md` as the source of truth for supported SQL surface unless the user explicitly asks to change the product contract.
- Do not weaken ACID guarantees, WAL behavior, recovery semantics, or CLI/binding stability to land feature work.
- Keep changes incremental and explicit. Prefer finishing one coherent slice or sub-slice cleanly over scattering partial edits across many unrelated areas.
- Do not add dependencies unless an ADR is required and the user approves it.
- Avoid `unwrap()` and `expect()` in library code unless there is a narrowly justified, localized reason.
- When a slice impacts the C ABI, CLI behavior, or bindings, update the affected tests and documentation as part of the same change.
- Preserve or improve the fast developer feedback loop: run focused checks first, then broader validation.

Mandatory loop:
1. Re-read the status table in `design/RUST_MISSING_FEATURE_PLAN.md` and identify the next incomplete slice.
2. Confirm the slice goal, scope, acceptance criteria, and likely touched modules before editing.
3. Inspect the current implementation and existing tests for that slice.
4. Make a short plan that names:
   - the exact sub-problem you will solve now,
   - the files/modules to change,
   - the main correctness risks,
   - the validation commands you expect to run.
5. Implement the smallest root-cause fix that materially advances the slice.
6. Add or update direct regression coverage for the slice behavior. Parser-only acceptance is not enough.
7. Run focused validation first. Then run broader Rust validation appropriate to the touched surface.
8. Update `design/RUST_MISSING_FEATURE_PLAN.md` before stopping:
   - keep the slice status accurate,
   - add concise progress notes if part of the slice is now done,
   - update the suggested next move if the execution order should change.
9. If the slice is completed and there is still time/context budget, continue to the next incomplete slice instead of stopping at a summary.
10. Only stop early if you encounter a genuine blocker such as an ADR-required architectural decision, an unsafe design tradeoff, or a failing prerequisite you cannot responsibly resolve in the current turn.

Execution policy by slice:
- Default to numeric slice order.
- You may reorder only when the plan itself identifies a safer dependency order, or when the current slice is genuinely blocked and another incomplete slice is unblocked.
- If a slice is too large for one turn, split it into coherent sub-steps and update the plan so the remaining work is explicit.
- Do not mark a slice `Completed` until its acceptance criteria are met with runtime evidence.

Required evidence standard:
- A documented feature counts as implemented only when the Rust rewrite supports it end-to-end:
  - parser acceptance,
  - AST/normalization support,
  - planner/executor behavior,
  - regression coverage or equivalent runtime proof.
- Parser-only support does not count.

Validation expectations:
- Run `cargo check` on the affected crate(s) when Rust code changes.
- Run `cargo clippy` on the affected crate(s) for completed slice work.
- Run targeted tests for the touched subsystem.
- If behavior crosses crate or binding boundaries, run the corresponding smoke coverage that proves the public surface still works.
- If a full validation step is too expensive for the current turn, run the strongest targeted validation you can and state what remains.

Plan-file maintenance requirements:
- Keep `design/RUST_MISSING_FEATURE_PLAN.md` as the authoritative execution tracker for this effort.
- Update statuses using clear states such as `Not started`, `In progress`, `Blocked`, or `Completed`.
- When you partially advance a slice, add short, concrete notes describing what landed and what remains.
- Do not silently leave the plan stale after code changes.

Definition of done for the overall prompt:
- Every slice from Slice 1 through Slice 10 is either:
  - `Completed` with code and tests landed, or
  - explicitly marked `Blocked` with the blocker and next decision recorded.
- The feature matrix and implementation are materially aligned.
- The plan file reflects the real state of the work.

Output requirements for each working turn:
- Lead with the active slice number and the exact sub-problem chosen.
- Show the concrete code, test, and documentation changes made.
- Summarize validation that was actually run.
- State whether the slice status changed.
- End with either:
  - the next slice/sub-step you are immediately continuing to, or
  - the real blocker that prevents safe continuation.

Priority guidance from the plan:
- Unless the user overrides the order, start with Slice 1.
- After finishing a slice, prefer continuing immediately rather than restating the roadmap.
- Keep momentum, but do not trade correctness for throughput.