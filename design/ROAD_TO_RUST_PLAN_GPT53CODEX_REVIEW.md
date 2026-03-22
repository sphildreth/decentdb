# Review of `design/ROAD_TO_RUST_PLAN.md` (GPT-5.3 Codex)
**Date:** 2026-03-22  
**Reviewer:** GPT-5.3 Codex  
**Scope:** Independent review of `design/ROAD_TO_RUST_PLAN.md` against project goals in `design/PRD.md`, and consistency checks against `design/SPEC.md`, `design/TESTING_STRATEGY.md`, and referenced ADRs.

## Executive Verdict
The roadmap has the right macro direction (bottom-up storage first, then relational layer, then ecosystem), but it currently has several **high-severity correctness and execution risks** that should be fixed before broad implementation starts.

The strongest issues are:
1. WAL instructions in the plan are out of sync with accepted WAL format ADR evolution.
2. Crash/differential verification is sequenced too late relative to ACID-first goals.
3. Agent instructions use paths that do not match the current Rust workspace layout.
4. ADR references are ambiguous due duplicate ADR numbers.
5. Some low-level layout guidance increases accidental `unsafe` pressure.

If those are addressed, the plan can become a strong execution document.

---

## Review Method
I evaluated the roadmap on five axes:
1. Goal alignment with the 7 PRD pillars.
2. Internal consistency with SPEC and testing strategy.
3. Consistency with accepted ADRs referenced by the roadmap.
4. Practical implementability in the current repo layout.
5. Risk of late discovery failures (correctness, compatibility, CI).

---

## What The Roadmap Gets Right
1. It correctly starts from storage and durability primitives (VFS, pager, WAL, recovery) before SQL/planner work.
2. It explicitly includes fault injection in VFS, which is exactly what the testing strategy requires for ACID hardening.
3. It calls out the lock-free snapshot LSN mechanism and reader retention constraints.
4. It explicitly preserves C-ABI and panic-safety concerns.
5. It uses slice-based decomposition, which is a good shape for iterative delivery.

---

## Findings (Severity Ranked)

## 1) High: WAL directives are partially stale vs accepted format decisions
**Problem**  
The roadmap says WAL writer should calculate checksums and recovery should detect torn writes by missing `payload size` or `checksum`. That no longer matches the accepted WAL format progression (v5-v8): checksum removed, LSN removed, payload length removed, header now carries logical end offset.

**Why this matters**  
This is not a style issue. It can produce incompatible WAL files, incorrect recovery logic, and false confidence in durability tests.

**Evidence**  
- Roadmap slice 1.3/1.4 still references checksum-based handling and payload-size checks.  
- SPEC and ADRs define v5-v8 behavior where checksum and payload size are not used as pre-v5 validators.

**Recommendation**  
Make WAL rules explicit and canonical in the roadmap:
1. Use frame-type-derived payload sizes.
2. Treat checksum as reserved field (if present) per format notes, not integrity guard.
3. Derive LSN from offset/end position.
4. Recovery scan bounds should be based on `wal_end_offset` from WAL header, not physical file end.
5. Prefer linking to a single “WAL Canonical Format” section to avoid ADR drift.

---

## 2) High: ACID verification is placed too late in the sequence
**Problem**  
Crash/differential harness integration is in Phase 4.2, after most engine functionality is built.

**Why this matters**  
The PRD and testing strategy require correctness-first and crash testing as a core design loop, not a release hardening step. Deferring this creates a high chance of redesign late in the project.

**Evidence**  
- PRD pillar #1 and testing strategy both prioritize crash correctness and layered tests from the start.
- Roadmap defers Python crash/differential wiring to Phase 4.

**Recommendation**  
Move a minimal crash harness integration to Phase 1:
1. During Slice 1.1/1.3, expose failpoints and run at least one crash-reopen scenario in CI.
2. Require “first crash loop green” gate before moving to Phase 2.
3. Keep long soak runs nightly, but retain a short deterministic crash subset in PR CI.

---

## 3) High: File/module directives do not match current workspace layout
**Problem**  
The roadmap repeatedly says “create `src/*.rs`”. Current repository is a workspace where core engine code lives in `crates/decentdb/src`.

**Why this matters**  
Agents following roadmap literally may create files in the wrong tree, causing fragmentation and wasted review cycles.

**Evidence**  
Workspace members and current crate layout are already established under `crates/`.

**Recommendation**  
Update each slice to reference concrete crate-scoped paths, e.g.:
- `crates/decentdb/src/vfs.rs`
- `crates/decentdb/src/pager.rs`
- etc.

Also specify module wiring expectations (`mod` declarations, public surface in `lib.rs`).

---

## 4) High: ADR references are ambiguous because ADR numbering is duplicated
**Problem**  
The roadmap cites ADR numbers (for example ADR-0035) as unique identifiers, but the repository currently contains duplicate ADR numbers.

**Why this matters**  
A roadmap instruction like “follow ADR-0035” is ambiguous and can route implementation to the wrong decision.

**Evidence**  
Duplicate ADR IDs exist (e.g., 0027, 0035, 0036, 0048, 0072).

**Recommendation**  
In roadmap directives, reference ADRs by **full filename + title**, not number only.  
Example: “Follow `design/adr/0035-btree-page-layout-v2.md`”.

---

## 5) High: `#[repr(C, packed)]` guidance is risky for safe-Rust-first implementation
**Problem**  
The roadmap offers `#[repr(C, packed)]` for B+Tree structs.

**Why this matters**  
Packed structs frequently force unaligned field access patterns that are hard to keep safe and ergonomic in Rust. This conflicts with the project’s “borrow checker as QA” and minimal-unsafe direction.

**Recommendation**  
Prefer explicit byte-slice parsing/serialization helpers and avoid packed field references. If exact layouts are needed, use fixed byte arrays + conversion helpers and test layout constants thoroughly.

---

## 6) Medium: Slice-level Definition of Done is under-specified
**Problem**  
Most slices include rough goals and a few test hints, but no clear entry/exit criteria tied to PRD pillars.

**Why this matters**  
Without measurable gates, “slice complete” can be interpreted too loosely, especially by parallel agents.

**Recommendation**  
Add a standard gate block per slice:
1. Required tests (unit/property/crash where applicable).
2. Required CI jobs.
3. Performance sanity checks.
4. API/doc updates.
5. Explicit “not in slice” exclusions.

---

## 7) Medium: Dependency governance is inconsistent with repo policy
**Problem**  
Roadmap suggests adding crates (`lru`, `flate2`, `pg_query`) in implementation directives without explicitly tying each to ADR status and dependency policy.

**Why this matters**  
Dependency additions can impact build portability, CI time budgets, and long-term maintenance.

**Recommendation**  
For each non-trivial dependency in roadmap slices, add one line:  
- “ADR exists: yes/no”  
- “Scope: runtime/dev/test-only”  
- “Fallback if unavailable”.

---

## 8) Medium: Reader snapshot and WAL index behavior for long readers needs explicit handling
**Problem**  
Roadmap defines `WalIndex` as `page_id -> frame_offset` (single latest). That is insufficient for long-lived snapshots unless additional version access behavior is defined.

**Why this matters**  
Single-offset maps may force expensive WAL rescans or can accidentally violate snapshot visibility semantics.

**Recommendation**  
Document one chosen strategy in Slice 1.3/1.4:
1. `page_id -> version chain`, or
2. `page_id -> latest` + bounded backward scan with index assists.

Then include tests for readers holding old snapshots during continued writes.

---

## 9) Medium: C-ABI parity arrives too late for compatibility risk reduction
**Problem**  
C-ABI work is in Phase 4.1, but compatibility with existing bindings is a major product goal.

**Why this matters**  
Late ABI integration can reveal naming/signature/memory-lifetime mismatches after engine internals are already set.

**Recommendation**  
Add an early “ABI skeleton” milestone in Phase 1:
1. Export minimal stable handles and error retrieval.
2. Add panic-safety wrappers from day one.
3. Run tiny smoke tests from one binding language early (even before full SQL support).

---

## 10) Low: Some directives are correct but too implicit for multi-agent execution
**Problem**  
The roadmap says agents must focus on current slice, but does not define ownership boundaries for parallel work.

**Why this matters**  
Parallel contributors can step on each other in shared files (`lib.rs`, common types, error enums).

**Recommendation**  
For each slice, add:
1. “Owned files/modules”.
2. “Shared touchpoints requiring coordination”.
3. “Expected public interfaces to freeze for next slice”.

---

## PRD Pillar Alignment Assessment

1. **ACID Compliance:** Partially aligned. Strong intent in Phase 1, but late crash harness integration and stale WAL details are significant risks.
2. **Performance:** Mostly aligned. Pager/WAL/query plans target speed, but performance budgets and regression gates are not explicitly embedded in slice completion criteria.
3. **Disk Footprint:** Aligned in concept (varints, compact layouts, overflow pages). Needs clearer policy on compression thresholds and format compatibility.
4. **Documentation:** Weakly represented in roadmap execution gates. Docs are only mentioned at end; rustdoc/testable examples should be incremental.
5. **Tooling/Bindings:** Intent present, but ABI validation is sequenced too late.
6. **CLI Experience:** Included, but currently broad and lacks quality acceptance criteria.
7. **Fast Feedback Loop:** Not sufficiently operationalized; no explicit per-slice CI duration or test partition strategy in the roadmap.

---

## Recommended Roadmap Adjustments (Concrete)

## A) Add a new “Phase 0: Execution Guardrails”
Include before Phase 1:
1. Canonical source-of-truth links for WAL format and B+Tree layout (single pointers, no ambiguous ADR number-only refs).
2. Workspace path conventions (`crates/decentdb/src/...`).
3. Slice DoD template.
4. CI partitioning rules (PR under 10 min, long tests nightly).

## B) Shift verification left
Move minimal crash harness integration from Phase 4.2 into Phase 1.1/1.3.  
Retain full differential suites later, but ensure early crash-reopen loops exist.

## C) Clarify WAL and snapshot semantics in slice text
In Slice 1.3/1.4 explicitly define:
1. Frame format version assumptions.
2. `wal_end_offset` update ordering.
3. Reader snapshot capture semantics (Acquire/Release pairing).
4. Truncation rules with active readers.

## D) Add ABI bootstrap milestone early
Before planner/executor expansion, export stable minimal C API and add panic-safety wrappers plus one binding smoke test.

## E) Enforce ADR reference precision
Use full filenames and, where possible, section anchors. Remove number-only references from implementation directives.

---

## Suggested Slice DoD Template
Use this for every slice in the roadmap:
1. `cargo check` and `cargo clippy` clean for touched crates.
2. Required unit/property/crash tests added and passing.
3. Failure-mode tests included (I/O faults, malformed input, race-sensitive paths where relevant).
4. CI impact documented (PR or nightly lane).
5. Public APIs and docs updated (including rustdoc examples where relevant).
6. Explicit statement of unresolved risks carried to next slice.

---

## Final Assessment
The plan is directionally strong and has good component decomposition, but it is not yet safe as an “execution contract” for multiple coding agents. Fixing the WAL spec drift, moving correctness verification earlier, resolving ADR ambiguity, and tightening slice gates would materially improve delivery confidence and reduce rewrite churn.

---

## Sources / References
All references below are from this repository.

1. `design/ROAD_TO_RUST_PLAN.md` (overall roadmap structure and slice directives).
2. `design/PRD.md` (7 pillars; ACID/performance/tooling/feedback-loop priorities).
3. `design/SPEC.md` (WAL format v5-v8 notes, snapshot semantics, checkpoint/truncation behavior, testing and CI expectations).
4. `design/TESTING_STRATEGY.md` (layered testing, FaultyVFS requirements, crash-injection workflow, CI split).
5. `design/adr/0003-snapshot-lsn-atomicity.md` (Acquire/Release semantics intent).
6. `design/adr/0019-wal-retention-for-active-readers.md` (no truncation of reader-needed WAL).
7. `design/adr/0033-wal-frame-format.md` + `0064/0065/0066/0068` (WAL evolution and current format constraints).
8. `design/adr/0035-btree-page-layout-v2.md` and duplicate ADR numbering in `design/adr/` (ambiguity risk).
9. `design/adr/0118-rust-ffi-panic-safety.md` and `0119-rust-vfs-pread-pwrite.md` (FFI panic safety and positional I/O model).
10. Workspace layout: root `Cargo.toml`, `crates/decentdb/`, `crates/decentdb-cli/`.
