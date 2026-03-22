# Road to Rust Plan Review (GPT-5.1 Codex)
**Date:** 2026-03-22
**Reviewer:** GPT-5.1 Codex

## Scope & Inputs
- Reviewed: `design/ROAD_TO_RUST_PLAN.md` (Active Execution, 2026-03-22) and `design/PRD.md` (7 product pillars).
- Supplemental context skimmed: `design/SPEC.md` (v1.0 baseline) and `design/TESTING_STRATEGY.md` (v0.1).
- Goal: Provide independent feedback (not echoing prior reviews) on plan adequacy vs. PRD pillars.

## High-Level Take
The slice-based plan is a solid bottom-up sequence that respects single-writer / multi-reader constraints and keeps durability first. However, several feedback-loop, observability, and compatibility items are late or implicit. Pulling test harness wiring, WAL safety checks, and API surface validation forward would derisk ACID and ecosystem goals.

## Pillar Alignment Snapshot (PRD)
- **ACID compliance:** Strong focus in Phase 1 & 1.3/1.4, but explicit torn-write detection, fsync lie tests, and reader tracking need to be codified early (PRD §1, SPEC §4.1–4.5, TESTING §3).
- **Performance:** Page cache and WAL are planned, yet no perf budget or microbench hooks are in early slices (PRD §2). Risk of regressions if metrics arrive only in Phase 4.4.
- **Disk footprint:** `#[repr(C)]` guidance appears in Phase 1.2/2.2 but compression and overflow handling could inflate without size tests (PRD §3).
- **Documentation:** Not scheduled in any slice; rustdoc/doctest work should be gated per module (PRD §4).
- **Bindings/tooling:** C-ABI and Python harness land in Phase 4; defers validation of the public surface vs. existing consumers (PRD §5, §7).
- **CLI experience:** Phase 4.3 exists but depends on late parser/planner maturity; consider earlier stub to exercise engine surface (PRD §6).
- **Fast feedback loop:** Unit/property/crash tests are mentioned but not placed on the timeline; CI-under-10-min goal (PRD §7) requires scoping per slice.

## Phase-by-Phase Observations & Risks
### Phase 1: Foundation
- **VFS (1.1):** Using `FileExt::read_at` is correct for lock-free reads, but Windows parity (PRD §1 portability expectation) needs explicit trait methods or cfg gates. Fault injection should log deterministic seeds to align with TESTING §3 reproducibility.
- **Pager & Cache (1.2):** Plan omits dirty-page flush policy and interaction with WAL checkpoints (SPEC §4.3). Eviction under pinning must avoid starvation; propose test cases for high pin counts.
- **WAL (1.3):** WalIndex is in-memory only; recovery relies on rebuild. Consider persisting minimal index metadata to bound startup time on large logs (SPEC §4.5). Snapshot acquisition should be spelled out (`AtomicU64::load(Ordering::Acquire)` already in SPEC §4.2) to avoid relaxed loads sneaking in.
- **Recovery & Checkpoint (1.4):** Reader-protection rule is cited, but enforcement mechanism (tracking active reader LSNs) is not assigned to a component. Risk: truncation bug if left implicit. Also need strategy for `wal_end_offset` vs physical file size mismatch (SPEC §4.1 v8 header).

### Phase 2: Data Structures
- **Record encoding (2.1):** Lexicographic sortability for key encoding is “document or ensure” — indexes need a definitive rule. Decimal/timestamp varint vs fixed-width trade-offs should be decided up front to avoid double rewrites (PRD §3 minimal footprint, SPEC §5 primary key rules). Proptest coverage is good; add corpus cases from TESTING §2.6 datasets.
- **B+Tree layout (2.2) & Mutations (2.3):** Overflow compression via `flate2` introduces a non-trivial dependency; consider ADR per repository rules and measure CPU cost (PRD §2 performance). Node split policies (balanced vs biased) aren’t specified; this affects fragmentation and VACUUM needs.
- **Trigram index (2.4):** Guardrails (pattern length thresholds, posting caps) are in SPEC §8 but absent here—risk of perf cliffs on small patterns.

### Phase 3: Relational Core
- **Parser (3.2):** libpg_query choice implies C FFI; need early ABI verification to keep bindings parity (PRD §5). If a temporary Rust parser is used, define swap-out milestone to avoid dual maintenance.
- **Planner/Exec (3.3):** Volcano iterators align with performance goals; suggest integrating buffer reuse benchmarks from TESTING §6.1 during development, not post-facto.
- **DML & Constraints (3.4):** Statement-level rollback is mandated; ensure WAL frame grouping mirrors statement boundaries to avoid partial visibility (SPEC §4.1 commit rule).

### Phase 4: Ecosystem
- **C-ABI (4.1):** Catch-unwind is called out, good. Missing: exact symbol parity checklist vs Nim exports; propose generating headers from Rust definitions and diffing against legacy to prevent drift.
- **Python crash/diff tests (4.2):** Landing this late risks discovering WAL/pager bugs after substantial stack is built. Move harness wiring up to follow completion of Phase 1.3/1.4 so durability regressions are caught early.
- **CLI (4.3):** CLI depends on planner/exec; consider an early “smoke” CLI that can run limited commands to support testing teams.
- **Release verification (4.4):** Benchmarks against Nim are noted but not tied to thresholds; align with TESTING §6.2 regression gates.

## Cross-Cutting Gaps & Recommendations
- **Feedback loop timing:** Introduce per-slice test gates: unit + proptest + crash-fault subset before advancing phases (PRD §7, TESTING §2–3).
- **Reader tracking:** Define a small “reader registry” module in Phase 1.4 to implement ADR-0019 rules explicitly; required for safe WAL truncation.
- **Observability:** Add lightweight structured logging counters (fsync counts, cache hits/misses, WAL growth) and make them queryable by tests; keeps PRD §2 performance honest without heavy deps.
- **Docs & rustdoc:** Require public API docs and doctests per module as it lands (PRD §4). Generate `cargo doc` in CI nightly at minimum.
- **Dependency governance:** Call out ADR triggers in the plan (e.g., `flate2`, libpg_query, lru crate). Avoid late-stage surprises with review gates.
- **Data-size guardrails:** Add size and fragmentation checks to Phase 2 deliverables to uphold PRD §3 (e.g., max inline cell size, overflow spill ratio, page fill factor targets).
- **ABI compatibility tests:** When C-ABI lands, immediately wire basic C/Python smoke tests so regressions surface before full binding work.

## Suggested Sequencing Tweaks
1) After Phase 1.3 WAL completes, wire Python crash harness subset (Phase 4.2 pulled forward) to validate durability continuously.
2) Add minimal CLI “inspect” subcommand once Pager/WAL are usable to aid debugging and testing (supports PRD §6 & §7).
3) Gate Phase 2 start on passing LRU cache eviction + WAL recovery property tests to avoid layering on unstable storage.
4) Defer trigram compression tuning until basic B+Tree mutation stability is proven; keep flag to disable compression for benchmarks.

## References
- Product goals: `design/PRD.md` §§1–7.
- Plan under review: `design/ROAD_TO_RUST_PLAN.md` Phases 1–4 slices.
- Specifications: `design/SPEC.md` §§2–8 (module architecture, WAL, SQL subset, trigram rules).
- Testing guidance: `design/TESTING_STRATEGY.md` §§2–6 (unit/property/crash, datasets, benchmarks).
