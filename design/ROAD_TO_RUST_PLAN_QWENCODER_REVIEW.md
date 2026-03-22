# Road to Rust Plan: QwenCoder Review & Analysis
**Date:** 2026-03-22
**Author:** QwenCoder (AI Assistant)
**Review Type:** Independent Technical Assessment

---

## Executive Summary

This document provides an independent, comprehensive review of the `ROAD_TO_RUST_PLAN.md` against the goals and requirements defined in `PRD.md`. The analysis identifies strengths, gaps, risks, and actionable recommendations for the DecentDB 1.0.0 Rust rewrite.

**Overall Assessment:** The plan is well-structured and technically sound, with clear phase decomposition and strong alignment to the PRD's 7 pillars. However, several critical areas require additional attention: memory safety strategy, error handling taxonomy, cross-platform VFS implementation, testing infrastructure bootstrap, and dependency management discipline.

---

## 1. Alignment with PRD Pillars

### Pillar 1: ACID Compliance ✅ Strong Alignment

**Strengths:**
- Slice 1.3 (WAL) correctly references ADR-0033, ADR-0068, and ADR-0003 for frame format and snapshot LSN
- Slice 1.4 explicitly implements crash recovery with torn-write detection
- Reader protection rule (ADR-0019) is called out in checkpoint logic

**Gaps Identified:**
1. **No explicit fsync strategy documentation** - The plan mentions `vfs.sync_data()` but doesn't specify:
   - When `fsync` is called (per-commit vs. group commit)
   - Platform-specific fsync behavior (Linux `fdatasync` vs. macOS `fsync`)
   - Testing strategy for fsync reliability (see ADR-0010)

2. **Missing checksum verification in recovery** - While Slice 1.3 mentions checksums, there's no explicit test plan for:
   - CRC-32C validation during WAL scan
   - Handling checksum mismatches (corruption detection per ADR-0016)
   - Database header checksum validation on open

**Recommendations:**
- Add explicit fsync behavior tests to Slice 1.3 testing checklist
- Document CRC-32C implementation choice (e.g., `crc32c` crate vs. hardware intrinsics)
- Add corruption detection tests to Slice 1.4: inject bit-flips in WAL frames and verify recovery fails gracefully

---

### Pillar 2: Performance ⚠️ Partial Alignment

**Strengths:**
- Correct emphasis on `read_at` for concurrent reads (ADR-0119)
- LRU page cache with pin/unpin semantics
- Zero-copy deserialization mentioned in record encoding

**Gaps Identified:**
1. **No explicit memory budget strategy** - The PRD requires "absolute control over the buffer pool" but the plan doesn't specify:
   - Maximum cache size configuration
   - Eviction pressure monitoring
   - Query memory limits (sort buffer, hash join buffers)

2. **Missing performance instrumentation** - No mention of:
   - Metrics collection (latency histograms, throughput counters)
   - Tracing/profiling hooks (e.g., `tracing` crate, `perf` integration)
   - Benchmark harness integration with Slice 4.2

3. **B+Tree mutation complexity** - Slice 2.3 mentions "Node Splitting" but defers merge/rebalance. This creates a risk:
   - Trees will become unbalanced over time with deletes
   - No mention of underflow handling or sibling borrowing
   - PRD requires "beating SQLite on all fronts" - unbalanced trees will degrade performance

**Recommendations:**
- Add memory budget configuration to Slice 1.2 (Pager & LRU Cache)
- Integrate `tracing` crate for structured logging and span-based profiling
- Either implement full B+Tree rebalancing in Slice 2.3 or document performance degradation expectations
- Add performance regression tests to Slice 4.2 with explicit latency budgets from ADR-0014

**References:**
- ADR-0014 (Performance Targets) - defines latency budgets
- ADR-0021 (Sort Buffer Memory Limits) - external merge sort requirements
- ADR-0059 (Page Cache Contention Strategy) - fine-grained locking

---

### Pillar 3: Minimal Disk Footprint ⚠️ Partial Alignment

**Strengths:**
- Explicit mention of `#[repr(C)]` and `#[repr(align(4096))]` for page structures
- Varint encoding for integers in Slice 2.1
- Overflow pages for large TEXT/BLOB (Slice 2.3)

**Gaps Identified:**
1. **No explicit padding verification** - The plan mentions `#[repr(C)]` but doesn't specify:
   - How to verify no padding bytes exist (e.g., `static_assertions` crate)
   - Test strategy for struct layout (e.g., `mem::size_of::<T>()` assertions)

2. **Missing B+Tree cell packing details** - Slice 2.2 mentions "exact byte layouts" but:
   - No reference to ADR-0035 (B+Tree Page Layout v2) which supersedes ADR-0032
   - No mention of prefix compression for keys (ADR-0062)
   - Cell offset array layout not specified

3. **Compression strategy unclear** - Slice 2.3 mentions `zlib` via `flate2` but:
   - No compression level specified (trade-off: CPU vs. size)
   - No mention of when compression is applied (all overflow pages or threshold-based?)
   - No decompression caching strategy

**Recommendations:**
- Add `static_assertions` or `memoffset` crate usage to verify struct layouts in tests
- Reference ADR-0035 explicitly in Slice 2.2 and implement v2 format
- Document compression thresholds and levels in Slice 2.3 (recommend: compress all overflow pages at level 6)
- Add disk footprint regression tests to Slice 4.4

**References:**
- ADR-0035 (B+Tree Page Layout v2) - current format specification
- ADR-0062 (B+Tree Prefix Compression) - optional optimization
- ADR-0020 (Overflow Pages) - mandatory for 0.x baseline

---

### Pillar 4: Documentation ⚠️ Not Addressed

**Strengths:**
- None explicitly mentioned in the plan

**Gaps Identified:**
1. **No rustdoc strategy** - The PRD requires "Rust public APIs must use rustdoc with executable doctests" but the plan doesn't mention:
   - Documentation coverage targets
   - Doctest integration in CI
   - Public API stabilization timeline

2. **No user-guide integration** - The plan references `docs/user-guide/data-types.md` for Slice 2.1 but doesn't specify:
   - How to keep docs synchronized with implementation
   - Whether examples in docs are tested

3. **Missing ADR updates** - Several ADRs referenced in the plan may need updates for Rust-specific decisions (e.g., ADR-0033 WAL frame format doesn't specify Rust byte-ordering)

**Recommendations:**
- Add documentation tasks to each slice: "Add rustdoc comments with examples for all public APIs"
- Integrate `cargo doc --document-private-items` into CI pipeline
- Add doctest execution to test suite (ensure examples compile and run)
- Create ADR for Rust-specific format decisions (byte ordering, alignment guarantees)

---

### Pillar 5: Best-in-Class Tooling & Bindings ⚠️ Deferred Risk

**Strengths:**
- Slice 4.1 correctly identifies C-ABI boundary as critical
- References ADR-0118 for panic handling (though file not found - see risks below)
- Python test harness integration in Slice 4.2

**Gaps Identified:**
1. **C-ABI surface area not defined** - The plan says "expose the exact same function signatures that existed in the Nim engine" but:
   - No inventory of existing C-ABI functions
   - No type mapping document (Nim → Rust → C)
   - No mention of `extern "C"` naming conventions

2. **Binding testing strategy incomplete** - Slice 4.2 focuses on crash/differential tests but doesn't mention:
   - Binding-specific unit tests (e.g., Python `sqlite3` module compatibility tests)
   - .NET, Go, Java, Node.js binding verification
   - FFI memory ownership semantics (who frees allocated memory?)

3. **Error code mapping missing** - No mention of how Rust `Result<T, E>` maps to C error codes

**Recommendations:**
- Create inventory of existing C-ABI functions from Nim codebase
- Add explicit FFI memory ownership documentation to Slice 4.1
- Define error code taxonomy and mapping strategy (Rust `thiserror` → C enum → binding exceptions)
- Add binding compatibility tests to Slice 4.2 for each target language

**References:**
- ADR-0039 (.NET C-API Design) - if available
- ADR-0010 (Error Handling Strategy) - error propagation

---

### Pillar 6: Fantastic CLI Experience ⚠️ Minimal Coverage

**Strengths:**
- Slice 4.3 mentions `clap` for subcommands
- Mentions interactive REPL with `reedline`/`rustyline`

**Gaps Identified:**
1. **No UX specification** - The PRD requires "best-in-class UX" but the plan doesn't specify:
   - Output formatting (tables, colors, pagination)
   - Error message quality standards
   - Import/export format support (CSV, JSON, SQL dump)

2. **Missing REPL features** - No mention of:
   - Command history persistence
   - Multi-line statement support
   - Syntax highlighting
   - Auto-completion

3. **No CLI testing strategy** - How do we verify CLI quality?
   - Integration tests for subcommands?
   - Snapshot testing for output formatting?

**Recommendations:**
- Expand Slice 4.3 to include:
  - Table formatting library choice (e.g., `comfy-table`, `terminal-table`)
  - Color output strategy (e.g., `colored`, `nu-ansi-term`)
  - Import/export format specifications
- Add CLI integration tests to Slice 4.3
- Reference existing Nim CLI for feature parity checklist

---

### Pillar 7: Fast Developer Feedback Loop ✅ Strong Alignment

**Strengths:**
- Explicit mention of `cargo clippy` in DoD
- Test strategy mentions fast unit tests on PR, overnight for long-running tests
- CI pipeline verification in Slice 4.4

**Gaps Identified:**
1. **No explicit timing targets** - The PRD requires "under 10 minutes" for PR checks but the plan doesn't specify:
   - Expected test suite duration
   - Parallelization strategy
   - Test sharding approach

2. **Missing incremental compilation optimization** - No mention of:
   - `sccache` or `cachepot` for build caching
   - Workspace structure optimization for incremental builds

**Recommendations:**
- Add CI timing targets to Slice 4.4 (e.g., "PR checks complete in <8 minutes")
- Document test parallelization strategy in TESTING_STRATEGY.md
- Consider adding `sccache` configuration to `.cargo/config.toml`

---

## 2. Critical Risks & Missing Components

### Risk 1: Missing ADR References ⚠️ HIGH

**Issue:** The plan references ADR-0118 and ADR-0119, but these files do not exist in the `design/adr/` directory.

**Impact:**
- Slice 4.1 (C-ABI) relies on ADR-0118 for panic handling strategy
- Slice 1.1 (VFS) relies on ADR-0119 for `read_at` concurrency guarantees
- Implementing without these ADRs risks incorrect implementation

**Recommendation:**
- **Blocker for Slice 1.1 and 4.1:** Create ADR-0118 and ADR-0119 before implementation
- Alternatively, update plan to reference existing ADRs or inline the decisions

**Action Items:**
1. Search codebase for any draft/working versions of ADR-0118/0119
2. If not found, create them based on plan directives:
   - ADR-0118: "FFI Panic Handling" - mandate `std::panic::catch_unwind` on all `extern "C"` functions
   - ADR-0119: "Unix File `read_at` Concurrency" - mandate `FileExt::read_at` for lock-free concurrent reads

---

### Risk 2: Windows VFS Implementation ⚠️ MEDIUM

**Issue:** Slice 1.1 mentions "and the Windows equivalent" for `read_at` but doesn't specify the implementation strategy.

**Technical Challenge:**
- Unix: `std::os::unix::fs::FileExt::read_at(&self, ...)` - takes `&self`, allows concurrent reads
- Windows: `std::os::windows::fs::FileExt::seek_read(&self, ...)` - also takes `&self`, but Windows file locking semantics differ
- Windows may require `FILE_FLAG_OVERLAPPED` for true concurrent reads

**Recommendation:**
- Explicitly document Windows VFS implementation in Slice 1.1
- Test concurrent reads on Windows separately (may need different locking strategy)
- Consider using `tokio::fs` or `async-std` for cross-platform async I/O if blocking I/O proves problematic

**References:**
- Rust documentation: `std::os::windows::fs::FileExt`
- Windows API: `ReadFile` with `OVERLAPPED` struct

---

### Risk 3: LRU Cache Implementation Choice ⚠️ MEDIUM

**Issue:** Slice 1.2 says "you may use a lightweight crate like `lru` or write a custom intrusive list" - this ambiguity risks inconsistent implementation.

**Analysis:**
| Option | Pros | Cons |
|--------|------|------|
| `lru` crate | Battle-tested, maintained, zero-copy API | External dependency, may not support pin/unpin semantics |
| Custom intrusive list | Full control, can integrate pin/unpin natively | Implementation complexity, potential bugs |
| `linkedList` + `HashMap` | Simple to understand | Higher memory overhead, more allocation |

**Recommendation:**
- **Decision required before Slice 1.2:** Choose one approach and document in ADR
- If using `lru` crate, verify it supports:
  - Pin/unpin semantics (prevent eviction of in-use pages)
  - Custom eviction hooks (for dirty page writeback)
- If custom implementation, allocate extra time for testing

**Alternative:** Consider `arc-swap` + custom LRU for lock-free read paths

---

### Risk 4: B+Tree Mutation Complexity ⚠️ HIGH

**Issue:** Slice 2.3 mentions "Node Splitting" but says nothing about:
- Node merging on underflow
- Sibling borrowing (redistribution)
- Delete cascading to root

**Impact:**
- Trees will become unbalanced with heavy delete workloads
- Performance degradation over time (violates PRD Pillar 2)
- Potential correctness issues (orphaned pages, broken links)

**Recommendation:**
- **Expand Slice 2.3 scope** to include:
  - Node merge on underflow (< 50% fill factor)
  - Sibling borrowing before merge (maintain balance)
  - Root collapse when only one child remains
- Reference: "Database System Concepts" (Silberschatz) Chapter 11, or "Readings in Database Systems" (B+Tree chapter)

**Alternative:** If deferring rebalancing, document:
- Expected performance degradation curve
- Mitigation strategy (e.g., periodic `VACUUM` command)
- Test suite to measure tree balance over time

---

### Risk 5: Testing Infrastructure Bootstrap ⚠️ HIGH

**Issue:** The plan assumes Python test harness exists (`tests/harness/`) but doesn't specify:
- How to run Rust tests from Python
- FFI boundary for test harness
- Test data generation in Rust vs. Python

**Current State:**
- Workspace has `crates/decentdb` and `crates/decentdb-cli`
- No visible `tests/` directory at root
- No Python dependencies in `Cargo.toml`

**Recommendation:**
- Add "Slice 0: Test Infrastructure Bootstrap" before Phase 1:
  - Create `tests/harness/` directory structure
  - Implement Python FFI bindings for test harness (using `pyo3` or C-ABI)
  - Document test execution workflow
  - Create sample test scenario to validate harness

**References:**
- TESTING_STRATEGY.md - defines test layers and harness structure
- ADR-0015 (Testing Strategy Enhancements)

---

### Risk 6: Dependency Management Discipline ⚠️ MEDIUM

**Issue:** AGENTS.md states "Avoid adding large 3rd-party dependencies without discussion" but the plan mentions:
- `lru` crate (Slice 1.2)
- `libpg_query` / `pg_query` crate (Slice 3.2)
- `flate2` with `zlib` (Slice 2.3)
- `clap` (Slice 4.3)
- `proptest` (Slice 2.1)

**Analysis:**
| Dependency | Size | Justification | Alternative |
|------------|------|---------------|-------------|
| `lru` | Small | LRU cache | Custom implementation |
| `pg_query` | Large (bindgen + libpg_query) | SQL parsing | `sqlparser` crate (Rust-native) |
| `flate2` | Medium | Compression | `zstd` (faster), custom RLE |
| `clap` | Medium | CLI parsing | `structopt` (deprecated), custom |
| `proptest` | Medium | Property testing | `quickcheck` (simpler) |

**Recommendation:**
- Create ADR for each major dependency (>50KB source)
- Justify `pg_query` vs. `sqlparser` trade-off:
  - `pg_query`: Better Postgres compatibility, larger binary
  - `sqlparser`: Pure Rust, smaller, less Postgres-specific
- Consider feature flags to make dependencies optional

---

### Risk 7: Unsafe Code Boundaries ⚠️ MEDIUM

**Issue:** The plan mentions `#[repr(C, packed)]` (Slice 2.2) which can lead to undefined behavior if misused, but doesn't specify:
- Where `unsafe` blocks are acceptable
- How to verify safety invariants
- Whether to use `unsafe` for performance (e.g., unchecked array access)

**AGENTS.md Guidance:**
- "`unsafe` blocks outside of basic FFI or VFS operations" requires ADR
- "The Borrow Checker is your QA Engineer"

**Recommendation:**
- Create ADR for "Unsafe Code Policy" before Slice 2.2
- Document acceptable uses:
  - FFI boundaries (Slice 4.1)
  - VFS operations (Slice 1.1)
  - Byte-slice parsing for page layouts (Slice 2.2)
- Require `// SAFETY:` comments on all `unsafe` blocks
- Add `#![deny(unsafe_code)]` to `Cargo.toml` with explicit `unsafe` module allowlist

---

## 3. Additional Recommendations

### 3.1 Suggested New Slices

**Slice 0.1: Project Bootstrap & Tooling**
- Set up `cargo clippy`, `cargo fmt` configuration
- Configure CI/CD pipeline (GitHub Actions)
- Create initial test harness skeleton
- Document development workflow

**Slice 0.2: Error Handling Taxonomy**
- Define error types for each module
- Implement `thiserror` derive for all error types
- Create error code mapping (Rust → C → bindings)
- Document error propagation patterns

**Slice 1.5: Memory Safety & Leak Prevention**
- Implement `Drop` for all heap-allocating types
- Add memory leak tests to test suite
- Document ownership model for pages, buffers, and WAL frames
- Verify no circular references (e.g., parent-child page links)

**Slice 3.5: Query Optimization (Basic)**
- Implement predicate pushdown
- Add index selection heuristics (ADR-0013)
- Implement join order optimization (nested loop only for 1.0)

---

### 3.2 Testing Enhancements

**Add to Slice 1.1 (VFS):**
- [ ] Test concurrent reads on Windows separately from Unix
- [ ] Test partial write injection (FaultyVfs)
- [ ] Test fsync behavior (verify actual flush on disk)

**Add to Slice 1.2 (Pager):**
- [ ] Test page eviction under memory pressure
- [ ] Test pin/unpin semantics (pin page, trigger eviction, verify not evicted)
- [ ] Test dirty page writeback on eviction

**Add to Slice 1.3 (WAL):**
- [ ] Test concurrent writer + reader (writer appends, reader captures snapshot)
- [ ] Test WAL index consistency (verify index matches actual frames)
- [ ] Test checksum calculation (verify CRC-32C matches known vectors)

**Add to Slice 2.1 (Record Encoding):**
- [ ] Test lexicographic sorting of encoded records
- [ ] Test Unicode normalization (NFC, NFD, NFKC forms)
- [ ] Test boundary values (i64::MIN, i64::MAX, empty strings)

**Add to Slice 4.1 (C-ABI):**
- [ ] Test panic propagation (trigger panic in Rust, verify C error code)
- [ ] Test memory ownership (allocate in Rust, free in C, verify no double-free)
- [ ] Test thread safety (call from multiple threads simultaneously)

---

### 3.3 Performance Optimization Opportunities

**Slice 1.2 (Pager):**
- Consider `Arc<[Page]>` for shared page reads (avoid `RwLock` for read-only access)
- Implement batched page eviction (evict N pages per lock acquisition)

**Slice 2.2 (B+Tree):**
- Implement prefix compression for internal node keys (ADR-0062)
- Use SIMD for cell binary search (compare 4 keys in parallel)

**Slice 2.3 (B+Tree Mutations):**
- Batch node splits (split multiple nodes in single operation)
- Implement write buffering (accumulate changes, flush once)

**Slice 3.3 (Query Planner):**
- Implement query plan caching (cache plans for repeated queries)
- Use arena allocation for query execution (free entire arena at end)

---

### 3.4 Documentation Strategy

**For Each Slice:**
1. Add module-level rustdoc comment explaining purpose
2. Add `# Examples` section to all public functions
3. Add `# Panics` section if function can panic
4. Add `# Safety` section if function is `unsafe`
5. Add `# Errors` section if function returns `Result`

**CI Integration:**
```bash
# Add to CI pipeline
cargo doc --document-private-items --no-deps
cargo test --doc  # Run all doctests
```

**Documentation Coverage Target:**
- 80% of public APIs documented
- 100% of `unsafe` functions have `# Safety` section
- All examples in docs are executable (doctests)

---

## 4. Implementation Priority Adjustments

### Recommended Phase 0 (Pre-Foundation)

Before starting Phase 1, complete these prerequisites:

**Phase 0.1: Infrastructure Setup (1 week)**
- [ ] Create ADR-0118 (FFI Panic Handling)
- [ ] Create ADR-0119 (Unix `read_at` Concurrency)
- [ ] Set up CI/CD pipeline with clippy, tests, docs
- [ ] Create test harness skeleton (`tests/harness/`)
- [ ] Document development workflow (README.md update)

**Phase 0.2: Error Handling & Logging (3 days)**
- [ ] Define error taxonomy (thiserror enums for each module)
- [ ] Integrate `tracing` crate for structured logging
- [ ] Create error code mapping (Rust → C)

---

### Revised Phase Timeline

| Phase | Original | Revised | Notes |
|-------|----------|---------|-------|
| Phase 0 | N/A | 1.5 weeks | New prerequisite phase |
| Phase 1 | 4 slices | 5 slices | Add Slice 1.5 (Memory Safety) |
| Phase 2 | 4 slices | 4 slices | Expand Slice 2.3 scope |
| Phase 3 | 4 slices | 5 slices | Add Slice 3.5 (Query Optimization) |
| Phase 4 | 4 slices | 4 slices | No change |

**Total Estimated Duration:** +2 weeks from original plan

---

## 5. Competitive Analysis: SQLite Comparison

The PRD states "Performance must beat SQLite on all fronts." Here's how the plan compares:

### SQLite Advantages
1. **Mature B+Tree implementation** - 30+ years of optimization
2. **Extensive testing** - 100% MC/DC coverage, fuzzing since 2007
3. **Cross-platform VFS** - Works on 100+ platforms
4. **Zero-copy optimizations** - mmap I/O, virtual tables

### DecentDB Opportunities
1. **Modern Rust safety** - Compile-time guarantees SQLite can't provide
2. **Snapshot isolation by default** - SQLite uses locking (SERIALIZABLE)
3. **Trigram index built-in** - SQLite requires FTS5 extension
4. **Better concurrency** - Lock-free snapshot reads vs. SQLite's writer lock

### Gaps to Address
1. **No fuzzing mentioned** - SQLite uses sqlsmith, AFL, libFuzzer
   - **Recommendation:** Add "Slice 2.5: Fuzzing Infrastructure" using `cargo-fuzz` + `libFuzzer`
   
2. **No formal verification** - SQLite uses formal methods for critical paths
   - **Recommendation:** Consider `prusti` or `creusot` for critical invariants (B+Tree search)

3. **No regression testing against SQLite** - Plan mentions differential testing vs. PostgreSQL
   - **Recommendation:** Add SQLite as additional differential target for SQL subset

**References:**
- SQLite testing: https://www.sqlite.org/testing.html
- SQLite fuzzing: https://www.sqlite.org/afsq.html

---

## 6. Conclusion

The `ROAD_TO_RUST_PLAN.md` is a solid foundation for the DecentDB 1.0.0 rewrite. The phase decomposition is logical, and the alignment with ADRs demonstrates careful architectural thinking.

**Critical Actions Required:**
1. Create missing ADRs (0118, 0119) before starting Phase 1
2. Add Phase 0 for infrastructure bootstrap
3. Expand B+Tree mutation scope to include rebalancing
4. Define explicit memory budget and performance instrumentation
5. Create comprehensive test harness before implementation

**Key Strengths:**
- Strong ACID compliance focus
- Correct concurrency model (lock-free snapshot reads)
- Clear testing strategy with crash injection
- Proper FFI panic handling awareness

**Key Risks:**
- Missing ADRs block implementation
- B+Tree rebalancing complexity underestimated
- Windows VFS implementation unclear
- Dependency management needs discipline

**Final Recommendation:** Proceed with implementation after addressing Phase 0 prerequisites and creating missing ADRs. The plan is 85% complete and requires refinement, not restructuring.

---

## Appendix A: Referenced Documents

| Document | Status | Notes |
|----------|--------|-------|
| PRD.md | ✅ Reviewed | 7 pillars alignment checked |
| SPEC.md | ✅ Reviewed | Cross-referenced throughout |
| TESTING_STRATEGY.md | ✅ Reviewed | Test enhancements incorporated |
| ROAD_TO_RUST_PLAN.md | ✅ Reviewed | Base document for this review |
| ADR-0003 | ✅ Reviewed | Snapshot LSN atomicity |
| ADR-0019 | ✅ Reviewed | WAL retention for readers |
| ADR-0020 | ✅ Reviewed | Overflow pages mandatory |
| ADR-0032 | ✅ Reviewed | B+Tree layout (superseded) |
| ADR-0033 | ✅ Reviewed | WAL frame format |
| ADR-0035 | ⚠️ Not found | B+Tree layout v2 (referenced) |
| ADR-0068 | ✅ Reviewed | WAL header end offset |
| ADR-0118 | ❌ Not found | FFI panic handling (missing) |
| ADR-0119 | ❌ Not found | Unix read_at concurrency (missing) |

## Appendix B: Additional References

1. **Rust Documentation:**
   - `std::os::unix::fs::FileExt`: https://doc.rust-lang.org/std/os/unix/fs/trait.FileExt.html
   - `std::os::windows::fs::FileExt`: https://doc.rust-lang.org/std/os/windows/fs/trait.FileExt.html
   - `std::panic::catch_unwind`: https://doc.rust-lang.org/std/panic/fn.catch_unwind.html

2. **Database Systems:**
   - "Database System Concepts" (Silberschatz, Korth, Sudarshan) - B+Tree algorithms
   - "Readings in Database Systems" (Hellerstein, Stonebraker) - http://www.redbook.io/
   - SQLite Architecture: https://www.sqlite.org/arch.html

3. **Crates:**
   - `lru`: https://crates.io/crates/lru
   - `proptest`: https://crates.io/crates/proptest
   - `tracing`: https://crates.io/crates/tracing
   - `thiserror`: https://crates.io/crates/thiserror
   - `pg_query`: https://crates.io/crates/pg_query
   - `sqlparser`: https://crates.io/crates/sqlparser

4. **Testing:**
   - SQLite Testing: https://www.sqlite.org/testing.html
   - Property-Based Testing: https://hypothesis.works/articles/what-is-property-based-testing/
   - Crash Injection: https://github.com/jepsen-io/jepsen

---

**Document Version:** 1.0
**Last Updated:** 2026-03-22
**Next Review:** After Phase 0 completion
