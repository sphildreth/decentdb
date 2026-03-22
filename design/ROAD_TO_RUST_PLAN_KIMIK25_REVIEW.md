# Review: Road to Rust Plan

**Reviewer:** Kimi K2.5  
**Date:** 2026-03-22  
**Documents Analyzed:**
- `design/ROAD_TO_RUST_PLAN.md`
- `design/PRD.md`
- `docs/user-guide/data-types.md`

---

## Executive Summary

The Road to Rust plan represents a methodical, bottom-up approach to rewriting DecentDB from Nim to Rust. It is well-structured into 4 phases and 16 slices, with clear directives for implementation agents. The plan demonstrates strong alignment with the 7 pillars defined in PRD.md, particularly ACID compliance (Pillar 1), performance (Pillar 2), and minimal disk footprint (Pillar 3).

**Overall Assessment:** The plan is technically sound, comprehensive, and appropriately ambitious. However, several areas require refinement, particularly around dependency management, testing integration, and timeline realism.

---

## Detailed Analysis by PRD Pillar

### Pillar 1: ACID Compliance is Forefront

**Strengths:**

1. **WAL-First Architecture**: The plan correctly prioritizes WAL implementation (Slice 1.3) before B+Tree mutations (Slice 2.3). This is the correct order—durability must exist before state modifications.

2. **Crash Recovery Integration**: Slice 1.4 explicitly includes crash recovery and checkpointing, with specific references to ADR-0019 (reader-protection rule). This demonstrates awareness of the complexities involved in maintaining consistency during WAL truncation.

3. **Fault Injection Framework**: Slice 1.1 mandates a `FaultyVfs` implementation to simulate dropped fsyncs and partial writes. This is exceptional—it shows the team understands that testing durability requires adversarial conditions, not just happy-path tests.

4. **Reader Protection**: The explicit mention of `min(active_reader_lsn) < wal_end_lsn` (ADR-0019) proves understanding of the fundamental challenge in MVCC: readers must not see truncated history.

**Concerns & Recommendations:**

1. **WAL File Growth**: The plan references ADR-0024 (WAL growth prevention) but doesn't explicitly include it as a slice. Without proactive WAL size management, long-running readers could cause unbounded disk growth. **Recommendation:** Add a dedicated slice or checkpoint criteria for WAL size management.

2. **Checksum Verification**: The plan mentions checksum calculation in Slice 1.3 but should explicitly reference ADR-0064 (WAL frame checksum removal) and ADR-0016 (database header checksum) to ensure consistency with recent design decisions.

3. **fsync Strategy Granularity**: While `sync_data()` is mentioned, the plan should clarify whether metadata syncs happen separately and under what conditions (Linux vs. macOS vs. Windows behavior differs significantly).

### Pillar 2: Uncompromising Performance

**Strengths:**

1. **Lock-Free Reads**: Slice 1.1 explicitly directs agents to use `read_at` with `&self` (immutable reference) to enable concurrent reads without Mutex. This aligns perfectly with PRD Pillar 2's "lock-free snapshot isolation" requirement.

2. **Zero-Copy Deserialization**: The B+Tree layout directives (Slice 2.2) mandate `#[repr(C, packed)]` and byte-slice parsing, which minimizes memory copies during reads.

3. **Pin/Unpin Semantics**: Slice 1.2 requires pin/unpin semantics for the page cache, preventing eviction of pages actively being used by queries—critical for avoiding thrashing during large scans.

4. **Buffer Reuse**: Slice 3.3 explicitly requires execution buffers to be reused during tight loops, preventing allocation overhead during query execution.

5. **Compression Strategy**: Slice 2.3 correctly mandates zlib compression for overflow pages only (>512 bytes), avoiding the CPU overhead of compressing small values where the space savings wouldn't justify the cost.

**Concerns & Recommendations:**

1. **LRU Cache Implementation**: The plan mentions using "a lightweight crate like `lru`" or a custom implementation. This is too vague—the choice here is performance-critical. **Recommendation:** Benchmark both options early; custom intrusive lists often outperform generic crates for database workloads due to cache locality and predictable eviction patterns.

2. **Memory Pressure Handling**: The plan specifies LRU eviction but doesn't discuss how to handle memory pressure from the OS or what happens when the system runs low on RAM. **Recommendation:** Include a slice or directive for memory pressure callbacks and graceful degradation.

3. **Checkpoint Frequency**: Slice 1.4 mentions checkpointing but doesn't specify frequency criteria. **Recommendation:** Reference ADR-0058 (background incremental checkpoint worker) and define checkpoint triggers (WAL size threshold, time-based, or dirty page ratio).

4. **Group Commit**: While not strictly necessary for 1.0.0, ADR-0037 (group commit/WAL batching) could significantly improve write throughput. Consider including this as a stretch goal in Phase 4.

### Pillar 3: Minimal Disk Footprint

**Strengths:**

1. **Explicit Memory Layouts**: The plan consistently mandates `#[repr(C)]` and `#[repr(packed)]` to eliminate padding bytes (Slices 1.2, 2.2). This directly addresses PRD Pillar 3.

2. **Varint Encoding**: Slice 2.1 requires Leb128 varint encoding for Int64, ensuring small integers take minimal space. The plan specifically calls out that "the number 5 should take 1 byte on disk, not 8."

3. **UUID as Binary**: Slice 2.1 explicitly requires UUID to be stored as 16 raw bytes, not a 36-character string—a 56% space savings.

4. **Overflow Page Threshold**: The 512-byte inline threshold for TEXT/BLOB (per `data-types.md`) is reasonable, balancing space efficiency with read locality.

**Concerns & Recommendations:**

1. **B+Tree Prefix Compression**: ADR-0062 describes prefix compression for B+Tree nodes, which could yield significant space savings for string-heavy workloads. **Recommendation:** Consider including this in Phase 2 or as a Phase 4 optimization.

2. **Free Space Management**: While Slice 2.3 mentions node splitting, the plan doesn't explicitly address how free space within pages is managed or reclaimed. **Recommendation:** Reference ADR-0012 (BTree space management) and ensure free space maps are part of the implementation.

3. **Freelist Atomicity**: ADR-0051 discusses freelist atomicity challenges. The plan should ensure that page allocation/deallocation is WAL-logged to prevent leaks during crashes.

### Pillar 4: World-Class Documentation

**Strengths:**

1. **Doctest Requirement**: Slice 4.4 mentions rustdoc with executable doctests, ensuring examples remain current.

2. **ADR References**: The plan consistently references ADRs, creating traceability between implementation and design decisions.

3. **Testing Directives**: Every slice includes testing requirements, which serve as executable documentation of expected behavior.

**Concerns & Recommendations:**

1. **Public API Documentation**: While the CLI (Slice 4.3) and C-API (Slice 4.1) are documented, the Rust crate's public API surface needs explicit documentation requirements. **Recommendation:** Add a requirement for comprehensive rustdoc coverage (>90%) before 1.0.0.

2. **Architecture Documentation**: The plan focuses on implementation but doesn't explicitly require updating architecture docs. As implementation reveals gaps or changes in the ADRs, documentation must be updated. **Recommendation:** Add a "documentation sync" checkpoint at the end of each phase.

3. **Migration Guide**: Since this is a rewrite from Nim, existing users will need migration guidance. **Recommendation:** Add Slice 4.5 or include migration documentation in Phase 4.

### Pillar 5: Best-in-Class Toolings & Bindings

**Strengths:**

1. **C-ABI First**: Slice 4.1 prioritizes the C-ABI boundary, correctly recognizing that all other bindings (Python, .NET, Go, Java, Node.js, Dart) depend on this foundation.

2. **Panic Safety**: The plan correctly emphasizes `std::panic::catch_unwind` on every `extern "C"` function (ADR-0118), preventing Rust panics from unwinding across FFI boundaries—which is undefined behavior.

3. **Multi-Language Support**: The PRD lists 7 target languages, and the plan acknowledges this via the C-ABI foundation.

**Concerns & Recommendations:**

1. **Binding Implementation Order**: The plan doesn't specify which bindings to implement first. **Recommendation:** Prioritize Python (for testing) and .NET (per existing .NET ADRs like ADR-0039, ADR-0043, ADR-0046) before other languages.

2. **FFI Performance**: While the C-ABI is zero-overhead, marshaling data across the boundary can be expensive. **Recommendation:** Include performance benchmarks for FFI calls and document best practices for batching operations.

3. **Error Propagation**: The C-API needs a consistent error handling strategy. ADR-0010 (error handling strategy) should be explicitly referenced in Slice 4.1.

### Pillar 6: Fantastic CLI Experience

**Strengths:**

1. **Clap Integration**: Slice 4.3 mandates using `clap`, the standard Rust CLI framework, which provides robust argument parsing and help generation.

2. **REPL Implementation**: The plan includes an interactive REPL using rustyline/reedline, essential for a database CLI.

3. **Command Completeness**: The listed commands (exec, repl, import, export, checkpoint, save-as) cover the essential CLI surface area.

**Concerns & Recommendations:**

1. **Formatting & Output**: While commands are listed, the plan doesn't specify output formatting requirements. **Recommendation:** Define requirements for table formatting (e.g., box-drawing characters), JSON output mode, and CSV export format.

2. **History & Completion**: The plan mentions a REPL but doesn't specify if it needs persistent history or tab completion. **Recommendation:** Include these as requirements for parity with `psql` and `sqlite3`.

3. **Error Presentation**: Error messages should be helpful and contextual. **Recommendation:** Reference ADR-0010 and ensure CLI errors include SQL context (line/column numbers) where applicable.

### Pillar 7: Fast Developer Feedback Loop

**Strengths:**

1. **Test Stratification**: The plan distinguishes between fast PR checks and overnight tests (crash-injection, differential fuzzing), aligning with the 10-minute PR check requirement.

2. **Python Test Harness**: Slice 4.2 explicitly hooks into the existing Python test harness, ensuring continuity with established testing infrastructure.

3. **CI Integration**: Slice 4.4 mentions ensuring CI pipelines are green, showing awareness of automation needs.

**Concerns & Recommendations:**

1. **Unit Test Granularity**: While the plan mentions unit tests, it doesn't specify coverage targets or testing frameworks. **Recommendation:** Explicitly require `cargo test` with coverage reporting (e.g., tarpaulin or llvm-cov) and set coverage thresholds (e.g., >80% for core modules).

2. **Property-Based Testing**: Slice 2.1 mentions `proptest` for record encoding—this should be expanded to other slices, particularly B+Tree operations and WAL recovery.

3. **Differential Testing**: The plan mentions differential testing against SQLite (Slice 4.2) but doesn't define scope or acceptance criteria. **Recommendation:** Specify which SQL features to differentially test and acceptable divergence thresholds.

4. **Benchmark Regression**: While Slice 4.4 mentions benchmarks against the Nim engine, it doesn't specify how regressions will be caught. **Recommendation:** Set up continuous benchmarking (e.g., Criterion.rs with CI integration) to catch performance regressions automatically.

---

## Phase-by-Phase Assessment

### Phase 1: Foundation (Storage & I/O)

**Assessment:** Critical and correctly prioritized. The dependency chain (VFS → Pager → WAL → Recovery) is logical.

**Key Dependencies:**
- Slices 1.1-1.4 must be completed sequentially; no parallelism possible here.
- Phase 2 depends entirely on Phase 1 completion.

**Risks:**
1. **LRU Cache Complexity**: Slice 1.2's LRU cache with pin/unpin semantics is deceptively complex. Getting eviction right under concurrent access is error-prone.
2. **Fault Injection Coverage**: Slice 1.1's FaultyVfs needs comprehensive failure modes (fsync failure, partial write, power loss at various points).

**Recommendations:**
- Consider using `parking_lot` for RwLock implementations—it's faster and more ergonomic than std::sync.
- Add explicit testing for torn writes at page boundaries (where a write spans two OS pages).

### Phase 2: Data Structures

**Assessment:** Well-structured, with read-only traversal preceding mutations. This allows early integration testing.

**Key Dependencies:**
- Slice 2.1 (Record Encoding) is foundational for 2.2-2.4.
- Slice 2.2 (Read-only B+Tree) must be stable before 2.3 (Mutations).

**Risks:**
1. **Overflow Page Chains**: Slice 2.3's overflow handling with compression adds complexity. The linked list structure can become corrupted during crashes.
2. **Trigram Index Performance**: Slice 2.4's posting list intersection must be optimized for common trigram queries or it will be a performance bottleneck.

**Recommendations:**
- For Slice 2.1, consider using the `leb128` crate rather than implementing varints manually—it's well-tested.
- For Slice 2.3, ensure overflow page compression is optional or has a threshold—compressing already-compressed data wastes CPU.

### Phase 3: Relational Core

**Assessment:** The most complex phase. The dependency on `libpg_query` (Slice 3.2) introduces external complexity.

**Key Dependencies:**
- Slice 3.1 (Catalog) must be completed early as it defines metadata storage.
- Slice 3.2 (SQL Parsing) depends on `libpg_query` integration.
- Slices 3.3-3.4 depend on both 3.1 and 3.2.

**Risks:**
1. **libpg_query Integration**: The plan mentions using `pg_query` crate or custom FFI. This is a major decision point. The `pg_query` crate may not expose all needed features.
2. **AST Conversion Overhead**: Converting Postgres AST JSON to Rust enums has allocation overhead. For high-throughput scenarios, this could be a bottleneck.
3. **Statement-Level Rollback**: Slice 3.4 requires statement-level rollback (if row 5 fails, rows 1-4 revert). This needs nested savepoints in the WAL, adding complexity.

**Recommendations:**
- Evaluate `pg_query` crate early in Phase 3; if insufficient, budget time for custom FFI bindings.
- For Slice 3.3, consider an async/await execution model rather than blocking iterators—it enables better resource utilization for I/O-bound operations.
- For Slice 3.4, implement savepoints in the WAL first, then build statement rollback on top.

### Phase 4: Ecosystem Parity

**Assessment:** Essential for 1.0.0, but should be parallelized where possible.

**Key Dependencies:**
- Slice 4.1 (C-ABI) is the foundation for 4.2.
- Slice 4.4 depends on all previous work.

**Risks:**
1. **FFI ABI Stability**: Maintaining exact Nim engine signatures may be challenging if Rust's ownership model forces API changes.
2. **Python Test Harness Compatibility**: The harness may rely on Nim-specific behaviors or memory layouts.

**Recommendations:**
- For Slice 4.1, use `cbindgen` to generate C headers automatically from Rust code—ensures headers stay in sync.
- For Slice 4.2, run the Python harness against the Rust implementation incrementally during development, not just at the end.

---

## Cross-Cutting Concerns

### 1. Dependency Management

The plan mentions several potential crate dependencies:
- `lru` or custom LRU (Slice 1.2)
- `flate2` for compression (Slice 2.3)
- `pg_query` or custom FFI (Slice 3.2)
- `clap` (Slice 4.3)
- `proptest` (Slice 2.1)

**Concern:** AGENTS.md explicitly states "Avoid adding dependencies to `Cargo.toml`; if you must, create an ADR." The plan should clarify which dependencies are pre-approved vs. requiring ADRs.

**Recommendation:** Create an ADR for the dependency strategy, pre-approving essential crates (clap, flate2, proptest) and defining criteria for adding new ones.

### 2. Error Handling Strategy

The plan mentions error handling in various slices but doesn't define a unified strategy.

**Recommendation:** Reference ADR-0010 and mandate a consistent error type across all modules. Consider using `thiserror` for ergonomic error definitions and `anyhow` for application-level error propagation.

### 3. Unsafe Code Policy

AGENTS.md states: "Using `unsafe` heavily where safe abstractions exist" is out of scope.

**Concern:** Slices 1.2 and 2.2 mention `#[repr(C)]` and byte-slice parsing, which often require `unsafe` for zero-copy deserialization.

**Recommendation:** Define clear boundaries for `unsafe` usage:
- Allowed: FFI boundaries, VFS file operations, explicit memory layout parsing
- Required: Extensive `// SAFETY:` comments explaining invariants
- Forbidden: Unsafe for performance optimization where safe alternatives exist

### 4. Concurrency Model

AGENTS.md states: "Current concurrency model: single process with one writer and multiple concurrent reader threads."

**Concern:** The plan doesn't explicitly address how the single-writer constraint is enforced. Is it at the VFS level? Pager level? Higher?

**Recommendation:** Add a slice or directive specifying the writer coordination mechanism (e.g., a writer mutex in the Pager, or WAL-level serialization).

### 5. Testing Strategy

While the plan mentions testing throughout, it lacks a comprehensive testing strategy document reference.

**Recommendation:** Reference `TESTING_STRATEGY.md` (mentioned in AGENTS.md) and ensure each slice's testing requirements align with the broader strategy.

---

## Strengths of the Plan

1. **Bottom-Up Architecture**: Starting with VFS and building up ensures each layer has a solid foundation. This is much safer than top-down approaches where high-level features are built on unproven storage.

2. **Test-First Mentality**: Every slice includes testing directives. This is crucial for a database where correctness is paramount.

3. **ADR Integration**: Consistent references to ADRs create traceability and ensure design decisions are documented.

4. **Performance Awareness**: The plan doesn't just focus on correctness—it explicitly addresses performance through zero-copy, lock-free reads, and buffer reuse.

5. **Safety Focus**: Mandating panic safety at FFI boundaries and proper error handling shows maturity in Rust development practices.

6. **Incremental Delivery**: The slice structure allows for incremental progress and early validation of foundational components.

---

## Areas for Improvement

### High Priority

1. **Add WAL Growth Management**: Reference ADR-0024 and include explicit slices or acceptance criteria for preventing unbounded WAL growth.

2. **Define Checkpoint Triggers**: Specify when checkpoints occur (size, time, dirty page ratio) and how background checkpointing works.

3. **Clarify SQL Parser Decision**: Decide early on `pg_query` vs. custom FFI, as this affects Phase 3 timeline significantly.

4. **Add Migration Guide**: Existing Nim users need a clear path to Rust DecentDB.

### Medium Priority

5. **Expand Property-Based Testing**: Apply `proptest` beyond Slice 2.1—particularly to WAL recovery and B+Tree operations.

6. **Define FFI Error Handling**: Specify how errors propagate across the C-ABI boundary.

7. **Add Continuous Benchmarking**: Set up Criterion.rs benchmarks that run on every PR to catch regressions.

8. **Document Memory Pressure Handling**: Define behavior when the system runs low on memory.

### Low Priority

9. **Consider Group Commit**: While not critical for 1.0.0, ADR-0037 could significantly improve write throughput.

10. **B+Tree Prefix Compression**: ADR-0062 could yield significant space savings.

11. **Async Execution**: Consider async/await for query execution to improve resource utilization.

---

## Timeline & Resource Estimation

**Estimated Effort:**

- **Phase 1** (Foundation): 6-8 weeks
  - Slice 1.1: 1 week
  - Slice 1.2: 2 weeks (LRU cache is complex)
  - Slice 1.3: 2 weeks (WAL is critical, needs thorough testing)
  - Slice 1.4: 1-2 weeks (recovery logic)

- **Phase 2** (Data Structures): 8-10 weeks
  - Slice 2.1: 1 week
  - Slice 2.2: 2 weeks (byte layout precision required)
  - Slice 2.3: 4 weeks (node splitting is complex)
  - Slice 2.4: 1-2 weeks

- **Phase 3** (Relational Core): 10-12 weeks
  - Slice 3.1: 2 weeks
  - Slice 3.2: 3-4 weeks (parser integration risk)
  - Slice 3.3: 3 weeks
  - Slice 3.4: 2-3 weeks

- **Phase 4** (Ecosystem): 6-8 weeks
  - Slice 4.1: 2 weeks
  - Slice 4.2: 2 weeks (test harness integration)
  - Slice 4.3: 1-2 weeks
  - Slice 4.4: 1-2 weeks

**Total:** Approximately 30-38 weeks (7-9 months) with 2-3 experienced Rust developers.

**Critical Path:**
Phase 1 → Phase 2 → Phase 3 → Phase 4.1 → Phase 4.2 → Phase 4.4

**Parallelizable Work:**
- CLI (Slice 4.3) can be developed in parallel with Core after Slice 3.2.
- Documentation updates can happen throughout.

---

## Risk Matrix

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|------------|
| libpg_query integration issues | Medium | High | Evaluate early; budget time for custom FFI |
| WAL corruption on crash | Low | Critical | Extensive FaultyVfs testing; formal verification of recovery logic |
| Performance regression vs. Nim | Medium | High | Continuous benchmarking; optimize hot paths |
| LRU cache bugs under concurrency | Medium | High | Property-based testing; miri for UB detection |
| FFI ABI incompatibility | Low | Medium | Use cbindgen; incremental testing with Python harness |
| Scope creep (new features) | High | Medium | Strict ADR process; defer non-essential features |
| Developer turnover/Rust learning curve | Medium | Medium | Code reviews; pair programming; comprehensive docs |

---

## Recommendations Summary

### Immediate Actions (Before Phase 1)

1. **Create ADR for Dependency Strategy**: Pre-approve essential crates; define criteria for new additions.
2. **Finalize SQL Parser Decision**: Evaluate `pg_query` crate; decide on custom FFI if needed.
3. **Define Acceptance Criteria**: Specify concrete benchmarks (vs. Nim, vs. SQLite) and coverage thresholds.

### During Phase 1

4. **Implement FaultyVfs Early**: Use it to test every VFS operation under failure conditions.
5. **Benchmark LRU Options**: Compare `lru` crate vs. custom implementation for real workloads.
6. **Document Unsafe Boundaries**: Create a policy document for where `unsafe` is acceptable.

### During Phase 2

7. **Property Test B+Tree**: Use `proptest` to generate random trees and verify invariants after operations.
8. **Profile Serialization**: Benchmark varint encoding/decoding; ensure it meets performance targets.

### During Phase 3

9. **Incremental Parser Testing**: Test SQL parsing incrementally; don't wait until the end.
10. **Implement Savepoints Early**: Required for statement-level rollback; test thoroughly.

### During Phase 4

11. **Parallel Binding Development**: Start .NET and Python bindings as soon as C-ABI is stable.
12. **Migrate Documentation**: Update all docs to reflect Rust implementation details.

---

## Conclusion

The Road to Rust plan is a solid, well-thought-out blueprint for rewriting DecentDB. It demonstrates deep understanding of database internals, Rust best practices, and the importance of ACID compliance. The phased approach mitigates risk by building from the ground up, and the emphasis on testing—particularly crash-injection testing—is exactly what a database project needs.

The primary areas for improvement are:
1. **Clarifying dependencies** and requiring ADRs for new crates
2. **Adding WAL growth management** to prevent disk exhaustion
3. **Defining checkpoint triggers** and background checkpointing
4. **Finalizing the SQL parser strategy** early to avoid Phase 3 delays
5. **Expanding property-based testing** beyond Slice 2.1

With these refinements, the plan provides a strong foundation for delivering DecentDB 1.0.0—a Rust-based database that meets all 7 pillars of the PRD.

---

## References

**Primary Documents:**
- `design/ROAD_TO_RUST_PLAN.md` - Implementation plan
- `design/PRD.md` - Product Requirements & 7 Pillars
- `docs/user-guide/data-types.md` - Type system specifications

**Referenced ADRs:**
- ADR-0001: Page Size
- ADR-0003: Snapshot LSN Atomicity
- ADR-0004: WAL Checkpoint Strategy
- ADR-0007: Trigram Postings Storage Strategy
- ADR-0008: Trigram Pattern Length Guardrails
- ADR-0009: Foreign Key Enforcement Timing
- ADR-0010: Error Handling Strategy
- ADR-0011: Memory Management Strategy
- ADR-0012: BTree Space Management
- ADR-0013: Index Statistics Strategy
- ADR-0015: Testing Strategy Enhancements
- ADR-0016: Database Header Checksum
- ADR-0017: Bulk Load API Design
- ADR-0018: Checkpointing Reader Count Mechanism
- ADR-0020: Overflow Pages for Blobs
- ADR-0021: Sort Buffer Memory Limits
- ADR-0023: Isolation Level Specification
- ADR-0024: WAL Growth Prevention Long Readers
- ADR-0026: Race Condition Testing Strategy
- ADR-0027: Bulk Load API Specification
- ADR-0031: Overflow Page Format
- ADR-0032: BTree Node Layout (assumed from reference)
- ADR-0033: WAL Frame Format
- ADR-0034: Compact Int64 Record Payload
- ADR-0035: BTree Node Layout (assumed from reference)
- ADR-0036: Integer Primary Key
- ADR-0037: Group Commit WAL Batching
- ADR-0038: Cost Based Optimization Deferred
- ADR-0039: .NET C API Design
- ADR-0043: .NET String Encoding
- ADR-0046: .NET Connection String Design
- ADR-0048: SQL Parameterized Limit Offset
- ADR-0049: Constraint Index Deduplication
- ADR-0050: Explain Statement
- ADR-0051: Freelist Atomicity
- ADR-0052: Trigram Durability
- ADR-0053: Fine Grained WAL Locking
- ADR-0054: Lock Contention Improvements
- ADR-0055: Thread Safety and Snapshot Context
- ADR-0056: WAL Index Pruning on Checkpoint
- ADR-0057: Transactional Freelist Header Updates
- ADR-0058: Background Incremental Checkpoint Worker
- ADR-0059: Page Cache Contention Strategy
- ADR-0061: Typed Index Key Encoding Text Blob
- ADR-0062: BTree Prefix Compression
- ADR-0063: Trigram Postings Paging Format
- ADR-0064: WAL Frame Checksum Removal
- ADR-0065: WAL Frame LSN Removal
- ADR-0066: WAL Frame Payload Length Removal
- ADR-0067: WAL Mmap Write Path
- ADR-0068: WAL Header End Offset
- ADR-0069: Composite Primary Keys and Indexes
- ADR-0118: Rust FFI Panic Safety
- ADR-0119: Rust VFS Pread Pwrite
- ADR-0120: Core Storage Engine BTree
- ADR-0121: CLI Engine Enhancements

**External References:**
- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- [SQLite Architecture](https://www.sqlite.org/arch.html)
- [PostgreSQL WAL](https://www.postgresql.org/docs/current/wal-intro.html)
- [Clap Documentation](https://docs.rs/clap/latest/clap/)
- [Proptest Book](https://altsysrq.github.io/proptest-book/)

---

*End of Review*
