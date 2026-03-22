# DecentDB 1.0 Rust Rewrite: Final Consolidated Requirements
**Date:** 2026-03-22  
**Status:** Consolidated Master Requirements

## Executive Summary
This document defines the consolidated requirements for the DecentDB rewrite from Nim to Rust. It is intended to replace the baseline execution plan and its separate reviews as the primary implementation-guiding requirements artifact for engineering, architecture, product, and coding agents.

The rewrite shall preserve DecentDB's core product intent: durable ACID writes first, fast reads second, compact storage, strong FFI compatibility, a robust CLI, and a fast developer feedback loop. The implementation shall remain single-process, with one writer and multiple concurrent readers, and shall use safe Rust by default.

The most important corrections incorporated into this consolidated document are:
- The WAL implementation shall target the current v8 format, not older intermediate WAL layouts.
- The B+Tree implementation shall use the accepted compact varint-based page layout and shall not rely on stale slotted-page assumptions.
- Foundational requirements omitted or underrepresented in the baseline plan are now in scope: database bootstrap and header validation, page allocation/freelist basics, reader tracking, shared WAL ownership, in-memory VFS support, early crash-harness integration, and explicit error-handling and documentation expectations.
- Release readiness shall be determined by explicit workload and correctness criteria, not by vague claims or late-stage validation alone.

## Objectives / Goals
- Deliver a Rust-based DecentDB core that preserves durable ACID semantics under sudden power loss, process termination, and partial-write scenarios.
- Provide high-performance point reads, range reads, and snapshot-isolated concurrent reads under the one-writer / many-readers model.
- Maintain compact on-disk storage through page-oriented design, compact encodings, overflow handling, and efficient index formats.
- Preserve the C-ABI boundary required by existing and future bindings.
- Provide a CLI and documentation set suitable for everyday developer and operator use.
- Keep the primary PR feedback loop fast, while shifting exhaustive crash, differential, and long-running performance validation into nightly lanes.

## Scope
### In Scope
- Rust rewrite of the DecentDB core engine and CLI within the existing Cargo workspace.
- Single-process concurrency model with exactly one active writer transaction and multiple concurrent readers.
- Database file bootstrap, header validation, format/version checks, page-size validation, root-page initialization, page allocation, and freelist management.
- Virtual file system abstractions for:
  - on-disk I/O,
  - fault injection for crash testing,
  - in-memory databases via `:memory:`.
- WAL-only durability with `fsync` on commit by default.
- WAL recovery, checkpointing, reader-aware WAL retention, and shared WAL ownership across multiple connections to the same on-disk database.
- Pager and bounded page cache with pin/unpin semantics, dirty tracking, and eviction policy.
- Record serialization, index-key encoding, overflow pages, B+Tree storage, B+Tree mutations, and trigram index storage/query support.
- System catalog, schema versioning, SQL parsing, planning, execution, DML, constraints, rollback behavior, and bulk-load support for the documented 1.0 subset.
- C-ABI compatibility, FFI error handling, and early smoke-test coverage for bindings.
- Python-driven crash and differential test harness bootstrap and ongoing use.
- CLI feature set, documentation, benchmarks, and CI gating required for 1.0 release readiness.

### Out of Scope
- Multi-process concurrency, shared-memory locking, or cross-process WAL coordination.
- Heavy `unsafe` usage outside basic FFI, VFS, or tightly bounded layout parsing with documented safety invariants.
- Large new runtime dependencies without an accepted ADR and explicit justification.
- Background or incremental checkpoint workers as a 1.0 requirement.
- B+Tree merge/rebalance as mandatory 1.0 behavior during normal deletes; 1.0 may rely on split handling plus compaction/rebuild strategies instead.
- SQL features outside the documented 1.0 subset in `SPEC.md`.

## Functional Requirements
### 1. Database Lifecycle and File Format
- The engine must support database create, open, validate, and close operations.
- The main database file must use the documented fixed header, including magic bytes, format version, page size, checksum, schema cookie, root page references, freelist metadata, and checkpoint metadata.
- Header validation must occur before normal engine startup. Invalid magic, unsupported format versions, invalid page sizes, or checksum mismatches must fail cleanly with corruption-class errors.
- Page size shall default to 4096 bytes and shall be configurable only at database creation time.
- Page allocation, freelist updates, and header updates must be crash-safe and consistent with WAL semantics.

### 2. Virtual File System and I/O Abstraction
- All storage I/O shall go through a VFS abstraction.
- The VFS surface shall cover positional reads/writes, sync operations, file-size queries, existence checks, and file removal where required by the engine lifecycle.
- The implementation must provide:
  - `OsVfs` for normal on-disk operation,
  - `FaultyVfs` for deterministic crash and fault injection,
  - `MemVfs` for `:memory:` databases.
- Primary page-cache I/O shall use positional file APIs (`read_at` / `write_at` or platform equivalents) and shall not depend on `mmap` for correctness.
- Cross-platform parity is required for Unix and Windows positional I/O behavior.

### 3. Transactions, Concurrency, WAL, Recovery, and Checkpointing
- The engine shall implement Snapshot Isolation as the default and only 1.0 isolation level.
- Snapshot acquisition shall use an atomic `wal_end_lsn` publication model with correct acquire/release ordering.
- Connections to the same on-disk database must share a single WAL instance through a process-global registry keyed by canonical path.
- The authoritative WAL file format for 1.0 is the current v8 format:
  - fixed 32-byte WAL header,
  - logical end offset (`wal_end_offset`),
  - frame types for page, commit, and checkpoint records,
  - LSNs derived from WAL byte offsets,
  - no per-frame payload-length field in the active format,
  - per-frame checksum reserved and not relied upon as the primary recovery validator in the active format.
- Recovery must scan only to `wal_end_offset`, not to physical file size.
- Recovery must guarantee:
  - committed transactions become visible after reopen,
  - uncommitted transactions do not become visible,
  - incomplete or invalid trailing WAL data does not corrupt the database view.
- Checkpointing must copy committed pages to the main DB file and must respect reader snapshot requirements.
- The system must track active readers and their snapshot LSNs.
- WAL truncation must never remove frames still required to satisfy an active reader's snapshot.
- Long-running readers may trigger warnings, monitoring events, or administrative controls, but 1.0 correctness must not depend on truncating WAL needed by active readers.
- If physical WAL truncation is deferred, the in-memory WAL index must still support safe pruning of obsolete entries after checkpoint completion.

### 4. Pager, Page Cache, and Memory Ownership
- The pager must provide fixed-size page management, page fetch/release, dirty tracking, page allocation, and cache integration.
- The page cache must be bounded by configuration and must support pin/unpin semantics so in-use pages are never evicted.
- Eviction must prefer the least-recently-used unpinned page.
- Dirty-page ownership and writeback responsibility must be explicit. The implementation must not leave ambiguity about whether the pager, WAL layer, or caller flushes dirty state.
- Runtime page buffers shall be represented as page-size-aware byte buffers. Exact on-disk layouts may use explicit header structs or byte parsing, but the implementation must avoid unsafe packed-field access patterns that violate alignment guarantees.
- The engine must define clear `Send` / `Sync` expectations for the VFS, pager, page cache, WAL structures, and reader-visible page handles.

### 5. Record Encoding, Index Keys, B+Tree Storage, and Overflow Pages
- Row/record serialization and index-key encoding shall be treated as separate concerns.
- Record encoding must support the documented core types: NULL, INT64, FLOAT64, BOOL, TEXT, BLOB, DECIMAL, UUID, and TIMESTAMP.
- Signed numeric encoding, including negative values, must be specified explicitly and tested; unsigned-only varint assumptions are insufficient.
- UUID values must be stored in 16-byte binary form.
- TIMESTAMP and DECIMAL encodings must be precisely defined so storage efficiency does not compromise correctness or ordering semantics.
- TEXT must remain valid UTF-8.
- Large TEXT and BLOB values must use overflow pages once the inline threshold is exceeded.
- Overflow pages are a mandatory 1.0 requirement and must use a documented page format with chain pointers and payload-length metadata.
- Compression, if used for overflow data, must be limited to ADR-approved behavior and must not be assumed for inline values.
- TEXT and BLOB index keys must use a typed comparable encoding sufficient for correct equality and ordering semantics; hash-only keying is not acceptable as the final 1.0 behavior.
- The authoritative B+Tree page layout shall be the accepted compact varint-based layout with sequentially parsed variable-length cells. Execution artifacts must not rely on superseded or conflicting page-layout assumptions such as stale cell-offset-array guidance.
- B+Tree mutations must support insert, update, delete, split, overflow allocation, and root-update behavior.
- Since merge/rebalance is not a mandatory 1.0 requirement, the system must include page-utilization monitoring and a compaction/rebuild path sufficient to manage delete-heavy degradation.

### 6. Trigram Indexing
- The engine must support trigram indexing for substring search over TEXT columns.
- Trigram generation, normalization, posting-list encoding, and intersection behavior must be explicitly defined and tested.
- Pattern-length and broad-pattern guardrails must be implemented to avoid pathological query behavior.
- Trigram indexes are secondary derived structures and must have explicit durability semantics.
- Query correctness must not depend on stale trigram index contents. The planner and executor must avoid false negatives by rebuilding, bypassing, or compensating for stale trigram state.
- If trigram deltas are not fully durable at transaction commit, crash recovery must mark affected trigram indexes as stale.
- Stale trigram indexes must be rebuilt lazily on first trigram-dependent use or via explicit maintenance tooling; until rebuild completes, the planner must fall back to correct non-trigram execution paths.

### 7. Catalog, SQL, Planning, and Execution
- The system catalog must store tables, indexes, views, constraints, and schema-version metadata.
- Schema changes must update the schema cookie and invalidate dependent cached metadata.
- The 1.0 parser of record shall be `libpg_query`, integrated through a vendored or pinned internal Rust wrapper rather than a temporary alternate parser path.
- The 1.0 SQL surface shall align with the documented 1.0 subset in `SPEC.md`, including:
  - core DDL for tables, indexes, views, and supported trigger operations,
  - core DML (`SELECT`, `INSERT`, `UPDATE`, `DELETE`),
  - `EXPLAIN`,
  - aggregates, ordering, limits, offsets, non-recursive CTEs, and the documented UPSERT / `RETURNING` subsets,
  - statement-time CHECK, NOT NULL, UNIQUE, and foreign-key enforcement.
- The planner and executor must support scans, seeks, filters, projections, nested-loop joins, sorting, and reusable execution buffers.
- Statement-level rollback is required. Multi-row statements must not leave partially applied effects visible when a later row fails.
- Savepoint-equivalent machinery or another explicit rollback mechanism is required to satisfy statement-level rollback semantics safely.
- Bulk-load behavior described in the baseline specification remains in scope and must preserve pre-load reader consistency while supporting deferred durability tradeoffs explicitly.

### 8. C-ABI, Bindings, and CLI
- The Rust core must preserve the established C-ABI contract or publish an approved compatibility mapping from the Nim engine surface.
- Every exported `extern "C"` function must use panic containment and return C-compatible error information.
- FFI pointer ownership, allocation, free semantics, and error retrieval behavior must be documented and tested.
- Early C-ABI smoke tests are required; Python and .NET are the default early validation priorities.
- 1.0 release scope requires stable C-ABI parity, production-ready Python and .NET validation, and smoke-tested compatibility coverage for Go, Java, Node.js, and Dart against the stable C-ABI.
- The CLI must provide `exec`, `repl`, `import`, `export`, `checkpoint`, and `save-as`.
- The CLI must support transaction-aware usage, clear error presentation, and both interactive and scripting workflows.
- The CLI and engine release cannot rely on placeholder or stubbed transaction control.

### 9. Testing and Documentation as First-Class Deliverables
- A Python-driven crash and differential harness must exist and be integrated early enough to validate storage correctness before higher layers are considered stable.
- Public Rust APIs must include rustdoc, and user-visible behavior changes must update end-user documentation.
- Examples in public documentation should be executable or otherwise validated during CI wherever practical.

## Non-Functional Requirements
### Security
- The implementation shall prefer safe Rust and shall minimize `unsafe`.
- All `unsafe` blocks must be justified, narrowly scoped, and documented with safety invariants.
- Corruption detection must fail safely and must not silently continue with invalid headers or invalid structural state.
- FFI boundaries must not allow Rust panics to unwind into host runtimes.

### Performance
- Performance acceptance shall be based on explicit workload-specific budgets, not a blanket claim alone.
- The project shall use the accepted performance targets as minimum release gates, including point lookup, join, substring search, bulk-load, and crash-recovery thresholds.
- Performance instrumentation and microbenchmarks must appear early enough to influence storage and execution design choices.
- Release benchmarking must use a mixed matrix:
  - DecentDB must beat the prior Nim engine on all named release workloads.
  - DecentDB must outperform SQLite on the workloads where its architecture is a first-order differentiator, especially concurrent snapshot reads and trigram-backed substring search.
  - For core single-threaded point lookup and similar baseline read paths, DecentDB 1.0 must at minimum remain competitively close to SQLite under explicit workload-specific thresholds.

### Scalability
- 1.0 scalability is within a single process only.
- The design must support many readers across multiple connections to the same on-disk database via shared WAL ownership.
- The system must operate correctly on datasets large enough to require overflow pages, checkpointing, and multi-million-row index workloads.

### Reliability / Availability
- The engine must recover deterministically after crashes and partial writes.
- Failures in the WAL, header, or page structures must surface as structured errors, not undefined behavior.
- The system should degrade gracefully under stale trigram state, memory pressure, and blocked WAL truncation without violating primary-data correctness.

### Maintainability
- Execution artifacts must reference crate-scoped paths in the current workspace layout.
- ADR references must use full filenames or titles, not bare numbers, because the repository contains duplicate ADR numbers.
- Dependency additions must follow ADR governance, including rationale, maintenance, license, unsafe surface, and fallback analysis.
- Every slice or milestone must have explicit scope, out-of-scope, tests, docs impact, and definition of done.

### Accessibility
- CLI output must remain usable without color and should support machine-readable export paths where appropriate.
- Interactive workflows should remain keyboard-driven and readable in standard terminals.
- Documentation should be text-first and example-driven rather than dependent on screenshots alone.

### Observability / Monitoring
- The engine must expose or log enough information to understand:
  - WAL size and checkpoint behavior,
  - active reader age and snapshot retention,
  - cache hits/misses and eviction behavior,
  - recovery duration,
  - crash-test outcomes,
  - structured error categories.

## Architecture / Design Considerations
### Source-of-Truth Rules
- This document is the primary consolidated requirements source for the rewrite.
- Accepted ADRs remain authoritative for individual design decisions, but execution artifacts must cite them precisely by full filename or title.
- Number-only ADR references are not acceptable in implementation plans because duplicate ADR numbers exist in the repository.

### Workspace and Module Boundaries
- Core engine code belongs in `crates/decentdb`.
- CLI code belongs in `crates/decentdb-cli`.
- Any derived execution plan must use crate-scoped paths and must not use ambiguous root-level `src/*.rs` instructions.

### Required Delivery Sequence
1. Establish execution guardrails and bootstrap.
   - Canonical source references, error taxonomy, CI/test skeleton, DB bootstrap, header validation, page allocator and freelist basics.
2. Build the storage foundation.
   - VFS variants, pager/cache, WAL v8, recovery, checkpointing, reader tracking, shared WAL ownership, early crash tests.
3. Build data structures and indexing.
   - Record encoding, index-key encoding, B+Tree read/write path, overflow pages, trigram storage and rebuild semantics.
4. Build the relational core.
   - Catalog, parser, planner/executor, transactions, constraints, rollback, and bulk-load behavior.
5. Complete ecosystem parity and release hardening.
   - C-ABI compatibility, binding smoke tests, CLI UX, documentation, benchmarks, and release verification.

## Data / Integration / External Dependencies
- Core persisted artifacts are the main database file and its WAL.
- File-format evolution must remain explicit and versioned.
- Existing bindings and external consumers depend on the C-ABI and must be treated as integration constraints from the start, not only at the end.
- PostgreSQL remains the default differential reference for the supported SQL subset.
- Competitive benchmarking against SQLite is useful but must not replace explicit functional correctness gates.
- Candidate dependencies for parser, compression, cache management, REPL, or other core subsystems must be ADR-backed before adoption.

## Risks and Constraints
- Source-of-truth drift between the baseline plan, accepted ADRs, the SPEC, and code comments can produce incorrect implementations unless actively managed.
- Duplicate ADR numbering creates an avoidable ambiguity risk for coding agents and reviewers.
- The repository does not currently contain the expected Python harness scaffolding path, so test-harness bootstrap is a real deliverable, not a documentation placeholder.
- Long-running readers can block WAL truncation and drive WAL growth.
- Deferring B+Tree merge/rebalance may degrade delete-heavy workloads if compaction is not implemented and monitored.
- Parser integration via `libpg_query` or equivalent FFI introduces build, packaging, and threading complexity.
- FFI compatibility and memory ownership rules are easy to get wrong late in the project if not validated early.

## Assumptions
- DecentDB 1.0 remains a single-process embedded database.
- Safe Rust is the default implementation strategy.
- Page size is selected at database creation and is immutable for the lifetime of a database file.
- Existing Nim-engine behavior and signatures are available as the compatibility baseline for the C-ABI where direct parity is required.
- Short CI suites run on every PR; extended crash, soak, and benchmark suites run nightly.
- Documentation continues to live in `docs/` and remains part of the product surface.

## Resolved Decisions
1. **WAL append path**
   - DecentDB 1.0 shall use positional I/O as the required WAL read/write path.
   - mmap-accelerated WAL append is not a 1.0 dependency and may be evaluated later as an optimization only if it preserves the same correctness guarantees.

2. **Parser packaging strategy**
   - DecentDB 1.0 shall standardize on `libpg_query` as the parser of record.
   - The parser shall be integrated through a vendored or pinned internal Rust wrapper to preserve reproducible builds and avoid temporary parser divergence.

3. **Overflow compression dependency choice**
   - Overflow compression remains a 1.0 requirement.
   - The compression format shall remain zlib-compatible and shall use a pure-Rust implementation path by default; native system-zlib dependence is not part of the preferred 1.0 baseline.

4. **Trigram post-crash recovery behavior**
   - On recovery, trigram indexes affected by uncheckpointed deltas shall be marked stale.
   - Stale trigram indexes shall rebuild lazily on first trigram-dependent use, and the planner shall fall back to correct non-trigram execution until rebuild completes.

5. **Competitive release benchmark matrix**
   - Release performance shall be judged with workload-specific gates rather than a single blanket claim.
   - The release matrix shall require wins versus the prior Nim engine on all named release workloads, wins versus SQLite on DecentDB’s architectural differentiators, and competitive thresholds versus SQLite on core baseline read paths.

6. **Binding rollout expectations for 1.0**
   - 1.0 requires stable C-ABI parity, production-ready validation for Python and .NET, and smoke-tested compatibility coverage for Go, Java, Node.js, and Dart.
   - A published compatibility matrix is required for release.

## Implementation Notes for Engineering
- Before any substantive implementation, establish:
  - canonical storage-format references,
  - error taxonomy,
  - missing test-harness skeleton,
  - CI partitioning rules,
  - active-slice status in the execution plan.
- Every implementation slice must state:
  - owned files/modules,
  - explicit dependencies and ADR references,
  - borrow-checker and ownership strategy,
  - required unit/property/crash/differential/performance tests,
  - documentation impact.
- Shared files such as crate roots, common error modules, FFI types, and configuration surfaces must be treated as coordination points in multi-agent work.
- Documentation updates are required whenever a slice changes a public API, storage format, SQL behavior, CLI behavior, or operational semantics.

## Acceptance Criteria / Definition of Done
### Global Release Gates
- `cargo check` passes for all touched crates.
- `cargo clippy` passes without warnings for all touched crates.
- Required unit, property, crash-injection, and differential tests pass for the implemented scope.
- PR-time checks remain within the fast-feedback budget; long-running suites are separated into nightly or extended lanes.
- Public APIs, user-visible behavior, and operational procedures are documented.
- Storage format behavior, WAL semantics, and B+Tree layout match the accepted current design decisions.
- FFI smoke tests, CLI smoke tests, and benchmark gates pass for the declared release scope.

### Per-Slice Minimum Gate
- Scope and out-of-scope are explicit.
- Tests for normal behavior and failure behavior are present.
- Performance smoke coverage exists where the slice affects a hot path.
- Documentation and examples are updated where applicable.
- Residual risks, if any, are recorded explicitly and carried forward.

## Appendix: Consolidation Notes
- Resolved in favor of the current accepted storage decisions:
  - WAL requirements now point to the current v8 behavior rather than older checksum- and payload-length-based assumptions.
  - B+Tree requirements now point to the compact varint layout and explicitly reject stale layout guidance.
- Promoted to explicit requirements because they were repeatedly identified as missing or too implicit:
  - DB bootstrap and header validation,
  - page allocation/freelist basics,
  - reader tracking,
  - shared WAL ownership,
  - in-memory VFS,
  - early crash-harness integration,
  - error taxonomy and documentation discipline.
- Intentionally excluded or deferred:
  - forced WAL truncation that would violate active reader snapshots,
  - broad `unsafe` or mmap-first storage design,
  - mandatory 1.0 B+Tree merge/rebalance,
  - unapproved dependency/tool choices presented as fixed requirements.
