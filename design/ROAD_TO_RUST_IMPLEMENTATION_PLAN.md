# Road to Rust: DecentDB 1.0 Implementation Plan
**Date:** 2026-03-22
**Status:** Active Execution
**Current Active Slice:** `Slice 2.1: Value and Record Encoding`

This document is the execution plan for the Rust rewrite. It is derived from:
- `design/PRD.md`
- `design/SPEC.md`
- `design/TESTING_STRATEGY.md`
- `design/ROAD_TO_RUST_PLAN.md`

Coding agents MUST implement only the current active slice unless the user explicitly moves the plan forward. This plan is prescriptive. Agents are expected to implement the specified behavior, paths, and sequence rather than re-deciding architecture.

## Operating Rules for Agents
- Use crate-scoped paths only. Core engine code lives in `crates/decentdb`. CLI code lives in `crates/decentdb-cli`.
- Cite ADRs by full filename in code comments, PR notes, and follow-up docs. Do not cite duplicate ADR numbers by number alone.
- Do not introduce alternate designs inside a slice. If a conflict exists between code and design docs, stop and update the design source first.
- Update rustdoc and user-facing docs for any public API, storage format, SQL behavior, CLI behavior, or operational behavior changed by the slice.
- Every slice is complete only when:
  - `cargo check` passes,
  - `cargo clippy` passes without warnings for touched crates,
  - the slice tests pass,
  - the slice docs are updated,
  - the next slice does not need to reinterpret unresolved behavior.

---

## Slice Map / Status

- [x] **Phase 0: Execution Guardrails & Bootstrap**
  - [x] **Slice 0.1: Workspace Guardrails & Module Skeleton**
  - [x] **Slice 0.2: Error Taxonomy & Config Surface**
  - [x] **Slice 0.3: Database Bootstrap & Header Validation**
- [x] **Phase 1: Storage Foundation**
  - [x] **Slice 1.1: VFS Core, Fault Injection, and MemVfs**
  - [x] **Slice 1.2: Pager, Page Cache, and Freelist**
  - [x] **Slice 1.3: WAL Core and Shared WAL Registry**
  - [x] **Slice 1.4: Recovery, Reader Registry, and Checkpointing**
  - [x] **Slice 1.5: Python Crash Harness and Storage Failure CI**
- [ ] **Phase 2: Data Structures and Indexes**
  - [ ] **Slice 2.1: Value and Record Encoding** **(ACTIVE)**
  - [ ] **Slice 2.2: Comparable Index Key Encoding**
  - [ ] **Slice 2.3: B+Tree Read Path**
  - [ ] **Slice 2.4: B+Tree Write Path and Overflow Pages**
  - [ ] **Slice 2.5: Trigram Storage, Query Support, and Rebuild Fallback**
- [ ] **Phase 3: Relational Core**
  - [ ] **Slice 3.1: Catalog and Schema Bootstrap**
  - [ ] **Slice 3.2: `libpg_query` Integration and AST Normalization**
  - [ ] **Slice 3.3: Read Planner and Executor**
  - [ ] **Slice 3.4: Transaction State, DML, Constraints, and Statement Rollback**
  - [ ] **Slice 3.5: Remaining 1.0 SQL Surface**
  - [ ] **Slice 3.6: Bulk Load and Maintenance Operations**
- [ ] **Phase 4: Ecosystem Parity and Release**
  - [ ] **Slice 4.1: C-ABI Core and Error Surface**
  - [ ] **Slice 4.2: Python and .NET Validation**
  - [ ] **Slice 4.3: Secondary Binding Smoke Coverage**
  - [ ] **Slice 4.4: CLI UX and Operator Workflows**
  - [ ] **Slice 4.5: Documentation, Benchmarks, CI, and 1.0 Release Verification**

---

## Phase 0: Execution Guardrails & Bootstrap

### Slice 0.1: Workspace Guardrails & Module Skeleton
**Goal:** Create the canonical Rust module layout so all later slices write code into stable locations.

**Implement in:**
- `crates/decentdb/src/lib.rs`
- `crates/decentdb/src/db.rs`
- `crates/decentdb/src/error.rs`
- `crates/decentdb/src/config.rs`
- `crates/decentdb/src/vfs/mod.rs`
- `crates/decentdb/src/storage/mod.rs`
- `crates/decentdb/src/wal/mod.rs`
- `crates/decentdb/src/record/mod.rs`
- `crates/decentdb/src/btree/mod.rs`
- `crates/decentdb/src/search/mod.rs`
- `crates/decentdb/src/catalog/mod.rs`
- `crates/decentdb/src/sql/mod.rs`
- `crates/decentdb/src/planner/mod.rs`
- `crates/decentdb/src/exec/mod.rs`

**Directives for Agents:**
- Create the module tree above and wire it in `crates/decentdb/src/lib.rs`.
- Export only the stable top-level types needed by later slices: `Db`, `DbConfig`, `DbError`, `Result`.
- Add module-level rustdoc for each top-level module explaining its purpose.
- Keep all implementation internals `pub(crate)` unless a later slice explicitly requires a public surface.
- Add crate-level linting that strengthens safety without blocking legitimate FFI/VFS work:
  - `#![deny(unsafe_op_in_unsafe_fn)]`
  - `#![deny(unused_must_use)]`
- Do not add feature logic in this slice beyond module wiring and type placeholders.

**Out of Scope for Slice:**
- Storage format logic
- WAL logic
- SQL logic
- FFI exports

**Testing and Exit Criteria:**
- `cargo check` passes with the new module tree.
- `cargo clippy` passes without warnings.
- `crates/decentdb/tests/engine_api_tests.rs` compiles against the new top-level exports.

### Slice 0.2: Error Taxonomy & Config Surface
**Goal:** Establish the error model and configuration surface that all later slices must use.

**Implement in:**
- `crates/decentdb/src/error.rs`
- `crates/decentdb/src/config.rs`
- `crates/decentdb/src/db.rs`

**Directives for Agents:**
- Define `DbError` in `error.rs` using `thiserror`, with categories matching `design/SPEC.md`:
  - `Io`
  - `Corruption`
  - `Constraint`
  - `Transaction`
  - `Sql`
  - `Internal`
  - `Panic`
- Define a stable numeric error-code enum in Rust now; later FFI slices will map directly to it.
- Define `type Result<T> = std::result::Result<T, DbError>`.
- Define `DbConfig` as the single configuration entry point for the engine. Include:
  - `page_size`
  - `cache_size_mb`
  - `wal_sync_mode`
  - `checkpoint_timeout_sec`
  - `trigram_postings_threshold`
  - `temp_dir`
- Define `WalSyncMode` with:
  - `Full`
  - `Normal`
  - `TestingOnlyUnsafeNoSync`
- Implement `Default` for `DbConfig` using the consolidated requirements defaults.
- Define `Db` as the stable owner for config, VFS, pager, WAL, and catalog handles. Do not populate those fields yet; add typed placeholders.

**Out of Scope for Slice:**
- Header parsing
- C-ABI wrappers
- CLI parsing

**Testing and Exit Criteria:**
- Unit tests cover `DbConfig::default()`.
- Unit tests cover error category construction and numeric-code mapping.
- Public rustdoc examples for `DbConfig` compile.

### Slice 0.3: Database Bootstrap & Header Validation
**Goal:** Implement database create/open bootstrap, the fixed database header, and root-page initialization.

**Implement in:**
- `crates/decentdb/src/storage/header.rs`
- `crates/decentdb/src/storage/checksum.rs`
- `crates/decentdb/src/storage/page.rs`
- `crates/decentdb/src/storage/freelist.rs`
- `crates/decentdb/src/db.rs`

**Directives for Agents:**
- Implement the 128-byte main DB header exactly as described in `design/SPEC.md`.
- Use:
  - `DB_FORMAT_VERSION = 8`
  - `WAL_HEADER_VERSION = 1`
  - default page size `4096`
- Implement CRC-32C locally in `storage/checksum.rs`. Do not add a checksum dependency in this slice.
- Implement header encode, decode, validate, and checksum verification.
- On database creation:
  - initialize page 1 with the validated header,
  - initialize page 2 as the empty catalog root page,
  - set freelist head/count to zero,
  - write the initial header checksum before returning success.
- Validate page size against the allowed set: `4096`, `8192`, `16384`.
- Fail invalid magic, format version, page size, or checksum with `DbError::Corruption`.
- Add `Db::open(path, config)` and `Db::create(path, config)` as the canonical Rust entry points.

**Out of Scope for Slice:**
- WAL
- Page cache
- B+Tree logic beyond reserving the catalog root page

**Testing and Exit Criteria:**
- Header roundtrip tests.
- Corrupt magic/version/page-size/checksum tests.
- Create/open/create-again behavior is deterministic and idempotent where intended.

---

## Phase 1: Storage Foundation

### Slice 1.1: VFS Core, Fault Injection, and MemVfs
**Goal:** Implement the VFS abstraction used by all file I/O paths, including testing and `:memory:` support.

**Read before coding:**
- `design/adr/0119-rust-vfs-pread-pwrite.md`
- `design/adr/0105-in-memory-vfs.md`

**Implement in:**
- `crates/decentdb/src/vfs/mod.rs`
- `crates/decentdb/src/vfs/os.rs`
- `crates/decentdb/src/vfs/faulty.rs`
- `crates/decentdb/src/vfs/mem.rs`

**Directives for Agents:**
- Define `Vfs` and `VfsFile` traits for:
  - open/create
  - `read_at`
  - `write_at`
  - `sync_data`
  - `sync_metadata`
  - `file_size`
  - `file_exists`
  - `remove_file`
- Implement `OsVfs` using positional I/O only:
  - Unix: `std::os::unix::fs::FileExt`
  - Windows: `std::os::windows::fs::FileExt`
- Do not use `mmap` in any primary page-cache or WAL path.
- Implement `FaultyVfs` as a wrapper over `OsVfs` with deterministic failpoints by label and counter.
- Support partial writes, dropped syncs, and injected I/O errors.
- Implement `MemVfs` for `:memory:` using memory-backed main-db and WAL files with the same logical behavior as on-disk databases.
- Expose failpoint logs for the test harness.

**Out of Scope for Slice:**
- Pager/cache
- WAL frame logic

**Testing and Exit Criteria:**
- Concurrent `read_at` tests on supported platforms.
- Partial-write and dropped-sync tests.
- `MemVfs` create/open/remove tests.
- `FaultyVfs` failpoint reproducibility tests.

### Slice 1.2: Pager, Page Cache, and Freelist
**Goal:** Implement bounded page management, custom LRU eviction, and page allocation/free behavior.

**Read before coding:**
- `design/adr/0001-page-size.md`

**Implement in:**
- `crates/decentdb/src/storage/pager.rs`
- `crates/decentdb/src/storage/cache.rs`
- `crates/decentdb/src/storage/page.rs`
- `crates/decentdb/src/storage/freelist.rs`

**Directives for Agents:**
- Implement a custom page cache. Do not add an external LRU crate.
- Represent cached pages as page-size-aware owned buffers guarded by per-page locks.
- Cursors and higher layers must store page IDs and reacquire pages through the pager. Do not hold long-lived references into page internals across pager calls.
- Implement pin/unpin semantics. A pinned page must never be evicted.
- Evict the least-recently-used unpinned page.
- If the cache is full and all pages are pinned, return a transaction-class error rather than guessing at eviction.
- Dirty-page ownership must be explicit:
  - the pager owns dirty-page tracking,
  - the WAL layer owns durable write ordering,
  - callers do not flush pages directly to the main DB file.
- Implement page allocation from the freelist first; grow the file only when the freelist is empty.

**Out of Scope for Slice:**
- WAL durability
- Checkpoint copyback

**Testing and Exit Criteria:**
- Cache-hit tests prove repeated reads do not hit the VFS.
- Eviction tests prove pinned pages are protected.
- File-growth and freelist-reuse tests pass.
- “all pages pinned” returns a deterministic error.

### Slice 1.3: WAL Core and Shared WAL Registry
**Goal:** Implement the current WAL format, commit path, snapshot publication, and shared WAL ownership.

**Read before coding:**
- `design/adr/0003-snapshot-lsn-atomicity.md`
- `design/adr/0064-wal-frame-checksum-removal.md`
- `design/adr/0065-wal-frame-lsn-removal.md`
- `design/adr/0066-wal-frame-payload-length-removal.md`
- `design/adr/0068-wal-header-end-offset.md`
- `design/adr/0117-shared-wal-registry.md`

**Implement in:**
- `crates/decentdb/src/wal/format.rs`
- `crates/decentdb/src/wal/index.rs`
- `crates/decentdb/src/wal/shared.rs`
- `crates/decentdb/src/wal/writer.rs`
- `crates/decentdb/src/wal/mod.rs`

**Directives for Agents:**
- Implement the v8 WAL layout exactly:
  - 32-byte fixed header at offset 0,
  - frames starting at offset 32,
  - frame types: page, commit, checkpoint,
  - page-frame payload = page size,
  - commit payload = 0 bytes,
  - checkpoint payload = 8 bytes,
  - LSN derived from frame end offset,
  - `wal_end_offset` is the logical end.
- Use positional I/O only for WAL reads and writes.
- Publish `wal_end_lsn` using `AtomicU64` with release semantics only after the append and required sync behavior are complete.
- Implement `WalIndex` as `HashMap<PageId, Vec<WalVersion>>`, ordered by increasing LSN per page, so long readers can resolve older versions without rescanning the file.
- Implement process-global shared WAL acquisition for on-disk databases keyed by canonical path.
- Exclude `:memory:` databases from shared WAL registry behavior.
- Implement commit durability according to `WalSyncMode`.

**Out of Scope for Slice:**
- Recovery scan
- Reader registry
- Checkpoint copyback

**Testing and Exit Criteria:**
- WAL frame encode/decode tests.
- Commit visibility tests.
- Shared WAL cross-connection visibility tests.
- Snapshot-read tests using old page versions from the WAL index.

### Slice 1.4: Recovery, Reader Registry, and Checkpointing
**Goal:** Implement crash recovery, reader-aware checkpointing, and WAL index pruning.

**Read before coding:**
- `design/adr/0004-wal-checkpoint-strategy.md`
- `design/adr/0018-checkpointing-reader-count-mechanism.md`
- `design/adr/0019-wal-retention-for-active-readers.md`
- `design/adr/0024-wal-growth-prevention-long-readers.md`
- `design/adr/0056-wal-index-pruning-on-checkpoint.md`

**Implement in:**
- `crates/decentdb/src/wal/recovery.rs`
- `crates/decentdb/src/wal/reader_registry.rs`
- `crates/decentdb/src/wal/checkpoint.rs`

**Directives for Agents:**
- Implement recovery by:
  - validating the WAL header,
  - scanning only to `wal_end_offset`,
  - rebuilding `WalIndex`,
  - accepting committed frames only,
  - ignoring incomplete trailing frames.
- Implement a reader registry that tracks:
  - reader ID,
  - snapshot LSN,
  - start timestamp.
- Implement checkpoint copyback of committed pages to the main DB file.
- Never truncate WAL frames required by an active reader.
- When physical truncation is blocked, prune obsolete in-memory `WalIndex` entries at or below the safe checkpoint LSN.
- Implement warnings and metrics for long readers; do not force truncation that violates snapshot semantics.
- Expose `Db::checkpoint()` as the canonical maintenance entry point.

**Out of Scope for Slice:**
- Python harness integration
- Derived trigram rebuild logic

**Testing and Exit Criteria:**
- Crash-reopen tests for committed vs uncommitted visibility.
- Active-reader tests that block truncation.
- WAL-index pruning tests when copyback succeeds but truncation is deferred.
- Corrupt-WAL-header tests.

### Slice 1.5: Python Crash Harness and Storage Failure CI
**Goal:** Wire the storage foundation into the Python crash harness before higher-level relational work begins.

**Implement in:**
- `tests/harness/runner.py`
- `tests/harness/scenarios/`
- `tests/harness/datasets/`
- `crates/decentdb/src/bin/decentdb-test-harness.rs`

**Directives for Agents:**
- Create the root `tests/harness/` tree now. Do not defer it to a later phase.
- Implement `decentdb-test-harness` as a storage-focused binary that accepts scenario JSON and emits deterministic JSON results.
- Support scenario operations for:
  - create/open DB,
  - install/clear failpoints,
  - begin/write/commit/checkpoint,
  - reopen and inspect header/WAL state.
- Make failpoints addressable by the same labels exposed by `FaultyVfs`.
- Add a short deterministic crash suite suitable for PR CI.
- Add a longer soak suite entry point for nightly runs.

**Out of Scope for Slice:**
- SQL differential testing
- Binding validation

**Testing and Exit Criteria:**
- Python harness can reproduce a seeded crash scenario.
- CI can run a short crash subset without manual setup.
- Storage slices 1.1 through 1.4 are exercised by the harness.

---

## Phase 2: Data Structures and Indexes

### Slice 2.1: Value and Record Encoding
**Goal:** Implement the canonical row-value model and row serialization.

**Implement in:**
- `crates/decentdb/src/record/value.rs`
- `crates/decentdb/src/record/row.rs`
- `crates/decentdb/src/record/mod.rs`

**Directives for Agents:**
- Define `Value` with the 1.0 types:
  - `Null`
  - `Int64(i64)`
  - `Float64(f64)`
  - `Bool(bool)`
  - `Text(String)`
  - `Blob(Vec<u8>)`
  - `Decimal { scaled: i64, scale: u8 }`
  - `Uuid([u8; 16])`
  - `TimestampMicros(i64)`
- Use ZigZag + LEB128 for signed varint row encoding where the 1.0 design requires compact variable-length storage.
- Store UUID as 16 raw bytes.
- Validate TEXT as UTF-8 at the record layer.
- Keep row encoding and index-key encoding separate. Do not attempt to make row encoding lexicographically comparable.

**Out of Scope for Slice:**
- Index keys
- Overflow pages

**Testing and Exit Criteria:**
- `proptest` roundtrip tests across all value variants.
- Boundary tests for signed integers, timestamps, decimals, empty strings, Unicode text, and blobs.
- Row encoding docs updated in rustdoc and `docs/` where applicable.

### Slice 2.2: Comparable Index Key Encoding
**Goal:** Implement the dedicated typed/comparable encoding used by indexes.

**Read before coding:**
- `design/adr/0061-typed-index-key-encoding-text-blob.md`

**Implement in:**
- `crates/decentdb/src/record/key.rs`

**Directives for Agents:**
- Implement a dedicated comparable key encoder for all indexed types.
- Support correct equality and ordering semantics for:
  - integers
  - decimals
  - timestamps
  - booleans
  - floats
  - UUID
  - TEXT
  - BLOB
- Do not use CRC32C or any hash-only surrogate as the final TEXT/BLOB index key representation.
- Expose comparison tests that prove index ordering matches row comparison semantics.

**Out of Scope for Slice:**
- B+Tree page parsing

**Testing and Exit Criteria:**
- Equality and range-order tests for every supported indexed type.
- Tests proving TEXT and BLOB ordering is not hash-based.

### Slice 2.3: B+Tree Read Path
**Goal:** Implement the accepted compact B+Tree page layout, search, and cursor traversal.

**Read before coding:**
- `design/adr/0035-btree-page-layout-v2.md`

**Implement in:**
- `crates/decentdb/src/btree/page.rs`
- `crates/decentdb/src/btree/read.rs`
- `crates/decentdb/src/btree/cursor.rs`

**Directives for Agents:**
- Implement the compact variable-length B+Tree page layout from `design/adr/0035-btree-page-layout-v2.md`.
- Parse cells sequentially from page bytes. Do not implement or rely on a slotted cell-offset array.
- Use explicit byte parsing. Do not use `#[repr(C, packed)]` for page structs.
- Implement:
  - internal node parsing,
  - leaf node parsing,
  - key search,
  - forward cursor,
  - backward cursor,
  - leaf-to-leaf traversal.

**Out of Scope for Slice:**
- Writes
- Overflow allocation

**Testing and Exit Criteria:**
- Search correctness tests.
- Cursor-order tests across page boundaries.
- Page-format roundtrip tests for internal and leaf nodes.

### Slice 2.4: B+Tree Write Path and Overflow Pages
**Goal:** Implement B+Tree mutation logic, overflow storage, and pure-Rust zlib-compatible compression for overflow values.

**Read before coding:**
- `design/adr/0020-overflow-pages-for-blobs.md`
- `design/adr/0031-overflow-page-format.md`

**Implement in:**
- `Cargo.toml` (workspace dependency update for compression)
- `crates/decentdb/src/btree/write.rs`
- `crates/decentdb/src/record/overflow.rs`
- `crates/decentdb/src/record/compression.rs`

**Directives for Agents:**
- Implement:
  - insert,
  - update,
  - delete,
  - root creation/update,
  - node split,
  - overflow-page allocation/free.
- Do not implement merge/rebalance in this slice.
- Use overflow pages for large TEXT/BLOB values once the inline threshold is exceeded.
- Store overflow pages as linked pages with next-page pointer and payload length.
- Add `miniz_oxide` as the pure-Rust zlib-compatible compression dependency for overflow pages. Do not link native system zlib in the baseline implementation.
- Keep all compression calls behind `record/compression.rs`; higher layers must depend on the wrapper module, not directly on the codec crate.
- On overwrite or delete, free old overflow chains through the freelist.

**Out of Scope for Slice:**
- Trigram postings
- B+Tree compaction command

**Testing and Exit Criteria:**
- Split tests with ascending, descending, and randomized inserts.
- Large TEXT/BLOB overflow roundtrip tests.
- Delete/update tests that prove overflow chains are released.
- Compression roundtrip tests.

### Slice 2.5: Trigram Storage, Query Support, and Rebuild Fallback
**Goal:** Implement trigram index storage, postings handling, and stale-index recovery behavior.

**Read before coding:**
- `design/adr/0007-trigram-postings-storage-strategy.md`
- `design/adr/0008-trigram-pattern-length-guardrails.md`
- `design/adr/0052-trigram-durability.md`

**Implement in:**
- `crates/decentdb/src/search/trigram.rs`
- `crates/decentdb/src/search/postings.rs`
- `crates/decentdb/src/search/rebuild.rs`
- `crates/decentdb/src/search/mod.rs`

**Directives for Agents:**
- Normalize trigram input to uppercase before tokenization.
- Store postings in a dedicated B+Tree keyed by trigram token with delta-encoded varint postings blobs.
- Maintain in-memory trigram delta buffers that flush at checkpoint, not at every commit.
- Mark trigram indexes stale on recovery if uncheckpointed deltas may have been lost.
- Implement lazy rebuild on first trigram-dependent use.
- Expose a planner-facing freshness check so later slices can bypass stale trigram indexes.
- Enforce short-pattern and broad-pattern guardrails.

**Out of Scope for Slice:**
- Planner integration
- SQL syntax

**Testing and Exit Criteria:**
- Trigram-generation and postings roundtrip tests.
- Guardrail tests for short and broad patterns.
- Recovery tests that mark stale trigram state and require rebuild or fallback.

---

## Phase 3: Relational Core

### Slice 3.1: Catalog and Schema Bootstrap
**Goal:** Implement the system catalog and schema metadata roundtrip.

**Implement in:**
- `crates/decentdb/src/catalog/schema.rs`
- `crates/decentdb/src/catalog/objects.rs`
- `crates/decentdb/src/catalog/mod.rs`

**Directives for Agents:**
- Implement catalog records for:
  - tables
  - indexes
  - views
  - constraints
- Store and update the schema cookie.
- Bootstrap the catalog into the root page initialized in Slice 0.3.
- Keep schema metadata normalized and serializable.
- Expose catalog lookup APIs used by parser, planner, and DDL execution.

**Out of Scope for Slice:**
- SQL parsing
- DDL statement execution

**Testing and Exit Criteria:**
- Catalog bootstrap tests.
- Schema-cookie increment tests.
- Metadata roundtrip tests for tables, indexes, and views.

### Slice 3.2: `libpg_query` Integration and AST Normalization
**Goal:** Implement the parser of record and normalize supported SQL into DecentDB’s internal AST.

**Implement in:**
- `Cargo.toml` (workspace update to add parser support crate)
- `crates/libpg_query_sys/Cargo.toml`
- `crates/libpg_query_sys/build.rs`
- `crates/libpg_query_sys/src/lib.rs`
- `vendor/libpg_query/`
- `crates/decentdb/src/sql/parser.rs`
- `crates/decentdb/src/sql/ast.rs`
- `crates/decentdb/src/sql/normalize.rs`

**Directives for Agents:**
- Add `crates/libpg_query_sys` as an internal workspace crate that vendors or pins `libpg_query` and exposes a minimal safe wrapper.
- Put the vendored parser sources under `vendor/libpg_query/` and compile them through `crates/libpg_query_sys/build.rs`.
- Do not introduce an alternate parser path in this plan.
- Normalize parser output into a strongly typed AST that covers the supported 1.0 subset only.
- Unsupported syntax must fail with deterministic `DbError::Sql`, not with parser panics or placeholder fallbacks.
- Keep all parser-specific allocation and FFI details inside `crates/libpg_query_sys` and `sql/parser.rs`.

**Out of Scope for Slice:**
- Planning
- Execution

**Testing and Exit Criteria:**
- Parse matrix tests for supported syntax.
- Explicit rejection tests for unsupported syntax.
- Thread-safety tests for repeated parser invocation.

### Slice 3.3: Read Planner and Executor
**Goal:** Implement planning and execution for the read path of the documented 1.0 subset.

**Implement in:**
- `crates/decentdb/src/planner/logical.rs`
- `crates/decentdb/src/planner/physical.rs`
- `crates/decentdb/src/planner/mod.rs`
- `crates/decentdb/src/exec/operators.rs`
- `crates/decentdb/src/exec/row.rs`
- `crates/decentdb/src/exec/mod.rs`

**Directives for Agents:**
- Implement physical operators for:
  - table scan
  - index seek
  - filter
  - project
  - nested-loop join
  - sort
  - limit/offset
  - aggregates used by the 1.0 subset
- Reuse row buffers. Do not allocate per row in hot loops.
- Use trigram indexes only when:
  - the index is fresh,
  - the query passes guardrails,
  - the planner benefits from trigram selectivity.
- Fall back to correct scan or B+Tree-based plans when trigram state is stale or unhelpful.
- Implement `EXPLAIN` plan rendering hooks as part of the planner output.

**Out of Scope for Slice:**
- DML
- Savepoints

**Testing and Exit Criteria:**
- Result-correctness tests across scans, seeks, joins, sorts, aggregates, and limit/offset.
- Plan-selection tests for indexed lookups and trigram-backed searches.
- Buffer-reuse smoke benchmarks.

### Slice 3.4: Transaction State, DML, Constraints, and Statement Rollback
**Goal:** Implement the writer lifecycle, DML, constraints, and statement rollback behavior.

**Read before coding:**
- `design/adr/0023-isolation-level-specification.md`

**Implement in:**
- `crates/decentdb/src/exec/txn.rs`
- `crates/decentdb/src/exec/dml.rs`
- `crates/decentdb/src/exec/constraints.rs`
- `crates/decentdb/src/wal/savepoint.rs`

**Directives for Agents:**
- Implement:
  - read transaction begin/end,
  - writer acquisition/release,
  - own-writes visibility,
  - insert,
  - update,
  - delete,
  - `INSERT ... RETURNING`,
  - `INSERT ... ON CONFLICT` variants in the documented subset.
- Enforce `NOT NULL`, `UNIQUE`, foreign-key, and CHECK constraints at statement time.
- Implement statement rollback using savepoint-equivalent WAL state. If row N fails, rows `< N` from the same statement must be reverted.
- Keep Snapshot Isolation semantics intact for concurrent readers.

**Out of Scope for Slice:**
- DDL changes
- Triggers and views

**Testing and Exit Criteria:**
- Statement rollback tests.
- Constraint-violation tests.
- Snapshot-isolation tests with one writer and multiple readers.
- `INSERT ... ON CONFLICT` tests.

### Slice 3.5: Remaining 1.0 SQL Surface
**Goal:** Implement the remaining SQL features required by the documented 1.0 baseline.

**Implement in:**
- `crates/decentdb/src/catalog/ddl.rs`
- `crates/decentdb/src/exec/ddl.rs`
- `crates/decentdb/src/exec/views.rs`
- `crates/decentdb/src/exec/triggers.rs`
- `crates/decentdb/src/sql/normalize.rs`

**Directives for Agents:**
- Implement the remaining supported SQL surface:
  - `CREATE TABLE`
  - `CREATE INDEX`
  - `DROP TABLE`
  - `DROP INDEX`
  - `CREATE VIEW`
  - `DROP VIEW`
  - `ALTER VIEW ... RENAME TO ...`
  - supported `ALTER TABLE` operations from `design/SPEC.md`
  - trigger subsets documented for 1.0
  - non-recursive CTEs
  - `EXPLAIN ANALYZE`
- Reject unsupported SQL explicitly and consistently.
- Update the schema cookie on all DDL changes.

**Out of Scope for Slice:**
- Post-1.0 SQL features
- Recursive CTEs

**Testing and Exit Criteria:**
- DDL roundtrip tests.
- View and trigger behavior tests for the supported subset.
- Explicit unsupported-feature rejection tests.

### Slice 3.6: Bulk Load and Maintenance Operations
**Goal:** Implement bulk-load semantics, explicit maintenance paths, and delete-heavy recovery tools.

**Implement in:**
- `crates/decentdb/src/exec/bulk_load.rs`
- `crates/decentdb/src/catalog/maintenance.rs`
- `crates/decentdb/src/db.rs`

**Directives for Agents:**
- Implement bulk-load options matching the documented baseline:
  - `batch_size`
  - `sync_interval`
  - `disable_indexes`
  - `checkpoint_on_complete`
  - durability mode
- Hold the single writer lock for the bulk-load session.
- Preserve pre-load reader visibility until load completion.
- Implement explicit maintenance entry points for:
  - checkpoint
  - index rebuild
  - all-index rebuild
  - B+Tree compaction/rebuild for delete-heavy tables/indexes

**Out of Scope for Slice:**
- Background checkpoint worker

**Testing and Exit Criteria:**
- Bulk-load correctness tests across durability modes.
- Reader-visibility tests during bulk load.
- Index-rebuild and compaction correctness tests.

---

## Phase 4: Ecosystem Parity and Release

### Slice 4.1: C-ABI Core and Error Surface
**Goal:** Expose the Rust engine safely through the stable C-ABI.

**Read before coding:**
- `design/adr/0118-rust-ffi-panic-safety.md`

**Implement in:**
- `crates/decentdb/src/c_api.rs`
- `include/decentdb.h`

**Directives for Agents:**
- Export the stable handle-based C-ABI surface from `c_api.rs`.
- Wrap every exported function in `std::panic::catch_unwind`.
- Return stable numeric error codes and store detailed panic/error strings in a thread-local error buffer.
- Make ownership explicit:
  - opaque handles are created/freed through exported functions,
  - borrowed buffers must never outlive the call boundary,
  - owned strings/blobs must have matching free functions.
- Maintain `include/decentdb.h` in-tree and add ABI-shape tests against the Rust exports.

**Out of Scope for Slice:**
- Full language binding UX

**Testing and Exit Criteria:**
- C smoke tests.
- Panic-containment tests.
- Ownership/free/double-free protection tests.

### Slice 4.2: Python and .NET Validation
**Goal:** Provide production-ready validation for the two priority bindings.

**Implement in:**
- `tests/bindings/python/`
- `tests/bindings/dotnet/`

**Directives for Agents:**
- Implement Python validation over the stable C-ABI with:
  - open/close
  - execute
  - parameter binding
  - result retrieval
  - error retrieval
  - transaction control
- Implement equivalent validation for .NET.
- Keep Python and .NET validation suites strong enough to catch ABI drift before release.

**Out of Scope for Slice:**
- Package publishing

**Testing and Exit Criteria:**
- Python smoke and integration tests pass.
- .NET smoke and integration tests pass.
- Error-code and ownership behavior matches the C-ABI contract.

### Slice 4.3: Secondary Binding Smoke Coverage
**Goal:** Add release-blocking compatibility smoke tests for the remaining target languages.

**Implement in:**
- `tests/bindings/go/`
- `tests/bindings/java/`
- `tests/bindings/node/`
- `tests/bindings/dart/`

**Directives for Agents:**
- Implement one minimal end-to-end smoke program per language that proves:
  - library load,
  - database open,
  - one write,
  - one read,
  - one error path.
- Keep these tests narrow and deterministic.
- Publish a compatibility matrix from the results in the docs slice.

**Out of Scope for Slice:**
- Full higher-level language APIs

**Testing and Exit Criteria:**
- Smoke tests pass in CI on the supported release matrix.

### Slice 4.4: CLI UX and Operator Workflows
**Goal:** Implement the CLI surface documented in the repository and required by the PRD.

**Implement in:**
- `Cargo.toml` (workspace dependency update for REPL support)
- `crates/decentdb-cli/src/main.rs`
- `crates/decentdb-cli/src/repl.rs`
- `crates/decentdb-cli/src/output.rs`
- `crates/decentdb-cli/src/commands/`

**Directives for Agents:**
- Add `rustyline` as the REPL dependency for `crates/decentdb-cli`.
- Implement the CLI commands documented in `docs/api/cli-reference.md`, including:
  - `exec`
  - `repl`
  - `import`
  - `export`
  - `bulk-load`
  - `checkpoint`
  - `save-as`
  - `info`
  - `describe`
  - `list-tables`
  - `list-indexes`
  - `list-views`
  - `dump`
  - `dump-header`
  - `rebuild-index`
  - `rebuild-indexes`
  - `completion`
- Implement output formats:
  - `json`
  - `csv`
  - `table`
- Implement REPL features:
  - multi-line SQL input
  - history persistence
  - transaction-aware behavior
  - helpful error messages
- Implement table-format rendering directly in `output.rs`. Do not add a table-formatting dependency in this slice.
- Keep CLI flags aligned with the checked-in CLI docs.

**Out of Scope for Slice:**
- New undocumented CLI commands

**Testing and Exit Criteria:**
- CLI integration tests cover each documented command at least once.
- Snapshot tests cover JSON/table output for representative commands.
- REPL smoke tests pass.

### Slice 4.5: Documentation, Benchmarks, CI, and 1.0 Release Verification
**Goal:** Complete the documentation surface, benchmark gates, and release checks required for 1.0.

**Implement in:**
- `docs/`
- `crates/decentdb/benches/`
- `.github/workflows/`

**Directives for Agents:**
- Update docs to match the implemented Rust engine behavior.
- Run rustdoc and doctests for public APIs.
- Add benchmark coverage for the named release workloads:
  - point lookup
  - FK join expansion
  - trigram-backed substring search
  - bulk load
  - crash recovery
- Split CI into:
  - PR-fast lane
  - nightly extended lane
- Enforce the release benchmark matrix from `design/ROAD_TO_RUST_PLAN.md`.
- Publish the compatibility matrix for the bindings covered in Phase 4.

**Out of Scope for Slice:**
- Post-1.0 product work

**Testing and Exit Criteria:**
- Docs build cleanly.
- Doctests pass.
- PR-fast and nightly workflows are green.
- Benchmarks produce named release metrics.
- Release verification confirms:
  - wins versus the Nim engine on named release workloads,
  - wins versus SQLite on concurrent snapshot reads and trigram-backed substring search,
  - competitive thresholds versus SQLite on core baseline reads.
