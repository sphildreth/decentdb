# Road to Rust: DecentDB 1.0.0 Implementation Plan
**Date:** 2026-03-22
**Status:** Active Execution

This document defines the systematic, bottom-up plan for rewriting DecentDB from Nim to Rust. It is divided into logical slices. **Coding agents MUST focus strictly on implementing the current active slice** before moving up the stack. All implementation must align with the 7 pillars defined in `PRD.md`.

---

## 🗺️ Slice Map / Status

- [ ] **Phase 1: Foundation (Storage & I/O)**
  - [ ] Slice 1.1: VFS (Virtual File System) & Fault Injection
  - [ ] Slice 1.2: Pager & LRU Page Cache
  - [ ] Slice 1.3: WAL (Write-Ahead Log) Writer & Reader
  - [ ] Slice 1.4: Crash Recovery & Checkpointing
- [ ] **Phase 2: Data Structures**
  - [ ] Slice 2.1: Record Encoding (Varints & Values)
  - [ ] Slice 2.2: B+Tree Node Layout & Traversal (Read-only)
  - [ ] Slice 2.3: B+Tree Mutations (Inserts, Splits, Overflow)
  - [ ] Slice 2.4: Trigram Index Storage
- [ ] **Phase 3: Relational Core**
  - [ ] Slice 3.1: System Catalog & Metadata
  - [ ] Slice 3.2: SQL Parsing (libpg_query integration)
  - [ ] Slice 3.3: Volcano Query Planner (Iterators)
  - [ ] Slice 3.4: DML Execution & Constraints
- [ ] **Phase 4: Ecosystem Parity**
  - [ ] Slice 4.1: The C-ABI Boundary (FFI)
  - [ ] Slice 4.2: Python Crash/Differential Test Wiring
  - [ ] Slice 4.3: DecentDB CLI (Clap)
  - [ ] Slice 4.4: 1.0.0 Release Verification

---

## 🛠️ Phase 1: Foundation (Storage & I/O)

### Slice 1.1: VFS (Virtual File System) & Fault Injection
**Goal:** Abstract all disk I/O to allow concurrent reads and rigorous crash testing.
**Directives for Agents:**
- Create `src/vfs.rs`.
- Define a trait `Vfs` with methods: `open`, `read_at`, `write_at`, `sync_data`, `sync_metadata`, `file_size`.
- *Crucial Context:* See `ADR-0119`. You MUST use `std::os::unix::fs::FileExt::read_at` (and the Windows equivalent) so that `read_at` takes `&self` (immutable), allowing multiple threads to read concurrently without a `Mutex`.
- Implement `OsVfs` for standard disk I/O.
- Implement `FaultyVfs` that wraps `OsVfs` but intercepts calls to simulate dropped `fsync` and partial writes (based on counters/flags).
- **Testing:** Add `#[cfg(test)] mod tests` at the bottom of the file proving concurrent `read_at` works without locking.

### Slice 1.2: Pager & LRU Page Cache
**Goal:** Fixed-size page manager with memory boundaries.
**Directives for Agents:**
- Create `src/pager.rs`.
- Define `Page` using `#[repr(C)]` or `#[repr(align(4096))]` to guarantee exact memory sizes (usually 4KB or 8KB, per DB config).
- Implement an LRU cache (you may use a lightweight crate like `lru` or write a custom intrusive list, but prioritize zero-allocation evictions).
- Implement `pin` and `unpin` semantics. A page cannot be evicted if it is pinned by a running query.
- Use `RwLock` carefully. The cache itself needs a lock for eviction, but page contents should ideally allow concurrent reads.
- **Testing:** Prove cache hits avoid VFS calls. Prove eviction drops the least recently used unpinned page.

### Slice 1.3: WAL (Write-Ahead Log) Writer & Reader
**Goal:** ACID durability for writes.
**Directives for Agents:**
- Create `src/wal.rs`.
- Follow `ADR-0033` (WAL Frame format) and `ADR-0068` (WAL Header).
- Implement the `AtomicU64` `snapshot_lsn` mechanism (see `ADR-0003`). Readers must use `load(Ordering::Acquire)` to grab their point-in-time snapshot.
- Implement `WalWriter`: Appends frames (Page, Commit, Checkpoint), calculates checksums, and calls `vfs.sync_data()` on Commit.
- Implement `WalIndex`: An in-memory map (`page_id` -> `frame_offset`) that allows readers to quickly find the latest version of a page in the WAL.

### Slice 1.4: Crash Recovery & Checkpointing
**Goal:** Recovering from power loss and truncating the log.
**Directives for Agents:**
- Add `recover()` to `wal.rs`: Scan the WAL from offset 0 to `wal_end_offset`. Ignore torn writes (frames missing their payload size or checksum). Populate the `WalIndex`.
- Implement `checkpoint()`: Copy all committed pages from the WAL into the main database file via the Pager.
- Implement the reader-protection rule (`ADR-0019`): Do not truncate the WAL if `min(active_reader_lsn) < wal_end_lsn`.

---

## 🌳 Phase 2: Data Structures

### Slice 2.1: Record Encoding
**Goal:** Compact, highly-indexable serialization of SQL rows with full type support.
**Directives for Agents:**
- Create `src/record.rs`.
- Read `docs/user-guide/data-types.md` for exact storage semantics.
- Define `pub enum Value { Null, Int64(i64), Float64(f64), Bool(bool), Text(String), Blob(Vec<u8>), Decimal(i64, u8), Uuid([u8; 16]), Timestamp(i64) }`.
- **Storage Constraints (CRITICAL):**
  - Implement Leb128 Varint encoding/decoding for `Int64` to maximize space efficiency (e.g., the number `5` should take 1 byte on disk, not 8).
  - `Uuid` MUST be stored as 16 raw bytes, never as a 36-character string.
  - `Timestamp` and `Decimal` are backed by `i64` and should also leverage varint encoding where mathematically safe/appropriate, or fixed 8-bytes if required for sortability.
  - `Text` must guarantee UTF-8 validity.
- Implement `encode_record(&[Value]) -> Vec<u8>` and `decode_record(&[u8]) -> Result<Vec<Value>>`.
- Ensure encoded byte arrays sort lexicographically correctly where possible, or document where index keys require specialized encoding.
- **Testing:** Use `proptest` to generate random `Vec<Value>` including multi-byte Unicode strings and boundary integers, ensuring `decode(encode(x)) == x`.

### Slice 2.2: B+Tree Node Layout & Traversal (Read-only)
**Goal:** Reading tables and indexes.
**Directives for Agents:**
- Create `src/btree.rs`.
- Define exact byte layouts for `BTreeHeader`, `InternalNode`, and `LeafNode` using `#[repr(C, packed)]` or explicit byte-slice parsing. No padding bytes allowed (`ADR-0032`, `ADR-0035`).
- Implement binary search within a page's cell offset array.
- Implement `Cursor` for forward/backward traversal across page boundaries.

### Slice 2.3: B+Tree Mutations
**Goal:** Modifying the tree.
**Directives for Agents:**
- Implement `insert()`, `update()`, `delete()`.
- Implement Node Splitting: Allocate new page from Pager, split records evenly, promote pivot key to parent.
- Implement Overflow Pages (`ADR-0020`, `ADR-0031`): If a `Value::Text` or `Blob` exceeds the maximum inline cell size (~512 bytes), spill it into a linked list of overflow pages.
- Integrate `zlib` compression (via `flate2` crate) for any `Text` or `Blob` that gets pushed to an overflow page, as mandated by the storage specs.

### Slice 2.4: Trigram Index Storage
**Goal:** Inverted index for fast text search.
**Directives for Agents:**
- Create `src/search.rs`.
- Implement trigram generation (tokenize strings into 3-char chunks).
- Store posting lists (lists of row IDs) as compressed delta-encoded varints.
- Implement posting list intersection logic.

---

## ⚙️ Phase 3: Relational Core

### Slice 3.1: System Catalog
**Goal:** Schema management.
**Directives for Agents:**
- Create `src/catalog.rs`.
- Define the root system table layout. Store table schemas, indexes, and constraints as strictly formatted strings or serialized metadata.
- Implement schema version tracking (schema cookie).

### Slice 3.2: SQL Parsing
**Goal:** Understanding the user.
**Directives for Agents:**
- Create `src/sql.rs`.
- Integrate `libpg_query`. (You may use the `pg_query` crate or write custom FFI bindings to the C library).
- Convert the Postgres AST JSON into a clean, strongly-typed Rust `Statement` enum (AST).

### Slice 3.3: Volcano Query Planner
**Goal:** Executing read queries.
**Directives for Agents:**
- Create `src/planner.rs` and `src/exec.rs`.
- Define the `Plan` tree.
- Implement `Iterator` traits for execution nodes: `TableScan`, `IndexSeek`, `Filter`, `Project`, `NestedLoopJoin`, `Sort`.
- Ensure execution buffers are reused to minimize heap allocations during tight loops.

### Slice 3.4: DML Execution & Constraints
**Goal:** Modifying data safely.
**Directives for Agents:**
- Implement `INSERT`, `UPDATE`, `DELETE`.
- Enforce `NOT NULL`, `UNIQUE`, and Foreign Key constraints at statement time.
- Implement statement-level rollback: If an insert fails a constraint on row 5, rows 1-4 must be reverted without aborting the entire transaction.

---

## 🌐 Phase 4: Ecosystem Parity

### Slice 4.1: The C-ABI Boundary
**Goal:** Connect to existing .NET / Python bindings.
**Directives for Agents:**
- Create `src/c_api.rs`.
- **CRITICAL:** Read and obey `ADR-0118`. Use `std::panic::catch_unwind` on EVERY exported `extern "C"` function.
- Expose the exact same function signatures that existed in the Nim engine.

### Slice 4.2: Python Crash/Differential Test Wiring
**Goal:** Prove ACID compliance.
**Directives for Agents:**
- Hook the existing Python test harness (`tests/harness/`) up to the new Rust `.so`/`.dll`.
- Run the crash-injection tests overnight to ensure the Rust WAL writer and Checkpointer never corrupt data under simulated kernel panics.

### Slice 4.3: DecentDB CLI
**Goal:** Terminal UX.
**Directives for Agents:**
- In `crates/decentdb-cli/src/main.rs`, use `clap` to implement the subcommands: `exec`, `repl`, `import`, `export`, `checkpoint`, `save-as`.
- Implement a basic interactive rustyline/reedline REPL.

### Slice 4.4: 1.0.0 Release Verification
**Goal:** Final checks.
**Directives for Agents:**
- Ensure all CI pipelines (PR and Nightly) are green.
- Compare benchmark metrics against the old Nim engine to ensure SQLite-beating performance is maintained.
- Generate final docs.