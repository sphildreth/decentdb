# Road to Rust Plan — Review by Claude Opus 4.6
**Date:** 2026-03-22
**Reviewer:** Claude Opus 4.6 (model ID: claude-opus-4.6)
**Documents Reviewed:**
- `design/ROAD_TO_RUST_PLAN.md` (the subject)
- `design/PRD.md` (7 Pillars)
- `design/SPEC.md` (Engineering Specification)
- `design/TESTING_STRATEGY.md`
- `AGENTS.md`
- 12 Architecture Decision Records (ADRs 0003, 0019, 0032, 0033, 0035×2, 0064–0068, 0105, 0117, 0118, 0119, 0120)
- All `Cargo.toml` files and current source code

---

## Executive Summary

The Road to Rust Plan is a well-structured, bottom-up implementation roadmap that correctly sequences the DecentDB rewrite from foundation (VFS/Pager/WAL) through data structures (B+Tree/Records) to relational core (SQL/Planner) and ecosystem parity (FFI/CLI). The four-phase structure aligns with the PRD's "ACID first, performance second" prioritization and the AGENTS.md directive to "start at the bottom."

However, the plan contains **several inconsistencies with its own ADRs**, **stale references to a superseded WAL frame format**, **file path mismatches with the actual workspace layout**, and **dependency decisions that contradict the project's own governance rules**. These issues, if unaddressed, will cause agent confusion and implementation errors.

**Overall Assessment: Solid foundation, needs a revision pass before agents execute beyond Phase 1.**

---

## Finding 1: File Path Mismatch — Plan vs. Workspace Layout

**Severity: HIGH (will cause agent errors immediately)**

The plan directs agents to create files at flat `src/` paths:

> *"Create `src/vfs.rs`"* (Slice 1.1, line 39)
> *"Create `src/pager.rs`"* (Slice 1.2, line 49)
> *"Create `src/wal.rs`"* (Slice 1.3, line 59)

But the actual project is a **Cargo workspace** with two crates:

```
crates/
├── decentdb/          # Core engine (lib crate)
│   ├── Cargo.toml
│   ├── src/
│   │   └── lib.rs     # Currently 15 lines
│   └── tests/
│       └── engine_api_tests.rs
└── decentdb-cli/      # CLI application (bin crate)
    ├── Cargo.toml
    └── src/
        └── main.rs    # Currently 27 lines
```

When an agent reads "Create `src/vfs.rs`", it could reasonably create the file at `./src/vfs.rs` (repository root), `crates/decentdb/src/vfs.rs`, or even ask for clarification — wasting time. The plan's Phase 4.3 partially acknowledges the workspace layout (*"In `crates/decentdb-cli/src/main.rs`"*), but Phases 1–3 do not.

**Recommendation:** Update all path references in Phases 1–3 to use fully qualified workspace paths. For example:
- `src/vfs.rs` → `crates/decentdb/src/vfs.rs`
- `src/btree.rs` → `crates/decentdb/src/btree.rs`
- `src/c_api.rs` → `crates/decentdb/src/c_api.rs`

Additionally, consider whether the flat module structure (`vfs.rs`, `pager.rs`) is appropriate or if a directory-based module layout (`vfs/mod.rs` + `vfs/os.rs` + `vfs/faulty.rs`) would better match the SPEC.md's `vfs/`, `pager/`, `wal/` module hierarchy (SPEC §2.1).

**References:**
- `SPEC.md` §2.1 uses directory-style module names: `vfs/`, `pager/`, `wal/`, `btree/`, etc.
- `Cargo.toml` (root, line 2–4): workspace members are `crates/decentdb` and `crates/decentdb-cli`
- Rust Reference on module layout: modules can be either `foo.rs` or `foo/mod.rs`; directory style is idiomatic for modules with sub-components (https://doc.rust-lang.org/reference/items/modules.html#module-source-filenames)

---

## Finding 2: WAL Frame Format — Plan References Superseded ADR

**Severity: HIGH (will produce incorrect implementation)**

Slice 1.3 (WAL Writer & Reader, lines 57–63) states:

> *"Follow `ADR-0033` (WAL Frame format) and `ADR-0068` (WAL Header)."*

ADR-0033 defines the **original** WAL frame layout with a 16-byte trailer (CRC-32C checksum + LSN per frame). However, **three subsequent ADRs strip this layout down significantly**:

| ADR | Decision | Format Version |
|-----|----------|----------------|
| ADR-0064 | Remove per-frame CRC-32C validation | v5 |
| ADR-0065 | Remove per-frame LSN field; derive from byte offsets | v6 |
| ADR-0066 | Remove `payload_length` field; derive from frame type | v7 |
| ADR-0068 | Add 32-byte WAL header with `wal_end_offset` | v8 |

**Original frame overhead (ADR-0033):** ~16 bytes per frame (checksum + LSN + payload_length)
**Final frame overhead (post ADR-0064/0065/0066):** **5 bytes per frame** (`frame_type` u8 + `page_id` u32)

This is a **68% reduction in per-frame overhead**, which is material for write performance (PRD Pillar 2) and disk footprint (PRD Pillar 3). An agent faithfully following the plan would implement the old, bulky frame format.

**Recommendation:** Update Slice 1.3 to explicitly state the target format version (v8) and reference ADRs 0064–0068 collectively. Consider replacing the ADR-0033 reference with a consolidated "WAL Implementation Guide" that reflects the final evolved format. The directive should read something like:

> *"Implement WAL frames per the v8 format: 5-byte header (frame_type u8 + page_id u32) with no trailer. LSNs are derived from byte offsets (ADR-0065). Payload sizes are implicit per frame type (ADR-0066). The WAL file begins with a 32-byte header containing `wal_end_offset` (ADR-0068). Recovery scans only up to `wal_end_offset`, not physical file size."*

**References:**
- `ADR-0064`: WAL Frame Checksum Removal (Accepted, 2026-02-05)
- `ADR-0065`: WAL Frame LSN Removal (Accepted, 2026-02-05)
- `ADR-0066`: WAL Frame Payload Length Removal (Accepted, 2026-02-05)
- `ADR-0068`: WAL Header End Offset (Accepted, 2026-02-06)
- `SPEC.md` §4.1: Already reflects the v5/v6/v7/v8 changes — the SPEC is more current than the plan

---

## Finding 3: Dependency Governance Violations

**Severity: MEDIUM (contradicts AGENTS.md, creates ADR debt)**

The plan directs agents to use specific third-party crates without corresponding ADRs:

| Crate | Where Referenced | ADR Exists? |
|-------|-----------------|-------------|
| `flate2` (zlib) | Slice 2.3, line 105 | ❌ No |
| `lru` | Slice 1.2, line 51 | ❌ No |
| `libpg_query` / `pg_query` | Slice 3.2, line 130 | ✅ ADR-0035 (SQL Parser) |
| `clap` | Slice 4.3, line 168 | ❌ No (but already in Cargo.toml) |
| `rustyline` / `reedline` | Slice 4.3, line 169 | ❌ No |

Both `AGENTS.md` (§3.2) and the project's custom instructions state:

> *"Avoid adding dependencies to `Cargo.toml`; if you must, create an ADR."*

And:

> *"If you must add a dependency (especially for major things like SQL parser, compression, hashing), create an ADR in `design/adr/` and ask the user for approval first."*

The `flate2` crate is explicitly called out as the kind of dependency that requires an ADR (compression). The `lru` crate is offered as optional (*"you may use"*), which is fine, but the plan should be clearer that a custom implementation is the default preference and `lru` requires an ADR if chosen.

ADR-0120 (Core Storage Engine) mentions "zlib compression" for overflow pages but does not analyze `flate2` specifically — it defers the implementation detail. This is a gap.

**Recommendation:**
- Create ADR for `flate2` before Slice 2.3 execution (or during Slice 2.3 planning). Evaluate alternatives: `miniz_oxide` (pure Rust, no C dependency), `zstd` (better compression ratios, via `zstd` crate), or `lz4` (faster compression/decompression).
- Clarify in Slice 1.2 that a custom LRU implementation is preferred unless an ADR justifies the `lru` crate.
- Create ADR for `rustyline` or `reedline` before Slice 4.3. These are non-trivial dependencies with different trade-offs (rustyline is mature but GPL-licensed in some configurations; reedline is newer, MIT-licensed, maintained by the Nushell team).

**References:**
- `AGENTS.md` §3.2: "Avoid adding dependencies to Cargo.toml; if you must, create an ADR."
- `ADR-0120` (Core Storage Engine): Mentions zlib compression at high level without crate analysis
- Rust crate comparison: `miniz_oxide` is the pure-Rust zlib backend already used by `flate2` internally; using it directly eliminates the C dependency layer

---

## Finding 4: ADR Numbering Collisions

**Severity: MEDIUM (ambiguous cross-references)**

Five ADR number prefixes are duplicated, creating 10 files with colliding identifiers:

| Number | File A | File B |
|--------|--------|--------|
| 0027 | `bulk-load-api.md` | `bulk-load-api-specification.md` |
| 0035 | `btree-page-layout-v2.md` | `sql-parser-libpg-query.md` |
| 0036 | `catalog-constraints-index-metadata.md` | `integer-primary-key.md` |
| 0048 | `optional-value-compression.md` | `sql-parameterized-limit-offset.md` |
| 0072 | `new-data-types-decimal-uuid.md` | `sql-cast-coercion-and-failure-semantics.md` |

When the Road to Rust Plan references "ADR-0035" in Slice 2.2 (B+Tree Node Layout), does it mean the B+Tree page layout v2 or the SQL parser decision? Context makes it clear to a human, but an AI coding agent may resolve the wrong file. This is especially dangerous because both ADR-0035 files are relevant to different parts of the plan.

**Recommendation:** Renumber the duplicates. The simplest fix: assign new numbers to the newer file in each pair (e.g., `0035-sql-parser-libpg-query.md` → `0123-sql-parser-libpg-query.md`). Alternatively, adopt a naming convention that prevents collisions, such as `NNNN-category-title.md` where categories are `storage-`, `sql-`, `dotnet-`, etc.

**References:**
- `design/adr/` directory listing (121 files)
- ADR-0035 referenced in Slice 2.2 (line 95): "No padding bytes allowed (ADR-0032, ADR-0035)"

---

## Finding 5: Missing In-Memory VFS (ADR-0105) from Phase 1

**Severity: MEDIUM (omitted feature that affects testing and API)**

Slice 1.1 directs implementation of two VFS variants:
1. `OsVfs` — standard disk I/O
2. `FaultyVfs` — crash/fault injection wrapper

However, **ADR-0105** (In-Memory VFS, Accepted) establishes a third variant: `MemVfs` for `:memory:` databases. This is critical for:
- **Unit testing**: In-memory databases make tests faster and avoid filesystem side effects
- **API completeness**: Users expect `:memory:` support (it's a standard SQLite feature)
- **Phase 4 ecosystem parity**: Language bindings will need `:memory:` for testing and ephemeral use cases

The plan completely omits `MemVfs`. If Slice 1.1 only implements `OsVfs` and `FaultyVfs`, later phases will either need to retrofit the VFS trait or create a separate slice for `MemVfs`.

**Recommendation:** Add `MemVfs` to Slice 1.1 or create a Slice 1.1b. Since ADR-0105 specifies that `MemVfs` reuses the WAL machinery unchanged, it should be straightforward to implement alongside `OsVfs`. The VFS trait design in Slice 1.1 should account for all three implementations from the start.

**References:**
- `ADR-0105`: In-Memory VFS (Accepted) — `:memory:` connection string, WAL enabled, no mmap
- SQLite documentation on in-memory databases: https://www.sqlite.org/inmemorydb.html

---

## Finding 6: Shared WAL Registry (ADR-0117) Not Addressed

**Severity: MEDIUM (architectural gap for multi-connection scenarios)**

ADR-0117 establishes that multiple connections to the same database file must share a single WAL instance via a process-global registry keyed by canonical path. This is important for Phase 4 (ecosystem parity) where language bindings may open multiple connections to the same `.ddb` file.

The plan's Slice 1.3 (WAL Writer & Reader) does not mention the shared WAL registry. If the WAL is implemented as a per-connection resource in Phase 1, retrofitting shared ownership in Phase 4 would require significant refactoring of the `WalWriter`, `WalIndex`, and snapshot LSN mechanisms.

**Recommendation:** Add a note to Slice 1.3 acknowledging ADR-0117 and designing the WAL ownership model to support sharing from the start. At minimum, the WAL should be behind an `Arc` and the `WalIndex` should use interior mutability patterns (`RwLock` or lock-free structures) that allow multiple connections to share it.

**References:**
- `ADR-0117`: Shared WAL Registry (Accepted) — process-global registry, canonical path keying, atomic `maxPageCount`
- `ADR-0003`: Snapshot LSN Atomicity — `AtomicU64` already supports multi-reader access, which aligns with shared WAL

---

## Finding 7: Missing `Send`/`Sync` Strategy Discussion

**Severity: MEDIUM (the Borrow Checker is the QA Engineer — plan should help it)**

The AGENTS.md states:

> *"The Borrow Checker is your QA Engineer [...] `Send` and `Sync` must be strictly respected."*

The PRD mandates *"lock-free snapshot isolation for concurrent readers"* (Pillar 2). The SPEC defines a *"single writer + multiple concurrent reader threads"* concurrency model (§5).

Yet the plan never discusses which types must be `Send`, which must be `Sync`, or how ownership flows across thread boundaries. This is the single most important design consideration in a Rust database engine. Getting it wrong in Phase 1 means rewriting in Phase 2.

Key questions the plan should address:
- Is `Vfs` trait `Send + Sync`? (Yes, per ADR-0119: `read_at` takes `&self`)
- Is the page cache `Send + Sync`? (Must be, for concurrent readers)
- Is the `WalWriter` `Send` but not `Sync`? (Likely, since only one writer exists)
- How does a `Page` reference flow from the cache to a reader thread? (Pin semantics + lifetimes, or `Arc<Page>`?)

**Recommendation:** Add a "Concurrency & Ownership" subsection to Phase 1 that explicitly maps out the `Send`/`Sync` boundaries for each core type. For example:

| Type | `Send` | `Sync` | Rationale |
|------|--------|--------|-----------|
| `OsVfs` | ✅ | ✅ | `read_at`/`write_at` use positional I/O, no internal mutation |
| `PageCache` | ✅ | ✅ | Interior mutability via `RwLock`; concurrent reads |
| `WalWriter` | ✅ | ❌ | Single writer; protected by external mutex |
| `WalIndex` | ✅ | ✅ | Lock-free reads via `AtomicU64` snapshot LSN |
| `Page` (pinned) | ✅ | ✅ | Immutable while pinned; `RwLock` for dirty writes |

**References:**
- Rust Reference on `Send` and `Sync`: https://doc.rust-lang.org/nomicon/send-and-sync.html
- `ADR-0003`: AtomicU64 with Acquire/Release ordering for snapshot isolation
- `ADR-0119`: `read_at` takes `&self` — the foundation of the `Sync` story
- `SPEC.md` §5: Concurrency model — "multiple concurrent readers, each reader uses snapshot_lsn"

---

## Finding 8: Slice 2.4 (Trigram) Dependency on Catalog (Phase 3)

**Severity: LOW (logical, but worth noting)**

Slice 2.4 (Trigram Index Storage) is in Phase 2 (Data Structures), but trigram indexes depend on knowing which columns are indexed, which is managed by the catalog (Phase 3, Slice 3.1). The plan implicitly assumes Slice 2.4 builds the storage format and posting list mechanics in isolation, with catalog integration happening later.

This is a reasonable decomposition — build the data structure first, wire it up later. However, the plan should make this explicit to prevent an agent from trying to build a fully integrated trigram indexing pipeline in Phase 2.

**Recommendation:** Add a clarifying note to Slice 2.4:

> *"This slice implements the trigram storage format, posting list encoding, and intersection logic as standalone library code. Integration with the catalog, SQL planner, and DML execution happens in Phase 3."*

**References:**
- `SPEC.md` §8: Trigram substring search index
- `ADR-0007`: Trigram Postings Storage Strategy

---

## Finding 9: C-ABI Boundary (Phase 4.1) Should Influence Phase 1 API Design

**Severity: LOW (architectural foresight)**

Phase 4.1 (C-ABI Boundary) comes last in the plan, but ADR-0118 (Rust FFI Panic Safety) and the PRD's Pillar 5 (bindings for Python, .NET, Go, Java, Node.js, Dart) suggest that the C-ABI is a first-class architectural constraint — not an afterthought.

If the internal Rust API is designed without considering FFI ergonomics, Phase 4.1 may require extensive adapter code or API redesign. For example:
- Functions that return `impl Iterator` cannot cross FFI boundaries
- Types using Rust-specific features (`Option<&str>`, `Result<T, E>`) need C-compatible wrappers
- The `Database` / `Connection` / `Statement` lifecycle must be expressible as opaque pointers with explicit `open` / `close` / `free` semantics

**Recommendation:** Add a "FFI Design Constraints" note to Phase 1 reminding agents that all public-facing types should be designed with eventual C-ABI exposure in mind. This doesn't mean implementing FFI in Phase 1, but it means avoiding API patterns that are hostile to FFI (e.g., returning closures, using generic type parameters in public types, relying on `Drop` as the only cleanup mechanism).

**References:**
- `ADR-0118`: Rust FFI Panic Safety — `catch_unwind` on every `extern "C"` function
- `ADR-0039`: .NET C-API Design — opaque handle pattern
- Rust Nomicon, FFI chapter: https://doc.rust-lang.org/nomicon/ffi.html
- PRD Pillar 5: "Maintain a strict, highly optimized C-ABI boundary"

---

## Finding 10: "Beat SQLite on All Fronts" Requires Nuance

**Severity: LOW (expectation management)**

PRD Pillar 2 states:

> *"Performance must beat SQLite on all fronts."*

And Phase 4.4 (Release Verification) states:

> *"Compare benchmark metrics against the old Nim engine to ensure SQLite-beating performance is maintained."*

SQLite is a 25-year-old project with extraordinary optimization. Beating it "on all fronts" is an extremely ambitious goal. Some areas where this is realistically achievable:

- **Concurrent reads**: SQLite's WAL mode allows concurrent readers, but each connection maintains its own WAL index. DecentDB's shared `WalIndex` with `AtomicU64` snapshot LSN (ADR-0003, ADR-0117) can theoretically provide lower contention.
- **Text search**: SQLite's FTS5 uses a different architecture than trigram indexes. For substring search (`LIKE '%pattern%'`), trigram indexes can outperform FTS5.
- **Type system**: SQLite's flexible typing (type affinity) introduces runtime overhead. DecentDB's strict typing can enable better code generation and skip type coercion checks.

Areas where beating SQLite is **extremely difficult**:

- **Point lookups**: SQLite's B-Tree implementation is battle-tested and has had decades of micro-optimization. A 1.0 Rust implementation is unlikely to match it on raw throughput without significant profiling and optimization.
- **Memory efficiency**: SQLite's page cache is exceptionally well-tuned. The default 2MB cache handles most workloads.
- **Cold start / small databases**: SQLite's minimal overhead for small databases is a competitive moat.

**Recommendation:** Phase 4.4 should define specific, measurable benchmarks with explicit targets rather than a blanket "beat SQLite." For example:
- *"Concurrent read throughput (8 threads): 2x SQLite WAL mode"*
- *"Substring search with trigram index: 10x SQLite LIKE scan"*
- *"Point lookup latency: within 20% of SQLite"* (acknowledging SQLite's maturity)

This sets realistic expectations and focuses optimization effort where DecentDB has structural advantages.

**References:**
- SQLite documentation on performance: https://www.sqlite.org/speed.html
- SQLite WAL mode: https://www.sqlite.org/wal.html
- SQLite FTS5: https://www.sqlite.org/fts5.html

---

## Finding 11: SPEC.md Contains Nim-Era Artifacts

**Severity: LOW (cosmetic, but signals incomplete migration)**

SPEC.md §16.3 (Configuration API) contains a code example in what appears to be Nim syntax, not Rust:

```
db = open("dbfile", config{
  page_size: 8192,
  cache_size_mb: 16,
  wal_sync_mode: FULL
})
```

Additionally, §16.1 references a compile flag `-d:allowUnsafeSyncMode` which uses Nim's `-d:` flag syntax, not Rust's `--cfg` or `--features` mechanism.

While SPEC.md is not the direct subject of this review, the Road to Rust Plan directs agents to read SPEC.md for context. Agents encountering Nim syntax in a Rust project's specification may generate incorrect code or make wrong assumptions about the API surface.

**Recommendation:** Update SPEC.md §16.3 to use idiomatic Rust syntax (builder pattern or struct-based configuration). Update the compile flag reference to use Cargo features (`--features unsafe-nosync`).

**References:**
- `SPEC.md` §16.1, §16.3
- Rust API Guidelines on builders: https://rust-lang.github.io/api-guidelines/type-safety.html#builders-enable-construction-of-complex-values-c-builder

---

## Finding 12: Error Handling Strategy (ADR-0010) Not Referenced

**Severity: LOW (implicit but should be explicit)**

ADR-0010 defines the project's error handling strategy, and `thiserror` is already in `Cargo.toml`. However, the plan never references ADR-0010 or directs agents to define error types as part of any slice. Error types are foundational — they should be established in Phase 1 and extended in each subsequent phase.

**Recommendation:** Add an error handling directive to Slice 1.1 or create a cross-cutting "Slice 0" that establishes the error type hierarchy before any implementation begins:

```rust
// crates/decentdb/src/error.rs
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("corruption: {0}")]
    Corruption(String),
    // ... extended per phase
}
```

**References:**
- `ADR-0010`: Error Handling Strategy
- `SPEC.md` §13: Error handling — defines `ERR_IO`, `ERR_CORRUPTION`, `ERR_CONSTRAINT`, etc.
- `Cargo.toml`: `thiserror = "2.0"` already declared

---

## Summary of Recommendations

| # | Finding | Severity | Action |
|---|---------|----------|--------|
| 1 | File paths don't match workspace layout | HIGH | Update all `src/` references to `crates/decentdb/src/` |
| 2 | WAL frame format references superseded ADR | HIGH | Reference ADRs 0064–0068; specify v8 format |
| 3 | Dependencies without ADRs (`flate2`, `lru`, `reedline`) | MEDIUM | Create ADRs before implementation |
| 4 | ADR numbering collisions (5 duplicates) | MEDIUM | Renumber duplicate ADRs |
| 5 | Missing `MemVfs` (ADR-0105) from Slice 1.1 | MEDIUM | Add to Slice 1.1 or create Slice 1.1b |
| 6 | Shared WAL Registry (ADR-0117) not addressed | MEDIUM | Add note to Slice 1.3 for Arc-based ownership |
| 7 | No `Send`/`Sync` strategy documented | MEDIUM | Add concurrency ownership table to Phase 1 |
| 8 | Trigram slice dependency on catalog unclear | LOW | Add clarifying note to Slice 2.4 |
| 9 | C-ABI should influence Phase 1 API design | LOW | Add FFI design constraints note |
| 10 | "Beat SQLite" needs measurable targets | LOW | Define specific benchmark targets in Phase 4.4 |
| 11 | SPEC.md contains Nim syntax artifacts | LOW | Update to Rust syntax |
| 12 | Error handling strategy (ADR-0010) not referenced | LOW | Add error type foundation to Phase 1 |

---

## What the Plan Gets Right

To be clear, the plan has substantial strengths:

1. **Bottom-up sequencing is correct.** VFS → Pager → WAL → B+Tree → SQL is exactly the right order for a database engine. Each layer depends only on the one below it.

2. **Agent directives are actionable.** Each slice has specific files to create, traits to define, and tests to write. This is far better than vague requirements.

3. **Testing is embedded in every slice.** The plan doesn't defer testing to "later" — each slice's directives include explicit testing requirements. This aligns with the PRD's "correctness before features" and TESTING_STRATEGY.md's layered defense.

4. **ADR cross-references are present.** The plan links to specific ADRs for non-obvious decisions (VFS pread/pwrite, WAL format, overflow pages). The references just need updating.

5. **The scope is realistic for 1.0.** By deferring B+Tree merges/rebalances, multi-process concurrency, and advanced window functions, the plan keeps the initial release tractable.

6. **The 4-phase structure maps cleanly to milestones.** Each phase produces a testable, demonstrable artifact: Phase 1 = durable storage, Phase 2 = indexed data, Phase 3 = SQL queries, Phase 4 = usable product.
