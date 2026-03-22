# Review of `design/ROAD_TO_RUST_PLAN.md`

**Author:** GPT-5.4 Pro  
**Date:** 2026-03-22  
**Scope:** Review the Rust rewrite execution plan against the goals in `design/PRD.md`, with supporting context from `design/SPEC.md`, `design/TESTING_STRATEGY.md`, `docs/user-guide/data-types.md`, and relevant ADRs.

---

## Executive Assessment

The plan has the right *direction* but not yet the right *level of rigor* to serve as the primary execution document for a 1.0 rewrite whose stated priorities are durable ACID writes first, performance second, and correctness-by-testing from day one.

My overall view is:

- The phase ordering is broadly sensible. Starting with VFS, pager, WAL, recovery, then moving upward into records, B+Tree, SQL, FFI, and CLI is the right shape for a storage engine rewrite.
- The plan also clearly understands several of the most important architectural choices: single process, one writer, many readers, WAL-based durability, paged storage, and a C-ABI boundary.
- However, the document currently mixes **multiple generations of design decisions**. The most serious examples are in the WAL and B+Tree sections, where the plan cites or implies older formats/algorithms that conflict with the current SPEC and newer ADRs.
- As written, the plan is too easy for an implementation agent to follow incorrectly. In a database project, that is dangerous. A plan that is only 80% aligned with the current storage format is worse than a smaller plan that is fully consistent.

In short: this is a promising skeleton, but it still needs source-of-truth cleanup, missing foundational slices, and much stronger acceptance criteria before it should be treated as the canonical implementation roadmap.

---

## What the Plan Gets Right

### 1. The bottom-up sequencing is fundamentally correct

The plan starts with storage primitives before SQL and UX layers. That matches both the product goals and the repo guidance.

- `design/PRD.md` makes ACID durability the top priority.
- `design/SPEC.md` describes the engine as WAL-based and page-oriented.
- `AGENTS.md` emphasizes correctness, borrow-checker discipline, and tests from day one.

Given that, Phase 1 being storage and I/O is exactly right.

### 2. The concurrency model is mostly aligned with the product goals

The plan's emphasis on:

- immutable positional reads in the VFS,
- lock-free snapshot capture via `AtomicU64`,
- one writer with many concurrent readers,
- reader-aware checkpointing,

is a strong match for:

- `design/PRD.md` pillar 2 (fast reads, lock-free snapshot isolation),
- `design/SPEC.md` sections on snapshot reads and concurrency,
- `design/adr/0003-snapshot-lsn-atomicity.md`,
- `design/adr/0119-rust-vfs-pread-pwrite.md`.

This is one of the plan's strongest areas conceptually.

### 3. The storage-density mindset is good

The plan repeatedly pushes toward compact storage:

- varints for integers,
- compact B+Tree nodes,
- overflow pages for large values,
- compressed postings for trigram indexes.

That is well aligned with `design/PRD.md` pillar 3 (minimal disk footprint), `design/SPEC.md`, `docs/user-guide/data-types.md`, and `design/adr/0120-core-storage-engine-btree.md`.

### 4. The plan remembers the ecosystem, not just the engine

It is good that the plan does not stop at "make the core work." It includes:

- the C-ABI boundary,
- Python crash/differential wiring,
- the CLI,
- release verification.

That is directionally aligned with the PRD's emphasis on bindings and a strong CLI experience.

### 5. The plan tries to keep work slice-based

This is a good instinct. For a rewrite, small vertical slices are safer than giant feature waves. The problem is not the slice approach itself. The problem is that some slices are missing, some are underspecified, and some point to stale design references.

---

## Critical Problems

## 1. The document says agents must focus on the "current active slice," but no slice is marked active

This is a small wording issue with large execution consequences.

The introduction says:

> Coding agents MUST focus strictly on implementing the current active slice.

But the status map shows every item unchecked, and nothing is actually marked active.

That means the plan gives a strict process rule without the metadata needed to follow it. For human contributors this is mildly annoying. For coding agents it is operationally dangerous, because multiple agents could make different assumptions about what is currently in scope.

Recommendation:

- Add an explicit status marker such as `Active`, `Blocked`, `Ready`, `Done`, or at minimum `Current Slice: 1.1` near the top of the document.
- If multiple slices can be worked in parallel, say so explicitly.

---

## 2. The WAL slices are the biggest source-of-truth problem in the plan

This is the most serious flaw in the document.

### Where the plan is out of sync

The plan says in Slice 1.3 and Slice 1.4 that agents should:

- follow `ADR-0033` and `ADR-0068`,
- calculate checksums,
- recover by scanning from offset 0,
- ignore torn writes that are missing payload size or checksum.

But the current storage description elsewhere says something materially different:

- `design/SPEC.md` says the WAL now has a **32-byte header**.
- `design/SPEC.md` says **LSNs are derived from WAL offsets**, not stored as a per-frame trailer field.
- `design/SPEC.md` says the per-frame checksum field is **reserved and written as zero** in current format notes.
- `design/SPEC.md` says payload sizes are **derived from frame type and page size** in the current format notes.
- `design/SPEC.md` says recovery scans only to `wal_end_offset`, not to raw physical file size.
- `design/adr/0064-wal-frame-checksum-removal.md` removes per-frame CRC validation.
- `design/adr/0065-wal-frame-lsn-removal.md` removes stored frame LSNs.
- `design/adr/0066-wal-frame-payload-length-removal.md` removes the payload length field.
- `design/adr/0068-wal-header-end-offset.md` adds the fixed WAL header and logical end offset.

### Why this matters

This is not a cosmetic inconsistency. This is the core durability format.

If an implementation agent follows the plan literally, it may build a WAL reader/writer that encodes checksum and payload-size assumptions that do not match the current SPEC. That would create one or more of the following outcomes:

- an implementation that does not match the intended on-disk format,
- recovery logic that bounds scans incorrectly,
- tests that validate the wrong invariants,
- future migration pain because the wrong format gets implemented first.

### Additional documentation problem

`design/adr/0033-wal-frame-format.md` itself appears stale relative to the current SPEC and later WAL ADRs. It still describes `payload_size`, CRC32C checksum, and an LSN trailer. The plan cites it as authoritative without explaining that later ADRs supersede parts of it.

Recommendation:

- Rewrite Slice 1.3 and Slice 1.4 so they name the **current WAL format explicitly** instead of relying on mixed ADR references.
- The slice should spell out:
  - WAL header starts at file offset 0.
  - Frames start at byte 32.
  - `wal_end_offset` is the logical end.
  - LSNs are derived from frame end offsets.
  - Whether checksum is reserved or active.
  - How torn-write detection works in the current format.
- If `ADR-0033` is partially superseded, say that directly in the plan.

---

## 3. Recovery and checkpointing are described too loosely for a system whose priority is ACID correctness

Even apart from the WAL format drift, the recovery/checkpoint slices are underspecified.

The plan correctly mentions:

- `recover()`,
- `checkpoint()`,
- reader protection,
- committed pages only.

But the plan does not give enough operational detail for such a sensitive area. Missing details include:

- how active readers are tracked,
- how checkpoint snapshot boundaries are chosen,
- whether new commits are allowed during checkpoint copy,
- what happens to the WAL index when truncation is not safe,
- how checkpoint interacts with in-flight writer state,
- what exact invariants must hold after recovery.

This matters because the repo already contains accepted guidance in:

- `design/adr/0019-wal-retention-for-active-readers.md`,
- `design/adr/0056-wal-index-pruning-on-checkpoint.md`,
- `design/adr/0004-wal-checkpoint-strategy.md`,
- `design/SPEC.md` section 4.

The plan currently compresses too much complexity into a few bullets.

Recommendation:

- Split crash recovery and checkpointing into separate slices, or at least separate acceptance criteria.
- Add explicit invariants such as:
  - committed transactions are visible after reopen,
  - uncommitted transactions are not visible,
  - WAL truncation never removes data needed by any active reader,
  - recovery never scans beyond logical end offset,
  - checkpoint can prune in-memory WAL index safely even when physical truncation is deferred.

---

## 4. Slice 1.2 gives misleading implementation guidance for page representation

The plan says:

- define `Page` using `#[repr(C)]` or `#[repr(align(4096))]` to guarantee exact memory sizes,
- page sizes are usually 4KB or 8KB per DB config.

This is problematic.

### Why it is problematic

`#[repr(align(4096))]` controls alignment, not the actual logical page size. Also, `design/adr/0001-page-size.md` and `design/SPEC.md` state that page size is configurable at database creation time. A single compile-time Rust struct does not naturally represent a runtime-selected page size of 4KB, 8KB, or 16KB.

So the current directive risks encouraging an implementation pattern that is too rigid for the stated configurability.

Recommendation:

- Reword this slice in terms of **page buffers and page-size invariants**, not a single magic `Page` struct.
- Specify that the page abstraction must preserve exact byte length, know the configured page size, and avoid hidden padding in on-disk representations.
- Keep `#[repr(C)]` and packed layouts for *on-disk headers*, not as the only framing for whole runtime page buffers.

---

## 5. The VFS slice is probably underspecified relative to accepted VFS scope

Slice 1.1 asks for a `Vfs` trait with:

- `open`, `read_at`, `write_at`, `sync_data`, `sync_metadata`, `file_size`.

That is a good minimum for disk I/O, but `design/adr/0105-in-memory-vfs.md` expands the VFS scope further to support memory-backed databases and additional file operations such as file existence and removal.

If `:memory:` support remains in scope for 1.0 or for the rewrite baseline, then the initial VFS trait in the plan is incomplete.

Even if the team wants to defer `MemVfs`, the plan should say so explicitly. Right now it reads as if the VFS abstraction is being finalized while an accepted ADR already expects a broader interface.

Recommendation:

- Either add `MemVfs` to the roadmap explicitly, or state that it is intentionally deferred.
- Reconcile the VFS trait surface with `design/adr/0105-in-memory-vfs.md`.

---

## 6. The B+Tree read slice conflicts with the accepted compact page-layout ADR

Slice 2.2 says:

- define exact byte layouts,
- implement binary search within a page's cell offset array,
- implement cursor traversal.

But `design/adr/0035-btree-page-layout-v2.md` describes a compact variable-length page format whose cells are parsed sequentially. That ADR explicitly says variable-length cells sacrifice O(1) random indexed access within the page.

This directly clashes with the plan's "cell offset array" language.

There is also a reference hygiene problem:

- `design/adr/0032-btree-page-layout.md` is superseded.
- The plan cites both `ADR-0032` and `ADR-0035`.
- The repo contains **two different ADR files numbered 0035**:
  - `design/adr/0035-btree-page-layout-v2.md`
  - `design/adr/0035-sql-parser-libpg-query.md`

That means "see ADR-0035" is ambiguous in this repository.

Recommendation:

- Pick one B+Tree page layout as authoritative.
- If the compact varint layout from `design/adr/0035-btree-page-layout-v2.md` is the winner, then remove the "cell offset array" wording.
- If a slotted-page layout is actually desired, create or update an ADR and stop pointing at the sequential-layout ADR.
- Stop referring to ADRs by bare number alone when duplicate numbering exists.

---

## 7. The plan is missing a foundational slice for DB file bootstrap, header validation, and page allocation

This is one of the largest missing pieces.

The plan begins with VFS, then pager, then WAL. But there is no explicit slice for:

- creating a brand-new database file,
- writing and validating the main DB header,
- format-version checks,
- page-size checks,
- schema cookie bootstrap,
- root-page bootstrap,
- freelist initialization,
- page allocation and reuse.

Yet `design/SPEC.md` defines the main DB header layout and freelist metadata, and later slices depend on page allocation existing.

Examples of downstream dependency:

- B+Tree mutation requires allocating pages.
- Overflow pages require page allocation.
- Checkpointing writes pages back into the main DB file.
- Catalog root pages need a bootstrap path.

Recommendation:

- Add a new early slice, before or immediately after the VFS slice, for:
  - main DB header format,
  - DB open/create path,
  - bootstrap root pages,
  - freelist / page allocator basics.

Without that, later slices have to smuggle in foundational storage concerns opportunistically.

---

## 8. The plan does not give transaction state and reader tracking their own first-class slice

The document references snapshot LSNs and active readers, but it does not clearly schedule the full transaction/state machinery needed to make those concepts real.

Missing or under-emphasized pieces include:

- active reader registration lifecycle,
- one-writer enforcement,
- read transaction begin/end semantics,
- statement vs transaction rollback,
- schema lock interaction,
- checkpoint blocking/coordination,
- own-writes visibility rules,
- WAL writer lifetime relative to connection lifetime.

This matters especially because:

- `design/PRD.md` emphasizes ACID correctness,
- `design/SPEC.md` describes snapshot isolation and one-writer semantics,
- `design/adr/0018-checkpointing-reader-count-mechanism.md` and `design/adr/0023-isolation-level-specification.md` exist,
- `design/adr/0121-cli-engine-enhancements.md` highlights transaction control and WAL lifetime concerns.

Recommendation:

- Add a dedicated slice for connection/transaction state management before DML execution.

Right now the plan risks implementing pieces of transactionality piecemeal across WAL, catalog, and DML work.

---

## 9. The testing strategy is not integrated early enough into the plan

This is another major weakness.

`design/PRD.md` and `design/TESTING_STRATEGY.md` both make a strong statement:

- crash testing is essential,
- correctness comes before features,
- long-running suites belong in nightly,
- every critical storage feature should be test-shaped from the beginning.

But the roadmap delays Python crash/differential harness wiring until Phase 4.

That is too late.

### Why this matters

If crash-injection integration arrives only after the WAL, recovery, checkpointing, and B+Tree mutation slices are already implemented, then the most failure-prone code in the system will have been developed without its primary validation harness.

That conflicts with both the PRD and the repository's own testing philosophy.

### Additional issue

The plan refers to an existing harness at `tests/harness/`, but at the time of review I did not find files under that path in the repository. That does not prove the harness concept is wrong, but it does mean the roadmap is currently pointing at a path that appears absent or stale.

Recommendation:

- Move crash-harness integration much earlier, ideally immediately after the first working WAL + recovery path.
- Distinguish clearly between:
  - PR-fast tests,
  - property tests,
  - crash/nightly suites,
  - performance regression suites.
- Add acceptance criteria per slice that explicitly say whether unit, property, crash, and differential tests are required.

---

## 10. Many slices have goals, but not real exit criteria

For a rewrite plan, goals alone are not enough. Several slices say "implement X" with one or two test bullets, but they do not define what it means for the slice to be done.

Examples:

- WAL slice does not define visibility guarantees after reopen.
- Catalog slice does not define how schema metadata is validated or versioned in tests.
- SQL parsing slice does not define the supported subset or AST normalization acceptance criteria.
- Planner slice lists operators but not plan-selection correctness tests.
- FFI slice says expose the same signatures, but not how ABI compatibility will be verified.

Recommendation:

- Every slice should include:
  - implementation scope,
  - explicit out-of-scope items,
  - required tests,
  - performance expectations if relevant,
  - documentation tasks if the slice changes user-visible behavior,
  - definition of done.

For a project like this, vague slices create rework.

---

## 11. The plan claims to be a 1.0.0 implementation plan, but its feature coverage looks closer to an engine MVP

This is an important product-management issue.

The title says this is the implementation plan for DecentDB 1.0.0. But `design/SPEC.md` includes a wider 1.0 baseline than what is represented in the roadmap.

Examples from the SPEC that are missing, underrepresented, or only indirectly implied:

- bulk load API,
- external merge sort behavior,
- `LIMIT` / `OFFSET` details,
- non-recursive CTE support,
- `INSERT ... RETURNING`,
- `INSERT ... ON CONFLICT` variants,
- CHECK constraints,
- trigger subsets,
- views,
- `ALTER TABLE` work,
- broader DDL surface,
- binding validation beyond Python/.NET adjacency.

Some of these may intentionally be post-1.0 or deferred in practice, which is fine. But then the roadmap should say so explicitly. Right now there is a title/scope mismatch between "1.0.0 implementation plan" and what the slices appear to cover.

Recommendation:

- Either expand the roadmap to cover the committed 1.0 baseline from `design/SPEC.md`, or rename/reposition this plan as a smaller milestone plan.

---

## 12. Record encoding and index-key encoding are at risk of being conflated

Slice 2.1 says encoded record byte arrays should sort lexicographically correctly where possible, or that key-specific encoding should be documented.

That wording is not wrong, but it is risky because the repo already recognizes that **record encoding** and **index key encoding** are not the same problem.

`design/adr/0061-typed-index-key-encoding-text-blob.md` exists because TEXT/BLOB indexes need a typed, comparable encoding with correct equality and ordering semantics. A generic row-record encoding is not automatically safe to reuse as an index key encoding.

Recommendation:

- The plan should explicitly separate:
  - row/record serialization,
  - index key normalization/encoding,
  - comparator semantics.

Otherwise there is a risk that a convenient but incorrect encoding gets reused for both.

---

## 13. Trigram durability semantics need to be called out more explicitly

The plan schedules trigram index storage in Phase 2 and later treats crash/differential wiring as Phase 4. But `design/adr/0052-trigram-durability.md` documents a softer durability model for trigram deltas relative to core B+Tree data.

That may be a sound tradeoff, but the plan should not leave it implicit, because the PRD's first pillar is uncompromising ACID compliance.

If trigram indexes are derived structures that may require rebuild after crash, the roadmap should say so explicitly and schedule the rebuild/repair path.

Recommendation:

- Add a note in the trigram slice describing the intended durability semantics and required recovery behavior.

---

## 14. The bindings story is thinner than the PRD implies

The PRD names first-class support targets for:

- Python,
- .NET / EF Core / Dapper,
- Go,
- Java,
- Node.js,
- Dart.

The roadmap includes:

- C-ABI boundary,
- Python crash/differential wiring,
- a generic statement about existing bindings.

That is a start, but not full alignment with the product goals. Even if the foreign-language packages themselves are outside the Rust-core rewrite, the roadmap should still define what "ecosystem parity" means in verifiable terms.

Recommendation:

- Add explicit validation tasks for binding compatibility, even if actual package work remains separate.
- At minimum, define ABI smoke tests and a compatibility matrix.

---

## 15. The CLI slice is too small relative to the PRD's ambition

The CLI slice currently asks for:

- `exec`, `repl`, `import`, `export`, `checkpoint`, `save-as`,
- a basic REPL.

That is a reasonable first implementation, but `design/PRD.md` asks for something closer to a polished operator experience: rich formatting, helpful errors, robust manipulation of `.ddb` files, and a feel comparable to `psql` or `sqlite3`.

The current slice reads more like a feature stub than a plan for a best-in-class CLI.

Recommendation:

- Expand the CLI slice with explicit UX acceptance criteria:
  - formatted result tables,
  - multi-line SQL input,
  - transaction control visibility,
  - exit codes and error formatting,
  - scripting mode vs interactive mode,
  - smoke tests for import/export/checkpoint workflows.

`design/adr/0121-cli-engine-enhancements.md` is relevant context here as well.

---

## 16. The documentation pillar is barely represented in the implementation plan

`design/PRD.md` makes documentation a first-class goal:

- docs live in `docs/`,
- Rust public APIs should use rustdoc,
- examples should be backed by doctests.

The roadmap mostly defers docs to the very end as "Generate final docs." That is too weak for a project that explicitly treats docs as part of product quality.

Recommendation:

- Add a standing rule to every slice: if the slice changes a public API, storage format, or user-visible SQL behavior, docs must be updated as part of the slice.
- For Rust public APIs, require rustdoc and doctests as part of slice completion where applicable.

---

## 17. Performance goals are acknowledged, but not operationalized early enough

The PRD says performance must beat SQLite. That is an aggressive claim. The roadmap ends with benchmark comparison, but it does not define the measurement scaffolding early enough.

This matters because performance is not something to "check at the end" for a storage engine. Choices about WAL format, cache contention, page layout, record encoding, and execution buffers all lock in performance characteristics early.

Supporting sources include:

- `design/PRD.md`,
- `design/TESTING_STRATEGY.md` performance regression section,
- `design/adr/0014-performance-targets.md`,
- `design/adr/0059-page-cache-contention-strategy.md`.

Recommendation:

- Introduce benchmark hooks and counters earlier in the roadmap.
- Add explicit performance checkpoints after pager, WAL, B+Tree read path, and planner/executor baseline.
- Separate PR-time microbenchmarks from nightly larger suites.

---

## Phase-by-Phase Feedback

## Phase 1: Foundation

This is the most important phase and the one that most needs cleanup.

### What is good

- Good ordering in spirit.
- Right emphasis on concurrent reads and crash testing.
- Good inclusion of fault injection.

### What is missing or weak

- No DB-header/bootstrap slice.
- WAL directives are partially stale.
- Recovery/checkpoint semantics are too compressed.
- No explicit page allocator/freelist slice.
- Test integration should happen here, not much later.

### Bottom line

Phase 1 should be expanded and made far more concrete, because every later phase rests on it.

## Phase 2: Data Structures

### What is good

- Correctly prioritizes records, B+Tree, overflow, and search.
- Strong attention to compact storage.

### What is weak

- B+Tree read-path instructions conflict with the accepted compact layout ADR.
- Record encoding risks being confused with index-key encoding.
- Compression source-of-truth is not cited clearly enough in the plan.
- Mutation slice should probably mention merge/rebalance deferral status explicitly.

### Bottom line

Phase 2 is viable, but only if page layout and encoding semantics are clarified before implementation begins.

## Phase 3: Relational Core

### What is good

- System catalog before planning/execution is sensible.
- Volcano-style iterator execution matches the SPEC.

### What is weak

- SQL subset and planner guarantees are not well delimited.
- DML slice assumes transaction machinery that the roadmap has not clearly established.
- 1.0 relational scope is broader in `design/SPEC.md` than what is shown here.

### Bottom line

Phase 3 needs either broader scope or a clearer statement that it is an MVP subset on the way to 1.0.

## Phase 4: Ecosystem Parity

### What is good

- Good that FFI and CLI are recognized as first-class work.
- Good that release verification is explicit.

### What is weak

- Crash/differential wiring is too late.
- Bindings validation is too narrow relative to PRD ambitions.
- CLI expectations are underdeveloped.

### Bottom line

Phase 4 should be narrowed to final parity/polish work, not foundational validation that the core storage engine should already be using.

---

## Recommended Revised Slice Order

If I were rewriting this plan, I would strongly consider an order closer to this:

1. **Slice 0.1: Format bootstrap**
   - DB header layout, create/open path, format validation, page size validation.

2. **Slice 0.2: VFS core**
   - `OsVfs`, positional reads/writes, sync semantics, fault injection hooks.

3. **Slice 0.3: Page allocation basics**
   - pager skeleton, page buffer abstraction, freelist, page allocation/reuse.

4. **Slice 0.4: WAL core format**
   - current WAL header, append path, `wal_end_offset`, snapshot LSN publication.

5. **Slice 0.5: Recovery + crash tests**
   - reopen semantics, incomplete-frame handling, crash harness integration.

6. **Slice 0.6: Reader tracking + checkpoint coordination**
   - active readers, safe truncation rules, checkpoint snapshot rules.

7. **Slice 1.0: Record encoding**
   - row encoding only, with separate index-key encoding follow-up.

8. **Slice 1.1: B+Tree read path**
   - authoritative page layout, search, cursor traversal.

9. **Slice 1.2: B+Tree write path + overflow pages**
   - insert/split/update/delete, overflow chain handling, compression policy.

10. **Slice 1.3: Trigram storage + rebuild semantics**
    - posting storage, recovery/rebuild model.

11. **Slice 2.0: Catalog + schema bootstrap**
    - tables, indexes, schema cookie, metadata round-trip.

12. **Slice 2.1: Parser + AST normalization**
    - supported subset explicitly listed.

13. **Slice 2.2: Planner/executor read baseline**
    - scans, seeks, filter, project, join, sort, limit/offset.

14. **Slice 2.3: Transaction state + DML + constraints**
    - write lifecycle, rollback behavior, constraint timing.

15. **Slice 3.0: FFI parity + binding smoke tests**

16. **Slice 3.1: CLI UX + scripting workflows**

17. **Slice 3.2: Release verification**
    - PR checks, nightly checks, benchmarks, docs completeness.

This is not the only workable ordering, but it would be more robust than the current one.

---

## Suggested Acceptance Criteria Template For Every Slice

The plan would improve substantially if each slice used the same structure.

For example:

### Scope

- What the slice implements.
- What it explicitly does not implement.

### Design constraints

- Which file format, concurrency, or API invariants are non-negotiable.

### Tests required

- unit,
- property,
- crash,
- differential,
- performance smoke test,
- docs/doctest updates if applicable.

### Definition of done

- `cargo check` passes,
- `cargo clippy` passes without warnings,
- required tests pass,
- docs updated,
- no unresolved format ambiguity remains.

This would align far better with both `AGENTS.md` and `design/TESTING_STRATEGY.md`.

---

## Final Judgment

`design/ROAD_TO_RUST_PLAN.md` is a strong starting outline, but not yet a safe execution plan for a database rewrite that puts durability first.

Its main strengths are architectural direction and sensible high-level sequencing. Its main weaknesses are:

- mixed-era storage references,
- missing foundational storage slices,
- under-specified testing integration,
- weak acceptance criteria,
- mismatch between roadmap scope and the broader 1.0 product/spec claims.

If the team fixes only one thing first, it should be this:

**Make the storage-core slices unambiguous and current with the actual WAL/B+Tree source of truth.**

If the team fixes the second thing after that, it should be this:

**Move crash validation and transaction/read-tracking concerns much earlier in the roadmap.**

Once those are corrected, the plan will be much closer to something that coding agents and human contributors can execute safely and consistently.

---

## Sources and References

Primary reviewed documents:

- `design/ROAD_TO_RUST_PLAN.md`
- `design/PRD.md`
- `design/SPEC.md`
- `design/TESTING_STRATEGY.md`
- `docs/user-guide/data-types.md`

Relevant ADRs reviewed:

- `design/adr/0001-page-size.md`
- `design/adr/0003-snapshot-lsn-atomicity.md`
- `design/adr/0004-wal-checkpoint-strategy.md`
- `design/adr/0014-performance-targets.md`
- `design/adr/0018-checkpointing-reader-count-mechanism.md`
- `design/adr/0019-wal-retention-for-active-readers.md`
- `design/adr/0020-overflow-pages-for-blobs.md`
- `design/adr/0023-isolation-level-specification.md`
- `design/adr/0032-btree-page-layout.md`
- `design/adr/0033-wal-frame-format.md`
- `design/adr/0035-btree-page-layout-v2.md`
- `design/adr/0035-sql-parser-libpg-query.md`
- `design/adr/0048-optional-value-compression.md`
- `design/adr/0052-trigram-durability.md`
- `design/adr/0056-wal-index-pruning-on-checkpoint.md`
- `design/adr/0059-page-cache-contention-strategy.md`
- `design/adr/0061-typed-index-key-encoding-text-blob.md`
- `design/adr/0064-wal-frame-checksum-removal.md`
- `design/adr/0065-wal-frame-lsn-removal.md`
- `design/adr/0066-wal-frame-payload-length-removal.md`
- `design/adr/0068-wal-header-end-offset.md`
- `design/adr/0105-in-memory-vfs.md`
- `design/adr/0118-rust-ffi-panic-safety.md`
- `design/adr/0119-rust-vfs-pread-pwrite.md`
- `design/adr/0120-core-storage-engine-btree.md`
- `design/adr/0121-cli-engine-enhancements.md`

External references already cited by repo documents and relevant to the review:

- SQLite documentation and design notes, as referenced by repository ADRs
- PostgreSQL snapshot-isolation and storage documentation, as referenced by repository ADRs
