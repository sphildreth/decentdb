# DecentDB SPEC (Compatibility Baseline)
**Date:** 2026-03-24  
**Status:** Compatibility Stub

> This file is no longer the authoritative end-to-end product specification.
> It remains in the repository to preserve section-level references used by ADRs,
> workflow docs, and historical design notes.

## Purpose

Use this file as a compatibility map, not as the source of truth for the full
current feature surface.

### Authoritative documents now are:

- `design/PRD.md` for product priorities and engineering constraints
- `docs/design/spec.md` for the actively maintained design-facing spec
- `design/RUST_MISSING_FEATURE_PLAN.md` for documented feature drift and
  implementation backlog
- `design/TESTING_STRATEGY.md` for validation expectations
- `design/adr/` for accepted architecture decisions and superseding details

This file intentionally preserves the legacy section and subsection numbering so
existing ADR references such as `design/SPEC.md §4.1` continue to land on a
meaningful anchor.

---

## 1. Overview

Historical role: broad engineering baseline for the embedded Rust engine.

Current guidance:

- Product priorities and constraints: `design/PRD.md`
- Current design scope: `docs/design/spec.md`
- Known implementation/documentation drift: `design/RUST_MISSING_FEATURE_PLAN.md`

Do not treat this file as the canonical statement of the current SQL surface.

## 2. Module architecture

Historical role: top-level subsystem map.

Current guidance: use `docs/design/spec.md` plus subsystem ADRs for current
details.

### 2.1 Engine modules (Rust)

The architectural decomposition remains broadly valid:

- `vfs/`
- `pager/`
- `wal/`
- `btree/`
- `record/`
- `catalog/`
- `sql/`
- `planner/`
- `exec/`
- `search/`

For current subsystem semantics, prefer `docs/design/spec.md` and the
referenced ADRs over historical prose here.

### 2.2 Testing modules (Python)

The Python harness remains part of the intended validation model.

Authoritative source: `design/TESTING_STRATEGY.md`.

---

## 3. File layout and formats

Historical role: baseline persistent-format reference.

Current guidance: this section is still useful as an index into the relevant
ADRs, but the ADRs are authoritative for exact format details.

### 3.1 Files

Baseline model still applies:

- main database file
- WAL file
- future multi-process coordination artifacts are separate work

Prefer the current WAL and storage ADRs for exact semantics.

### 3.2 Main DB header (page 1)

Authoritative references:

- `design/adr/0016-database-header-checksum.md`
- any later storage-format ADRs that supersede individual header fields

### 3.4 Catalog record encoding (v2)

Authoritative references:

- catalog and schema-related ADRs
- current catalog implementation in `crates/decentdb/src/catalog/`

Treat old field listings in historical docs as context only.

### 3.3 Page types

Authoritative references:

- `design/adr/0032-btree-page-layout.md`
- `design/adr/0031-overflow-page-format.md`
- `design/adr/0029-freelist-page-format.md`

---

## 4. Transactions, durability, and recovery (WAL-only)

Historical role: central durability baseline.

Current guidance: this family of sections remains important, but exact semantics
must come from the WAL and recovery ADRs plus the code.

### 4.1 WAL frame format (Compatibility Anchor)

Authoritative references:

- `design/adr/0033-wal-frame-format.md`
- `design/adr/0064-wal-frame-checksum-removal.md`
- `design/adr/0065-wal-frame-lsn-removal.md`
- `design/adr/0066-wal-frame-payload-length-removal.md`

If this file and the ADRs disagree, the ADRs win.

### 4.2 Snapshot reads

Authoritative references:

- `design/adr/0003-snapshot-lsn-atomicity.md`
- current WAL/index implementation

### 4.3 Checkpointing and WAL Retention

Authoritative references:

- `design/adr/0004-wal-checkpoint-strategy.md`
- `design/adr/0018-checkpointing-reader-count-mechanism.md`
- later ADRs affecting checkpoint semantics

### 4.4 Bulk Load API

Authoritative references:

- `design/adr/0017-bulk-load-api-design.md`
- current engine implementation

### 4.5 Crash recovery

Authoritative references:

- WAL/recovery ADRs
- `design/TESTING_STRATEGY.md`

---

## 5. Concurrency model (single process)

This section still expresses an important project invariant: one writer, many
readers, single process.

Authoritative references:

- `design/PRD.md`
- `AGENTS.md`
- concurrency-related ADRs

### 5.1 Writer

The single-writer model remains the baseline unless an ADR explicitly changes
it.

### 5.2 Readers

Concurrent reader semantics remain part of the intended baseline.

### 5.3 Locks and latches (Compatibility Anchor)

For exact implementation details, prefer current code plus any concurrency ADRs.

### 5.4 Deadlock detection and prevention

Treat this as design intent; current behavior should be validated against code
and tests.

### 5.5 Isolation Levels (LOW-001)

Authoritative references:

- isolation-level ADRs
- current transaction/WAL implementation

---

## 6. SQL parsing & compatibility (Postgres-like)

This section is the least reliable part of the old SPEC.

Current guidance:

- parser and SQL-surface aspirations: `docs/design/spec.md`
- actual implementation gaps: `design/RUST_MISSING_FEATURE_PLAN.md`

### 6.1 Parser choice

Use parser ADRs and current code as authoritative references.

Primary reference:

- `design/adr/0035-sql-parser-libpg-query.md`

### 6.2 Supported SQL subset (Compatibility Anchor)

Do not use this subsection as the source of truth for supported SQL features.

Instead:

- use `docs/design/spec.md` for the design target
- use `design/RUST_MISSING_FEATURE_PLAN.md` for audited implementation reality
- use user-guide docs only when they have been reconciled with tests/code

### 6.3 Parameterization

Primary reference:

- `design/adr/0005-sql-parameterization-style.md`

---

## 7. Relational features

Historical role: baseline SQL semantics for keys, constraints, and schema.

Current guidance: use ADRs plus current code for exact behavior.

### 7.1 Primary keys

Use current catalog/DML implementation and any PK-related ADRs.

### 7.2 Foreign keys

Primary references:

- `design/adr/0006-foreign-key-index-creation.md`
- `design/adr/0009-foreign-key-enforcement-timing.md`
- `design/adr/0081-foreign-key-on-delete-actions-v0.md`

### 7.3 CHECK constraints

Primary reference:

- `design/adr/0080-check-constraints-v0.md`

---

## 8. Trigram substring search index

This section still adds value as a conceptual index for the trigram subsystem,
but exact behavior belongs to the ADRs and code.

### 8.1 Why trigrams

Primary reference:

- search subsystem docs and current implementation

### 8.2 Index data model

Primary reference:

- `design/adr/0007-trigram-postings-storage-strategy.md`

### 8.3 Query evaluation

Primary reference:

- `design/adr/0008-trigram-pattern-length-guardrails.md`

### 8.4 Broad-pattern guardrails

Primary reference:

- `design/adr/0008-trigram-pattern-length-guardrails.md`

### 8.5 Storage format for postings (Compatibility Anchor)

Primary references:

- `design/adr/0007-trigram-postings-storage-strategy.md`
- later durability/search ADRs if they supersede storage details

---

## 9. Planner rules for target workload

Historical role: planner baseline and intended heuristics.

Current guidance: planner behavior is in flux and should be read from current
implementation, `docs/design/spec.md`, and the implementation backlog.

### 9.1 Index statistics (heuristic-based)

Historical reference only.

Primary references now:

- `design/adr/0013-index-statistics-strategy.md`
- any superseding planner-statistics ADRs
- `design/RUST_MISSING_FEATURE_PLAN.md` for current `ANALYZE` / stats backlog

---

## 10. Testing strategy (critical)

This section should be treated only as a pointer.

Primary reference:

- `design/TESTING_STRATEGY.md`

### 10.1 Test layers

See `design/TESTING_STRATEGY.md`.

### 10.2 Faulty VFS requirements

See `design/TESTING_STRATEGY.md` and the test-related ADRs/code.

### 10.3 CI requirements

See `design/PRD.md`, `AGENTS.md`, and current CI configuration.

---

## 11. Benchmarks and performance budgets

Primary references now are the active benchmark docs and current benchmark
scripts/results rather than this historical summary.

Use:

- benchmark guides under `design/`
- current benchmark harnesses in the repo

---

## 12. Future compatibility: Npgsql / PostgreSQL wire protocol

Treat this as historical future-looking context only.

Any future work here should be captured in a dedicated ADR or design note when
it becomes active.

---

## 13. Error handling

Primary references:

- `design/adr/0010-error-handling-strategy.md`
- current error types and API contracts

### 13.1 Error codes

Use current C ABI / Rust error definitions as authoritative.

### 13.2 Error propagation

Use code and error-handling ADRs as authoritative.

### 13.3 Error messages

Use current implementation and user-facing docs as authoritative.

---

## 14. Memory management

Primary references:

- `design/adr/0011-memory-management-strategy.md`
- current engine implementation

### 14.1 Memory pools

Compatibility anchor only.

### 14.2 Memory limits

Compatibility anchor only.

### 14.3 Out-of-memory handling

Compatibility anchor only.

### 14.4 Memory leak prevention and monitoring

Use testing strategy, leak tests, and current code as authoritative.

---

## 15. Schema versioning and evolution

Historical role: compatibility and migration baseline.

Current guidance: use storage/catalog ADRs and current format-handling code for
exact semantics.

### 15.1 Schema cookie

Compatibility anchor only; see current catalog/storage code.

### 15.2 Backward compatibility

Use versioning docs and storage ADRs as authoritative.

### 15.3 Schema changes (Compatibility Anchor)

Use:

- current DDL implementation
- ALTER TABLE ADRs
- `design/RUST_MISSING_FEATURE_PLAN.md` for audited scope gaps

### 15.4 Migration strategy

Use versioning guidance and any future migration ADRs as authoritative.

---

## 16. Configuration system

Primary references:

- current configuration code
- relevant ADRs

### 16.1 Configuration options

Compatibility anchor only.

### 16.2 Runtime configuration

Compatibility anchor only.

### 16.3 Configuration API

Compatibility anchor only.

---

## 17. B+Tree space management

Primary references:

- `design/adr/0012-btree-space-management.md`
- `design/adr/0032-btree-page-layout.md`
- current B+Tree implementation

### 17.1 Node split

Compatibility anchor only.

### 17.2 Page utilization monitoring

Compatibility anchor only.

### 17.3 Merge/rebalance (post-1.0)

Compatibility anchor only.

---

## Migration note

If this file becomes more burden than value in the future, it can be deleted
only after:

- ADR references are updated to point directly at the replacement docs/ADRs
- `AGENTS.md` no longer instructs contributors to read `design/SPEC.md`
- docs pages that point here are migrated to the newer spec sources

Until then, this stub is the lowest-risk way to keep the repository navigable
without pretending this file is still the full current spec.
