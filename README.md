# DecentDb

![Status](https://img.shields.io/badge/status-pre--alpha-orange)
![Language](https://img.shields.io/badge/language-Nim-2d9cdb)
![License](https://img.shields.io/badge/license-Apache--2.0-blue)

ACID first. Everything else... eventually.

DecentDb is a pre-alpha, embedded relational database engine focused on:
- Durable ACID writes (WAL-based)
- Fast reads (paged storage + B+Trees)
- Single-process concurrency: 1 writer + many concurrent readers (snapshot isolation)
- PostgreSQL-like SQL subset (good-enough CRUD + joins)
- Fast substring search for `LIKE '%pattern%'` via a trigram inverted index (on selected columns)

Status: the repository currently contains the design (PRD/SPEC/ADRs) and a phased implementation plan. The engine implementation is intended to be built incrementally from Phase 0 onward.

Important:
- Not production-ready.
- Persistent formats are expected to change until a versioned compatibility policy is established.

## Table Of Contents
- [Quick Links](#quick-links)
- [Repo Layout](#repo-layout)
- [Project Status](#project-status)
- [Goals (MVP)](#goals-mvp)
- [Non-Goals (MVP)](#non-goals-mvp)
- [Architecture (Planned)](#architecture-planned)
- [Concurrency Model (MVP)](#concurrency-model-mvp)
- [Durability + Recovery (MVP)](#durability--recovery-mvp)
- [Testing Philosophy](#testing-philosophy)
- [Roadmap (Phased)](#roadmap-phased)
- [Performance Targets (Acceptance)](#performance-targets-acceptance)
- [Representative Queries (Acceptance Shapes)](#representative-queries-acceptance-shapes)
- [Development Setup](#development-setup)
- [Contributing](#contributing)
- [License](#license)

## Quick Links
- Product requirements: `design/PRD.md`
- Engineering spec: `design/SPEC.md`
- Testing strategy: `design/TESTING_STRATEGY.md`
- Implementation phases (agents should follow this): `design/IMPLEMENTATION_PHASES.md`
- ADR index (decisions that affect formats/ACID semantics): `design/adr/README.md`
- Agent workflow rules (how changes are expected to land): `AGENTS.md`
- License: `LICENSE`

## Repo Layout
- `README.md`: project overview (this file)
- `AGENTS.md`: rules for coding agents and contributor workflow
- `design/PRD.md`: product requirements and MVP milestones
- `design/SPEC.md`: engineering design (modules, formats, concurrency, WAL)
- `design/TESTING_STRATEGY.md`: unit/property/crash/differential testing plan
- `design/IMPLEMENTATION_PHASES.md`: phased, checkbox-driven implementation plan
- `design/adr/`: architecture decision records (anything format/ACID-sensitive lands here first)

## Project Status
Pre-alpha.

What exists today:
- A PRD + engineering spec + testing strategy under `design/`
- ADRs documenting decisions that affect formats and ACID semantics under `design/adr/`
- A phased, checkbox-driven implementation plan under `design/IMPLEMENTATION_PHASES.md`

What is intentionally prioritized:
- Durable commits and crash safety before feature breadth
- Deterministic testing from day one (unit + property + crash injection + differential)

## Goals (MVP)
- WAL-only durability (fsync on commit by default)
- Paged storage + page cache
- B+Tree tables and secondary indexes
- SQL subset:
  - DDL: CREATE TABLE, CREATE INDEX, DROP TABLE, DROP INDEX
  - DML: SELECT, INSERT, UPDATE, DELETE
  - JOINs: INNER/LEFT
  - WHERE, ORDER BY, LIMIT/OFFSET, parameters `$1, $2, ...`
  - Aggregates: COUNT/SUM/AVG/MIN/MAX with GROUP BY (and HAVING)
- Foreign keys enforced (MVP: RESTRICT/NO ACTION, statement-time enforcement)
- Trigram substring index for accelerating `LIKE '%pattern%'` on configured TEXT columns

## Non-Goals (MVP)
- Multi-process concurrency / shared-memory locking
- PostgreSQL wire protocol compatibility
- Cost-based optimizer / full statistics system
- Full-text ranking/stemming/tokenization beyond trigrams
- Online vacuum/compaction beyond minimal hooks

## Architecture (Planned)
DecentDb is designed as a set of explicit modules (see `design/SPEC.md`):
- `vfs`: OS I/O abstraction + test-only Faulty VFS hooks
- `pager`: fixed-size pages, cache, eviction, freelist
- `wal`: append-only WAL, checksums, recovery, in-memory WAL index
- `btree`: B+Tree tables and secondary indexes
- `record`: typed record encoding/decoding + mandatory overflow pages for large TEXT/BLOB
- `catalog`: schema metadata, schema cookie, system tables
- `sql`: SQL parse + binding + lowering to logical plan
- `planner`: rule-based planner for the target workload
- `exec`: volcano iterator engine (scan/seek/filter/project/join/sort/agg)
- `search`: trigram inverted index + query evaluation/guardrails

## Concurrency Model (MVP)
- Exactly one writer transaction at a time.
- Many concurrent readers across threads in the same process.
- Readers run under snapshot isolation:
  - each reader captures a `snapshot_lsn` at transaction start
  - reads consult a WAL overlay to see the latest committed page <= `snapshot_lsn`

## Durability + Recovery (MVP)
- WAL frames include checksums and LSNs; a transaction becomes committed when its COMMIT frame is durably written.
- Recovery on open scans the WAL, ignores torn/incomplete frames, and rebuilds a read overlay up to the last commit boundary.
- Checkpointing and WAL truncation must respect active readers (never truncate away frames needed by a live snapshot).

## Testing Philosophy
Correctness is the project constraint, not a stretch goal.

The test pyramid (see `design/TESTING_STRATEGY.md`) includes:
- Nim unit tests for each core module (pager/WAL/btree/record/search)
- Property tests for invariants (e.g., index results match scans)
- Crash-injection durability tests (Faulty VFS + failpoints)
- Differential tests vs PostgreSQL for the supported SQL subset

## Roadmap (Phased)
Agents should implement the project by working the phases in order. The canonical checklist lives in `design/IMPLEMENTATION_PHASES.md`.

Phase map:
- [x] Phase 0: Foundations (project + test harness + VFS)
- [x] Phase 1: DB File + Pager + Page Cache (read/write pages)
- [ ] Phase 2: Records + Overflow Pages + B+Tree Read Path
- [ ] Phase 3: WAL + Transactions + Recovery + Snapshot Reads
- [ ] Phase 4: B+Tree Write Path + Catalog + SQL/Exec MVP
- [ ] Phase 5: Constraints + Foreign Keys + Trigram Search (v1)
- [ ] Phase 6: Checkpointing + Bulk Load + Performance + Hardening

How to update the roadmap:
- Update the checkboxes above and in `design/IMPLEMENTATION_PHASES.md` when work lands.
- Keep completed items (do not delete); append new scope when needed.

## Performance Targets (Acceptance)
See `design/PRD.md` for the current acceptance targets. Highlights:
- PK point lookup: P95 < 10ms
- FK join expansion (artist->albums->tracks): P95 < 100ms
- Trigram substring search: P95 < 200ms
- Crash recovery: < 5 seconds for a 100MB database

## Representative Queries (Acceptance Shapes)
These query shapes drive planner and index decisions (see `design/PRD.md`):

```sql
SELECT a.id, a.name, al.name, t.trackNumber, t.name
FROM artist a
LEFT JOIN album al ON (a.id = al.artistId)
LEFT JOIN track t on (al.id = t.albumId)
WHERE al.name like '%COLDSPRING%'
AND a.name like '%JOEL%'
ORDER BY a.name, al.name, t.trackNumber;
```

## Development Setup
At the moment, this repository is design-first: there is no stable build/run workflow documented yet because the engine implementation is being built from Phase 0.

Planned developer dependencies (see `design/SPEC.md` and `design/TESTING_STRATEGY.md`):
- Nim compiler (engine)
- Python 3 (test harness)
- PostgreSQL 15.x (differential testing; CI should cover PG14/PG15/PG16)

## Developer Onboarding
Prerequisites:
- Nim (includes `nim` and `nimble`)
- Python 3

Common commands:
- Build CLI: `nimble build`
- Run all unit tests: `nimble test`
- Run Nim unit tests only: `nimble test_nim`
- Run Python harness tests only: `nimble test_py`
- Run lint/static checks: `nimble lint`

Notes:
- The engine is still a stub; `decentdb_cli` currently supports open/close and a stubbed `--sql` path.

## Contributing
This repo is optimized for incremental, test-driven implementation.

1. Read `AGENTS.md` and the design docs under `design/`.
2. Pick the earliest unchecked item in `design/IMPLEMENTATION_PHASES.md`.
3. Implement exactly that item, plus tests.
4. If you change anything persistent-format-sensitive (DB header/page layout/WAL frames/postings format), write an ADR first and update the format version + compatibility tests.

Tips for PRs in this repo:
- Keep PRs small: aim to complete exactly one checklist item from `design/IMPLEMENTATION_PHASES.md`.
- Prefer explicit implementations over clever ones.
- If your change touches durability or persistent formats, add crash-injection coverage or explain why not.

## License
Apache-2.0. See `LICENSE`.
