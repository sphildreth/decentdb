# AGENTS.md
**Date:** 2026-01-28

This file defines how coding agents should operate in this repository.

## 1. North Star
- **Priority #1:** Durable ACID writes
- **Priority #2:** Fast reads
- MVP supports **single process** with **one writer** and **multiple concurrent reader threads**.
- Correctness is enforced via **tests from day one** (unit + property + crash-injection + differential testing).

## 2. Scope boundaries
### MVP In Scope
- WAL-only durability (fsync on commit by default)
- Paged storage + page cache
- B+Tree tables and secondary indexes
- SQL subset (Postgres-like syntax):
  - DDL: CREATE TABLE, CREATE INDEX
  - DML: SELECT/INSERT/UPDATE/DELETE
  - JOINs: INNER/LEFT
  - WHERE, ORDER BY, LIMIT/OFFSET, parameters
- Foreign keys enforced (MVP: RESTRICT/NO ACTION)
- Trigram substring index for LIKE '%pattern%' on configured columns
- Python harness to run:
  - crash-injection durability tests
  - differential checks vs PostgreSQL for supported subset

### MVP Out of Scope (do not implement without an ADR)
- Multi-process concurrency / shared-memory locking
- PostgreSQL wire protocol (Npgsql compatibility)
- Full PostgreSQL system catalogs / extensions
- Cost-based optimizer / statistics-driven planning
- Online compaction/vacuum beyond minimal maintenance hooks

## 3. Expected agent workflow
### 3.1 Before coding
1. Read: PRD.md, SPEC.md, TESTING_STRATEGY.md
2. Determine if your change requires an ADR (see docs/adr/README.md)
3. Create a small implementation plan:
   - Scope (what’s included/excluded)
   - Modules/files to change
   - Test plan (unit + crash if relevant)
   - Backward compatibility concerns (file formats, WAL formats)

### 3.2 While coding
- Keep changes small and incremental.
- Avoid adding dependencies; if you must, create an ADR.
- Prefer boring, explicit implementations over clever ones.
- Add tests in the same change set.

### 3.3 Definition of Done (DoD)
A change is done only when:
- ✅ Unit tests cover the main behavior and key edge cases
- ✅ Invariants are tested (property tests where applicable)
- ✅ If durability/format-sensitive: crash-injection tests are added or updated
- ✅ Documentation is updated (SPEC/PRD/ADR) if behavior changes
- ✅ CI passes on all target OSes

## 4. Commit / PR hygiene
- Use clear commit messages (imperative, scoped).
- Avoid mixing unrelated refactors with feature work.
- If you changed any persistent format (db header, page layout, WAL frame format, postings format):
  - Create an ADR
  - Bump format version and add migration/recovery notes in SPEC
  - Add compatibility tests (open old file, verify behavior)

## 5. ADR-required decisions (non-exhaustive)
Create an ADR **before** implementing any of the following:
- File format layout or versioning strategy
- WAL frame format, checksums, commit markers, fsync policy levels
- Checkpoint strategy and truncation rules
- Concurrency/locking semantics that affect correctness
- SQL dialect decisions (parameter syntax, NULL/LIKE semantics, collation rules)
- Trigram canonicalization rules and postings storage format
- Adding or replacing major dependencies (SQL parser, compression, hashing)
- Any change that could break existing databases or alter ACID guarantees
