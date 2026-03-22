## Catalog constraint and index metadata encoding
**Date:** 2026-01-28
**Status:** Accepted

### Decision
Extend catalog records (format v2) to persist:
- Column constraints: NOT NULL, UNIQUE, PRIMARY KEY, and single-column FOREIGN KEY references.
- Index metadata: index kind (`btree` vs `trigram`) and unique flag.

This is encoded within existing catalog records:
- Table records store column flags in the column-encoding string.
- Index records append `kind` and `unique` fields.

Format version is bumped to **v2**; v1 databases are rejected on open.

### Rationale
Phase 5 requires durable constraint enforcement and trigram indexes after restart. Persisting constraint and index metadata in the catalog is the smallest change consistent with existing catalog storage.

### Alternatives Considered
- Add new system tables for constraints/index types.
- Store FK metadata in a separate catalog B+Tree.
- Keep constraints in-memory only (not durable).

### Trade-offs
- Compact string encoding is less structured but minimizes schema changes.
- Bumping the format version breaks v1 compatibility without migration tooling.

### References
- `design/SPEC.md` §3.4, §7.2, §8
- ADR-0006 (FK index creation)
- ADR-0007 (trigram postings storage)
- ADR-0008 (trigram guardrails)
- ADR-0009 (FK enforcement timing)
