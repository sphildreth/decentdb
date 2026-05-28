# Database Identity For Coordination Sidecars
**Date:** 2026-05-27
**Status:** Accepted

### Decision

Cross-process coordination requires a stable, non-secret database identity stored
in the database header. The current header has reserved bytes but no explicit
durable database UUID. V1 will add a 128-bit random database identity to the
database header, covered by the existing page-1 header checksum from ADR 0016.

The identity is generated when a new database file is created. It is stable for
the lifetime of that database file and is copied when a database is intentionally
copied as a database artifact. Tools that create a logically new database by
importing/exporting rows generate a new identity.

The coordination sidecar stores:

- the 16-byte database identity from the header;
- the database format version and page size observed when the sidecar was built;
- a 32-byte SHA-256 identity fingerprint over:
  `DECENTDB_COORD_ID_V1 || database_identity || format_version || page_size`.

The fingerprint is not a security boundary. It exists to detect stale or
mismatched sidecars, including a `.coord` left beside a replaced database. It
must not include TDE key material, row data, SQL text, indexed terms, or other
user data.

Adding the database identity consumes reserved header space and requires a
database format version bump. Per ADR 0131, the same change must add a read-only
parser for the previous database format to `decentdb-migrate`.

The coordination sidecar has its own sidecar format version. Because the sidecar
is rebuildable from the database header and WAL, `decentdb-migrate` does not need
to parse historical sidecar formats unless a future ADR makes sidecar state
authoritative user data. Migration tools may ignore/delete/rebuild sidecars.

### Rationale

The sidecar must distinguish "same database, same coordination state" from "old
sidecar left next to a different database path." Path names and file sizes are
not enough. Hashing mutable header fields is also insufficient because normal
database operations change header state.

A random 128-bit identity is simple, non-secret, stable, and independent of user
data. Storing it in the database header lets a process that cannot decrypt an
encrypted database still report a clear sidecar/database mismatch before joining
coordination. The process still cannot join as a reader or writer unless it can
open/decrypt the database normally.

Using reserved header bytes keeps the layout small, but it is still a file
format change because older engines do not understand the identity field. The
format bump and migration parser rule from ADR 0131 therefore apply.

### Alternatives Considered

1. **Use path canonicalization as identity.** Rejected. Move/copy/replace
   operations make paths unreliable.
2. **Hash the full database header.** Rejected. Header fields such as checkpoint
   LSN and schema cookie are mutable.
3. **Hash encrypted database bytes.** Rejected. It depends on mutable data,
   creates unnecessary I/O, and complicates TDE error handling.
4. **UUID v7.** Rejected for v1. Time ordering is not needed and would reveal
   creation timing. A random 128-bit identity is enough.
5. **Sidecar-only generated identity.** Rejected. Losing or rebuilding the
   sidecar would lose identity continuity and weaken stale-sidecar detection.

### Trade-offs

- Requires a database format version bump and `decentdb-migrate` parser update.
- Copies of a database retain identity unless tools intentionally create a new
  logical database. That is acceptable because copied database artifacts should
  bring or rebuild matching sidecars.
- The identity is non-secret metadata. It must not be documented as an
  authentication or anti-tamper mechanism.

### References

- `design/WIN_CROSS_PROCESS_WAL_COORDINATION_SPEC.md`
- `design/adr/0016-database-header-checksum.md`
- `design/adr/0131-legacy-format-migrations.md`
- `design/adr/0177-cross-process-coordination-sidecar-and-locking.md`

