## Composite Primary Keys and Composite Indexes
**Date:** 2026-02-06
**Status:** Accepted

### Decision

Extend DecentDB to support **composite primary keys** (multiple columns forming the PK) and **composite indexes** (multi-column secondary indexes). The changes are:

1. **SQL AST**: Change `Statement.columnName: string` → `columnNames: seq[string]` for `skCreateIndex`. Change `IndexMeta.column: string` → `columns: seq[string]`. Allow `primaryKey` on multiple `ColumnDef` columns.

2. **Catalog encoding**: Extend `makeIndexRecord`/`parseCatalogRecord` to store a `;`-delimited column list instead of a single column name. The existing single-column encoding is a valid subset (one element list), so this is **backward-compatible on read** for existing databases.

3. **Composite key encoding**: Composite index keys are encoded by hashing the concatenation of individual `indexKeyFromValue` results via CRC32C, producing a single `uint64` B+tree key. On lookup, hash collisions are resolved by reading the full row and verifying all column values — identical to the existing TEXT/BLOB index verification pattern.

4. **Composite PK behavior**: Tables with composite PKs do **not** use the INT64-rowid optimization (where a single INT64 PK becomes the B+tree rowid). Instead, they use auto-generated rowids and a **composite unique index** that enforces uniqueness across the PK column combination.

5. **Binder**: Remove the `primaryCount > 1` error. Validate that composite PK columns all exist and are not NULL.

### Rationale

Composite primary keys are standard SQL and required for many real-world schemas (junction/association tables). The PostgreSQL import tool currently skips tables with composite PKs, losing data. Supporting them within the existing B+tree architecture (hash-based composite keys with row verification) requires no changes to the B+tree or page format.

### Alternatives Considered

- **Variable-length composite B+tree keys**: Would require changing the B+tree key type from `uint64` to `seq[byte]`, affecting page format, comparison logic, and every B+tree operation. Much larger change surface.
- **Synthetic surrogate keys only**: Import tool adds an auto-increment PK and ignores the composite PK semantics. Loses uniqueness enforcement.
- **Concatenated integer encoding**: Pack two INT64 columns into a single uint64 via bit-shifting. Only works for small integer combinations, not general-purpose.

### Trade-offs

- **Hash collisions**: Composite keys use CRC32C hashing, which can collide. Uniqueness checks and lookups must verify actual column values (same as TEXT/BLOB indexes). This adds a row read per collision but is acceptable for correctness.
- **No composite key range scans**: Hash-based composite keys do not support ordered range scans across the composite. This is consistent with existing TEXT/BLOB index behavior.
- **Catalog format**: The column list encoding (`col1;col2` in the index record) is a minor catalog change but does **not** require a format version bump since single-column indexes remain a valid single-element list.

### References

- ADR-0036: Catalog constraints and index metadata encoding
- ADR-0036: Integer primary key optimization
- ADR-0061: Typed index key encoding (proposed, orthogonal)
