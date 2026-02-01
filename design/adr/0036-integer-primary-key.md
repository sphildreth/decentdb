# 36. Integer Primary Key Optimization

Date: 2026-01-31

## Status

Accepted

## Context

Currently, DecentDB assigns a hidden 64-bit integer `rowid` to every row in a table. This `rowid` is used as the key for the table's main B+Tree (clustering key). 

If a user defines a table with an explicit primary key, such as:

```sql
CREATE TABLE users (
  id INT PRIMARY KEY,
  name TEXT
);
```

DecentDB currently performs the following:
1.  Allocates a hidden `rowid` for the row.
2.  Stores `(id, name)` in the main table B+Tree keyed by `rowid`.
3.  Creates a *separate* secondary B+Tree index for `id`, mapping `id -> rowid`.

This results in redundancy:
- The `id` is stored twice (once in the table payload, once in the index key).
- The `rowid` is stored in the secondary index.
- Lookups by `id` require traversing the secondary index to get the `rowid`, then traversing the main table B+Tree.
- Inserts require updating two B+Trees.

## Decision

We will optimize tables that have a single `INT64` column marked as `PRIMARY KEY` (an "Integer PK").

For such tables:
1.  The Integer PK column **becomes** the `rowid`.
2.  The main table B+Tree will use the value of this column as its key.
3.  No secondary index will be created for this column.
4.  The column value will still be stored in the record payload (to simplify record decoding for now, though eventually it could be omitted).

### Changes

- **Catalog:** When saving a table definition, if an Integer PK is detected, the engine will skip creating a default `IndexMeta` for it.
- **Storage:** 
    - `insertRow`: Check if the table has an Integer PK. If so, extract the value from the provided values and use it as the `rowid`. Ensure uniqueness (the B+Tree will enforce this naturally).
    - `updateRow`: Allow updating the row. If the PK changes, it effectively becomes a delete of the old key and insert of the new key (though `updateRow` might need adjustment to handle this, or we restrict PK updates for MVP. The prompt instructions imply `updateRow` needs handling).
    - `deleteRow`: Ensure deletion uses the correct key.

### Constraints

- Only applies to single-column Primary Keys.
- Only applies if the column type is `INT64`.
- If the value is not provided (e.g. NULL or missing), we might need to fallback to auto-increment logic, but for this specific iteration, we assume the value is provided or we generate one if it's compatible with `nextRowId`.

## Consequences

### Positive
- **Reduced Storage:** Eliminates the secondary index for the PK.
- **Faster Lookups:** Lookups by PK become direct B+Tree traversals (O(log N) instead of 2 * O(log N)).
- **Faster Inserts:** Only one B+Tree to update.

### Negative
- **Complexity:** `storage.nim` must now be aware of the schema's PK definition to determine the `rowid`.
- **Updates:** Updating a Primary Key is expensive (requires moving the row in the B+Tree), whereas updating a non-PK column is in-place (or overflow).

## Implementation status

Implemented in the engine and covered by unit tests ("Primary Key Optimization"). This ADR does not require a `FormatVersion` bump because it does not change the on-disk page/record encoding; it changes how rowids are assigned and which indexes are created for integer-PK tables.
