## Native DateTime (TIMESTAMP) Type
**Date:** 2026-02-26
**Status:** Accepted

### Decision

Add a native `TIMESTAMP` column type (`ctDateTime` / `vkDateTime`) backed by a
new `ValueKind` ordinal (17) encoded as a zigzag-varint int64 of **microseconds
since Unix epoch UTC** — identical to PostgreSQL's internal timestamp format.

### Motivation

Previously, `DATE`, `TIMESTAMP`, `TIMESTAMPTZ`, and `DATETIME` column type
keywords were silently mapped to `ctText` (TEXT storage).  Values were stored
as raw strings and compared lexicographically; `NOW()` returned a TEXT string;
`EXTRACT` performed string parsing on every call; there was no way to bind a
native timestamp value from bindings.

A native datetime type enables:
- Correct chronological `ORDER BY` without text-sort artifacts
- Efficient per-row `EXTRACT` without re-parsing text
- Accurate microsecond-precision timestamps (not milliseconds)
- Native bind/read in all language bindings (Python `datetime.datetime`,
  Go `time.Time`, .NET `DateTime`, Java `Timestamp`, etc.)

### Storage Format

- **Encoding:** Zigzag-varint int64 (identical to `vkInt64` / ordinal 1).
  No compact shortcuts (no `vkDateTime0` / `vkDateTime1`) because zero and one
  microseconds are extremely rare in practice.
- **Unit:** Microseconds since Unix epoch UTC.
- **Ordinal:** `vkDateTime = 17` — appended at the end of `ValueKind` to
  preserve all existing ordinal assignments (0–16).

### Backward Compatibility

- Existing databases with `DATE` / `TIMESTAMP` columns stored as `ctText` are
  unaffected: those catalog entries read as `"TEXT"` and decode as `vkText`.
- New `TIMESTAMP` columns created after this change store `"TIMESTAMP"` in the
  catalog and decode as `vkDateTime`.
- `NOW()` / `CURRENT_TIMESTAMP` now return `vkDateTime` instead of `vkText`.
  Callers that previously expected `vkText` will need updating (the test in
  `test_sql_exec.nim` checking the DATE column format was updated accordingly).

### Shared Utilities

Datetime conversion helpers (`datetimeToMicros`, `microsToDatetime`,
`formatDatetimeMicros`, `parseDatetimeMicros`) live in `src/utils/datetime.nim`
to avoid a circular dependency between `exec.nim` and `storage.nim`.

### C API Surface

Two new exported functions:

```c
int    decentdb_bind_datetime(void *stmt, int col, int64_t micros_utc);
int64_t decentdb_column_datetime(void *stmt, int col);
```

`decentdb_row_view` encodes `vkDateTime` in `int64Val` with `kind = 17`.

### Alternatives Considered

- **Store as TEXT** (prior behavior): correct for display but incorrect for
  ordering, arithmetic, and zero-overhead extraction.
- **Store as INT64 with a column-level "subtype" flag**: adds complexity to the
  catalog and type-check code; ordinals would be overloaded.
- **Separate vkDate / vkTime / vkDateTime**: three separate ordinals; rejected
  as over-engineering for the current scope. A single TIMESTAMP covers all
  date/time needs; date-only values are stored as midnight UTC.
