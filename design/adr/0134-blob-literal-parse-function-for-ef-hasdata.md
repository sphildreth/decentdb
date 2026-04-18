## ADR-0134: BLOB Literal Parse Function for EF Core `HasData`
**Date:** 2026-04-18
**Status:** Proposed

### Decision

DecentDB should add a built-in scalar function that converts hex text to native
`BLOB` values for literal SQL contexts:

- Preferred API: `BLOB_PARSE(hex_text)` returning `BLOB`
- Accept uppercase/lowercase hex; reject odd-length or non-hex input with a
  clear SQL error

Until that function exists, the .NET EF Core provider will throw a
`NotSupportedException` when asked to generate inline SQL literals for `byte[]`
(e.g., `HasData`), instead of emitting invalid SQL.

### Rationale

`HasData` uses inline SQL literals, not bound parameters. DecentDB currently has
no accepted BLOB literal syntax in this path.

Probe results against a real DecentDB connection:

1. `X'00FF'` → `cannot cast Text("x00FF") to BLOB`
2. `CAST('00FF' AS BLOB)` → `cannot cast Text("00FF") to BLOB`
3. `FROM_HEX('00FF')` → `unsupported scalar function from_hex`
4. `BLOB_PARSE('00FF')` → `unsupported scalar function blob_parse`
5. `CAST(X'00FF' AS BLOB)` → `cannot cast Text("x00FF") to BLOB`

Because no executable literal form exists, continuing to emit provider-default
`X'...'` syntax causes runtime migration/seed failures.

### Alternatives Considered

#### 1. Keep emitting `X'...'`

Rejected. DecentDB parses `X'...'` as text, causing deterministic failures for
`BLOB` columns.

#### 2. Use `CAST('<hex>' AS BLOB)`

Rejected. Current engine cast semantics do not decode hex text into BLOB data.

#### 3. Force parameterized seed insertion

Rejected for this slice. EF Core `HasData` generation is literal-based and does
not route through provider parameter binding.

### Consequences

- Positive: seed-data failures become explicit and actionable instead of
  silently shipping broken literal SQL.
- Positive: once `BLOB_PARSE` exists, providers can emit one stable literal
  pattern.
- Negative: `HasData` for `byte[]` remains unavailable until engine support for
  parseable BLOB literals lands.

### References

- `design/EF_MISSING_TESTS.md` (Slice S5)
- `bindings/dotnet/src/DecentDB.EntityFrameworkCore/Storage/DecentDBByteArrayTypeMapping.cs`
- `docs/user-guide/sql-reference.md`
