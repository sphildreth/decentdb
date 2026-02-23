# ADR 0102: JSON Scalar Functions (json_array_length, json_extract)

## Status

Accepted

## Context

The EF Core provider for DecentDB needs to support `string[]` properties stored as JSON (e.g., `["Rock","Jazz"]`). EF Core translates LINQ operations on these collections into SQL using `json_each()` table-valued functions. DecentDB does not support table-valued functions and adding them would be a significant engine change.

The EF provider intercepts `json_each()` patterns and rewrites them to scalar function equivalents:
- `.Any()` / `.Length > 0` → `json_array_length(column) > 0`
- `.Count()` → `json_array_length(column)`
- `array[index]` → `json_extract(column, '$[N]')`

These scalar functions must exist in the DecentDB SQL engine for the rewritten queries to execute.

## Decision

Add `json_array_length(text [, path])` and `json_extract(text, path)` as built-in scalar functions in `exec.nim`. These functions:

- Parse JSON using Nim's `std/json` module
- Support JSONPath-style `$` root and `$[N]` array index notation
- Return NULL for NULL inputs (SQL NULL propagation)
- Return NULL for invalid JSON (no runtime errors for malformed data)
- `json_array_length` returns an integer count of array elements (or object keys)
- `json_extract` returns the extracted value as TEXT, INT64, FLOAT64, BOOL, or NULL depending on the JSON node type

These are general-purpose SQL functions useful beyond EF Core — any SQL user can call them.

## Consequences

- Two new scalar functions available to all DecentDB users via SQL
- Depends on `std/json` (Nim stdlib — no new external dependency)
- JSON parsing occurs at query time per row; no pre-built JSON indexes
- Enables the EF Core provider to handle primitive collection patterns without core table-valued function support
