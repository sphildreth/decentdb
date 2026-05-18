# Stable Tooling Metadata Contract

**Status:** Implemented
**Owner:** DecentDB core

DecentDB exposes stable schema and query-contract metadata for external tooling
without owning generated SDK models, package layouts, or workbench UX. Decent
Bench and other generators can consume this surface as the engine-owned source
of truth.

## Rust API

- `Db::get_tooling_metadata() -> Result<ToolingMetadata>`
- `Db::describe_query_contract(sql: &str) -> Result<QueryContract>`

Both structs are serializable and re-exported from the `decentdb` crate.

## C ABI

- `ddb_db_get_tooling_metadata_json(db, &out_json)`
- `ddb_db_describe_query_json(db, sql, &out_json)`

Successful calls return owned UTF-8 JSON strings. Callers free them with
`ddb_string_free`.

## Metadata Contract

`ToolingMetadata` contains:

- `metadata_version`
- engine and database format versions
- schema and temp-schema cookies
- `schema_fingerprint`
- `schema_fingerprint_algorithm`
- rich `schema` snapshot
- `column_type_metadata` for DecentDB native types
- `capabilities`

The schema fingerprint is SHA-256 over stable schema-shape data. It deliberately
excludes row counts and index freshness so data-only changes do not invalidate
generated code or cached query plans. Schema objects, indexes, triggers, native
type declarations, and spatial type metadata are included.

## Query Contract

`QueryContract` contains:

- `contract_version`
- normalized SQL text used for preparation
- statement kind and read-only flag
- schema cookies and schema fingerprint
- positional parameter metadata
- result-column metadata
- diagnostics for unknown or ambiguous inference

Query description parses and analyzes SQL without executing it. The analyzer
infers known parameter and result-column types from catalog columns, casts,
`INSERT` targets, `RETURNING`, common expression forms, and native spatial
functions. Unknown function or expression result types are reported through
diagnostics instead of guessed.

## Binding Exposure

Bindings expose the same JSON contract directly:

- Python: `Connection.get_tooling_metadata()` and `describe_query_contract(sql)`
- Go: `DB.GetToolingMetadataJson()` and `DescribeQueryJson(sql)`
- .NET: `DecentDB.GetToolingMetadataJson()` / `DescribeQueryJson(sql)` and
  matching ADO.NET connection helpers
- Node.js: `Database.getToolingMetadata()` and `describeQueryContract(sql)`
- Java/JDBC: `DecentDBDatabaseMetaData.getToolingMetadataJson()` and
  `describeQueryJson(sql)`
- Dart: `Schema.getToolingMetadata()` and `describeQueryContract(sql)`
