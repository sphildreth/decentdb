# Lua Extension Runtime And Package Model

**Date:** 2026-05-21
**Status:** Accepted for ADR-backed v1 implementation
**Roadmap:** [`FUTURE_WINS.md`](FUTURE_WINS.md)
**Audience:** Core engine developers, SQL planner/executor maintainers, C ABI maintainers, binding maintainers, CLI maintainers, documentation authors, coding agents
**Related inputs:** Lua 5.4 Reference Manual, `design/FUTURE_WINS.md`, `design/WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md`, `design/adr/0111-table-valued-functions.md`, `design/adr/0118-rust-ffi-panic-safety.md`, ADR 0169-0173

---

## 1. Executive Summary

DecentDB should support a single extension language: Lua.

The goal is not SQLite-compatible native `.load` support. The goal is a
supportable, portable, sandboxed extension model where users can add SQL-visible
behavior without giving extension code access to storage internals, process
execution, arbitrary native modules, or host-language-specific callback systems.

Extensions should be installed as packages with a manifest:

```text
text_tools/
  decentdb-extension.toml
  main.lua
  install.sql
  uninstall.sql
  tests/
    behavior.sql
    main_test.lua
  README.md
```

Users should experience extensions through normal SQL:

```sql
CREATE EXTENSION text_tools;

SELECT slugify(title) FROM posts;
SELECT score_email(subject, body) FROM messages;
```

Lua should only implement behavior. DecentDB remains the authority for SQL
types, function signatures, planner-visible metadata, NULL handling, error
reporting, transaction boundaries, and extension trust policy.

The core rule:

```text
Use Lua for behavior.
Use DecentDB for type authority, SQL registration, sandboxing, and durability.
Use the manifest as the contract.
```

2.6.0 v1 scope is package lifecycle plus sandboxed scalar functions. Lua
table-valued functions, aggregates, collations, and persisted schema
expressions are deliberately deferred by
[ADR 0173](adr/0173-lua-extension-function-kind-phasing.md).

---

## 2. Product Positioning

SQLite and DuckDB both have strong extension stories, but they optimize for a
broad ecosystem of native and optional packages. DecentDB should not chase that
model directly. A single Lua extension language gives DecentDB a narrower and
more supportable path:

- one authoring language
- one package format
- one runtime behavior model
- one docs path
- one test harness
- one binding surface
- no arbitrary native `.so` / `.dll` loading in the first extension system

This feature is valuable because it lets DecentDB users adapt the engine to
application-specific workflows without waiting for every helper function, file
parser, scoring rule, normalization routine, or table-valued helper to become a
core feature.

Examples:

- custom text normalization and scoring
- app-specific validation helpers
- deterministic masking functions for policy-aware SQL
- read-only table-valued parsers for compact domain formats
- domain-specific aggregate calculations
- controlled collations for application sort order
- local data quality functions used by migrations and import pipelines

This is not meant to replace native geospatial, FTS, JSONB, vector search,
storage compression, or other planner/storage-sensitive features. Those should
remain native DecentDB capabilities when performance, indexing, or storage
contracts matter.

---

## 3. Goals

1. Provide one official extension language: Lua.
2. Make extension behavior callable from SQL across every binding.
3. Keep extension installation, activation, and invocation explicit.
4. Preserve DecentDB type correctness for `INT64`, `DECIMAL`, `UUID`, `DATE`,
   `TIMESTAMP`, `BLOB`, JSON, and future rich types.
5. Keep extension code away from WAL, pager, B+Tree, catalog internals, and
   transaction mutation APIs.
6. Use manifest-declared signatures so SQL binding and planning can resolve
   functions before execution.
7. Treat extension errors as SQL errors with precise diagnostics.
8. Make extension execution resource-bounded and cancellable.
9. Provide CLI and binding lifecycle APIs for validation, install, list, enable,
   disable, and test workflows.
10. Make v1 useful with scalar functions before adding table-valued functions,
    aggregates, and collations.

---

## 4. Non-Goals

The first Lua extension runtime does not support:

- SQLite-compatible `.load`
- arbitrary native extension modules
- multiple extension languages
- Python, JavaScript, WASM, Guile, or host-language callback extensions
- filesystem access from Lua by default
- network access from Lua
- process execution from Lua
- direct database handles inside Lua
- writes from extension code
- WAL, pager, page-cache, B+Tree, or catalog access from Lua
- dynamic return schemas
- runtime-discovered SQL function signatures
- loose JavaScript/Python-style coercions
- auto-running extension code when an untrusted database is opened
- using Lua functions in expression indexes, generated columns, CHECK
  constraints, or persisted schema objects until determinism and trust rules are
  explicitly accepted by ADR

---

## 5. Accepted ADRs

This feature required ADRs before implementation because it introduces a major
runtime dependency, a new SQL-visible extension surface, C ABI impact, sandbox
rules, and catalog/trust policy. The required decision topics are now covered
by the accepted ADRs below.

Accepted ADR coverage:

1. [ADR 0169](adr/0169-lua-extension-runtime-dependency-and-sandbox.md):
   runtime dependency, build strategy, sandbox, resource limits, and
   wasm/browser deferral.
2. [ADR 0170](adr/0170-lua-extension-package-catalog-and-trust.md):
   package layout, manifest authority, versioning inputs, content hashing,
   catalog storage, enablement, purge, and connection-level trust.
3. [ADR 0171](adr/0171-lua-extension-sql-type-and-planner-contract.md):
   SQL function registration, strict manifest signatures, DecentDB-owned type
   boundary, NULL handling, planner contract, and persisted-expression
   restrictions.
4. [ADR 0172](adr/0172-lua-extension-cli-c-abi-and-binding-contract.md):
   CLI lifecycle, C ABI JSON bridge, binding responsibilities, inspection
   surfaces, and documentation expectations.
5. [ADR 0173](adr/0173-lua-extension-function-kind-phasing.md):
   v1 scalar-function scope and explicit deferral of table-valued functions,
   aggregates, collations, and persisted schema expressions.

Accepted dependency direction:

- Use Lua 5.4 semantics as the language target through `mlua`.
- Use vendored Lua for official native release artifacts so they do not require
  a system Lua installation.
- Hide the selected Rust crate behind DecentDB-owned extension runtime traits so
  public DecentDB APIs do not expose third-party runtime types.
- Include Lua extension support in official native 2.6.0 artifacts by default,
  while preserving a no-Lua build path for embedders and stable unsupported
  behavior in wasm/browser builds.

---

## 6. Extension Package Format

Package layout:

```text
my_extension/
  decentdb-extension.toml
  main.lua
  install.sql
  uninstall.sql
  tests/
    behavior.sql
    main_test.lua
  README.md
```

File responsibilities:

| File | Purpose |
|---|---|
| `decentdb-extension.toml` | Declares metadata, exports, signatures, permissions, limits, and API version |
| `main.lua` | Returns a Lua module table containing exported functions |
| `install.sql` | Optional SQL setup for views, metadata tables, or helper SQL objects |
| `uninstall.sql` | Optional SQL teardown |
| `tests/behavior.sql` | SQL behavior tests run by DecentDB tooling |
| `tests/main_test.lua` | Lua unit tests run inside the same sandbox profile |
| `README.md` | User-facing extension documentation |

The manifest is the source of truth for SQL-visible behavior. `install.sql` may
create ordinary SQL objects, but it must not define function signatures that are
missing from the manifest.

---

## 7. Manifest Contract

Example manifest:

```toml
name = "text_tools"
version = "1.0.0"
language = "lua"
api_version = 1
entry = "main.lua"
strict_types = true

[runtime]
max_steps = 100000
max_memory_bytes = 4194304
max_string_bytes = 1048576
max_blob_bytes = 1048576
max_rows = 10000

[permissions]
filesystem = false
network = false
process = false
database_read = false
database_write = false
native_modules = false
clock = false
random = false

[[functions]]
name = "slugify"
export = "slugify"
kind = "scalar"
args = ["TEXT"]
returns = "TEXT"
deterministic = true
null_handling = "returns_null"

[[functions]]
name = "score_email"
export = "score_email"
kind = "scalar"
args = ["TEXT", "TEXT"]
returns = "INT64"
deterministic = true
null_handling = "called_on_null"

[[functions]]
name = "tax_amount"
export = "tax_amount"
kind = "scalar"
args = ["DECIMAL", "DECIMAL"]
returns = "DECIMAL"
deterministic = true
null_handling = "returns_null"
```

Manifest validation must verify:

1. The manifest parses successfully.
2. `language = "lua"`.
3. `api_version` is supported.
4. Package name and function names are valid and canonical.
5. Entry file exists.
6. Exported Lua functions exist and are callable.
7. Declared SQL types are known.
8. Return types are supported.
9. NULL handling mode is valid.
10. Permission requests are valid.
11. Runtime limits are present or defaulted safely.
12. Function overloads are unambiguous.
13. Table-valued schemas are static and valid.
14. Collation definitions are deterministic if supported.

Install-time validation should catch as many extension authoring errors as
possible before any SQL query calls the extension.

---

## 8. Trust And Activation Model

Opening a database must never auto-run untrusted Lua code.

Recommended v1 lifecycle:

1. `decentdb extension validate ./text_tools`
2. `decentdb extension install --db app.ddb ./text_tools`
3. Application opens the database with an explicit extension policy.
4. SQL enables the already installed package:

```sql
CREATE EXTENSION text_tools;
```

5. SQL can call extension functions only when both conditions are true:
   - the database has enabled the extension
   - the current connection permits that extension by name and content hash

Example CLI:

```bash
decentdb exec \
  --db app.ddb \
  --allow-extension text_tools@sha256:abc123 \
  --sql "SELECT slugify(title) FROM posts"
```

Example Rust shape:

```rust
let db = Db::open_with_config(
    path,
    DbConfig {
        extension_policy: ExtensionPolicy::AllowListed(vec![
            ExtensionTrust::new("text_tools", "sha256:abc123"),
        ]),
        ..DbConfig::default()
    },
)?;
```

ADR 0170 chooses the exact persistence storage:

- `extension install` stores a canonical manifest, Lua source, and content hash
  in DecentDB-owned internal catalog storage.
- installed extension code is inert data until enabled and allowed.
- `CREATE EXTENSION` creates a database-level enablement record, not a trust
  grant for every connection.
- the connection or CLI invocation still decides whether Lua execution is
  allowed.
- `DROP EXTENSION` disables the SQL-visible extension objects.
- `decentdb extension purge --db app.ddb text_tools --confirm` removes
  installed package content through an explicit administrative command.

This keeps databases portable while avoiding silent code execution when a user
opens a database from an untrusted source.

---

## 9. SQL Surface

Initial SQL surface:

```sql
CREATE EXTENSION text_tools;
DROP EXTENSION text_tools;

SELECT * FROM sys.extensions;
SELECT * FROM sys.extension_functions;
```

Possible future SQL surface:

```sql
ALTER EXTENSION text_tools UPDATE TO '1.1.0';
ALTER EXTENSION text_tools DISABLE;
ALTER EXTENSION text_tools ENABLE;
```

`CREATE EXTENSION FROM '/path'` should not exist in v1. Loading code from
filesystem paths inside SQL makes query text a code-loading surface. Package
installation belongs in explicit CLI/API operations.

Extension functions use normal SQL invocation:

```sql
SELECT slugify(title) FROM posts;
SELECT score_email(subject, body) FROM messages;
SELECT tax_amount(subtotal, tax_rate) FROM invoices;
```

Table-valued functions are deferred by ADR 0173. A future table-function slice
may support:

```sql
SELECT *
FROM parse_log_blob(payload)
WHERE level = 'WARN';
```

---

## 10. Runtime Architecture

Proposed engine modules:

```text
crates/decentdb/src/extensions/
  mod.rs
  manifest.rs
  registry.rs
  catalog.rs
  values.rs
  errors.rs
  lua/
    mod.rs
    runtime.rs
    sandbox.rs
    wrappers.rs
    scalar.rs
    table.rs
    aggregate.rs
    collation.rs
```

Conceptual flow:

```text
SQL parser/normalizer
  -> SQL binder resolves function name and argument types
  -> ExtensionRegistry finds manifest-declared function
  -> executor converts DecentDB values to Lua-safe values
  -> Lua sandbox invokes exported module function
  -> runtime validates result against manifest return type
  -> executor receives DecentDB Value
```

The extension registry must sit behind DecentDB-owned abstractions:

```rust
pub(crate) trait ExtensionRuntime {
    fn validate(&self, package: &ExtensionPackage) -> Result<ValidationReport>;
    fn invoke_scalar(
        &mut self,
        function: &ExtensionFunction,
        args: &[Value],
    ) -> Result<Value>;
}
```

The SQL planner and executor must not depend on Lua-specific crate types.

Runtime state:

- Lua state is per connection and per extension package, not global process
  state.
- Extension module globals are isolated to the extension runtime.
- Extension runtime cache invalidates when the installed package hash changes.
- Extension calls receive cancellation and resource-limit checks.
- Extension panics/errors are caught and converted to `DbError::sql`.

---

## 11. Type Boundary

The type-resolution design input is accepted as the correct direction:

> DecentDB owns the type system. Lua is only the implementation language.

Every exported function must declare SQL-facing types in the manifest. SQL
binding resolves overloads before Lua execution. Lua should not decide which
SQL overload is being invoked by inspecting values.

Conversion path:

```text
SQL Value
  -> DecentDB Value
  -> Lua-safe primitive or wrapper
  -> Lua result
  -> DecentDB Value
  -> SQL result
```

Recommended input representation:

| DecentDB Type | Lua Representation |
|---|---|
| `NULL` | `nil` or `ddb.null()` depending on context |
| `BOOL` | Lua boolean |
| `TEXT` | Lua string |
| `INT64` | `ddb.Int64` wrapper by default |
| `FLOAT64` | Lua number |
| `DECIMAL` | `ddb.Decimal` wrapper |
| `UUID` | `ddb.UUID` wrapper |
| `DATE` | `ddb.Date` wrapper |
| `TIMESTAMP` | `ddb.Timestamp` wrapper |
| `BLOB` | `ddb.Blob` wrapper |
| JSON text | `ddb.Json` wrapper |

Simple return values:

| Declared Return Type | Accepted Lua Return |
|---|---|
| `TEXT` | Lua string or `ddb.text(...)` |
| `BOOL` | Lua boolean or `ddb.bool(...)` |
| `INT64` | safe Lua integer in range or `ddb.int64(...)` |
| `FLOAT64` | Lua number or `ddb.float64(...)` |
| `DECIMAL` | `ddb.decimal(...)` or existing `ddb.Decimal` |
| `UUID` | `ddb.uuid(...)` or existing `ddb.UUID` |
| `DATE` | `ddb.date(...)` or existing `ddb.Date` |
| `TIMESTAMP` | `ddb.timestamp(...)` or existing `ddb.Timestamp` |
| `BLOB` | `ddb.blob(...)`, `ddb.blob_hex(...)`, or existing `ddb.Blob` |
| JSON text | `ddb.json(...)` or existing `ddb.Json` |

Strict type mode is mandatory in v1:

- no implicit `TEXT -> INT64`
- no implicit `TEXT -> UUID`
- no implicit `FLOAT64 -> DECIMAL`
- no implicit `BLOB -> TEXT`
- no implicit `TEXT -> BLOB`
- no implicit timezone conversion
- no dynamic return schema
- no generic `ANY` return type

`DECIMAL`, `UUID`, date/time values, `BLOB`, and JSON must use typed wrappers so
precision and structure are preserved across every host language binding.

---

## 12. Lua Host API

Expose a small `ddb` namespace inside the sandbox.

Constructors:

```lua
ddb.null()
ddb.text(value)
ddb.bool(value)
ddb.int64(value)
ddb.float64(value)
ddb.decimal(value)
ddb.uuid(value)
ddb.date(value)
ddb.timestamp(value)
ddb.blob(value)
ddb.blob_hex(value)
ddb.blob_base64(value)
ddb.json(value)
```

Type checks:

```lua
ddb.type_of(value)
ddb.is_null(value)
ddb.is_text(value)
ddb.is_bool(value)
ddb.is_int64(value)
ddb.is_float64(value)
ddb.is_decimal(value)
ddb.is_uuid(value)
ddb.is_date(value)
ddb.is_timestamp(value)
ddb.is_blob(value)
ddb.is_json(value)
```

Minimal wrapper methods:

```lua
amount:to_string()
amount:add(other)
amount:sub(other)
amount:mul(other)
amount:div(other)
amount:cmp(other)

uuid:to_string()

date:year()
date:month()
date:day()
date:to_string()

timestamp:to_string()

blob:len()
blob:to_hex()
blob:to_base64()
blob:slice(start, length)

json:to_string()
```

Do not expose a database handle in v1.

---

## 13. Function Kinds

### Scalar Functions

Scalar functions are the first useful slice.

Manifest:

```toml
[[functions]]
name = "slugify"
export = "slugify"
kind = "scalar"
args = ["TEXT"]
returns = "TEXT"
deterministic = true
null_handling = "returns_null"
```

Lua:

```lua
local M = {}

function M.slugify(value)
  value = string.lower(value)
  value = string.gsub(value, "[^a-z0-9]+", "-")
  value = string.gsub(value, "^-+", "")
  value = string.gsub(value, "-+$", "")
  return value
end

return M
```

### Table-Valued Functions

Deferred by [ADR 0173](adr/0173-lua-extension-function-kind-phasing.md);
not part of the 2.6.0 v1 implementation scope.

Table-valued functions must declare a static schema.

Manifest:

```toml
[[functions]]
name = "parse_log_blob"
export = "parse_log_blob"
kind = "table"
args = ["BLOB"]

[[functions.columns]]
name = "level"
type = "TEXT"

[[functions.columns]]
name = "message"
type = "TEXT"

[[functions.columns]]
name = "created_at"
type = "TIMESTAMP"
```

Rules:

- static output schema only
- row count limit enforced
- extra columns rejected by default
- missing nullable columns become `NULL`
- missing non-nullable columns fail
- streaming iterator support requires an ADR before implementation

### Aggregate Functions

Deferred by [ADR 0173](adr/0173-lua-extension-function-kind-phasing.md);
not part of the 2.6.0 v1 implementation scope. Aggregate functions are later
than scalar and table-valued functions.

Rules:

- typed input
- typed state
- explicit state memory limit
- deterministic finalization rules
- no database access during step/final

### Collations

Deferred by [ADR 0173](adr/0173-lua-extension-function-kind-phasing.md);
not part of the 2.6.0 v1 implementation scope. Lua-backed collations are later
than scalar and table-valued functions because they can affect indexes and
persistent ordering.

Rules:

- TEXT only in v1
- return `-1`, `0`, or `1`
- deterministic required
- no index usage until persistence and rebuild semantics are accepted by ADR

---

## 14. NULL Handling

Manifest values:

```toml
null_handling = "returns_null"
null_handling = "called_on_null"
null_handling = "rejects_null"
```

`returns_null`:

- default for scalar functions
- DecentDB does not call Lua if any argument is `NULL`
- result is SQL `NULL`

`called_on_null`:

- Lua receives `nil` or typed `ddb.null()` depending on context
- extension code handles null values explicitly

`rejects_null`:

- any `NULL` input is a SQL error before Lua is called

For table-valued functions, `ddb.null()` is preferred in returned row objects
because Lua table fields with `nil` disappear.

---

## 15. Security And Resource Limits

Default sandbox:

```text
No filesystem access.
No network access.
No process execution.
No native Lua modules.
No unrestricted package loading.
No unrestricted debug library.
No unrestricted os library.
No unrestricted io library.
No direct database handle.
No writes from extension code.
```

Allowed standard libraries should be minimal:

- selected base functions
- string
- table
- math without `math.random` unless explicitly permitted later
- UTF-8 helpers if supported safely

Disabled or restricted:

- `io`
- `os`
- `debug`
- `package.loadlib`
- arbitrary `require`
- `dofile`
- `loadfile`
- environment-variable access

Required v1 scalar resource limits:

- instruction/step limit
- memory allocation limit
- maximum returned string size
- maximum returned BLOB size
- recursion depth limit
- cancellation check integration

Future function-kind limits:

- maximum table-valued rows
- maximum aggregate state size

Resource-limit errors must be SQL errors:

```text
Extension fraud.score_transaction exceeded CPU step limit.
```

Sandbox violation errors must identify the forbidden capability:

```text
Extension image_tools.decode attempted to use disabled module 'io'.
```

---

## 16. Planner And Determinism Rules

Manifest metadata:

```toml
deterministic = true
stable = false
volatile = false
```

Only one volatility category should be allowed. The first slice can support
only `deterministic = true` and `deterministic = false`.

V1 planner rules:

- Lua functions may run in ordinary expression evaluation.
- Lua functions may run in `SELECT`, `WHERE`, projections, and DML expressions
  where the executor already evaluates scalar expressions.
- Lua functions are not allowed in expression indexes, generated columns, CHECK
  constraints, foreign-key actions, or persisted schema expressions until a
  later determinism, trust, and dependency-tracking ADR allows it.
- Lua table-valued function planner rules are deferred by ADR 0173. A later ADR
  must decide scan ownership, predicate pushdown, row limits, lateral behavior,
  and cancellation.
- Lua collation planner/index rules are deferred by ADR 0173. A later ADR must
  decide persistent index participation, rebuild semantics, and package
  dependency tracking.

Future planner metadata:

- estimated cost
- estimated row count for table-valued functions
- nullability
- monotonicity
- predicate pushdown support
- deterministic/pure guarantees strong enough for expression indexes

---

## 17. Error Handling

Bad error:

```text
Lua conversion failed.
```

Good errors:

```text
Extension text_tools.slugify returned INT64, but manifest declares return type TEXT.
```

```text
Extension billing.tax_amount returned FLOAT64 for DECIMAL result.
Use ddb.decimal("...") or return a ddb.Decimal value.
```

```text
Extension log_tools.parse_log row 14 column created_at returned TEXT, expected TIMESTAMP.
```

```text
Extension text_tools.normalize argument 1 expected UUID, received TEXT.
```

Runtime errors:

- become SQL errors
- include extension name and function name
- do not expose Rust panic payloads or host internals
- do not poison the process
- leave transaction state consistent with existing statement error semantics

---

## 18. CLI Surface

ADR 0172 accepts the CLI lifecycle surface below.

Recommended commands:

```bash
decentdb extension validate ./text_tools
decentdb extension test ./text_tools
decentdb extension install --db app.ddb ./text_tools
decentdb extension list --db app.ddb --format table
decentdb extension show --db app.ddb text_tools --format json
decentdb extension enable --db app.ddb text_tools
decentdb extension disable --db app.ddb text_tools
decentdb extension purge --db app.ddb text_tools --confirm
```

Execution commands must require explicit trust:

```bash
decentdb exec \
  --db app.ddb \
  --allow-extension text_tools@sha256:abc123 \
  --sql "SELECT slugify('Hello World')"
```

The REPL may support trusted extension activation:

```bash
decentdb repl --db app.ddb --allow-extension text_tools@sha256:abc123
```

The REPL must not provide a generic `.load` command in the first Lua extension
release.

---

## 19. Binding Surface

ADR 0172 accepts the binding and C ABI direction. The runtime lives in
DecentDB, not in each host binding.

Every binding should expose the same lifecycle shape:

```text
validate extension package
install extension package into database
list installed extensions
enable/disable extension
open connection with explicit extension allowlist
query normally
```

Rust shape:

```rust
db.extensions().install("./text_tools")?;
db.extensions().enable("text_tools")?;

let db = Db::open_with_config(path, config_with_extension_allowlist)?;
let rows = db.execute("SELECT slugify(title) FROM posts")?;
```

C ABI JSON bridge shape:

```c
ddb_extension_install_json(db, request_json, &response_json, &err);
ddb_extension_enable_json(db, request_json, &response_json, &err);
ddb_extension_list_json(db, request_json, &response_json, &err);
ddb_config_allow_extension(config, "text_tools", "sha256:abc123");
```

Bindings should wrap the C ABI rather than reimplement Lua behavior, manifest
parsing, package hashing, or trust policy.

---

## 20. Internal Catalog And Inspection

Initial internal catalog concepts:

```text
installed extension package
extension version
extension content hash
extension manifest
extension source files
enabled extension record
exported SQL function metadata
extension validation report
```

User-visible inspection:

```sql
SELECT * FROM sys.extensions;
SELECT * FROM sys.extension_functions;
SELECT * FROM sys.extension_validation;
```

Do not expose raw Lua source through `sys.*` by default. CLI/API can expose
source only through explicit administrative commands.

Internal extension tables must be filtered out of ordinary schema listings, like
sync metadata tables.

---

## 21. Implementation Slices

2.6.0 v1 implementation scope includes slices 1-4 plus the scalar-function
parts of slice 8. Slice 0 is complete at the design level because ADR
0169-0173 are accepted. Slices 5-7 are explicit post-v1 follow-ons.

### Slice 0: ADRs And Dependency Gate

Status: `DONE`

Deliverables:

- ADRs listed in this document accepted.
- Lua runtime crate/build choice accepted.
- sandbox capabilities documented.
- C ABI impact reviewed.
- wasm/browser impact documented.

Definition of done:

- no code dependency added before ADR approval
- threat model reviewed
- release packaging impact understood for Linux, macOS, Windows, and CI

### Slice 1: Manifest Validator And CLI Validation

Status: `TODO`

Deliverables:

- manifest parser
- package loader
- Lua entry file existence checks
- static manifest validation
- `decentdb extension validate`
- validation report output in JSON and table formats

Definition of done:

- invalid manifests produce precise errors
- package hash is stable and documented
- tests cover duplicate names, unknown types, invalid permissions, bad exports,
  and bad package layouts

### Slice 2: Sandboxed Scalar Functions With Simple Types

Status: `TODO`

Deliverables:

- Lua runtime creation
- restricted standard library
- scalar invocation from SQL
- simple type boundary: `NULL`, `BOOL`, `TEXT`, `INT64`, `FLOAT64`
- strict return validation
- error conversion
- CPU and memory limits

Definition of done:

- `SELECT slugify('Hello World')` works
- disabled modules cannot be used
- Lua errors become SQL errors
- timeout/cancellation path is covered
- extension calls cannot write to the database

### Slice 3: Typed Wrappers

Status: `TODO`

Deliverables:

- `ddb.Decimal`
- `ddb.UUID`
- `ddb.Date`
- `ddb.Timestamp`
- `ddb.Blob`
- `ddb.Json`
- constructors and type checks
- strict conversion errors

Definition of done:

- no lossy `FLOAT64 -> DECIMAL` conversion
- no implicit `TEXT -> UUID` conversion
- BLOB/TEXT confusion is rejected
- wrapper methods are covered by tests

### Slice 4: Install, Enable, Trust, And Inspection

Status: `TODO`

Deliverables:

- extension package install into database-owned catalog storage
- `CREATE EXTENSION`
- `DROP EXTENSION`
- connection-level allowlist
- CLI `--allow-extension`
- `sys.extensions`
- `sys.extension_functions`

Definition of done:

- opening an untrusted database does not execute extension code
- installed extension code is inert until enabled and allowed
- content hash mismatch blocks execution
- enabled extension survives reopen
- schema listings hide internal extension catalog objects

### Slice 5: Table-Valued Functions

Status: `DEFERRED BY ADR 0173`

Deliverables:

- manifest-declared static output schemas
- row validation
- row count limits
- integration with existing table-valued function executor path

Definition of done:

- table-valued functions work in `FROM`
- wrong column type errors name row and column
- row limit errors are clear
- dynamic schemas are rejected

### Slice 6: Aggregates

Status: `DEFERRED BY ADR 0173`

Deliverables:

- aggregate lifecycle functions
- typed aggregate state
- aggregate memory limits
- NULL handling

Definition of done:

- aggregate state cannot access engine internals
- aggregate errors leave statement/transaction state coherent
- memory limit behavior is tested

### Slice 7: Collations

Status: `DEFERRED BY ADR 0173`

Deliverables:

- Lua-backed TEXT collation registration
- deterministic comparison contract
- no index persistence until ADR permits it

Definition of done:

- comparison return values are validated
- nondeterministic capability use is blocked
- collation behavior is documented as scan/sort-only until index semantics exist

### Slice 8: Binding And Documentation Polish

Status: `TODO`

Deliverables:

- Rust API
- C ABI
- .NET wrapper
- Python wrapper
- CLI reference
- user guide
- extension authoring guide
- examples

Definition of done:

- at least one full extension package example ships in docs/examples
- binding smoke tests install and invoke a scalar function
- docs explain trust and sandbox policy clearly

---

## 22. Testing Strategy

Required test categories:

- manifest validation
- package hashing
- sandbox module denial
- resource limits
- scalar invocation
- type conversion
- strict return validation
- NULL handling
- SQL error messages
- transaction behavior after extension failure
- install/enable/drop/purge lifecycle
- reopen persistence
- allowlist enforcement
- CLI command behavior
- C ABI panic safety
- binding smoke tests

Crash/fault tests:

- extension error during statement evaluation
- extension timeout during statement evaluation
- extension memory-limit failure
- database reopen after extension install but before enable
- database reopen after enable
- corrupted installed package metadata

Security tests:

- `io.open` denied
- `os.execute` denied
- `package.loadlib` denied
- `debug` denied
- native module loading denied
- content hash mismatch denied
- unallowed extension invocation denied

---

## 23. Example Complete Extension

`decentdb-extension.toml`:

```toml
name = "text_tools"
version = "1.0.0"
language = "lua"
api_version = 1
entry = "main.lua"
strict_types = true

[permissions]
filesystem = false
network = false
process = false
database_read = false
database_write = false
native_modules = false
clock = false
random = false

[[functions]]
name = "slugify"
export = "slugify"
kind = "scalar"
args = ["TEXT"]
returns = "TEXT"
deterministic = true
null_handling = "returns_null"

[[functions]]
name = "score_email"
export = "score_email"
kind = "scalar"
args = ["TEXT", "TEXT"]
returns = "INT64"
deterministic = true
null_handling = "called_on_null"

[[functions]]
name = "tax_amount"
export = "tax_amount"
kind = "scalar"
args = ["DECIMAL", "DECIMAL"]
returns = "DECIMAL"
deterministic = true
null_handling = "returns_null"
```

`main.lua`:

```lua
local M = {}

function M.slugify(value)
  value = string.lower(value)
  value = string.gsub(value, "[^a-z0-9]+", "-")
  value = string.gsub(value, "^-+", "")
  value = string.gsub(value, "-+$", "")
  return value
end

function M.score_email(subject, body)
  local score = 0
  local text = string.lower((subject or "") .. " " .. (body or ""))

  if string.find(text, "urgent") then score = score + 10 end
  if string.find(text, "invoice") then score = score + 5 end
  if string.find(text, "unsubscribe") then score = score - 5 end

  return score
end

function M.tax_amount(amount, rate)
  return amount:mul(rate)
end

return M
```

SQL:

```sql
CREATE EXTENSION text_tools;

SELECT slugify(title) FROM posts;
SELECT score_email(subject, body) FROM messages;
SELECT tax_amount(subtotal, tax_rate) FROM invoices;
```

---

## 24. Resolved Decisions And Remaining Follow-Ups

The original open questions for this spec are resolved for the 2.6.0 v1
implementation by ADR 0169-0173.

Resolved decisions:

1. Runtime binding and build flags: ADR 0169 selects Lua 5.4 through `mlua`
   with vendored Lua for native builds.
2. Release packaging: ADR 0169 includes Lua extension support in official
   native 2.6.0 artifacts by default, preserves a no-Lua build path for
   embedders, and makes wasm/browser execution unsupported with stable errors
   until a later browser ADR.
3. Package persistence: ADR 0170 stores canonical manifest/source/hash metadata
   in database-owned internal catalog storage in the main database file. No
   sidecar source store is used in v1.
4. SQL syntax and trust: ADR 0170 makes `CREATE EXTENSION name` transactional
   enablement for an already installed package. SQL does not load extension
   packages from filesystem paths, and execution still requires a connection
   name/hash allowlist.
5. Lifecycle split: ADR 0170 keeps package install and purge as administrative
   CLI/API operations while `CREATE EXTENSION` and `DROP EXTENSION` handle
   database enablement.
6. Bundle interaction: ADR 0170 keeps package content in the database-owned
   catalog so future bundle/support-artifact work can include extension records
   through existing database inspection rather than chasing sidecars.
7. Persisted schema expressions: ADR 0171 and ADR 0173 reject Lua functions in
   generated columns, CHECK constraints, DEFAULT expressions, expression
   indexes, partial indexes, and other persisted schema expressions in v1.
8. WASM/browser behavior: ADR 0169 defers browser Lua execution and requires
   stable unsupported-capability behavior.
9. C ABI minimum surface: ADR 0172 accepts JSON lifecycle entry points plus
   connection allowlist configuration as the v1 ABI baseline.
10. Signatures: ADR 0170 defers signed packages and uses exact content hashes
    plus explicit allowlists as the v1 trust mechanism.

Remaining follow-ups after v1:

- table-valued functions, including row streaming/materialization, lateral
  behavior, cancellation, and row limits
- aggregate functions, including aggregate state representation, memory limits,
  and error behavior
- Lua-backed collations, including persisted index participation and rebuild
  semantics
- persisted schema expression support with extension dependency tracking
- browser/wasm Lua execution and package delivery
- signed package manifests and bundle-level trust chains
- typed C ABI structs after the JSON bridge has stabilized

---

## 25. References

- Lua 5.4 Reference Manual: https://www.lua.org/manual/5.4/
- `design/adr/0111-table-valued-functions.md`
- `design/adr/0118-rust-ffi-panic-safety.md`
- `design/adr/0169-lua-extension-runtime-dependency-and-sandbox.md`
- `design/adr/0170-lua-extension-package-catalog-and-trust.md`
- `design/adr/0171-lua-extension-sql-type-and-planner-contract.md`
- `design/adr/0172-lua-extension-cli-c-abi-and-binding-contract.md`
- `design/adr/0173-lua-extension-function-kind-phasing.md`
- `design/WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md`
- `design/FUTURE_WINS.md`
