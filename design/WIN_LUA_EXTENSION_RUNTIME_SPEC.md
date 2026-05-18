# Lua Extension Runtime And Package Model

**Date:** 2026-05-18  
**Status:** Proposed  
**Roadmap:** [`FUTURE_WINS.md`](FUTURE_WINS.md)  
**Audience:** Core engine developers, SQL planner/executor maintainers, C ABI maintainers, binding maintainers, CLI maintainers, documentation authors, coding agents  
**Related inputs:** Lua 5.4 Reference Manual, `design/FUTURE_WINS.md`, `design/WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md`, `design/adr/0111-table-valued-functions.md`, `design/adr/0118-rust-ffi-panic-safety.md`

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
SELECT * FROM parse_log_blob(payload);
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

## 5. Required ADRs

This feature requires ADRs before implementation because it introduces a major
runtime dependency, a new SQL-visible extension surface, C ABI impact, sandbox
rules, and catalog/trust policy.

Required ADRs:

1. Lua runtime dependency and build strategy.
2. Extension package manifest and versioning.
3. Extension trust, allowlist, and activation policy.
4. Extension catalog and persistence model.
5. SQL function registration and planner contract.
6. Lua type boundary and strict return validation.
7. Sandbox and resource-limit contract.
8. C ABI and binding lifecycle APIs.
9. Table-valued function streaming and ownership model before table functions.
10. Collation/index interaction before Lua-backed collations.

Recommended dependency direction:

- Use Lua 5.4 semantics as the language target.
- Prefer a vendored Lua build through a Rust Lua binding crate so official
  release artifacts do not require a system Lua installation.
- Hide the selected Rust crate behind DecentDB-owned extension runtime traits so
  public DecentDB APIs do not expose third-party runtime types.

The ADR must make the exact crate and feature decision. That decision is not
made by this document.

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

The ADR must choose the exact persistence storage, but the recommended model is:

- `extension install` stores a canonical manifest, Lua source, and content hash
  in DecentDB-owned internal catalog storage.
- installed extension code is inert data until enabled and allowed.
- `CREATE EXTENSION` creates a database-level enablement record, not a trust
  grant for every connection.
- the connection or CLI invocation still decides whether Lua execution is
  allowed.
- `DROP EXTENSION` disables the SQL-visible extension objects and optionally
  removes installed package content with an explicit purge command.

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

Table-valued functions, after the table-function slice:

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

Aggregate functions are later than scalar and table-valued functions.

Rules:

- typed input
- typed state
- explicit state memory limit
- deterministic finalization rules
- no database access during step/final

### Collations

Lua-backed collations are later than scalar and table-valued functions because
they can affect indexes and persistent ordering.

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

Required resource limits:

- instruction/step limit
- memory allocation limit
- maximum returned string size
- maximum returned BLOB size
- maximum table-valued rows
- maximum aggregate state size
- recursion depth limit
- cancellation check integration

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
  determinism and trust ADR allows it.
- Lua table-valued functions are scan sources only; no predicate pushdown in the
  first table-function slice.
- Lua collations do not participate in persisted indexes until collation
  persistence and rebuild semantics are accepted.

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

The runtime lives in DecentDB, not in each host binding.

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

C ABI shape:

```c
ddb_extension_install(db, "./text_tools", &err);
ddb_extension_enable(db, "text_tools", &err);
ddb_config_allow_extension(config, "text_tools", "sha256:abc123");
```

Bindings should wrap the C ABI rather than reimplement Lua behavior.

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

### Slice 0: ADRs And Dependency Gate

Status: `TODO`

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

Status: `TODO`

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

Status: `TODO`

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

Status: `TODO`

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

## 24. Open Questions

1. Which Rust Lua binding and build flags should DecentDB use?
2. Should official builds always include Lua extension support, or should it be
   a cargo feature plus release-build variant?
3. Should installed extension source live inside the main database file, a
   sidecar store, or both?
4. What is the exact SQL syntax for `CREATE EXTENSION` version/hash selection?
5. Should `CREATE EXTENSION` be transactional with package enablement only, or
   should package installation also be available through SQL?
6. How should extension lifecycle interact with application database bundles?
7. Should deterministic Lua functions ever be allowed in generated columns,
   CHECK constraints, or expression indexes?
8. How should Lua extension execution work in future WASM/browser builds?
9. What is the minimum C ABI surface required before higher-level bindings can
   expose extension lifecycle APIs?
10. Should extension packages support signed manifests in v1 or defer signing to
    the application bundle format?

---

## 25. References

- Lua 5.4 Reference Manual: https://www.lua.org/manual/5.4/
- `design/adr/0111-table-valued-functions.md`
- `design/adr/0118-rust-ffi-panic-safety.md`
- `design/WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md`
- `design/FUTURE_WINS.md`
