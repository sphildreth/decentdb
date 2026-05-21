# Lua Extensions

DecentDB 2.6.0 includes a sandboxed Lua extension model for adding SQL-visible
behavior without loading arbitrary native code.

Extensions are ordinary package directories with a `decentdb-extension.toml`
manifest and Lua source. Installing a package stores a canonical copy in the
database-owned internal catalog. Enabling the package makes its declared SQL
objects visible to the database, but code still does not run unless the current
connection explicitly trusts the package name and exact content hash.

This gives extension authors one portable package model and gives applications a
clear security boundary:

- install packages through CLI, Rust, or C ABI lifecycle APIs
- enable or disable installed packages through SQL or lifecycle APIs
- allow execution per connection by package name plus `sha256:<hash>`
- validate signed packages with Ed25519 trust anchors
- use an explicit unsigned-development override only for local development
- run Lua inside a restricted runtime with CPU, memory, row, string, BLOB,
  aggregate-state, and collation-comparison limits

DecentDB does not support SQLite-style `.load`, native modules, filesystem
access, network access, process execution, direct database handles inside Lua,
or database writes from extension code.

## Package Layout

The repository includes a complete example package at
`docs/examples/lua/text_tools`. The snippets in this section are intentionally
complete enough to copy into a new package and adapt.

```text
text_tools/
  decentdb-extension.toml
  main.lua
  tests/
    behavior.sql
  README.md
```

`decentdb-extension.toml` is the contract. Lua source cannot add SQL-visible
functions dynamically; every exported scalar, table-valued function, aggregate,
and collation must be declared in the manifest.

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

[runtime]
max_steps = 100000
max_memory_bytes = 1048576
max_string_bytes = 1048576
max_blob_bytes = 1048576
max_rows = 10000
max_row_bytes = 65536
max_aggregate_state_bytes = 1048576
max_collation_steps = 10000

[[functions]]
name = "slugify"
export = "slugify"
kind = "scalar"
args = ["TEXT"]
returns = "TEXT"
deterministic = true
null_handling = "returns_null"

[[functions]]
name = "split_words"
export = "split_words"
kind = "table"
args = ["TEXT"]

[[functions.columns]]
name = "word"
type = "TEXT"

[[functions]]
name = "lua_sum"
kind = "aggregate"
args = ["INT64"]
returns = "INT64"
step = "lua_sum_step"
finalize = "lua_sum_final"
null_handling = "returns_null"

[[functions]]
name = "reverse_text"
export = "reverse_text"
kind = "collation"
deterministic = true
```

Supported manifest function kinds are:

| Kind | SQL surface |
|---|---|
| `scalar` | `SELECT slugify(title) FROM posts` |
| `table` | `SELECT word FROM split_words(body)` |
| `aggregate` | `SELECT lua_sum(amount) FROM invoices` |
| `collation` | `ORDER BY title COLLATE reverse_text` |

## Lua Entry Module

The entry file returns a table containing the manifest-declared exports.

```lua
local M = {}

function M.slugify(value)
  value = string.lower(value)
  value = string.gsub(value, "[^a-z0-9]+", "-")
  value = string.gsub(value, "^-+", "")
  value = string.gsub(value, "-+$", "")
  return value
end

function M.split_words(value)
  local rows = {}
  for word in string.gmatch(value or "", "%S+") do
    table.insert(rows, { word = word })
  end
  return rows
end

function M.lua_sum_step(state, value)
  return (state or 0) + (value or 0)
end

function M.lua_sum_final(state)
  return state or 0
end

function M.reverse_text(left, right)
  if left == right then return 0 end
  if left > right then return -1 end
  return 1
end

return M
```

The sandbox exposes `string`, `table`, deterministic `math`, `utf8`, and a
small `ddb` namespace for strict typed wrappers. Denied Lua libraries include
`io`, `os`, `debug`, unrestricted `require`, `dofile`, `loadfile`, and native
module loading.

## Behavior Test Example

`decentdb extension test` looks for `tests/behavior.sql` in the package
directory. Use that file for smoke tests that exercise every exported SQL
object before installing the package into an application database.

```sql
CREATE TABLE words(name TEXT);
INSERT INTO words VALUES ('hello'), ('decent'), ('database');

SELECT slugify('Hello, DecentDB');
SELECT word FROM split_words('a bb c');
SELECT lua_sum(length(name)) FROM words;
SELECT name FROM words ORDER BY name COLLATE reverse_text;
```

Run the test package in unsigned development mode while authoring it:

```bash
decentdb extension test docs/examples/lua/text_tools --allow-unsigned
```

## Type Boundary

DecentDB owns the SQL type system. Lua receives only values compatible with the
manifest signature and Lua results are converted back through the declared
return type.

| Manifest type | Lua representation |
|---|---|
| `NULL` | `nil` |
| `BOOL` | boolean |
| `TEXT` | string |
| `INT64` | integer |
| `FLOAT64` | number |
| `DECIMAL` | `ddb.decimal(...)` wrapper |
| `UUID` | `ddb.uuid(...)` wrapper |
| `DATE` | `ddb.date(...)` wrapper |
| `TIMESTAMP` | `ddb.timestamp(...)` wrapper |
| `BLOB` | `ddb.blob(...)` wrapper |
| `JSON` | string or `ddb.json(...)` wrapper containing valid JSON |

Scalar examples:

```toml
[[functions]]
name = "safe_divide"
export = "safe_divide"
kind = "scalar"
args = ["FLOAT64", "FLOAT64"]
returns = "FLOAT64"
deterministic = true
null_handling = "called_on_null"

[[functions]]
name = "invoice_total"
export = "invoice_total"
kind = "scalar"
args = ["DECIMAL", "DECIMAL"]
returns = "DECIMAL"
deterministic = true
null_handling = "rejects_null"

[[functions]]
name = "event_payload"
export = "event_payload"
kind = "scalar"
args = ["TEXT", "INT64"]
returns = "JSON"
deterministic = true
```

```lua
function M.safe_divide(left, right)
  if left == nil or right == nil or right == 0 then
    return nil
  end
  return left / right
end

function M.invoice_total(subtotal, tax)
  return subtotal:add(tax)
end

function M.event_payload(kind, count)
  return ddb.json(string.format('{"kind":"%s","count":%d}', kind, count))
end
```

`null_handling = "returns_null"` skips scalar Lua execution when any argument
is `NULL` and returns SQL `NULL`. `null_handling = "called_on_null"` passes Lua
`nil`. `null_handling = "rejects_null"` raises a SQL error before Lua runs.

Table-valued functions return an array-like Lua table of row tables. Every
output column must be declared statically in the manifest.

```toml
[[functions]]
name = "kv_pairs"
export = "kv_pairs"
kind = "table"
args = ["TEXT"]

[[functions.columns]]
name = "key"
type = "TEXT"

[[functions.columns]]
name = "value"
type = "TEXT"
```

```lua
function M.kv_pairs(input)
  local rows = {}
  for item in string.gmatch(input or "", "[^,]+") do
    local key, value = string.match(item, "^%s*([^=]+)=([^=]+)%s*$")
    if key ~= nil then
      table.insert(rows, { key = key, value = value })
    end
  end
  return rows
end
```

Aggregate functions declare separate step and finalize exports. The state is a
Lua value owned by the extension runtime and is checked against
`max_aggregate_state_bytes`.

```toml
[[functions]]
name = "lua_avg"
kind = "aggregate"
args = ["FLOAT64"]
returns = "FLOAT64"
step = "lua_avg_step"
finalize = "lua_avg_final"
null_handling = "called_on_null"
```

```lua
function M.lua_avg_step(state, value)
  state = state or { sum = 0.0, count = 0 }
  if value ~= nil then
    state.sum = state.sum + value
    state.count = state.count + 1
  end
  return state
end

function M.lua_avg_final(state)
  if state == nil or state.count == 0 then
    return nil
  end
  return state.sum / state.count
end
```

Collations receive two text values and must return `-1`, `0`, or `1`.

```toml
[[functions]]
name = "length_then_text"
export = "length_then_text"
kind = "collation"
deterministic = true
```

```lua
function M.length_then_text(left, right)
  if #left < #right then return -1 end
  if #left > #right then return 1 end
  if left < right then return -1 end
  if left > right then return 1 end
  return 0
end
```

## CLI Lifecycle

Validate a local package:

```bash
decentdb extension validate ./text_tools --allow-unsigned
```

Install and enable it:

```bash
decentdb extension install --db app.ddb ./text_tools --allow-unsigned
decentdb extension enable --db app.ddb text_tools
```

The install output includes a stable content hash such as
`sha256:7b3f...`. Use that exact hash when opening a connection that should run
the extension:

```bash
decentdb exec \
  --db app.ddb \
  --allow-extension text_tools@sha256:7b3f... \
  --sql "SELECT slugify('Hello, DecentDB')"
```

For local package development only, `--allow-unsigned-extensions` allows the
current connection to execute installed unsigned packages without a hash
allowlist. Do not use it for untrusted databases or production applications.

Other lifecycle commands:

```bash
decentdb extension test ./text_tools --allow-unsigned
decentdb extension list --db app.ddb
decentdb extension show --db app.ddb text_tools --format json
decentdb extension disable --db app.ddb text_tools
decentdb extension purge --db app.ddb text_tools --confirm
decentdb extension dependencies --db app.ddb
decentdb extension rebuild --db app.ddb text_tools
```

JSON output is useful for packaging automation:

```bash
decentdb extension validate ./text_tools --allow-unsigned --format json
decentdb extension show --db app.ddb text_tools --format json
decentdb extension list --db app.ddb --format json
```

Production connections should prefer explicit content-hash allowlists:

```bash
HASH="sha256:7b3f..."
decentdb exec \
  --db app.ddb \
  --allow-extension "text_tools@${HASH}" \
  --sql "SELECT slugify(title) FROM posts"
```

If a package is signed, validation and installation can also require an
Ed25519 public key for the exact package hash and key id:

```bash
decentdb extension validate ./text_tools \
  --trust-extension "text_tools@sha256:7b3f...@release-2026-05@base64:PUBLIC_KEY"

decentdb extension install --db app.ddb ./text_tools \
  --trust-extension "text_tools@sha256:7b3f...@release-2026-05@base64:PUBLIC_KEY"
```

## SQL Surface

Enable or disable an already installed package:

```sql
CREATE EXTENSION text_tools;
DROP EXTENSION text_tools;
ALTER EXTENSION text_tools ENABLE;
ALTER EXTENSION text_tools DISABLE;
```

Inspect installed packages and SQL-visible extension objects:

```sql
SELECT * FROM sys.extensions;
SELECT * FROM sys.extension_functions;
SELECT * FROM sys.extension_collations;
SELECT * FROM sys.extension_dependencies;
SELECT * FROM sys.extension_validation;
```

Call scalar functions like built-ins:

```sql
SELECT slugify('Hello, DecentDB') AS slug;
SELECT slugify(title) FROM posts WHERE title IS NOT NULL;
```

Use table-valued functions in `FROM`:

```sql
SELECT word
FROM split_words('fast durable embedded database')
ORDER BY word;
```

Use aggregates in grouped or ungrouped aggregate queries:

```sql
SELECT lua_sum(amount_cents) FROM invoice_lines;

SELECT customer_id, lua_sum(amount_cents)
FROM invoice_lines
GROUP BY customer_id;
```

Use extension collations only at query time:

```sql
SELECT name
FROM words
ORDER BY name COLLATE reverse_text;
```

The following persistent-collation forms are intentionally rejected in 2.6.0:

```sql
CREATE TABLE words(name TEXT COLLATE reverse_text);
CREATE INDEX words_name_reverse ON words(name COLLATE reverse_text);
```

Internal extension catalog tables are hidden from ordinary schema listings.

## Rust API

```rust
use decentdb::{
    Db, DbConfig, ExtensionTrustAnchor, ExtensionValidationOptions,
};

let report = decentdb::validate_extension_package(
    "./text_tools",
    ExtensionValidationOptions::unsigned_development(),
)?;
let hash = report.content_hash.expect("validated package hash");

let mut config = DbConfig::default();
config.extension_trust_anchors.push(ExtensionTrustAnchor::new(
    "text_tools",
    hash,
));
let db = Db::open_or_create("app.ddb", config)?;
db.extensions().install_with_options(
    "./text_tools",
    ExtensionValidationOptions::unsigned_development(),
)?;
db.extensions().enable("text_tools")?;
# Ok::<(), decentdb::DbError>(())
```

List installed packages and inspect dependencies:

```rust
let installed = db.extensions().list()?;
for package in installed {
    println!(
        "{} {} {} enabled={}",
        package.name, package.version, package.content_hash, package.enabled
    );
}

for dependency in db.extensions().dependencies()? {
    println!(
        "{} {} depends on {}",
        dependency.object_kind, dependency.object_name, dependency.extension_name
    );
}
# Ok::<(), decentdb::DbError>(())
```

## C ABI

The C ABI exposes lifecycle functions as JSON bridges:

```c
char *json = NULL;
ddb_extension_validate_json(
  "{\"path\":\"./text_tools\",\"allow_unsigned\":true}",
  &json);
ddb_string_free(&json);
```

Install, enable, list, and purge through the same JSON bridge pattern:

```c
char *json = NULL;

ddb_extension_install_json(
  db,
  "{\"path\":\"./text_tools\",\"allow_unsigned\":true}",
  &json);
ddb_string_free(&json);

ddb_extension_enable_json(db, "{\"name\":\"text_tools\"}", &json);
ddb_string_free(&json);

ddb_extension_list_json(db, "{}", &json);
ddb_string_free(&json);

ddb_extension_purge_json(
  db,
  "{\"name\":\"text_tools\",\"confirm\":true}",
  &json);
ddb_string_free(&json);
```

Open-time trust is supplied through `ddb_db_open_with_options`,
`ddb_db_create_with_options`, or `ddb_db_open_or_create_with_options`:

```c
ddb_db_t *db = NULL;
ddb_db_open_or_create_with_options(
  "app.ddb",
  "allow_extension=text_tools@sha256:7b3f...",
  &db);
```

## Current Boundaries

Lua extensions in 2.6.0 are enabled by default for native builds. Embedders can
build without Lua support by disabling the `lua-extensions` cargo feature; in
that build, package lifecycle APIs remain available but SQL execution of Lua
objects returns an explicit unsupported-runtime error.

Browser/WASM artifacts keep the same package catalog and trust model, but do
not execute Lua in 2.6.0. This avoids shipping a second, less-audited browser
runtime behind the same trust contract. Applications that need browser-side
extension execution should treat that as a separate target-support decision.

Lua collations work for query-time comparisons and ordering. Persistent column
collations and persisted index collations remain rejected because DecentDB's
storage/index metadata does not yet persist collation semantics in a way that
can safely make an index depend on executable package code.
