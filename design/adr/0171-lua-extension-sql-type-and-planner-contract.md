# ADR 0171: Lua Extension SQL, Type, And Planner Contract
**Date:** 2026-05-21
**Status:** Accepted

## Context

Lua extension functions are SQL-visible behavior. DecentDB must decide how SQL
resolves extension functions, how values cross the Lua boundary, where Lua is
allowed to execute, and how planner-visible determinism is interpreted.

The extension language must not take ownership of DecentDB's type system.

## Decision

The Lua extension runtime supports **manifest-declared SQL extension objects**
with strict DecentDB-owned type validation:

- scalar functions;
- table-valued functions;
- aggregate functions;
- collations;
- deterministic metadata for dependency inspection and future persisted-schema
  eligibility.

### 1. SQL-visible registration

Extension functions become visible only when all conditions are true:

1. the package is installed;
2. the package is enabled through `CREATE EXTENSION`;
3. the current connection allowlist accepts the package name and content hash;
4. the manifest declares the function signature;
5. the function or collation kind is declared in the manifest.

Scalar functions are invoked with ordinary SQL function syntax:

```sql
SELECT slugify(title) FROM posts;
```

Table-valued functions are invoked in `FROM`:

```sql
SELECT *
FROM parse_log_blob(payload);
```

Aggregate functions are invoked through ordinary aggregate syntax:

```sql
SELECT account_id, risk_score(events)
FROM account_events
GROUP BY account_id;
```

Lua collations are referenced through ordinary collation syntax:

```sql
SELECT title FROM posts ORDER BY title COLLATE natural_title;
```

Function and collation namespace collisions with built-ins are rejected at
extension enable time. Extension objects must not override built-ins.

### 2. Manifest-declared signatures

Every function declares:

- SQL function name;
- Lua export name;
- kind;
- ordered argument types;
- return type;
- determinism metadata;
- NULL handling mode;
- runtime limits or package defaults.

Table-valued functions also declare:

- static output column names;
- static output column types;
- row and row-byte limits;
- streaming/materialization policy hints.

Aggregate functions also declare:

- Lua step export;
- Lua final export;
- aggregate state type;
- aggregate state memory budget;
- NULL handling for step/final behavior.

Collations declare:

- collation name;
- Lua comparison export;
- deterministic flag;
- version metadata;
- comparison resource budget.

No runtime-discovered SQL signatures are allowed. Lua does not inspect values to
choose overloads. DecentDB resolves the SQL call before Lua execution.

### 3. Type boundary

DecentDB values convert to Lua-safe primitives or wrappers and back to DecentDB
values. Strict type mode is mandatory.

Primitive values:

- `NULL`;
- `BOOL`;
- `TEXT`;
- `INT64`;
- `FLOAT64`.

Typed wrappers are required for:

- `DECIMAL`;
- `UUID`;
- `DATE`;
- `TIMESTAMP`;
- `BLOB`;
- JSON text as `ddb.Json`.

The following implicit conversions are rejected:

- `TEXT -> INT64`;
- `TEXT -> UUID`;
- `FLOAT64 -> DECIMAL`;
- `BLOB -> TEXT`;
- `TEXT -> BLOB`;
- timezone-changing timestamp coercions;
- any dynamic return schema.

### 4. NULL handling

The manifest supports:

```toml
null_handling = "returns_null"
null_handling = "called_on_null"
null_handling = "rejects_null"
```

`returns_null` is the default for scalar functions and skips Lua execution when
any argument is `NULL`. `called_on_null` passes nulls through the DecentDB Lua
wrapper boundary. `rejects_null` returns a SQL error before calling Lua.

### 5. Planner and persisted-expression rules

Lua scalar functions may execute in ordinary expression evaluation:

- `SELECT` lists;
- `WHERE`;
- `ORDER BY`;
- `HAVING`;
- DML expressions where scalar functions are already evaluated.

Lua table-valued functions may execute as scan sources in `FROM`. The planner
may estimate table-valued function cost and row counts from manifest metadata
and observed runtime statistics. Predicate pushdown into Lua is not required.

Lua aggregate functions may execute in grouped aggregate plans. Aggregate state
is owned by the extension runtime boundary and accounted against explicit
memory limits.

Lua collations execute in query-time sort/comparison. Persistent column
collations and persisted index collations remain rejected in 2.6.0 because the
current storage/index metadata does not persist collation semantics in a way
that can safely make a B+Tree key depend on executable package code.

Deterministic Lua scalar functions execute in ordinary runtime expression
contexts. Persisted schema expressions that would store Lua-dependent values or
index keys remain rejected unless a later ADR adds exact persisted dependency
metadata to the relevant schema object and proves rebuild/reopen semantics for
that object kind.

The extension dependency catalog and rebuild-reporting APIs are included for
inspection and future persisted-object compatibility. They do not make current
persistent indexes, generated columns, CHECK constraints, DEFAULT expressions,
partial-index predicates, or view definitions depend on Lua code.

### 6. Determinism metadata

The runtime accepts explicit volatility metadata:

```toml
deterministic = true
stable = false
volatile = false
```

Only one volatility category may be true. The planner may use deterministic
metadata for diagnostics and cost estimates. Persisted expression and persisted
collation-index eligibility is not granted in 2.6.0.

### 7. Error behavior

Type mismatch, missing function, missing collation, missing package trust, Lua
runtime errors, row conversion errors, aggregate state errors, collation return
errors, and return conversion errors are SQL errors naming the extension and
object. Panic payloads and host internals are not exposed.

## Rationale

Manifest-declared SQL objects keep DecentDB, not Lua, in charge of typing,
planning, dependency tracking, and persistence. Strict typing preserves
DecentDB's cross-binding behavior and prevents Lua from introducing lossy
coercions that would be hard to debug.

Keeping persisted schema/index use out of the initial execution contract keeps
DecentDB from storing values or B+Tree keys whose meaning depends on executable
code that may be disabled, untrusted, or upgraded at reopen. The extension
catalog still records the dependency model needed for administrative inspection
and future persisted-object work.

## Consequences

- The feature covers scalar, table-valued, aggregate, and query-time collation
  use.
- Function name collision rules are conservative.
- The type wrapper API must be implemented before rich DecentDB types are safe
  across Lua.
- Persistent schema objects and persistent collation indexes do not execute Lua
  in 2.6.0.

## Alternatives Considered

1. **Let Lua functions dynamically accept/return any value.** Rejected because
   it breaks DecentDB's typed SQL and binding contracts.
2. **Allow deterministic Lua functions in persisted indexes and schema
   expressions immediately.** Rejected because the current catalog and index
   metadata cannot yet make stored values depend on executable package code
   without a broader persistence decision.
3. **Allow persisted Lua use without dependency metadata.** Rejected because
   package upgrades, trust changes, and index rebuilds need explicit dependency
   semantics.
4. **Namespace all extension functions as `extension.function`.** Rejected for
   the initial complete model. Ordinary function syntax is simpler; collisions
   are rejected.
5. **Expose database handles to Lua for richer functions.** Rejected because it
   would break transaction, mutation, and sandbox boundaries.

## Validation Requirements

Implementation is not complete until tests cover:

- scalar invocation in projections and filters;
- table-valued invocation in `FROM`;
- aggregate invocation in grouped plans;
- Lua collation invocation in query-time sorts;
- query-time Lua collation sorting and comparison;
- manifest overload resolution;
- unknown function and ambiguous function errors;
- built-in name collision rejection;
- primitive type conversions;
- typed wrapper conversions;
- strict return validation;
- all NULL handling modes;
- persisted column/index collations and persisted Lua schema expressions are
  rejected with explicit errors;
- extension errors leave statement and transaction state coherent.

## References

- `design/FUTURE_WINS.md`
- `design/_archive/WIN_LUA_EXTENSION_RUNTIME_SPEC.md`
- `design/adr/0111-table-valued-functions.md`
