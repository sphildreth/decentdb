# ADR 0171: Lua Extension SQL, Type, And Planner Contract
**Date:** 2026-05-21
**Status:** Accepted

## Context

Lua extension functions are SQL-visible behavior. DecentDB must decide how SQL
resolves extension functions, how values cross the Lua boundary, where Lua is
allowed to execute, and how planner-visible determinism is interpreted.

The extension language must not take ownership of DecentDB's type system.

## Decision

The first Lua extension runtime supports **manifest-declared scalar functions**
with strict DecentDB-owned type validation.

### 1. SQL-visible registration

Extension functions become visible only when all conditions are true:

1. the package is installed;
2. the package is enabled through `CREATE EXTENSION`;
3. the current connection allowlist accepts the package name and content hash;
4. the manifest declares the function signature;
5. the function kind is supported by the current runtime.

Functions are invoked with ordinary SQL function syntax:

```sql
SELECT slugify(title) FROM posts;
```

The function namespace is the ordinary scalar-function namespace in v1. Name
collisions with built-in functions are rejected at extension enable time unless
a later ADR defines namespacing or override rules.

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

No runtime-discovered SQL signatures are allowed. Lua does not inspect values to
choose overloads. DecentDB resolves the SQL call before Lua execution.

### 3. Type boundary

DecentDB values convert to Lua-safe primitives or wrappers and back to DecentDB
values. Strict type mode is mandatory.

Primitive v1 values:

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

Lua functions are not allowed in persisted schema expressions in the first
runtime:

- expression indexes;
- generated columns;
- CHECK constraints;
- DEFAULT expressions;
- foreign-key actions;
- partial-index predicates;
- view definitions stored without extension dependency metadata.

This restriction holds even when a manifest marks a function deterministic.
Persisted expression support requires a later ADR covering trust, dependency
tracking, dump/reopen behavior, index rebuilds, and extension upgrades.

### 6. Determinism metadata

The first runtime accepts `deterministic = true` or `deterministic = false`.
The planner may use deterministic metadata only for diagnostics and future
costing. It must not use Lua determinism to persist results, fold constants in
schema objects, or build indexes in v1.

### 7. Error behavior

Type mismatch, missing function, missing package trust, Lua runtime errors, and
return conversion errors are SQL errors naming the extension and function.
Panic payloads and host internals are not exposed.

## Rationale

Manifest-declared scalar functions make the first runtime useful while keeping
the planner and storage contracts conservative. Strict typing preserves
DecentDB's cross-binding behavior and prevents Lua from introducing lossy
coercions that would be hard to debug.

Forbidding persisted schema expressions avoids the hardest upgrade/reopen/index
questions in the first implementation.

## Consequences

- The first feature is scalar-function focused.
- Extension functions are unavailable for schema-level constraints and indexes.
- Function name collision rules are conservative.
- The type wrapper API must be implemented before rich DecentDB types are safe
  across Lua.
- Future persisted-expression support will need extension dependency metadata.

## Alternatives Considered

1. **Let Lua functions dynamically accept/return any value.** Rejected because
   it breaks DecentDB's typed SQL and binding contracts.
2. **Allow deterministic Lua functions in indexes immediately.** Rejected
   because package upgrades, trust changes, and index rebuilds need explicit
   dependency semantics.
3. **Namespace all extension functions as `extension.function`.** Deferred.
   Ordinary function syntax is simpler for v1; collisions are rejected.
4. **Expose database handles to Lua for richer functions.** Rejected because it
   would break transaction, mutation, and sandbox boundaries.

## Validation Requirements

Implementation is not complete until tests cover:

- scalar invocation in projections and filters;
- manifest overload resolution;
- unknown function and ambiguous function errors;
- built-in name collision rejection;
- primitive type conversions;
- typed wrapper conversions;
- strict return validation;
- all NULL handling modes;
- use in persisted schema expressions is rejected;
- deterministic metadata is parsed but not used for index/schema persistence;
- extension errors leave statement and transaction state coherent.

## References

- `design/FUTURE_WINS.md`
- `design/WIN_LUA_EXTENSION_RUNTIME_SPEC.md`
- `design/adr/0111-table-valued-functions.md`
