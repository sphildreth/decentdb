# Rust Bindings Migration Plan

## Decision

Use `bindings` as the umbrella term for DecentDB's cross-language integration
surface.

Do not rename the repo-level umbrella to `drivers`. `driver` is only correct for
some packages, such as JDBC, Python DB-API, or Go `database/sql`.

## Scope

This plan covers migration of packaged language integrations from the old Nim-era
repository shape to the Rust engine.

This plan does not include Nim as a supported language target. The Nim repository
is reference material only.

## Terminology

At the repo level:

- `bindings/` is the umbrella directory for shipped language integrations.
- `tests/bindings/` remains the home for ABI validation and smoke coverage.

At the package level:

- `binding`: thin wrapper over the stable C ABI
- `driver`: JDBC, DB-API, `database/sql`, or equivalent
- `provider`: ADO.NET or EF Core provider
- `dialect` or `adapter`: SQLAlchemy, Knex, Dapper-style integration
- `plugin` or `extension`: tool-specific integrations such as DBeaver

## Current State

The Rust repository currently ships:

- a stable C ABI in `include/decentdb.h`
- language validation and smoke coverage in `tests/bindings/`

The Rust repository does not yet ship the higher-level packaged language APIs
that existed in the Nim repository.

## Product Inventory to Port

Port these products as Rust-engine-backed packages, not as Nim-compatible
artifacts:

- Python DB-API package
- Python SQLAlchemy dialect
- Python import tools
- .NET native interop layer
- .NET ADO.NET provider
- .NET MicroOrm package
- .NET EF Core provider
- .NET EF Core NodaTime plugin
- Go `database/sql` driver
- Java JDBC driver
- Java DBeaver extension
- Node low-level package
- Node Knex dialect
- Dart FFI package

## Migration Rules

- Keep the Rust C ABI as the only native boundary shared across language
  packages.
- Do not revive the old Nim native ABI as a compatibility layer.
- Use the old Nim repository only as a source of:
  - tests
  - examples
  - API contracts
  - package layout ideas
- Do not copy generated binaries, libraries, package caches, or build outputs.
- Keep package-specific toolchains out of the Rust core crates.
- Reuse `tests/bindings/` as release-blocking ABI and smoke coverage even after
  packaged bindings exist.

## Expected ABI Work

The old Nim-era packages relied on a more statement-oriented native surface than
the current Rust C ABI. Before porting higher-level packages, audit the ABI for
gaps in:

- prepared statements
- parameter binding lifecycle
- row streaming
- column metadata
- schema introspection payloads
- open options and capability discovery

Additive ABI expansion is acceptable when needed, but it must preserve the stable
handle-based design and panic containment guarantees.

## Execution Plan

### Phase 0: Repository Setup

- Create a top-level `bindings/` home for shipped packages.
- Keep `tests/bindings/` dedicated to validation and smoke coverage.
- Update `.gitignore` to exclude binding build outputs across `bindings/` and
  `tests/bindings/`.
- Remove tracked generated artifacts that should not live in the repository.

### Phase 1: ABI Gap Audit

- Compare each old package's native expectations against `include/decentdb.h`.
- Produce a per-language gap matrix.
- Capture the initial gap audit in `design/bindings/ABI_GAP_AUDIT.md`.
- Land required additive C ABI changes before porting package code.

### Phase 2: Priority Packaged Bindings

Port the two highest-priority ecosystems first:

- Python
  - DB-API package
  - SQLAlchemy dialect
  - import tools
- .NET
  - native interop layer
  - ADO.NET provider
  - MicroOrm
  - EF Core provider

These packages should reuse the existing Phase 4 smoke tests and grow them into
package-level integration suites.

### Phase 3: Secondary Drivers and Bindings

Port the remaining primary language packages:

- Go `database/sql` driver
- Java JDBC driver
- Node low-level package
- Dart FFI package

Each package must have:

- package-local tests
- one release-blocking smoke path in `tests/bindings/`
- clear versioning against the Rust engine ABI

### Phase 4: Adapters, Dialects, and Tooling

After the base packages are stable, port the higher-level integration layers:

- SQLAlchemy
- Dapper-style MicroOrm surface
- EF Core NodaTime plugin
- Knex dialect
- DBeaver extension
- Python import tools

## Suggested Target Layout

```text
bindings/
  python/
  dotnet/
  go/
  java/
  node/
  dart/
tests/
  bindings/
    python/
    dotnet/
    go/
    java/
    node/
    dart/
```

## Definition of Done Per Package

- Uses the Rust C ABI only
- Passes package-local tests
- Passes the corresponding `tests/bindings/` smoke path
- Avoids checked-in generated artifacts
- Documents whether it is a binding, driver, provider, dialect, or plugin
