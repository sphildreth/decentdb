# DecentDB Bindings

This directory contains the language integration source trees being migrated to
the Rust DecentDB engine.

These directories were copied from the legacy Nim repository as source-only
migration input. They preserve package layout, tests, examples, and API
expectations, but they should not be assumed to work with the current Rust
engine yet.

## Naming

`bindings/` is the umbrella term for the repo-level directory.

Use the more specific industry term for each package where it applies:

- `binding`: thin language wrapper over the stable C ABI
- `driver`: JDBC, Python DB-API, Go `database/sql`, or similar database client surface
- `provider`: .NET ADO.NET or EF Core provider
- `dialect` or `adapter`: SQLAlchemy, Knex, Dapper-style integration layers
- `plugin` or `extension`: tool integrations such as DBeaver

## Repository Split

- `bindings/`: packaged language source trees under migration
- `tests/bindings/`: current Rust-engine ABI validation and smoke coverage
- `design/bindings/`: migration notes and design documents

## Current Status

What is validated against the Rust engine today:

- `tests/bindings/python/`
- `tests/bindings/dotnet/`
- `tests/bindings/go/`
- `tests/bindings/java/`
- `tests/bindings/node/`
- `tests/bindings/dart/`

What has been copied into `bindings/` for migration work:

- `bindings/python/`: Python DB-API package, SQLAlchemy dialect, import tools,
  tests, and benchmarks
- `bindings/dotnet/`: `DecentDB.Native`, `DecentDB.AdoNet`,
  `DecentDB.MicroOrm`, `DecentDB.EntityFrameworkCore`,
  `DecentDB.EntityFrameworkCore.Design`, `DecentDB.EntityFrameworkCore.NodaTime`,
  tests, and benchmarks
- `bindings/go/`: Go `database/sql` driver source in `decentdb-go/`
- `bindings/java/`: JDBC driver, JNI/native shim, Gradle build, and DBeaver
  extension
- `bindings/node/`: low-level `decentdb/` package and `knex-decentdb/`
- `bindings/dart/`: Dart package, native glue, scripts, and examples

## Migration Policy

  migration input for the Rust rewrite.
- These packages are not yet treated as supported Rust-engine packages.
- Do not commit generated artifacts from any language toolchain into this tree.

## Layout

The following language integration trees now live in-repo under `bindings/`:

- `bindings/python/`
- `bindings/dotnet/`
- `bindings/go/`
- `bindings/java/`
- `bindings/node/`
- `bindings/dart/`
