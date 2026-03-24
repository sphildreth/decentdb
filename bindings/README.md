# DecentDB Bindings

This directory contains DecentDB's in-tree language integration packages,
examples, and ecosystem-specific support code.

## Naming

`bindings/` is the umbrella term for the repo-level directory.

Use the more specific industry term for each package where it applies:

- `binding`: thin language wrapper over the stable C ABI
- `driver`: JDBC, Python DB-API, Go `database/sql`, or similar database client surface
- `provider`: .NET ADO.NET or EF Core provider
- `dialect` or `adapter`: SQLAlchemy, Knex, Dapper-style integration layers
- `plugin` or `extension`: tool integrations such as DBeaver

## Repository split

- `bindings/`: packaged language source trees, package metadata, and examples
- `tests/bindings/`: release-blocking ABI validation and smoke coverage
- `design/bindings/`: design documents, packaging strategy, and ABI notes

## Current validation

The following integration surfaces are validated against the current Rust
engine:

- `tests/bindings/python/`
- `tests/bindings/dotnet/`
- `tests/bindings/go/`
- `tests/bindings/java/`
- `tests/bindings/node/`
- `tests/bindings/dart/`

The following package trees live under `bindings/`:

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

## Repository policy

- Keep bindings aligned with the stable `ddb_*` C ABI in `include/decentdb.h`.
- Pair package-local tests with release-blocking smoke or validation coverage in
  `tests/bindings/` where applicable.
- Do not commit generated artifacts from any language toolchain into this tree.

## Layout

The following language integration trees now live in-repo under `bindings/`:

- `bindings/python/`
- `bindings/dotnet/`
- `bindings/go/`
- `bindings/java/`
- `bindings/node/`
- `bindings/dart/`
