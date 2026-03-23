# DecentDB Bindings

This directory is the home for packaged language integrations built on the Rust
DecentDB engine.

## Naming

`bindings/` is the umbrella term for the repo-level directory.

Use the more specific industry term for each package where it applies:

- `binding`: thin language wrapper over the stable C ABI
- `driver`: JDBC, Python DB-API, Go `database/sql`, or similar database client surface
- `provider`: .NET ADO.NET or EF Core provider
- `dialect` or `adapter`: SQLAlchemy, Knex, Dapper-style integration layers
- `plugin` or `extension`: tool integrations such as DBeaver

## Repository Split

- `bindings/`: shipped language packages for the Rust engine
- `tests/bindings/`: ABI validation and narrow smoke coverage
- `design/bindings/`: migration notes and design documents

## Migration Policy

- Nim is not a supported target in this repository.
- `/home/steven/source/decentdb-nim/bindings` is reference material only for API
  shape, tests, examples, and package layout.
- Do not copy generated artifacts from any language toolchain into this tree.

## Planned Layout

As packages are ported, prefer a stable language-first layout:

- `bindings/python/`
- `bindings/dotnet/`
- `bindings/go/`
- `bindings/java/`
- `bindings/node/`
- `bindings/dart/`
