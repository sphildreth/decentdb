# .NET bindings

DecentDB ships several in-tree .NET packages under `bindings/dotnet/`.

## Package surfaces

The current .NET source tree includes:

- `bindings/dotnet/src/DecentDB.Native` — native library loading and P/Invoke
  layer over the stable `ddb_*` C ABI
- `bindings/dotnet/src/DecentDB.AdoNet` — ADO.NET provider
- `bindings/dotnet/src/DecentDB.MicroOrm` — LINQ-style Micro ORM
- `bindings/dotnet/src/DecentDB.EntityFrameworkCore` — EF Core provider
- `bindings/dotnet/src/DecentDB.EntityFrameworkCore.Design` — design-time
  services
- `bindings/dotnet/src/DecentDB.EntityFrameworkCore.NodaTime` — NodaTime
  integration

Tests and benchmarks live alongside the packages under `bindings/dotnet/tests/`
and `bindings/dotnet/benchmarks/`.

## Use via NuGet

For normal application development, prefer the published NuGet packages. You do
not need to build DecentDB from source or download native binaries separately
just to consume the .NET provider surface in your project.

Typical package choices:

```bash
dotnet add package DecentDB.AdoNet
dotnet add package DecentDB.MicroOrm
dotnet add package DecentDB.EntityFrameworkCore
dotnet add package DecentDB.EntityFrameworkCore.Design
dotnet add package DecentDB.EntityFrameworkCore.NodaTime
```

Install the package(s) that match your stack:

- `DecentDB.AdoNet` for direct ADO.NET usage
- `DecentDB.MicroOrm` for the higher-level LINQ-style Micro ORM
- `DecentDB.EntityFrameworkCore` for EF Core
- `DecentDB.EntityFrameworkCore.Design` for EF Core design-time tooling
- `DecentDB.EntityFrameworkCore.NodaTime` if your EF Core model uses NodaTime

These packages are the right choice for application developers. The source
build and smoke-validation steps below are mainly for contributors, binding
development, and low-level verification.

## Build the native library

From the repository root:

```bash
cargo build -p decentdb
```

This produces the shared library consumed by the .NET smoke program and the
in-tree packages:

- Linux: `target/debug/libdecentdb.so`
- macOS: `target/debug/libdecentdb.dylib`
- Windows: `target/debug/decentdb.dll`

## Run the in-tree .NET test suite

```bash
cd bindings/dotnet
dotnet test DecentDB.NET.sln -v minimal
```

## Run the C ABI smoke validation

The repository also keeps a narrow .NET smoke program under
`tests/bindings/dotnet/Smoke/` to validate the stable native surface directly.

```bash
cargo build -p decentdb
LD_LIBRARY_PATH=$PWD/target/debug dotnet run --project tests/bindings/dotnet/Smoke/Smoke.csproj
```

On macOS or Windows, use the equivalent platform-specific native library search
setup when running the standalone smoke program.
