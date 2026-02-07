# DecentDB + Dapper (basic)

A tiny C# console app demonstrating the intended usage pattern for DecentDB via **ADO.NET** + **Dapper**.

This example is designed to stay lightweight and focused:
- create a table
- insert a few rows
- query rows back with Dapper

## Status

This example depends on a DecentDB **ADO.NET provider** (planned). Until the provider is implemented/packaged, the app will print a helpful message and exit.

Related design doc: [design/DAPPER_SUPPORT.md](../../../design/DAPPER_SUPPORT.md)

## Prerequisites

- .NET SDK 10+

## Running

From the repo root:

```bash
# Restore packages and run the example

dotnet run --project examples/dotnet/dapper-basic
```

## Provider wiring (when available)

The app uses `DbProviderFactories.GetFactory("DecentDB")` so it can compile without taking a hard dependency on the provider assembly.

Once the provider exists, it will need to be made discoverable either by:

1) Registering the factory in the provider package, or
2) Registering it in your app at startup (example pattern):

```csharp
// Example only — names/types will match the provider implementation
DbProviderFactories.RegisterFactory("DecentDB", DecentDBFactory.Instance);
```

The provider is expected to accept named parameters (e.g. `@id`) and rewrite them to DecentDB engine parameters (`$1`, `$2`, …) per ADR-0005.
