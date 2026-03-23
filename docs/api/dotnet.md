# .NET Validation

Phase 4 validates the stable C ABI from .NET with a small `net10.0` smoke/integration program.

The validation lives in:

```text
tests/bindings/dotnet/Smoke/
```

It covers:
- open / close
- execute
- parameter binding
- result retrieval
- error retrieval
- explicit transaction control
- `save_as`

## Run locally

```bash
cargo build -p decentdb
LD_LIBRARY_PATH=$PWD/target/debug dotnet run --project tests/bindings/dotnet/Smoke/Smoke.csproj
```
