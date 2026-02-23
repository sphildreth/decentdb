# Examples

This directory contains small, focused example projects that show how to use DecentDB from other languages and ecosystems. Each binding includes both a **file-based** example and an **in-memory** (`:memory:`) example.

## .NET

- [dotnet/dapper-basic/](dotnet/dapper-basic/) — Dapper with file-based database
- [dotnet/dapper-memory/](dotnet/dapper-memory/) — Dapper with `:memory:` database
- [dotnet/microorm-linq/](dotnet/microorm-linq/) — MicroOrm LINQ with file-based database
- [dotnet/microorm-memory/](dotnet/microorm-memory/) — MicroOrm with `:memory:` database
- [dotnet/entityframework/](dotnet/entityframework/) — Entity Framework Core with `:memory:` database

## Go

- [go/main.go](go/main.go) — `database/sql` with file-based database
- [go/main_memory.go](go/main_memory.go) — `database/sql` with `:memory:` database

## Node.js

- [node/example.js](node/example.js) — N-API native addon with file-based database
- [node/example_memory.js](node/example_memory.js) — N-API native addon with `:memory:` database

## Python

- [python/example.py](python/example.py) — DB-API 2.0 with file-based database
- [python/example_memory.py](python/example_memory.py) — DB-API 2.0 with `:memory:` database

## Other

- [showcase.sh](showcase.sh) — Quick CLI walkthrough of core DecentDB features
- [run_all.py](run_all.py) — Runs all examples and reports results

## Running All Examples

Use `run_all.py` to run every example and verify they all work:

```bash
# Run all examples
python examples/run_all.py

# Run with verbose output
python examples/run_all.py -v

# Filter by language
python examples/run_all.py --python
python examples/run_all.py --dotnet
python examples/run_all.py --node
python examples/run_all.py --go

# Filter by storage type
python examples/run_all.py --memory    # only in-memory examples
python examples/run_all.py --file      # only file-based examples

# Skip .NET build step (if already built)
python examples/run_all.py --no-build
```

**Prerequisites:** The native library must be built first (`nimble build_lib`).
