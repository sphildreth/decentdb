# DecentDB

**Durable. Fast. Embedded.**

DecentDB is an embedded, single-machine relational database engine focused on **durable ACID writes** and **fast reads**.

## Features

- **ACID Transactions** — Full durability with WAL-based persistence and snapshot isolation
- **Single Writer + Many Readers** — Optimized for read-heavy workloads
- **PostgreSQL-Compatible SQL** — JOINs, CTEs, window functions, aggregates, upsert, RETURNING
- **Rich Data Types** — INT64, FLOAT64, TEXT, BLOB, BOOL, DECIMAL, UUID
- **Full-Text Substring Search** — Trigram inverted index for `LIKE '%pattern%'` queries
- **Auto-Increment Primary Keys** — `INTEGER PRIMARY KEY` columns auto-assign IDs
- **Foreign Keys** — Referential integrity with CASCADE, SET NULL, RESTRICT
- **Multiple Language Bindings** — [.NET](api/dotnet.md), [Go](api/go.md), [Python](api/python.md), [Node.js](api/node.md)
- **Cross-Platform** — Linux, macOS, Windows

## Releases

Releases are driven by Git tags and published via GitHub Actions:

- Engine binaries (GitHub Releases): `docs/development/releases.md`
- NuGet package (`DecentDB.MicroOrm`, .NET 10 only): `docs/development/releases.md`

## Quick Start

```bash
# Install DecentDB
nimble install decentdb

# Create a database
decentdb exec --db=mydb.ddb --sql="CREATE TABLE users (id INT PRIMARY KEY, name TEXT)"

# Insert data
decentdb exec --db=mydb.ddb --sql="INSERT INTO users (name) VALUES ('Alice')"

# Query
decentdb exec --db=mydb.ddb --sql="SELECT * FROM users"
```

## Use Cases

- **Music Library Apps** - Fast queries across artists, albums, tracks
- **Embedded Applications** - Local data storage with SQL interface
- **Analytics & Reporting** - Aggregate functions and GROUP BY support
- **Search-Heavy Workloads** - Trigram indexes for text search

## Performance

- Point lookups: P95 < 10ms
- FK joins: P95 < 100ms
- Substring search: P95 < 200ms
- Bulk load: 100k records < 20 seconds

## Getting Started

- [Installation](getting-started/installation.md)
- [Quick Start Guide](getting-started/quickstart.md)
- [SQL Reference](user-guide/sql-reference.md)

## Links

- [GitHub Repository](https://github.com/sphildreth/decentdb)
- [Issue Tracker](https://github.com/sphildreth/decentdb/issues)
- [License](about/license.md) (Apache-2.0)
