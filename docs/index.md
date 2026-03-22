# DecentDB

**Durable. Fast. Embedded.**

DecentDB is an embedded, single-machine relational database engine focused on **durable ACID writes** and **fast reads**.

## Features

- **ACID Transactions** — Full durability with WAL-based persistence and snapshot isolation
- **Single Writer + Many Readers** — Optimized for read-heavy workloads
- **PostgreSQL-Compatible SQL** — JOINs (INNER, LEFT, RIGHT, FULL OUTER, CROSS, NATURAL), CTEs (including WITH RECURSIVE), window functions, aggregates (with DISTINCT), upsert, RETURNING, savepoints
- **Rich Data Types** — INT64, FLOAT64, TEXT, BLOB, BOOL, DECIMAL, UUID, DATE, TIMESTAMP
- **Full-Text Substring Search** — Trigram inverted index for `LIKE '%pattern%'` queries
- **Auto-Assigned Primary Keys** — If a table has a single INT64 primary key column, omitting the value on INSERT will auto-assign an ID (INT/INTEGER/INT64/BIGINT are synonyms)
- **Foreign Keys** — Referential integrity with CASCADE, SET NULL, RESTRICT
- **Generated Columns** — `GENERATED ALWAYS AS (expr) STORED` for computed values
- **Temporary Objects** — Session-scoped TEMP tables and views
- **JSON Support** — Scalar functions, table-valued functions (`json_each`, `json_tree`)
- **Multiple Language Bindings** — [.NET](api/dotnet.md), [Go](api/go.md), [Python](api/python.md), [Node.js](api/node.md), [JDBC](api/jdbc.md)
- **Cross-Platform** — Linux x86_64/arm64 (including 64-bit Raspberry Pi OS), macOS, Windows

## Releases

Releases are driven by Git tags and published via GitHub Actions:

- Engine binaries (GitHub Releases): native Linux x86_64/arm64 (including 64-bit Raspberry Pi OS on Pi 3/4/5), macOS, and Windows builds. [Releases](development/releases.md)
- NuGet packages (`.NET 10`): `DecentDB.AdoNet`, `DecentDB.MicroOrm`, `DecentDB.EntityFrameworkCore`, `DecentDB.EntityFrameworkCore.Design`, `DecentDB.EntityFrameworkCore.NodaTime`

## Quick Start

```bash
# Install DecentDB
nimble install decentdb

# Create a database
# Note: auto-increment works for a single INT64 PRIMARY KEY column (spelling INT/INTEGER/INT64 doesn’t matter).
decentdb exec --db=mydb.ddb --sql="CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"

# Insert data (id auto-assigned)
decentdb exec --db=mydb.ddb --sql="INSERT INTO users (name) VALUES ('Alice')"

# Query
decentdb exec --db=mydb.ddb --sql="SELECT * FROM users"
```

## Use Cases

- **Offline-first desktop app** — local relational cache for a UI-heavy app (fast reads, durable writes), with `saveAs` for backups/migration.
- **Music library / media server** — trigram indexes for fast search across artist/album/track names and JSON metadata.
- **IoT / edge device data logger** — append-only event table with native `TIMESTAMP`, periodic checkpoints, and snapshot exports.
- **Game tools / editors** — temporary tables/views for import pipelines and fast iteration, with savepoints for “undo” style workflows.
- **Embedded analytics & reporting** — `GROUP BY`/HAVING + window functions for dashboards on a single machine.
- **Config/state store for services** — ACID transactions + foreign keys for consistent config + relational integrity.
- **ETL staging / ingestion** — bulk-load CSV + generated columns for derived values and normalized search keys.
- **Search-heavy workloads** — `%pattern%` queries accelerated by trigram indexes when you need substring matching.

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
