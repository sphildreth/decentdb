# DecentDB

**Durable embedded SQL for local-first applications.**

DecentDB is a Rust-native embedded relational database engine focused on
durable ACID writes, fast reads, predictable correctness, and application
integration. It is built around a single-process concurrency model with one
writer and many concurrent readers under snapshot isolation.

DecentDB is not trying to be "SQLite with more features." Its strongest lane is
embedded SQL for modern applications that need local durability, rich relational
queries, syncable offline data, and language bindings that feel native.

## What Stands Out

| Capability | Why it matters |
|---|---|
| Durable ACID storage | WAL-based persistence and crash-safe recovery are central design goals. |
| Local-first sync | Built-in change journals, batch exchange, scoped peer replication, conflict workflows, retention tooling, sync doctor, CLI commands, and a typed .NET sync SDK. |
| Built-in HTTP and web console | `decentdb serve` exposes a local HTTP API and embedded browser console for inspection, SQL execution, EXPLAIN, schema browsing, CSV export, and scripting. |
| Browser WASM and OPFS | `@decentdb/web` runs DecentDB in a Dedicated Worker with OPFS persistence, an async TypeScript API, binary result transport, and browser smoke/benchmark coverage. |
| Branch, diff, restore, and time travel | Durable named snapshots, branch-local writes, diff reports, guarded restore, and constrained merge workflows for migration rehearsal and support/debugging. |
| Practical PostgreSQL-like SQL | Familiar DDL/DML, joins, CTEs, window functions, set operations, upsert, `RETURNING`, savepoints, triggers, generated columns, JSON functions, and rich scalar functions. |
| Application-friendly types | Native `INT64`, `FLOAT64`, `BOOL`, `TEXT`, `BLOB`, `DECIMAL`, `UUID`, `DATE`, and `TIMESTAMP`. |
| Indexed substring search | Native trigram indexes accelerate interactive `LIKE '%pattern%'` queries. |
| Multi-language embedding | C ABI plus .NET, Go, Python, Node.js, Dart/Flutter, and JDBC bindings. |
| Operational tooling | Queryable `sys.*` metrics for WAL, write queue, storage, and sync status; CLI inspection, checkpoints, index rebuilds, import/export, bulk load, doctor reports, and DBeaver/JDBC integration. |

## Core Features

- **ACID transactions** with WAL durability and crash recovery.
- **One writer, many readers** for predictable embedded concurrency.
- **B+Tree tables and secondary indexes** with page caching.
- **Foreign keys** with referential integrity and supported actions such as
  `CASCADE`, `SET NULL`, and `RESTRICT`.
- **Generated columns** in `STORED` and `VIRTUAL` modes.
- **Temporary tables and views** scoped to the current session.
- **JSON support** including scalar functions and table-valued functions such
  as `json_each` and `json_tree`.
- **Triggers** for application-side logic, including supported `AFTER` and
  `INSTEAD OF` trigger paths.
- **Bulk-load, CSV, and JSON import/export** workflows.
- **Queryable operational metrics** through stable `sys.*` inspection views for
  WAL, write queue, storage, and sync status snapshots.
- **Branch, diff, restore, and time-travel workflows** with named snapshots,
  branch-local writes, primary-key row diffs, guarded restore, and constrained
  merge.
- **Built-in HTTP API and Web Console** through `decentdb serve` for local
  inspection, schema browsing, SQL/EXPLAIN execution, CSV export, and scripting.
- **Browser WASM/OPFS binding** through `@decentdb/web`, with a Dedicated
  Worker runtime, OPFS-backed persistence, binary result transport, checkpoint,
  import/export, and persistence helpers.
- **In-memory databases** using `:memory:` plus save-as support for snapshots.
- **Cross-platform release builds** for Linux x86_64/arm64, macOS, and Windows.

## Quick Start

Install the CLI from the latest release:

1. Download the archive for your platform from
   [GitHub Releases](https://github.com/sphildreth/decentdb/releases).
2. Extract the archive.
3. Put `decentdb` or `decentdb.exe` on your `PATH`.

Release archives are published for Linux x86_64/arm64, macOS, and Windows.
Verify the CLI is available:

```bash
decentdb --help
```

If you are developing DecentDB itself, you can also install the CLI from a
local checkout:

```bash
cargo install --path crates/decentdb-cli
```

Create a database, insert a row, and query it:

```bash
decentdb exec --db ./app.ddb --sql "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)"
decentdb exec --db ./app.ddb --sql "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com') RETURNING id"
decentdb exec --db ./app.ddb --sql "SELECT * FROM users"
```

Open an interactive SQL shell:

```bash
decentdb repl --db ./app.ddb
```

Enable local-first sync on a database:

```bash
decentdb sync init --db ./app.ddb --replica-id node-a
decentdb sync status --db ./app.ddb --format table
decentdb sync pending --db ./app.ddb --since 0 --limit 10 --format table
```

## Where To Start

- New users: [Installation](getting-started/installation.md) and
  [Quick Start](getting-started/quickstart.md)
- SQL users: [SQL Reference](user-guide/sql-reference.md),
  [SQL Feature Matrix](user-guide/sql-feature-matrix.md), and
  [Data Types](user-guide/data-types.md)
- Local-first applications: [Local-first sync](user-guide/sync/index.md)
- Browser applications: [WASM / Browser](api/wasm.md)
- Operational workflows: [Doctor](user-guide/doctor.md),
  [Performance Tuning](user-guide/performance.md), and
  [Benchmarks](user-guide/benchmarks.md)
- Comparing engines: [Comparison Overview](user-guide/comparison.md),
  [DecentDB vs SQLite](user-guide/decentdb-vs-sqlite.md), and
  [DecentDB vs DuckDB](user-guide/decentdb-vs-duckdb.md)
- Language integrations: [C/C++ ABI](api/c-cpp.md), [.NET](api/dotnet.md),
  [Go](api/go.md), [Python](api/python.md), [Node.js](api/node.md),
  [Dart/Flutter](api/dart.md), [JDBC](api/jdbc.md), and
  [WASM / Browser](api/wasm.md)
- CLI users: [Interactive SQL Shell](user-guide/repl.md),
  [Built-In Web Console](user-guide/web-console.md), and
  [CLI Reference](api/cli-reference.md)

## Built-In Web Console At A Glance

`decentdb serve --db ./app.ddb` starts a local HTTP API and lightweight browser
console at `http://localhost:7373`. It is designed for quick inspection,
simple ad hoc SQL, schema browsing, and scripting support without installing a
full IDE.

- embedded HTML, CSS, and vanilla JavaScript served from the CLI binary
- no CDN, external fonts, telemetry, frontend build pipeline, or internet
  dependency
- transparent ephemeral auth for default localhost sessions
- read-only mode for safe inspection
- table detail, schema, query, explain, CSV export, and local query history

Start with the [Built-In Web Console](user-guide/web-console.md) guide.

## Browser WASM At A Glance

`@decentdb/web` loads the Rust engine compiled to WASM inside a Dedicated
Worker and stores database bytes in OPFS through synchronous access handles. The
main-thread API stays async while the engine preserves the same one-writer,
WAL-first design.

- OPFS-backed browser persistence
- async TypeScript API for `open`, `exec`, `query`, `prepare`, checkpoint,
  import/export, and persistence requests
- binary worker result transport with JSON retained for compatibility/debugging
- automated Chromium OPFS smoke and scheduled transport benchmark coverage

Start with the [WASM / Browser](api/wasm.md) guide.

## Local-First Sync At A Glance

DecentDB sync is built into the engine and exposed through CLI commands, SQL
inspection surfaces, and .NET APIs. The current sync surface includes:

- durable row-level change capture in a sidecar sync journal
- replica IDs, peer catalogs, and peer-to-scope bindings
- manual JSON batch export/import
- localhost/dev HTTP `sync run` and `sync serve` workflows
- scoped replication with validated row filters
- conflict recording, inspection, resolution, reopen, and policy commands
- canonical `sys.*` operational inspection views with `sys_sync_*`
  compatibility names
- retention reports, safe prune dry-runs, peer lag, and sync doctor guidance

Start with the [sync overview](user-guide/sync/index.md) or jump directly to
the [sync quickstart](user-guide/sync/quickstart.md).

## Language Bindings

| Language | Surface |
|---|---|
| C/C++ | [Stable C ABI](api/c-cpp.md) through `include/decentdb.h`; C++ consumers can include the same header directly |
| .NET | Native wrapper, ADO.NET provider, Micro ORM, EF Core provider, NodaTime support, and typed sync SDK |
| Go | `database/sql` driver with DecentDB-specific helpers |
| Python | DB-API and SQLAlchemy dialect |
| Node.js | Native addon and Knex dialect |
| Dart/Flutter | FFI binding for desktop Flutter applications |
| Java/JDBC | In-process JNI-backed JDBC driver, including DBeaver integration |
| Web | TypeScript API over WASM and OPFS in a Dedicated Worker |

All bindings sit above the native C ABI. The Rust engine remains the
authoritative implementation.

## Current Constraints

- DecentDB is an embedded, single-process database engine.
- The concurrency model is one writer with many concurrent readers.
- The built-in HTTP surfaces are local-first. `decentdb serve` can bind to a
  non-localhost host only with explicit bearer-token configuration, but it is
  still a lightweight inspection/API server rather than a hardened public
  database service.
- Browser v1 support is worker-owned OPFS storage. It does not provide
  cross-tab writes, service worker ownership, or cross-worker WAL coordination.
- DecentDB does not currently expose a general-purpose loadable SQL extension
  or UDF plugin system.
- Some roadmap items, including policy-aware SQL, vector search, and full-text
  ranking, are planned work rather than shipped features.

## Releases And Packages

GitHub Releases publish native archives for Linux x86_64/arm64, macOS, and
Windows. Release bundles include the CLI and native C API library. JDBC and
DBeaver assets are published alongside the native bundles.

.NET packages include `DecentDB.AdoNet`, `DecentDB.MicroOrm`,
`DecentDB.EntityFrameworkCore`, `DecentDB.EntityFrameworkCore.Design`, and
`DecentDB.EntityFrameworkCore.NodaTime`.

See [Release process](development/releases.md) and
[GitHub Releases](https://github.com/sphildreth/decentdb/releases).

## Project Links

- [GitHub repository](https://github.com/sphildreth/decentdb)
- [Issue tracker](https://github.com/sphildreth/decentdb/issues)
- [License](about/license.md)
