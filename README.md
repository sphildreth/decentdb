# DecentDB

[![Status](https://img.shields.io/badge/status-pre--alpha-orange)](#status)
[![Language](https://img.shields.io/badge/language-Nim-2d9cdb)](#)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

```text                                       
  ___                 _   ___  ___ 
 |   \ ___ __ ___ _ _| |_|   \| _ )
 | |) / -_) _/ -_) ' \  _| |) | _ \
 |___/\___\__\___|_||_\__|___/|___/
                                                             
```
                                                  
ACID first. Everything else… eventually.

DecentDB is a embedded relational database engine focused on **durable writes**, **fast reads**, and **predictable correctness**. It targets a single process with **one writer** and **many concurrent readers** under snapshot isolation. It is not intended to be the best embedded database engine, but not terrible, a decent better than some engine.

---

## Highlights

- **WAL‑backed ACID** with crash‑safe recovery
- **B+Trees** for tables + secondary indexes
- **Snapshot reads** for concurrent readers
- **Postgres‑like SQL subset** (DDL/DML, joins, ORDER BY, LIMIT/OFFSET)
- **Trigram index** acceleration for `LIKE '%pattern%'`
- **Deterministic tests** (unit + property + crash injection + differential)

---

## Quick Start (Developer)

### Prerequisites
- **Nim** (includes `nim` + `nimble`)
- **Python 3**
- **libpg_query** (C library + headers)

> `nim.cfg` enables `-d:libpg_query` and links `-lpg_query`.  
> If headers/libs are in a non‑standard path, set `CFLAGS`/`LDFLAGS` or `NIMFLAGS`.

### Build
```bash
nimble build
```

### Test
```bash
nimble test
```

### Lint
```bash
nimble lint
```

### Benchmarks
```bash
nimble bench
nimble bench_compare
```

---

## CLI Reference

DecentDB ships a single CLI tool named `decentdb`. All commands and options are under this tool.

### Global Usage
```bash
decentdb --help
```

### exec (run SQL / engine controls)
```bash
decentdb exec --db path/to.db --sql "SELECT 1"
```

Database creation:
- The database file is created automatically on first open if it does not exist.
- Any command that takes `--db` will create the file when missing.

Examples:
```bash
decentdb exec --db ./my.db --openClose
decentdb exec --db ./my.db --sql "CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT)"
```

Options:
- `--db`, `-d` (required): database file path
- `--sql`, `-s`: SQL statement to execute
- `--openClose`: open and close without executing SQL (testing)
- `--timing`, `-t`: include timing info in JSON output
- `--cachePages`: number of 4KB pages to cache (default 64)
- `--cacheMb`: cache size in MB (overrides `--cachePages`)
- `--checkpoint`: force WAL checkpoint and exit
- `--readerCount`: show active reader count and exit
- `--longReaders`: show readers active longer than N ms
- `--dbInfo`: show DB header/config info and exit
- `--warnings`: include WAL warnings in output
- `--verbose`, `-v`: include verbose diagnostics in output
- `--checkpointBytes`: auto-checkpoint when WAL reaches N bytes
- `--checkpointMs`: auto-checkpoint when N ms elapse since last checkpoint
- `--format`: output format for results: json, csv, table (default json)
- `--params`: bind parameters in order (repeatable). Use `type:value`, e.g. `int:1`, `text:hi`, `null`
- `--walFailpoints`: set WAL failpoints (repeatable). Format `label:kind[:bytes][:count]`
- `--clearWalFailpoints`: clear all WAL failpoints before executing

### Schema introspection
```bash
decentdb list-tables --db path/to.db
decentdb describe --table users --db path/to.db
decentdb list-indexes --db path/to.db
decentdb list-indexes --table users --db path/to.db
```

Options:
- `list-tables`: `--db`, `-d`
- `describe`: `--table`, `-t` and `--db`, `-d`
- `list-indexes`: `--db`, `-d`, optional `--table`, `-t`

### Index maintenance
```bash
decentdb rebuild-index --index users_name_idx --db path/to.db
decentdb verify-index --index users_name_idx --db path/to.db
```

Options:
- `rebuild-index`: `--index`, `-i` and `--db`, `-d`
- `verify-index`: `--index`, `-i` and `--db`, `-d`

### Import / Export / Dump
```bash
decentdb import --table users --input data.csv --db path/to.db
decentdb import --table users --input data.json --db path/to.db --format=json

decentdb export --table users --output users.csv --db path/to.db
decentdb export --table users --output users.json --db path/to.db --format=json

decentdb dump --db path/to.db --output backup.sql
```

Options:
- `import`: `--table`, `-t`; `--input`; `--db`, `-d`; `--batchSize` (default 10000); `--format` (csv|json, default csv)
- `export`: `--table`, `-t`; `--output`; `--db`, `-d`; `--format` (csv|json, default csv)
- `dump`: `--db`, `-d`; optional `--output` (defaults to stdout)

### Bulk load (CSV)
```bash
decentdb bulk-load --table users --input data.csv --db path/to.db --batchSize=50000 --durability=deferred
```

Options:
- `--table`, `-t`; `--input`; `--db`, `-d`
- `--batchSize` (default 10000)
- `--syncInterval` (batches between fsync when durability is deferred, default 10)
- `--durability` (full|deferred|none, default deferred)
- `--disable-indexes` (default true)
- `--no-checkpoint` (skip checkpoint after load)

### Maintenance & diagnostics
```bash
decentdb checkpoint --db path/to.db
decentdb stats --db path/to.db
decentdb info --db path/to.db
decentdb dump-header --db path/to.db
decentdb verify-header --db path/to.db
```

Options:
- `checkpoint`: `--db`, `-d`; `--warnings`; `--verbose`
- `stats`: `--db`, `-d`
- `info`: `--db`, `-d`
- `dump-header`: `--db`, `-d`
- `verify-header`: `--db`, `-d`

### REPL & completion
```bash
decentdb repl --db path/to.db
decentdb completion --shell=bash
```

Options:
- `repl`: `--db`, `-d`; `--format` (json|csv|table, default table)
- `completion`: `--shell` (bash|zsh, default bash)

### Config file

If present, `~/.decentdb/config` provides defaults. Supported keys:
- `db` (default database path)
- `cachePages`
- `cacheMb`

---

## Repository Guide

- `design/PRD.md` — product requirements
- `design/SPEC.md` — engineering spec (modules, formats, concurrency)
- `design/TESTING_STRATEGY.md` — testing plan + benchmarks
- `design/IMPLEMENTATION_PHASES.md` — phased checklist (canonical)
- `design/adr/` — architecture decision records (format/ACID)
- `AGENTS.md` — contributor/agent workflow rules

---

## Architecture (MVP)

Core modules (see `design/SPEC.md`):
- `vfs` — OS I/O abstraction + fault injection
- `pager` — fixed pages + cache + freelist
- `wal` — append‑only log + recovery + checkpoints
- `btree` — tables + secondary indexes
- `record` — typed encoding + overflow pages
- `catalog` — schema metadata
- `sql` / `planner` / `exec` — parsing, planning, execution
- `search` — trigram index

---

## Concurrency Model

- **Single writer**, many concurrent readers
- Readers capture a **snapshot LSN** at start
- Reads consult WAL overlay for `lsn <= snapshot`

---

## Durability & Recovery

- WAL frames include checksums + LSN
- Recovery scans WAL to last committed boundary
- Checkpointing copies committed pages and **never truncates frames needed by active readers**

---

## Contributing

This repo is optimized for incremental, test‑driven changes.

1. Read `AGENTS.md` and the design docs under `design/`
2. Pick the earliest unchecked item in `design/IMPLEMENTATION_PHASES.md`
3. Implement **exactly** that item + tests
4. If you change any persistent format, add an ADR + version bump + compatibility tests

---

## License

Apache‑2.0. See `LICENSE`.
