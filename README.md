# DecentDb

[![Status](https://img.shields.io/badge/status-pre--alpha-orange)](#status)
[![Language](https://img.shields.io/badge/language-Nim-2d9cdb)](#)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

ACID first. Everything else… eventually.

DecentDb is a pre‑alpha embedded relational database engine focused on **durable writes**, **fast reads**, and **predictable correctness**. It targets a single process with **one writer** and **many concurrent readers** under snapshot isolation.

---

## Highlights

- **WAL‑backed ACID** with crash‑safe recovery
- **B+Trees** for tables + secondary indexes
- **Snapshot reads** for concurrent readers
- **Postgres‑like SQL subset** (DDL/DML, joins, ORDER BY, LIMIT/OFFSET)
- **Trigram index** acceleration for `LIKE '%pattern%'`
- **Deterministic tests** (unit + property + crash injection + differential)

---

## Status

Pre‑alpha. Formats may change until a compatibility policy is established.

- ✅ Foundations, pager, records, B+Tree read/write, catalog, SQL MVP
- ✅ WAL + recovery + checkpoints + bulk load + performance guardrails
- ✅ Benchmarks + regression thresholds

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

## Roadmap (Phases)

The canonical checklist lives in `design/IMPLEMENTATION_PHASES.md`.

- [x] Phase 0: Foundations
- [x] Phase 1: DB file + pager + cache
- [x] Phase 2: Records + B+Tree read path
- [x] Phase 3: WAL + transactions + recovery
- [x] Phase 4: B+Tree write path + catalog + SQL MVP
- [x] Phase 5: Constraints + FKs + trigram search
- [x] Phase 6: Checkpointing + bulk load + perf hardening

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
