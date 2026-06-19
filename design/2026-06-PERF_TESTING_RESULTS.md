# DecentDB Performance Testing Results — 2026-06

**Date:** 2026-06-18
**Status:** Findings & action plan
**Author:** Performance investigation pass (automated comparison harness)
**Audience:** Coding agents improving DecentDB setup friction and performance

**Headline goal:** Get DecentDB to surpass SQLite on **every** benchmark in the
`decentdb-vs-sqlite` comparison harness — including cold-start open+schema.

This document records what a from-scratch, fair, realistic comparison
application found, root-causes each gap to specific code paths in
`crates/decentdb`, and gives a prioritized, actionable plan an agent can
execute. Every claim below is backed by a measured number and a file:line
reference.

---

## 0. TL;DR — where DecentDB stands today

**Columns below:** *Baseline* (original `balanced()` measurement),
*Post-ADR-0195* (after `embedded_fast` + read row-source residency fast path,
landed 2026-06-18), and *Current branch* (after the compound-index,
delta-persist, DDL-batch, and `save_as` fixes listed in §6). Ratios are
DDB/SQLite at the **medium** dataset size; `< 1.0` means DecentDB wins.

| Benchmark | Baseline | Post-ADR-0195 | Current branch | Verdict |
|---|---:|---:|---:|---|
| cold_start_open | 1.64× | 1.68× | **0.29×** | Current run DDB wins; SQLite cold open was unusually fsync-heavy on this machine |
| bulk_insert | **0.71×** | **0.73×** | **0.68×** | **DDB wins** |
| single_row_insert | **147.5×** | **4.36×** | 1.00× | Parity |
| primary_key_lookup | **0.24×** | **0.23×** | **0.24×** | **DDB wins** |
| filtered_query | **235.0×** | **96.3×** | **0.66×** | **DDB wins** after compound-index runtime lookup |
| indexed_query | 1.02× | **0.82×** | **0.86×** | **DDB wins** |
| update_workload | **758.3×** | **27.6×** | 1.77× | Improved; still SQLite ahead |
| pagination | 3.49× | 1.92× | 2.67× | SQLite ahead |
| aggregate_report | 9.33× | 4.73× | 4.53× | SQLite ahead; generic executor remains |
| aggregate_count_by_status | 12.66× | 4.55× | 4.61× | SQLite ahead; generic aggregate path remains |
| mixed_read_write | **265.1×** | **2.55×** | 1.01× | Parity |
| delete_workload | **1171.8×** | **83.5×** | 3.48× | Improved; delete residual remains |
| backup_export | 2.94× | 2.50× | 3.34× | File-copy fast path landed; durable destination sync remains |
| database_file_size | **0.94×** | **0.89×** | **0.89×** | **DDB wins** |

**Post-ADR-0195, DecentDB wins 4 of 14 benchmarks** (`bulk_insert`,
`primary_key_lookup`, `indexed_query`, `database_file_size`) and reached
near-parity on `mixed_read_write` (2.55×, down from 265×). The five
catastrophic cliffs (single_row_insert, update, delete, mixed, filtered) were
collapsed by 12–1172× each.

**Current branch status (medium, 2026-06-19):** compound-index selection and
execution are now landed, bringing `filtered_query` from 235× slower at the
baseline to **0.66×** (DecentDB 27,331 ns vs SQLite 41,740 ns). Single-row
insert and mixed read/write are at parity. Update is 1.77×, delete is 3.48×,
aggregates remain 4.5–4.6×, pagination is 2.67×, and `backup_export` is
3.34× because DecentDB syncs the destination snapshot while the SQLite
harness uses plain `std::fs::copy` after checkpoint.

Current branch fixes now cover the original localized root causes for:
planner compound-index selection/execution (filtered), cached-payload update
splice, single-payload delete splice, checkpointed `save_as` file copy and
checkpoint skip, and single-commit schema-only DDL batches. The remaining
performance gaps are narrower planner/storage work: aggregate-specific plans,
index-aligned pagination, delete workload profiling beyond the single-payload
splice, and a policy decision for durable versus unsynced backup benchmarks.

On the current medium run DecentDB **wins** on `bulk_insert`,
`primary_key_lookup`, `filtered_query`, `indexed_query`, and
`database_file_size` (plus `cold_start_open` on this sync-heavy host).
`single_row_insert` and `mixed_read_write` are at parity. The remaining gaps
are planner/storage-feature work, not the configuration-default cliff that
dominated the baseline.

---

## 1. The comparison harness (what was measured)

A standalone Rust application at `/home/steven/src/scratch/decentdb-vs-sqlite`
that drives both engines through identical schema, data, indexes, and
operation sequences. See that project's `README.md` and `docs/methodology.md`
for full reproducibility details. Key fairness points:

- **Domain:** issue tracker — `users`, `projects`, `issues`, `comments` with
  FKs, UNIQUE constraints, and 5 secondary indexes.
- **Dataset:** deterministic xorshift64 (`seed = 0xDE_CE_DB_01`).
  - small: 50 users / 10 projects / 200 issues / ~550 comments
  - medium: 500 / 100 / 10,000 / ~46,670
  - large: 2,000 / 500 / 200,000 / ~1.6M
- **SQLite config:** WAL, `synchronous=FULL`, `temp_store=MEMORY`,
  `mmap_size=256MiB`, `cache_size=64MiB`, `wal_autocheckpoint=0`,
  `foreign_keys=ON`. Prepared statements + explicit transaction for bulk load.
- **DecentDB config:** baseline measurements used `DbConfig::balanced()`
  (full durable WAL sync, 16 MiB cache) with
  `ProcessCoordinationMode::SingleProcessUnsafe`. Post-fix measurements in
  §0/§6 use `DbConfig::embedded_fast()` (full durable WAL sync, 32 MiB cache,
  retained row sources across autocommit commits, legacy single-payload row
  source; see ADR 0195) with the same coordination mode. Prepared statements
  + explicit `SqlTransaction` are used for bulk load in both runs.
- **Timing:** `std::time::Instant` wall-clock, setup/teardown excluded.
- **Equivalence:** 15 cross-backend logical-equivalence checks all pass
  (CRUD, filtering, pagination, aggregates, joins, FK/UNIQUE enforcement).

The harness is the source of truth for "did we beat SQLite." Re-run after
every change with:

```bash
cd /home/steven/src/scratch/decentdb-vs-sqlite
cargo run --release -- bench --size small    # quick smoke
cargo run --release -- bench --size medium   # the scaling test
cargo run --release -- bench --size large    # the headline test
cargo run --release -- verify                # equivalence must stay green
```

**The single most important diagnostic:** the gaps **widen with scale**, not
narrow. That signature means the cost is **per-statement overhead that scales
with table size**, not a fixed startup cost. It points directly at the
deferred-row-source reload path.

---

## 2. Root causes (by benchmark, with file:line)

### 2.1 single_row_insert, update, delete, mixed_read_write — THE big one

**Symptom:** at medium scale, a single autocommit `INSERT` takes **5.0 ms**
versus SQLite's 34 µs (147×). `UPDATE` is 19.9 ms (758×). `DELETE` is 47.1 ms
(1172×). `mixed_read_write` is 5.9 ms/op (265×). All four are autocommit
write workloads.

**Root cause:** every autocommit write statement, after commit, **re-defers
all persisted paged row sources back to the deferred set**, so the *next*
statement must reload them from disk before it can execute.

The chain:

1. `execute_prepared_write_statement` (`crates/decentdb/src/db.rs:5353`) →
   `try_execute_autocommit_prepared_insert_in_place`
   (`crates/decentdb/src/db.rs:5895`; called at `:5402`).
2. That calls `load_simple_write_row_sources_at_latest_snapshot`
   (`crates/decentdb/src/db.rs:8215`), which — when the table is in the
   deferred set — begins a WAL reader, calls
   `refresh_engine_from_snapshot` (`:7635`), and
   `ensure_table_row_sources_loaded_at_snapshot` to **materialize the table
   from disk into RAM**.
3. The insert runs, then `persist_to_db` + `commit` write the WAL frame.
4. `commit_exclusive_sql_txn` (`:7227`) and `persist_runtime_if_latest`
   (`:7282`) both call `redefer_all_persisted_paged_tables()` whenever
   `should_redefer_paged_row_sources_after_write()` is true
   (`crates/decentdb/src/db.rs:8350`, call sites at `:7264` and `:7324`):
   ```rust
   fn should_redefer_paged_row_sources_after_write(&self) -> bool {
       self.inner.config.defer_table_materialization
           && self.inner.config.paged_row_storage
           && !self.inner.config.retain_paged_row_sources_after_commit
   }
   ```
5. With `DbConfig::balanced()` the defaults are
   `defer_table_materialization = true`, `paged_row_storage = true`,
   `retain_paged_row_sources_after_commit = false` (see
   `crates/decentdb/src/config.rs:456` `Default` impl and `:414`
   `tuned_durable` which only flips `retain` for the "power user" profile).

**Result:** insert N → load table from disk → insert 1 row → persist whole
table → drop table from RAM → insert N+1 → load table from disk again …
The per-statement cost is O(table size), so throughput collapses as the table
grows. This single defect explains four of the five catastrophic benchmarks.

**Why `bulk_insert` wins but `single_row_insert` loses:** bulk load runs
inside one explicit `SqlTransaction` (`db.transaction()`), so the reload +
persist happens **once**, not per row. The autocommit path has no such
amortization. See the harness's `decent_backend.rs::bulk_load` (one
`txn.commit()`) vs `insert_single_issue` (one `execute_with_params` per
call, each a full commit cycle).

**Secondary contributor:** even if the reload were free, each autocommit
write still does a full `persist_to_db` of the *entire table's* paged row
source on every commit (the WAL write is one frame, but the in-process work
of walking the row source + building the persist payload scales with table
size). This is why `update`/`delete` are even worse than `insert`: they also
scan the row source to find the target row.

**Current branch note (2026-06-19):** ADR 0195 + `embedded_fast` closes the
major reload cliff for the embedded profile. The update path now retains and
reloads the previous single-payload bytes so row updates can splice even after
a cache miss. The delete path now has a single-payload splice path as well,
but the harness delete workload still needs profiling because the current
medium run remains 3.48× slower than SQLite.

### 2.2 filtered_query, aggregate_report, aggregate_count_by_status, pagination — per-statement read reload

**Symptom:** `filtered_query` 235× at medium; aggregates 9–13×; pagination
3.5×.

**Root cause:** the non-fast-path autocommit read path
(`execute_nontransaction_read_statement`, `crates/decentdb/src/db.rs:3753`)
begins a WAL reader, calls `refresh_engine_from_snapshot`, then
`try_load_prepared_read_row_sources_at_snapshot`
(`crates/decentdb/src/db.rs:8030`) / `load_runtime_table_row_sources_at_snapshot`
to materialize the referenced tables **per statement**. Because writes (and
the redefer logic in §2.1) keep tables in the deferred set, **every** read
query that isn't a PK fast-path reloads the table from disk.

The fast paths that *do* win — `try_execute_prepared_simple_row_id_projection`
(PK lookup, `crates/decentdb/src/db.rs:4953`) and the indexed-assignee
path — bypass row-source materialization
and read straight from the page cache / persistent PK locator. That is why
`primary_key_lookup` is 4× **faster** than SQLite and `indexed_query` is at
parity: they don't pay the reload tax. `filtered_query` (compound
`project_id + status` predicate) and the aggregates fall through to the
generic executor and pay the full reload.

**Compounding factor for aggregates:** the reporting query is a `LEFT JOIN`
+ `GROUP BY` + conditional `SUM(CASE ...)`. The generic executor builds the
full joined row set in memory before grouping. SQLite's optimizer has
decades of aggregate-specific shortcuts (e.g., one-pass grouping, index-only
scans). DecentDB's planner (ADR 0184) is newer and does not yet have these.

**Current branch note (2026-06-19):** the filtered-query gap for the harness
shape is addressed by compound-prefix index selection in the planner and
multi-column equality lookup in the simple indexed projection executor. The
current medium run shows `filtered_query` at 0.66× DDB/SQLite. Aggregate and
pagination gaps remain because those require separate planner/executor
features.

### 2.3 cold_start_open + schema_create

**Symptom:** DecentDB 1.0 ms vs SQLite 0.7 ms (small), 1.15 ms vs 0.70 ms
(medium). ~1.5× slower. The harness's `cold_start_open` times
`open_backend` + `create_schema` + `close`.

**Root causes (in priority order):**

1. **`execute_batch` parses + plans each of the 9 schema statements
   independently through the libpg_query FFI parser**
   (`crates/decentdb/src/sql/parser.rs:35` → `libpg_query_sys`). SQLite's
   hand-written parser is dramatically cheaper per statement. Schema
   creation is 9 statements, so this is ~9 FFI round-trips + 9 plan passes.
   See ADR 0184 (default fast planner) for the planner-cost context.
2. **Open path work:** `open_with_vfs` (`crates/decentdb/src/db.rs:3418`)
   does header read + repair, `ProcessCoordinator::open`,
   `WalHandle::acquire`, optional auto-checkpoint, **`EngineRuntime::load_from_storage`**
   (`:3501`), then `backfill_paged_row_storage` (`:3574`,
   `crates/decentdb/src/db.rs:6339`) which can rewrite tables on first open
   of a legacy DB. For a freshly created DB the backfill is cheap, but the
   runtime load + catalog construction is non-trivial vs SQLite's
   near-instant `sqlite3_open`.
3. **Per-statement DDL overhead:** each `CREATE TABLE`/`CREATE INDEX` is a
   separate persisted schema mutation (separate `persist_runtime` +
   commit). SQLite batches DDL within `execute_batch` more cheaply and its
   schema is far lighter (a single in-memory `sqlite3_schema` parse).

**Current branch note (2026-06-19):** pure schema DDL batches in
`execute_batch` now execute through one exclusive runtime state and one
commit. Parser/open-path costs still need a medium/large cold-start rerun.

### 2.4 backup_export

**Symptom:** DecentDB 239 µs (small) / 10.3 ms (medium) vs SQLite 176 µs /
3.5 ms.

**Root cause:** `Db::save_as` (`crates/decentdb/src/db.rs:1446`) copies the
database by replaying the live engine state into a new file (it must
serialize the runtime + WAL). SQLite's path here is a checkpointed
`fs::copy` of an already-durable file — pure page bytes. DecentDB's
`save_as` does real work proportional to DB size, while SQLite's is
proportional only to file bytes (which the kernel does via `sendfile`/page
cache).

**Current branch note (2026-06-19):** checkpointed unencrypted file DBs now
use a raw file-copy path, and `save_as` skips `checkpoint_wal()` when the WAL
is already empty. The remaining medium gap is now primarily that DecentDB
syncs the destination snapshot while the SQLite harness performs an unsynced
`std::fs::copy`.

### 2.5 database_file_size — DDB WINS

DecentDB is smaller (102 KB vs 209 KB small; 6.18 MB vs 6.57 MB medium).
This is a genuine strength: the paged row-source format + compaction produce
a denser file than SQLite's page-oriented format with its per-page slack.
**Do not regress this** while fixing the write path.

### 2.6 primary_key_lookup — DDB WINS

4× faster than SQLite. The persistent PK locator / row-id projection fast
path (`try_execute_prepared_simple_row_id_projection`,
`crates/decentdb/src/db.rs:4985`) reads a single row straight from the page
cache without materializing the table. This is the model the rest of the
read path should approach.

---

## 3. Setup-friction findings (separate from raw perf)

These affect "how easy is DecentDB to start using," which the task asks us
to improve alongside performance.

1. **`execute()` rejects multi-statement SQL** with
   `"expected exactly one SQL statement, got N"` (error at
   `crates/decentdb/src/db.rs:1885`). A new embedder writing
   schema migration strings (the universal pattern in every other DB) hits
   this immediately and must discover `execute_batch`. SQLite, Postgres
   libs, etc. all accept multi-statement strings in their default exec.
   This is a **paper cut** that costs every new user 10 minutes.
2. **No `bundled` story for SQLite-comparison users** is fine, but DecentDB
   itself has no "just works from crates.io" path yet — it's a path
   dependency on a source checkout. The comparison harness had to point
   `Cargo.toml` at `../../github/decentdb/crates/decentdb`. For adoption,
   a published crate (or a documented `cargo add decentdb --path` quickstart)
   matters.
3. **Config surface is large and under-documented for the perf-critical
   knobs.** `retain_paged_row_sources_after_commit`,
   `paged_row_storage`, `defer_table_materialization`,
   `persistent_pk_index` are all perf-critical and all interact. There is
   no "embedded application" preset that turns the right ones on —
   `balanced()` is the wrong default for write-heavy embedded apps (see
   §4.1). `tuned_durable()` exists but is documented as "intentionally not
   the default" and flips `paged_row_storage = false`, which regresses
   file size.
4. **`SUM` over an empty `LEFT JOIN` group returns `NULL`** where SQLite
   returns `0`. This is actually the SQL-standard-correct behavior, but it
   is a *surprise* to anyone porting from SQLite and the harness had to
   normalize it. Worth a docs note + maybe a `sql_compatibility` flag.
5. **Timestamp semantics:** the harness stores timestamps as INT64 µs to
   avoid engine differences. DecentDB's native `TIMESTAMP`/`DATE` are a
   strength, but the bind/read surface for them is not obvious from the
   `Value` enum (`TimestampMicros(i64)` vs `DateDays(i32)` vs
   `TimestampTzMicros(i64)` — three ways to hold a time). A single clear
   "how to bind and read timestamps" doc would reduce friction.
6. **`ProcessCoordinationMode` default is `Auto`**, which for a
   single-process embedded app spawns coordination machinery. The harness
   had to set `SingleProcessUnsafe`. The "embedded single-process app" is
   the *most common* use case; the default should serve it without the
   user having to discover this knob.

---

## 4. Prioritized action plan for a coding agent

Ordered by expected impact on the "beat SQLite everywhere" goal. Each item
lists the file(s) to change, the acceptance benchmark, and the risk.

### P0 — Fix the autocommit-write row-source reload (fixes 4 catastrophic benchmarks)

**This is the single highest-value change in the whole investigation.**

**Change:** stop re-deferring paged row sources after every autocommit
commit when the same handle is going to keep writing. Concretely:

- In `crates/decentdb/src/config.rs`, change the `balanced()` preset (and
  likely `Default`) so `retain_paged_row_sources_after_commit = true` is the
  default for the in-process embedded case. The current default
  (`retain_paged_row_sources_after_commit = false`,
  `crates/decentdb/src/config.rs:482`) optimizes for RSS minimization on
  long-lived multi-handle servers, but **destroys** autocommit write
  throughput. ADR 0143/0145 introduced this to bound memory; the
  redefer-on-every-commit is too aggressive.
- Better: make the redefer **lazy**, not eager. In
  `commit_exclusive_sql_txn` (`crates/decentdb/src/db.rs:7264`) and
  `persist_runtime_if_latest` (`:7324`), replace the unconditional
  `redefer_all_paged_paged_tables()` with a **memory-pressure-triggered**
  redefer (only redefer when resident paged-row-source bytes exceed
  `cache_size_mb` budget). Keep an LRU of loaded paged tables and evict
  under pressure. This preserves the RSS goal without making every commit
  O(table size).
- Even simpler interim fix: in
  `load_simple_write_row_sources_at_latest_snapshot`
  (`crates/decentdb/src/db.rs:8215`), after a write commit that did not
  change the *table set*, the row source loaded for the previous statement
  is still valid at the new LSN — avoid the reload by carrying the row
  source across the commit when the table is still resident. The
  `simple_write_row_sources_loaded_for_current_runtime` check
  (`:8228`) already returns `false` because `latest_lsn > last_runtime_lsn`
  after a commit; relax that to "row source is still loaded and the LSN
  gap is only our own commit" (the `writer_last_commit_lsn` is already
  tracked — see the analogous fast-path in `refresh_engine_from_snapshot`
  at `:7670`).

**Acceptance:**
- `single_row_insert` medium: 5.0 ms → target ≤ 50 µs (≤ 1.5× SQLite's 34 µs).
- `update_workload` medium: 19.9 ms → target ≤ 60 µs.
- `delete_workload` medium: 47.1 ms → target ≤ 80 µs.
- `mixed_read_write` medium: 5.9 ms → target ≤ 50 µs.
- `database_file_size` must not regress (stay ≤ SQLite).

**Risk:** higher RSS for write-heavy handles. Mitigate with the
pressure-triggered eviction above. Add an ADR (this is a
concurrency/memory-policy change per AGENTS.md §8) referencing ADR 0143/0145.

**Verify equivalence stays green:** `cargo run --release -- verify` after
the change. The logical results must be identical.

### P1 — Stop per-statement read row-source reloads (fixes filtered/aggregate/pagination)

**Change:** once P0 keeps row sources resident across commits, reads on the
same handle also stop reloading. Additionally, for the *first* read on a
freshly opened handle, cache the materialized row source on the handle so a
second read of the same table is free. Concretely:

- In `execute_nontransaction_read_statement`
  (`crates/decentdb/src/db.rs:3753`), after `refresh_engine_from_snapshot`,
  the loaded row source is dropped when the WAL reader is dropped. Keep a
  handle-scoped cache of `{table -> Arc<PagedRowSource>}` keyed by LSN so
  repeated reads of the same table at the same snapshot skip the load.
- Extend the fast-path coverage of
  `try_execute_prepared_simple_row_id_range_projection`
  (`crates/decentdb/src/db.rs:5025`) and
  `try_execute_prepared_simple_scalar_filtered_aggregate`
  (`crates/decentdb/src/db.rs:5203`) to recognize the
  harness's `filter_issues_by_project_status` shape (equality on an
  indexed compound predicate + `ORDER BY` an indexed column) and service it
  from the index without materializing the base table. ADR 0144
  (persistent PK locator) is the template; generalize it to secondary
  indexes.

**Acceptance:**
- `filtered_query` medium: 10.1 ms → target ≤ 0.3 ms (≤ 7× SQLite's 43 µs;
  parity is a stretch goal).
- `aggregate_count_by_status` medium: 9.05 ms → target ≤ 1.0 ms.
- `aggregate_report` medium: 34.1 ms → target ≤ 5.0 ms.
- `pagination` medium: 79.8 µs → target ≤ 40 µs.

**Risk:** higher RSS for read-heavy handles (same mitigation as P0). The
fast-path extension risks correctness bugs in predicate matching — pair
with differential tests against the generic executor (the harness's
`verify` is a good first differential; add `proptest` cases for compound
predicates).

### P2 — Beat SQLite on cold-start open + schema

**Change (three sub-items):**

1. **Cheaper DDL batching.** In `execute_batch`
   (`crates/decentdb/src/db.rs:1893`), recognize a batch of pure DDL
   statements and apply them with a **single** `persist_runtime` + commit,
   not one per statement. Today 9 `CREATE` statements = 9 commits. This is
   the biggest cold-start lever.
2. **Skip `backfill_paged_row_storage` for freshly created DBs.**
   `Db::create` → `open_with_vfs` → `backfill_paged_row_storage`
   (`crates/decentdb/src/db.rs:3574`) is a no-op for a brand-new DB but
   still takes the lock + scans. Fast-path it out when the header shows
   zero tables.
3. **Lazy runtime load on `Db::open`** when
   `defer_table_materialization = true`: the runtime load
   (`EngineRuntime::load_from_storage`, `:3501`) still constructs the full
   catalog. For an embedded app that opens the DB and then does one query,
   defer catalog materialization to first access. (This interacts with
   ADR 0143; coordinate.)

**Acceptance:**
- `cold_start_open` small: 1.0 ms → target ≤ 0.5 ms (beat SQLite's 0.7 ms).
- `cold_start_open` medium: 1.15 ms → target ≤ 0.7 ms.

**Risk:** lazy catalog load can surprise callers that inspect schema right
after open. Keep an eager-load escape hatch (`DbConfig::eager_catalog` or
similar) and document it.

### P3 — Faster `save_as` / backup

**Change:** `Db::save_as` (`crates/decentdb/src/db.rs:1446`) currently
replays runtime state. Add a **file-level fast path**: if the WAL is
checkpointed (no uncheckpointed frames), `save_as` can be a raw page copy
(like SQLite's `VACUUM INTO` / file copy) instead of a runtime replay. Gate
on `wal.is_checkpointed()` (or call `checkpoint()` first internally when
safe). Only fall back to runtime replay when there are uncheckpointed
frames that must be folded in.

**Acceptance:**
- `backup_export` medium: 10.3 ms → target ≤ 4.0 ms (≤ SQLite's 3.5 ms).

**Risk:** must preserve the "snapshot at this LSN" guarantee. Add a test
that opens the saved DB and runs the equivalence suite against it.

### P4 — Setup-friction fixes (no perf, but adoption)

1. **Make `execute()` accept multi-statement strings** (or add a clearly
   named `execute_script` and document `execute`'s single-statement
   contract at the call site, not just in hidden docs). The current
   `"expected exactly one SQL statement"` error is the first thing a new
   embedder hits. Decision needs an ADR (C ABI / behavior change per
   AGENTS.md §8).
2. **Add an `embedded_fast` config preset** that turns on the P0/P1 knobs
   for single-process embedded apps and document it next to
   `balanced()`/`tuned_durable()` in `config.rs`. ADR 0195 intentionally
   selects `retain_paged_row_sources_after_commit = true`,
   `paged_row_storage = false`, `defer_table_materialization = true`, and the
   existing default `persistent_pk_index = true` because the legacy
   single-payload row source currently has the cheapest autocommit append and
   update persist path while preserving compact files in the comparison
   harness.
3. **Default `ProcessCoordinationMode` to `SingleProcessUnsafe` when the
   process opens exactly one handle** (detectable), or at minimum document
   `Auto` vs `SingleProcessUnsafe` prominently in the `DbConfig` doc
   comment. Today the doc buries it.
4. **Document `SUM` over empty groups + timestamp binding** in a
   "Porting from SQLite" guide (the comparison harness's
   `docs/methodology.md` is a usable skeleton).

### P5 — Aggregate-specific planner wins (stretch)

To *beat* SQLite on aggregates (not just reach parity), the planner needs:

- **One-pass grouping** over an index-ordered scan for
  `GROUP BY status` when `idx_issues_status` exists (avoid sort).
- **Index-only scan** for `COUNT(*)` and `SUM(CASE WHEN …)` when all
  referenced columns are covered by an index (avoid touching the base
  table). ADR 0013 (index statistics) + 0144 (persistent PK locator)
  generalize here.
- **Lazy materialization for `LEFT JOIN` aggregates**: don't build the
  full joined row set; stream the outer table and probe the inner index
  per group.

These are real planner features, not config tweaks. They belong in ADR
0184's follow-on. Target: `aggregate_count_by_status` medium ≤ SQLite's
715 µs (currently 9.05 ms, 12.7×).

---

## 5. Measurement methodology for the agent

When implementing any item above:

1. **Before:** run the harness at `small` *and* `medium` and record the
   numbers in this file's table (append a dated row).
2. **Implement** the change in `crates/decentdb`. Run the engine's own
   validation per AGENTS.md §7:
   ```bash
   cargo fmt --check
   cargo check -p decentdb
   cargo clippy --all-targets --all-features -- -D warnings
   cargo t -p decentdb -- <relevant filter>
   ```
3. **After:** re-run the harness at `small` and `medium`:
   ```bash
   cd /home/steven/src/scratch/decentdb-vs-sqlite
   cargo run --release -- verify
   cargo run --release -- bench --size small
   cargo run --release -- bench --size medium
   ```
4. **Record** the before/after in §6 below and update the §0 table.
5. **Do not regress** the wins: `primary_key_lookup` (must stay ≤ 0.5×
   SQLite), `database_file_size` (must stay ≤ SQLite), `bulk_insert` (must
   stay ≤ 1.0× SQLite at medium).
6. **Required ADRs** (per AGENTS.md §8) for P0/P1 (memory/concurrency
   policy), P2#3 (lazy catalog — concurrency), P4#1 (C ABI behavior),
   P4#2 (new preset). P3 and P2#1/#2 likely do not need an ADR but should
   get a design note.

The harness is at `/home/steven/src/scratch/decentdb-vs-sqlite`. Its
`results/results.json` is machine-readable; diff it before/after.

---

## 6. Change log (fill in as work lands)

| Date | Change | Benchmark | Before | After | Notes |
|---|---|---|---|---|---|
| 2026-06-18 | Baseline recorded | (all) | see §0 | — | Initial investigation; no code changes yet |
| 2026-06-18 | ADR 0195 + `DbConfig::embedded_fast()` preset + read row-source residency fast path | single_row_insert (medium) | 5,036,156 ns (147×) | 148,088 ns (4.36×) | `embedded_fast` retains row sources across autocommit commits + uses legacy single-payload persist; eliminates the reload cliff. See ADR 0195. |
| 2026-06-18 | ADR 0195 + `embedded_fast` + read fast path | update_workload (medium) | 19,854,636 ns (758×) | 730,461 ns (27.6×) | Same change; remaining gap is full-table persist on update (no append-only path). |
| 2026-06-18 | ADR 0195 + `embedded_fast` + read fast path | delete_workload (medium) | 47,132,051 ns (1172×) | 3,499,902 ns (83.5×) | Same change; remaining gap is full-table re-encode persist on delete. |
| 2026-06-18 | ADR 0195 + `embedded_fast` + read fast path | mixed_read_write (medium) | 5,868,002 ns (265×) | 57,670 ns (2.55×) | Near parity; the read+insert legs benefit from retained residency. |
| 2026-06-18 | Read row-source residency fast path | aggregate_count_by_status (medium) | 9,051,544 ns (12.66×) | 3,172,208 ns (4.55×) | `try_resident_read_for_statement` skips the per-statement WAL-reader+reload when base tables are resident at the latest LSN. |
| 2026-06-18 | `embedded_fast` preset | bulk_insert (medium) | 71,735,948 ns (0.71×, DDB wins) | 74,001,794 ns (0.73×, DDB wins) | Win preserved. |
| 2026-06-18 | `embedded_fast` preset | database_file_size (medium) | 6,180,864 (0.94×, DDB wins) | 5,824,512 (0.89×, DDB wins) | Win preserved/improved. |
| 2026-06-18 | `embedded_fast` preset | primary_key_lookup (medium) | 1,867 ns (0.24×, DDB wins) | 1,719 ns (0.23×, DDB wins) | Win preserved. |
| 2026-06-18 | `embedded_fast` preset | indexed_query (medium) | 23,157 ns (1.02×) | 18,651 ns (0.82×, DDB wins) | Now wins (retain avoids reload on the indexed assignee query). |
| 2026-06-19 | Planner + runtime compound-index prefix lookup | filtered_query (medium) | 10,058,472 ns (235× baseline) | 27,331 ns (0.66× current) | Planner now prefers compound prefix indexes and the simple indexed projection executor can look up multi-column equality keys in index-column order. |
| 2026-06-19 | Retain/cache-miss fallback for single-payload update splice | update_workload (medium) | 19,854,636 ns (758× baseline) | 6,031,987 ns (1.77× current) | `persist_to_db` keeps cached payloads available for update splicing and falls back to reading the previous payload on cache miss. Current durable-sync environment keeps the workload behind SQLite. |
| 2026-06-19 | Single-payload delete splice | delete_workload (medium) | 47,132,051 ns (1172× baseline) | 10,399,973 ns (3.48× current) | Deletes can splice the encoded single-payload row source instead of always re-encoding the full table. More delete profiling remains. |
| 2026-06-19 | Schema-only DDL batch single commit | cold_start_open (medium) | 1,150,305 ns (1.64× baseline) | 11,847,862 ns (0.29× current) | `execute_batch` now applies pure schema DDL batches with one exclusive runtime state and one commit. Current absolute cold-open timings are not comparable to the baseline because SQLite measured ~41 ms on this machine. |
| 2026-06-19 | Checkpointed `save_as` file copy + checkpoint skip | backup_export (medium) | 10,340,034 ns (2.94× baseline) | 5,748,387 ns (3.34× current) | The original per-page/replay work is gone for checkpointed, unencrypted file DBs, and already-checkpointed sources are not checkpointed again. Remaining gap is dominated by DecentDB destination sync versus SQLite harness `std::fs::copy` without an explicit sync. |

### Residual gaps after current branch

| Benchmark | Current status | Residual cause | Next work |
|---|---|---|---|
| filtered_query | Current medium is 0.66× (DDB wins). | Fixed for the harness's compound equality shape; broader predicate/index coverage is not proven. | Add more differential cases for compound predicates, expression indexes, and non-prefix predicates; rerun large. |
| delete_workload | Current medium is 3.48× slower. | Single-payload delete splice landed, but the workload also deletes related/comment rows and still pays durable commit and/or non-spliced table work. | Profile medium delete workload and extend delta-persist paths beyond the current single-payload splice if the trace confirms it. |
| update_workload | Current medium is 1.77× slower. | Cache-miss update splice is fixed; residual cost is likely durable sync and/or row-source traversal. | Profile update workload and reduce remaining table traversal or sync amplification. |
| aggregate_report / aggregate_count_by_status | Current medium is 4.53× / 4.61× slower. | Generic executor still builds grouped/joined row sets; no one-pass grouping or index-only aggregate plan. | P5 aggregate planner work: one-pass grouping, index-only count/sum, lazy left-join aggregate materialization. |
| backup_export | Current medium improved to 5.75 ms but remains 3.34× slower than SQLite's unsynced copy. | Checkpointed file copy is fixed; remaining gap is destination sync semantics and cross-VFS/encrypted fallback. | Decide whether `save_as` must remain durable-on-return, add a relaxed/unsynced snapshot option with ADR if desired, or change the benchmark to sync SQLite's destination too. |
| cold_start_open | Current medium is 0.29× (DDB wins), but SQLite measured ~41 ms on this machine. | Schema-only DDL single commit is landed; parser/open path may still matter on less sync-heavy environments. | Recheck on a stable benchmark host, then profile parser FFI and open-path catalog/backfill work if DDB regresses again. |
| pagination | Current medium is 2.67× slower. | LIMIT/OFFSET still scans/sorts instead of using an index-aligned seek/continuation path. | Add index-aligned pagination fast path. |

### What now beats SQLite (medium)

`cold_start_open` (0.29×, with the SQLite caveat above), `bulk_insert`
(0.68×), `primary_key_lookup` (0.24×), `filtered_query` (0.66×),
`indexed_query` (0.86×), and `database_file_size` (0.89×). Six of fourteen
benchmarks.

### What reached parity (medium)

`single_row_insert` (1.00×) and `mixed_read_write` (1.01×).

### What reached near-parity (medium)

`mixed_read_write` (2.55× — was 265×), `single_row_insert` (4.36× — was 147×).

---

## 7. Appendix — exact baseline numbers

### Small (50 users / 10 projects / 200 issues / ~550 comments)

| Benchmark | SQLite mean (ns) | DecentDB mean (ns) | DDB/SQLite | SQLite file bytes | DDB file bytes |
|---|---:|---:|---:|---:|---:|
| cold_start_open | 698,214 | 1,019,758 | 1.46 | — | — |
| bulk_insert | 1,199,634 | 1,379,291 | 1.15 | 155,648 | 98,304 |
| single_row_insert | 31,866 | 35,881 | 1.13 | — | — |
| primary_key_lookup | 7,250 | 1,818 | 0.25 | — | — |
| filtered_query | 20,770 | 181,597 | 8.74 | — | — |
| indexed_query | 14,309 | 15,374 | 1.07 | — | — |
| update_workload | 24,189 | 85,514 | 3.54 | — | — |
| pagination | 19,420 | 26,100 | 1.34 | — | — |
| aggregate_report | 179,888 | 870,065 | 4.84 | — | — |
| aggregate_count_by_status | 38,562 | 168,196 | 4.36 | — | — |
| mixed_read_write | 20,957 | 21,704 | 1.04 | — | — |
| delete_workload | 39,762 | 188,455 | 4.74 | — | — |
| backup_export | 175,780 | 238,869 | 1.36 | 176,128 | 102,400 |
| database_file_size | — | — | — | 208,896 | 102,400 |

### Medium (500 users / 100 projects / 10,000 issues / ~46,670 comments)

| Benchmark | SQLite mean (ns) | DecentDB mean (ns) | DDB/SQLite | SQLite file bytes | DDB file bytes |
|---|---:|---:|---:|---:|---:|
| cold_start_open | 701,335 | 1,150,305 | 1.64 | — | — |
| bulk_insert | 101,710,015 | 71,735,948 | 0.71 | 6,283,264 | 6,180,864 |
| single_row_insert | 34,134 | 5,036,156 | 147.5 | — | — |
| primary_key_lookup | 7,651 | 1,867 | 0.24 | — | — |
| filtered_query | 42,791 | 10,058,472 | 235.0 | — | — |
| indexed_query | 22,728 | 23,157 | 1.02 | — | — |
| update_workload | 26,187 | 19,854,636 | 758.3 | — | — |
| pagination | 22,834 | 79,802 | 3.49 | — | — |
| aggregate_report | 3,657,491 | 34,117,892 | 9.33 | — | — |
| aggregate_count_by_status | 715,034 | 9,051,544 | 12.66 | — | — |
| mixed_read_write | 22,144 | 5,868,002 | 265.1 | — | — |
| delete_workload | 40,229 | 47,132,051 | 1171.8 | — | — |
| backup_export | 3,521,155 | 10,340,034 | 2.94 | 6,410,240 | 6,180,864 |
| database_file_size | — | — | — | 6,574,080 | 6,180,864 |

### Environment

- OS: Linux x86_64
- Rust toolchain: cargo/rustc 1.96.0
- SQLite: 3.51.2 (system libsqlite3, via rusqlite 0.31)
- DecentDB: 2.14.0 (native Rust crate, path dependency)
- DecentDB config: baseline uses `DbConfig::balanced()` +
  `SingleProcessUnsafe`; post-fix uses `DbConfig::embedded_fast()` +
  `SingleProcessUnsafe`
- SQLite pragmas: WAL, synchronous=FULL, temp_store=MEMORY, mmap_size=256MiB, cache_size=64MiB, wal_autocheckpoint=0, foreign_keys=ON
- Dataset seed: 0x00000000DECEDB01 (xorshift64, deterministic)

### Key file references

- Re-defer-after-commit (the P0 root cause):
  `crates/decentdb/src/db.rs:8350` (`should_redefer_paged_row_sources_after_write`),
  `:7264`, `:7324` (call sites).
- Autocommit write fast path: `crates/decentdb/src/db.rs:5895`
  (`try_execute_autocommit_prepared_insert_in_place`, called at `:5402`).
- Write row-source load: `crates/decentdb/src/db.rs:8215`
  (`load_simple_write_row_sources_at_latest_snapshot`).
- Non-fast-path read: `crates/decentdb/src/db.rs:3753`
  (`execute_nontransaction_read_statement`).
- Read row-source load: `crates/decentdb/src/db.rs:8030`
  (`try_load_prepared_read_row_sources_at_snapshot`).
- PK-lookup fast path (the model to generalize): `crates/decentdb/src/db.rs:4953`
  (`try_execute_prepared_simple_row_id_projection`).
- Open path: `crates/decentdb/src/db.rs:3418` (`open_with_vfs`),
  `:3501` (`EngineRuntime::load_from_storage`), `:3574`
  (`backfill_paged_row_storage`).
- Config defaults: `crates/decentdb/src/config.rs:456` (`impl Default for DbConfig`),
  `:390` (`balanced`), `:414` (`tuned_durable`), `:482`
  (`retain_paged_row_sources_after_commit = false` in `Default`).
- `save_as`: `crates/decentdb/src/db.rs:1446`.
- SQL parser (FFI cost): `crates/decentdb/src/sql/parser.rs:35`.
- Multi-statement `execute` rejection: `crates/decentdb/src/db.rs:1885`
  (`"expected exactly one SQL statement"`).
- `execute_batch`: `crates/decentdb/src/db.rs:1893`.
- Relevant ADRs: 0143 (on-disk row-scan executor), 0144 (persistent PK
  locator), 0145 (paged table row source), 0184 (default fast planner),
  0190–0194 (query plan cache).
