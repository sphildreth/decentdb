# DecentDB .NET Bindings — Review Findings Implementation Plan

**Date:** 2026-04-22
**Scope:** `bindings/dotnet/src/DecentDB.AdoNet/**`,
`bindings/dotnet/src/DecentDB.MicroOrm/**`,
`bindings/dotnet/src/DecentDB.EntityFrameworkCore/**`, plus targeted
documentation/error-message touch-ups in
`crates/decentdb/src/sql/normalize.rs`.
**Companion artifacts:**
- `/tmp/tmp-opus47-decentdb-net-tests/` — the 50K artist / 500K album / ~2.75M
  song benchmark whose run produced these findings.
- `/tmp/tmp-opus47-decentdb-net-tests/CHALLENGES.md` — the 27-item challenge
  log this plan slices up.
- `design/2026-04-20.DART-REVIEW.md` — slice/format precedent used here.

## 0. Motivation — measured gap between .NET bindings

Identical schema (3 tables, 6 indexes, 1 view), identical seed plan
(seed=42, 50K artists / 500K albums / 2 748 922 songs), engine `2.3.0`.
On-disk file is **byte-identical** across all three bindings (144.8 MB),
so the engine is not the bottleneck.

### Original measurement (default API surfaces)

| binding   | total | seed_artists r/s | seed_albums r/s | seed_songs r/s | peak heap |
|-----------|------:|-----------------:|----------------:|---------------:|----------:|
| AdoNet    |   20s |          377 389 |         496 057 |        519 443 |    324 MB |
| MicroOrm  |  131s |          119 289 |          73 296 |         24 851 |    n/a    |
| EfCore    | 1 350s|           33 144 |           7 947 |          2 200 |    541 MB |

EF Core seeded songs ~**236× slower** than ADO.NET on the *same* engine.
MicroOrm was ~**21× slower**. Read-side queries are within 2× across all
three bindings; the gap is almost entirely on the write path.

### Validation — consumer-side EF Core refactor (2026-04-22)

The benchmark was re-run after rewriting the EF Core seed path to use
the standard EF Core escape hatch
(`ctx.Database.GetDbConnection()` + a single prepared single-row INSERT
rebound and executed per row, with a fresh transaction every 65 536
rows). Queries stayed on EF Core LINQ; only seeding bypassed the change
tracker.

| binding             | total | seed_artists r/s | seed_albums r/s | seed_songs r/s | peak heap |
|---------------------|------:|-----------------:|----------------:|---------------:|----------:|
| AdoNet              |  20s  |          381 896 |         501 652 |        528 336 |    324 MB |
| MicroOrm            | 130s  |          119 051 |          72 551 |         25 026 |    327 MB |
| **EfCore (refactored)** | **27s** |     **353 499** |    **334 420** |    **250 474** |    **324 MB** |

Three things this validation establishes for the slice plan below:

1. **The EF Core binding is not architecturally broken.** When the
   consumer bypasses the change tracker, EF Core lands within ~2× of
   ADO.NET on every metric. The remaining gap is per-`DbCommand`
   allocation in EF Core's pipeline and per-`ExecuteNonQuery` rewriter
   cost, not anything intrinsic to DecentDB. Slice N1 still ships, but
   its **realistic target shifts from "10× speedup" to "≤ 2× of
   AdoNet"** (i.e. ~250K → ~500K rows/s on `seed_songs`).
2. **MicroOrm's 25K rows/s ceiling is unchanged** — and matches the
   consumer-side EF Core baseline before refactor. Confirms slice N2's
   diagnosis (per-row `DbCommand` allocation in `InsertManyAsync`) and
   its fix path (cache one prepared statement, rebind+execute per row).
3. **Multi-row `VALUES` was empirically *slower* than single-row
   prepared reuse** in the consumer-side experiment. Slices N1 and N2
   should keep multi-row `VALUES` on the table (it removes per-call
   parser/rewriter cost), but the implementation must benchmark
   single-row reuse vs multi-row reuse with the same chunk size before
   shipping. If multi-row regresses, ship the prepared single-row
   path.

### Raw-Rust baseline — establishing the engine ceiling (2026-04-22)

To make "how much further can the bindings realistically go?" a
*measured* question rather than an opinion, the same 12-step benchmark
was re-implemented as a standalone Rust binary that links the
`decentdb` crate directly and uses the engine's hot-path API:

```text
Db::create()                       -- fresh database, no FFI, no marshalling
db.transaction()                   -- exclusive SqlTransaction
txn.prepare("INSERT ... VALUES")   -- once per shape
prepared.execute_in(&mut txn, ...) -- per row, no LINQ, no rewriter
txn.commit()                       -- single WAL commit per logical batch
```

This is the same pattern the internal `decentdb-benchmark` scenarios
use. It represents the **theoretical engine ceiling** every binding
could approach but never beat. Source lives at
`/tmp/tmp-opus47-decentdb-net-tests/rust-baseline/` (workspace
path-dep on `crates/decentdb`, ~600 lines, deterministic SplitMix64
seed plan).

Engine `2.3.1`, scale `full` (50 000 artists, 500 000 albums,
~2.75 M songs):

| step / metric                        |   RustRaw |    AdoNet | EFCore (refactored) |   MicroOrm |
|--------------------------------------|----------:|----------:|--------------------:|-----------:|
| `seed_artists` r/s                   |   792 664 |   381 896 |             353 499 |    119 051 |
| `seed_albums` r/s                    |   786 594 |   501 651 |             334 419 |     72 550 |
| `seed_songs` r/s                     |   672 241 |   528 335 |             250 473 |     25 026 |
| `seed_songs` slowdown vs RustRaw     |  **1.00×**| **1.27×** |          **2.68×**  | **26.85×** |
| `query_aggregate_durations` (s)      |     0.880 |     0.923 |               1.062 |      0.971 |
| `query_top10_artists_by_songs` (s)   |     1.709 |     1.920 |               1.762 |      1.846 |
| `query_top10_albums_by_songs` (s)    |     3.235 |     3.363 |               2.538 |      3.251 |
| `query_view_first_1000` (s)          |     2.354 |     2.555 |               2.358 |      2.535 |
| `query_count_songs` (s)              |     0.000 |     0.003 |               0.243 |      0.003 |
| `query_artist_by_id` (s)             |     0.001 |     0.001 |               0.047 |      0.018 |
| **peak RSS**                         | **2.2 GB**| **2.3 GB**|         **2.6 GB**  |  **2.6 GB**|
| DB size                              |  144.9 MB |  144.8 MB |            144.8 MB |   144.8 MB |
| WAL size at end                      |      32 B |       0 B |                 0 B |        0 B |

What the baseline confirms or overturns:

1. **AdoNet is essentially at the ceiling.** 1.27× on the heaviest
   write step and ≤ 1.21× on every read. The remaining headroom is
   FFI marshalling, `SqlParameterRewriter`, and `IDataReader`
   materialization. Future AdoNet-only optimization will produce
   single-digit-percent wins, not multiples.
2. **EFCore (refactored) at 2.68× is reasonable for a full ORM**, and
   slice N1's "≤ 2× of AdoNet" target translates to roughly 1.2–1.5×
   of RustRaw — **achievable**, with margin.
3. **MicroOrm's 26.85× gap is the real outlier.** It is also the
   single largest absolute time on the suite (109 s vs 4 s on
   `seed_songs`). This re-validates slice N2's P0 priority and the
   diagnosis (per-row `DbCommand` allocation).
4. **Heavy reads are engine-bound, not binding-bound.** Every binding
   sits within ≤ 1.21× of RustRaw on aggregates, top-N joins, and
   view scans. Read-side binding work will produce only marginal
   gains; meaningful read-side wins must come from the engine
   (planner, GROUP BY codegen, view inlining).
5. **EFCore has a measurable LINQ-translation tax on small queries**:
   `COUNT(*)` 0 ms → 243 ms, `query_artist_by_id` 1 ms → 47 ms.
   Invisible at .NET-vs-.NET scale; obvious here. Captured as new
   slice **N19** below (compiled-query plan cache).
6. **Memory pressure is engine-side, not binding-side.** The raw
   Rust baseline — with zero binding overhead — also climbs to
   2.2 GB peak RSS on a 145 MB database during read evaluation. The
   .NET layers add only +100 MB (AdoNet) to +400 MB (EFCore /
   MicroOrm) on top of that floor. **Diagnostically: the bindings
   are not the cause of the absolute memory footprint; the engine
   is.** Captured as a new engine backlog item in §6 ("query-time
   intermediate buffers retained until `Db` drop").

Slowdown summary (lower = closer to engine, > 1.5× = real gap):

```text
                       seed_artists  seed_albums  seed_songs  query_top10_albums  query_view_1000
RustRaw                       1.00×        1.00×       1.00×               1.00×           1.00×
AdoNet                        2.08×        1.57×       1.27×               1.04×           1.09×
EFCore (refactored)           2.24×        2.35×       2.68×               0.78×*          1.00×
MicroOrm                      6.66×       10.84×      26.85×               1.00×           1.08×

* EFCore top-10 is faster than RustRaw because the rewriter coalesces it differently;
  noise in this neighborhood and not actionable.
```

The full per-step / per-binding table and the JSON inputs live at
`/tmp/tmp-opus47-decentdb-net-tests/rust-baseline/results/comparison-full.md`.

### Engine-side observations from the same benchmark

- **Stale WAL → misleading "catalog root page magic is invalid".** A
  hung previous EF Core run left a 67 MB `*.ddb.wal` file on disk. The
  next open of the (re-created) database returned
  `DecentDB error 2: database corruption: catalog root page magic is
  invalid`. The data file was fine — the WAL referenced pages from a
  different generation. Engine should detect this and emit
  "stale WAL detected" instead. (Engine backlog, see §6.)
- **`COUNT(*)` cold-start latency.** A second agent measured
  `SELECT COUNT(*) FROM artists` (50K rows) at ~8 s on first
  execution across all three bindings. This is consistent with a full
  table scan with no covering shortcut. (Engine backlog, see §6.)

## 1. Priorities (reminder, from `AGENTS.md`)

1. Durable ACID writes.
2. Fast reads.
3. Stable, ergonomic multi-language integrations.

This plan does not change the engine on-disk format, WAL format, or the
exported C ABI. Two ABI-touching ideas (`ddb_stmt_bind_batch`, public WAL
stats) are deliberately **out of scope** here and are listed in §6
"Deferred — needs ADR".

## 2. How to use this document

- Each slice (N1–N11, then N-REL) is independently implementable by a
  separate coding agent.
- Slices are ordered top-to-bottom by **expected user impact × ease**, but
  there are explicit dependency notes at the top of each slice; respect
  them when scheduling.
- A slice is **only** complete when every item under **"Acceptance
  criteria"** is satisfied.
- Each slice includes:
  - `Problem` — what was observed and why it matters.
  - `Verified evidence` — file:line citations into the engine and bindings
    (so the agent does not have to redo the diagnosis).
  - `Design decisions already made` — the agent must not relitigate these.
  - `Implementation steps` — explicit, file-by-file.
  - `Tests to add` — list of named test cases with expected behavior.
  - `Validation` — the exact commands to run.
  - `Acceptance criteria` — checklist for "done".
- Between slices, run:
  ```bash
  cd /home/steven/source/decentdb
  python3 scripts/do-pre-commit-checks.py --mode fast
  ```
  Before the release slice, run:
  ```bash
  python3 scripts/do-pre-commit-checks.py
  ```
- **Do not run `git commit`, `git push`, or any git write operation
  without explicit user approval.** A diff is not approval. Silence is not
  approval. (`AGENTS.md` §8.)

## 3. Slice index

| Slice | Title                                                                       | Bindings touched | Pri | Engine? | Status    |
|-------|-----------------------------------------------------------------------------|------------------|----:|---------|-----------|
| N1    | EF Core multi-row `VALUES` coalescing in `ModificationCommandBatch`         | EF Core          | P0  | no      | **Done**    |
| N2    | MicroOrm `InsertManyAsync` — multi-row `VALUES` + cached parameter shape    | MicroOrm         | P0  | no      | **Done**    |
| N3    | MicroOrm POCO portability — ignore unmapped reference/collection properties | MicroOrm         | P1  | no      | **Done**    |
| N4    | MicroOrm `QueryRawAsync<T>` for keyless DTOs                                | MicroOrm         | P1  | no      | **Done**    |
| N5    | MicroOrm `InsertManyAsync` `RETURNING` parity with `InsertAsync`            | MicroOrm         | P1  | no      | **Done**    |
| N6    | EF Core + MicroOrm — `Random.Next`/non-deterministic LINQ translator handling| EF Core, MicroOrm| P1  | no      | **Done**    |
| N7    | EF Core `UseDecentDB` — accept bare paths consistently with ADO.NET         | EF Core          | P1  | no      | **Done**    |
| N8    | AdoNet `DecentDBConnectionStringBuilder` — strongly-typed `Pooling` + docs  | AdoNet           | P2  | no      | **Done**    |
| N9    | Engine — accept `CREATE VIEW IF NOT EXISTS` (or document the workaround)    | engine + docs    | P2  | yes (parser only) | Pending   |
| N10   | Engine — improve subquery-in-FROM error context for the .NET surface        | engine + AdoNet  | P2  | yes (error message only) | Pending   |
| N11   | Bindings docs — feature-parity matrix and POCO/connection-string notes      | docs             | P3  | no      | Pending   |
| N12   | AdoNet — accept multi-statement SQL via `SqlStatementSplitter` in `Prepare`/`ExecuteReader` | AdoNet | P1 | no | **Done**    |
| N13   | AdoNet `SqlStatementSplitter` — handle `CREATE TRIGGER ... BEGIN ... END` bodies | AdoNet           | P2  | no      | **Done**    |
| N14   | EF Core — register an `IQueryTranslationPostprocessor` to rewrite correlated `Count` subqueries as `LEFT JOIN ... GROUP BY` | EF Core | P1 | no | Pending   |
| N15   | EF Core — document and trim the ~7–8 s first-`DbContext` startup cost      | EF Core + docs   | P2  | no      | **Done**    |
| N16   | AdoNet — unify WAL filename convention and expose a `DeleteDatabaseFiles` helper | AdoNet           | P2  | no      | **Done**    |
| N17   | AdoNet `DecentDBConnection` — fire `StateChange` on `Open()` and `Close()` | AdoNet           | P1  | no      | **Done**    |
| N18   | AdoNet `GetSchema("Indexes")` — distinguish auto-PK indexes from user indexes | AdoNet           | P2  | no      | **Done**    |
| N19   | EF Core — cache compiled query plans across DbContext lifetime              | EF Core + docs   | P2  | no      | Pending   |
| N-REL | Release: version bump, regenerate benchmark numbers, smoke tests            | release plumbing | —   | no      | Pending   |

There is no slice for `EF1001` suppression — that issue was retracted
during verification (the EF Core binding builds with zero warnings without
any consumer suppression; see `CHALLENGES.md` "Verification pass").

---

## Slice N1 — EF Core: coalesce same-shape `INSERT`s into a single multi-row `VALUES (…),(…),…`

**Priority:** P0 (largest user-visible win in the whole plan).
**Depends on:** none.
**Estimated diff size:** ~150 lines added, ~30 lines removed in one file.

### Problem

`SaveChanges`/`SaveChangesAsync` with `AddRange(N entities)` issues `N`
separate `INSERT` statements, even after the existing prepared-statement
reuse. On the original benchmark this capped EF Core at ~2.2K rows/s vs
ADO.NET's ~520K rows/s on the same engine.

A consumer-side validation (2026-04-22, see §0) bypassed
`SaveChanges` entirely by reaching the underlying `DecentDBConnection`
via `ctx.Database.GetDbConnection()` and running a hand-rolled
prepared single-row INSERT loop. That reached **250 K rows/s** without
touching the binding. The 250 K vs 520 K residual gap is per-`DbCommand`
allocation cost in EF Core's pipeline plus parameter-rewriter cost per
`ExecuteNonQuery`.

The goal of this slice is to close that residual gap so an ordinary
`SaveChanges` user gets the consumer-workaround number **without
needing the workaround**. Realistic target after this slice:
**`seed_songs ≥ 250 K rows/s`** (matching the consumer workaround) on
the benchmark; **stretch goal `≥ 400 K rows/s`** if the engine's
multi-row VALUES path is faster than single-row reuse at
chunk size = 256.

> **Important calibration note.** The consumer-side experiment showed
> that **single-row prepared reuse beat multi-row VALUES by ~3×** at
> medium scale on engine 2.3.0 (393 K vs 12 K rows/s with chunk 256).
> Implementer must benchmark both modes inside this slice (see
> "Implementation steps" §6) and ship the faster one. A binding fall-back
> to single-row reuse is acceptable and may be the chosen default;
> coalescing into a multi-row VALUES is only worthwhile if it measurably
> beats single-row reuse with an identical transaction strategy.

### Verified evidence

- The engine grammar accepts multi-row `VALUES`:
  - `crates/decentdb/src/sql/normalize.rs:163-168` —
    `normalize_query_body` routes `statement.values_lists` to
    `QueryBody::Values(normalize_values_lists(...))`.
  - `crates/decentdb/src/exec/mod.rs:4161` — `evaluate_values_body` is the
    executor entry point.
  - There are existing parser tests for the multi-row form; we are
    extending neither parser nor executor.
- The binding's existing fast path stops short of coalescing:
  - `bindings/dotnet/src/DecentDB.EntityFrameworkCore/Update/Internal/DecentDBModificationCommandBatch.cs:77-113`
    (`ExecuteWithStatementReuse`) already reuses one prepared statement
    via `Reset/ClearBindings/rebind`, but executes once per row.
  - `BuildSqlTemplate` (lines 120-198) emits a single `VALUES (...)` clause.

### Design decisions already made (do not relitigate)

1. Coalescing happens **only for `EntityState.Added`** commands. `Modified`
   and `Deleted` keep the existing one-statement-per-row reuse path —
   batched UPDATE/DELETE require shape-equivalent WHERE clauses and concurrency
   handling that is out of scope for this slice.
2. Coalescing happens **only across commands with identical SQL shape**:
   same table, same write-column set, same read-column set
   (`RETURNING`), same `Schema`. Any change in shape flushes the current
   coalesced batch and starts a new one.
3. The maximum coalesced row count is governed by a constant
   `MaxCoalescedRows = 256`. Justification: keeps the rendered SQL string
   under ~64 KB for typical column counts (≤ 32 cols × 12-byte placeholder
   × 256 rows ≈ 100 KB worst-case) without building unbounded buffers.
   This is a hard cap; we do not expose it as configurable in this slice.
4. When a command has any `IsRead` columns (i.e. needs `RETURNING`),
   **disable coalescing for that shape** in this slice. EF Core needs to
   match each returned row back to a tracked `IUpdateEntry`, and the
   single-row path already does that. A separate slice (deferred) can
   tackle multi-row `RETURNING` once N1 is in.
5. Parameter binding stays positional `$1, $2, ...` in row-major order:
   row 0 cols, then row 1 cols, etc. This matches what
   `evaluate_values_body` expects.

### Implementation steps

All edits in
`bindings/dotnet/src/DecentDB.EntityFrameworkCore/Update/Internal/DecentDBModificationCommandBatch.cs`.

1. Add a private constant near the top of the class:
   ```csharp
   private const int MaxCoalescedRows = 256;
   ```
2. Introduce a private helper `BuildShapeKey(IReadOnlyModificationCommand command)`
   that returns a string of the form
   `"INS|<schema>.<table>|<col1>,<col2>,...|<read1>,<read2>,..."`.
   Same shape ⇒ same key. Use the *raw* `ColumnName`s (not the delimited
   ones) for the key so the comparison is allocation-cheap.
3. Refactor `ExecuteWithStatementReuse` (lines 77-113) into a state
   machine that:
   - Walks `_commands` in order.
   - Maintains a `currentShape` (string), a `currentSqlTemplate` (string,
     the *single-row* `VALUES (...)` template used by the existing reuse
     path), the count of rows currently buffered, and a `List<IReadOnlyModificationCommand>`
     of the buffered commands themselves.
   - For each command:
     - Compute its shape key.
     - If `command.EntityState != Added` **or** `command` has any `IsRead`
       column, **flush** the current buffer (see step 5), then run the
       command through the existing single-row path verbatim (preserve
       `BuildSqlTemplate` + `BindAllParameters` + `ExecuteStepAndRead`
       behavior).
     - Else if the buffer is empty, or the shape differs, or
       `count == MaxCoalescedRows`: flush the buffer, start a new buffer
       with this command.
     - Else: append this command to the buffer.
   - At the end, flush.
4. Add a private helper `BuildMultiRowSql(string singleRowTemplate,
   List<IColumnModification> writeColumns, int rowCount)` that:
   - Parses the existing single-row template only enough to find the
     `INSERT INTO <table> (<cols>) VALUES (...)` prefix and re-emits the
     `VALUES` list as `(...),(...)...` with a *new* positional parameter
     ordinal sequence (`$1..$N` total, where N = `rowCount * writeColumns.Count`).
   - **Easier alternative (preferred):** do not parse the existing
     template at all. Instead, extract the shared "build the prefix and
     column list" code path out of `BuildSqlTemplate` into a private
     helper, and have both the single-row path and the multi-row path
     call it. The multi-row path then loops to emit
     `($k, $k+1, ...), ($k+w, ...)` clauses.
5. Implement the **flush** action `FlushCoalescedInserts(...)`:
   - If `count == 0`: return.
   - If `count == 1`: execute that one command via the existing single-row
     path (so we don't pay the multi-row cost for trivially short batches).
   - Else:
     - Build the multi-row SQL string once.
     - Prepare a *new* statement (do not try to share with the single-row
       prepared cache; the SQL text differs by `count`). Cache the prepared
       statement keyed by `(shape, count)` only if straightforward; if not
       straightforward, prepare fresh and Dispose at end of flush.
     - For each buffered command, for each write column, call
       `BindValue(stmt, paramIndex++, ConvertToProviderValue(col, col.Value))`.
     - Call `stmt.Step()` exactly once. Multi-row `INSERT … VALUES`
       executes in a single step.
     - On non-zero `stepResult` other than success, throw a
       `DbUpdateException` whose message names the table and the row
       count attempted; include the inner exception.
     - Clear the buffer.
6. Leave the public surface (`Execute`, `ExecuteAsync`, `RequiresTransaction`,
   `TryAddCommand`, `Complete`) unchanged.
7. Do **not** change `DecentDBModificationCommandBatchFactory` or
   `DecentDBUpdateSqlGenerator`.

### Tests to add

Add to `bindings/dotnet/tests/DecentDB.EntityFrameworkCore.Tests/`. If a
file `BatchInsertTests.cs` already exists, append; otherwise create.

1. `Insert_OneEntity_UsesSingleRowPath` — `AddAsync`+`SaveChanges` for one
   entity. Expectation: behavior unchanged, one row inserted, returned PK
   populated when applicable.
2. `Insert_TwoEntitiesSameShape_UsesMultiRowPath` — `AddRange` of 2
   entities of the same type. Expectation: both rows visible after
   `SaveChanges`.
3. `Insert_257EntitiesSameShape_SplitsAtMaxCoalescedRows` — `AddRange` of
   257 entities. Expectation: 257 rows visible; assert that the binding
   issued at most ⌈257/256⌉ = 2 multi-row INSERTs by hooking
   `DiagnosticSource` events `Microsoft.EntityFrameworkCore.Database.Command.CommandExecuting`.
4. `Insert_MixedShapes_FlushesBetweenShapes` — `AddRange` containing 5
   `Artist`s, then 5 `Album`s, then 5 more `Artist`s. Expectation: 3 SQL
   statements observed (5-row, 5-row, 5-row), all rows present.
5. `Insert_WithReturning_UsesSingleRowPath` — entity with database-generated
   PK (`int identity`-equivalent). Expectation: behavior unchanged, PK
   populated on entity. Document inside the test that this case
   intentionally bypasses coalescing.
6. `Insert_MixedAddedAndModified_FlushesBeforeUpdate` — 3 `Add` then 1
   `Update` then 2 `Add`. Expectation: 3-row INSERT, 1-row UPDATE, 2-row
   INSERT, in that order.
7. `Insert_LargeBatch_PerformanceSanity` — `[Fact(Skip="perf-only")]`
   benchmark style: 10 000 entities, asserts insert wall-time is under
   2× the wall-time of a single `dbConn.ExecuteNonQueryAsync` issuing the
   same multi-row SQL by hand. (Detects regressions; not a strict perf
   gate.)

### Validation

```bash
cd /home/steven/source/decentdb
dotnet test bindings/dotnet/tests/DecentDB.EntityFrameworkCore.Tests/DecentDB.EntityFrameworkCore.Tests.csproj -c Release
python3 scripts/do-pre-commit-checks.py --mode fast
```

Then re-run the comparison benchmark and copy the new EF Core numbers
into this slice's PR description:
```bash
cd /tmp/tmp-opus47-decentdb-net-tests
dotnet run -c Release --project src/Runner/Runner.csproj -- --scale smoke --only EfCore
```
Expected smoke target: EF Core seed_songs r/s ≥ 50 000 (current: 2 200).
Expected full-scale target: EF Core total ≤ 250s (current: 1 350s).

### Acceptance criteria

- [ ] All seven new tests above pass.
- [ ] Existing `DecentDB.EntityFrameworkCore.Tests` continue to pass.
- [ ] `dotnet build -c Release` of the binding produces zero warnings
      (no new `EF1001`, `CS0618`, etc.).
- [ ] Smoke benchmark `seed_songs r/s` ≥ 50 000.
- [ ] No change to `DecentDBModificationCommandBatchFactory` public API.
- [ ] No new `unsafe`, no new dependency.

---

## Slice N2 — MicroOrm: `InsertManyAsync` multi-row `VALUES` + cached parameter shape

**Priority:** P0.
**Depends on:** none. (Independent of N1; same engine capability.)
**Estimated diff size:** ~120 lines.

### Problem

`DbSet<T>.InsertManyAsync` rebuilds the SQL prefix every call and
allocates a fresh `List<(string,object?,int?)>`, fresh `DbCommand`, and
fresh `DbParameter[]` **per row** in a loop. On the benchmark this caps
MicroOrm at ~25K rows/s vs ADO.NET's ~520K rows/s.

### Verified evidence

- `bindings/dotnet/src/DecentDB.MicroOrm/DbSet.cs:245-311` — current
  implementation. Note especially the per-row allocations on lines
  271-288.
- The single-row path on lines 176-231 shows what "good" looks like for
  MicroOrm style (cached `cols`/`vals`, single `cmd`).
- Engine accepts multi-row `VALUES` (see N1 verified evidence).

### Design decisions already made (do not relitigate)

1. `InsertManyAsync` uses a multi-row `VALUES (…),(…),…` statement, **not**
   a per-row prepared-statement reuse pattern. This matches N1 and avoids
   another round of fast-path rework.
2. Maximum rows per multi-row INSERT: `MaxRowsPerInsert = 256`. Same
   reasoning as N1.
3. Parameter shape (`cols`, `vals` template, per-property bind plan) is
   computed **once** at the top of `InsertManyAsync` and reused for the
   whole call. We do not yet hoist it to a per-`EntityMap` cache — that
   is a future optimization but not in this slice.
4. The integer-PK auto-omission behavior from `InsertAsync`
   (`IsDefaultIntegerPk`, lines 233-243) **is not** applied in this
   slice; `InsertManyAsync` requires the caller to supply PKs. This
   matches today's behavior and is documented in the rustdoc/XML doc on
   `InsertManyAsync`. (`RETURNING` for many-row INSERT is N5.)
5. Transaction handling stays as-is: own a transaction if there isn't one,
   otherwise enlist in `_context.CurrentTransaction`.

### Implementation steps

All edits in
`bindings/dotnet/src/DecentDB.MicroOrm/DbSet.cs`.

1. Add a private nested struct `InsertPlan` capturing:
   - `string SqlPrefix` — `"INSERT INTO <table> (col1, col2, …) VALUES "`.
   - `string SingleRowPlaceholder` — `"($1, $2, …)"` (1-based and shifted
     per row at render time; see step 4).
   - `PropertyMap[] BindProps` — the non-ignored properties in column
     order.
   - `int ColumnCount`.
2. In `InsertManyAsync`, build the `InsertPlan` once before the loop. Use
   a single `StringBuilder` shared with the row-emission step below.
3. Materialize `entities` into a `List<T>` once (single pass) so we can
   know the row count and slice it. If the caller passed an `ICollection<T>`
   use its `Count`; otherwise enumerate into a `List<T>`.
4. Loop in chunks of `MaxRowsPerInsert`. For each chunk:
   - Reset the `StringBuilder` to the `SqlPrefix`.
   - Append per-row parameter clauses with running ordinal `1..(chunk*ColumnCount)`.
     Format: `($k, $k+1, …, $k+columnCount-1)`, comma-separated between rows.
   - Build a single `DbCommand` using `CreateCommand(scope.Connection,
     sb.ToString(), parameters)` where `parameters` is a
     `List<(string,object?,int?)>` of size `chunk * ColumnCount` that you
     pre-size with `new List(...)(capacity)`.
   - Iterate the chunk's entities; for each entity iterate `BindProps`
     and append `("@p<ordinal>", value, MaxLength)` to the parameters
     list. Use a single running ordinal to match the SQL placeholders.
   - Validate non-nullability the same way as today (preserve the
     `ArgumentException` text exactly).
   - Attach the transaction if `tx != null` and `await
     cmd.ExecuteNonQueryAsync(cancellationToken)`.
5. Preserve the outer try/catch/finally rollback semantics on lines
   267-310 verbatim.
6. Update the XML doc comment on `InsertManyAsync` to state:
   - "Inserts entities in chunks of up to 256 rows per `INSERT … VALUES`
     statement."
   - "Auto-generated PKs are not supported by this method; pre-assign
     PKs on the entities or use `InsertAsync` per row. (See `InsertManyReturningAsync`
     once available.)"

### Tests to add

In `bindings/dotnet/tests/DecentDB.Tests/` (the MicroOrm test project lives
here), add `MicroOrm/InsertManyTests.cs`.

1. `InsertMany_Empty_NoOp` — passes an empty `IEnumerable<T>`. Expectation:
   no statements issued, no exception.
2. `InsertMany_OneRow_Works` — single entity in the enumerable.
   Expectation: row visible.
3. `InsertMany_257Rows_TwoStatements` — pass 257 rows. Expectation: 257
   rows visible; hook `_context.SqlExecuting` to count statements; assert
   exactly two `INSERT INTO` statements were issued.
4. `InsertMany_RollsBackOnNotNullViolation` — pass 5 rows where row 3
   has a `null` non-nullable property. Expectation: `ArgumentException`
   thrown; the table contains 0 rows after the call.
5. `InsertMany_EnlistsInOuterTransaction` — open a `BeginTransaction`,
   call `InsertManyAsync`, then `Rollback`. Expectation: 0 rows visible.

### Validation

```bash
cd /home/steven/source/decentdb
dotnet test bindings/dotnet/tests/DecentDB.Tests/DecentDB.Tests.csproj -c Release
python3 scripts/do-pre-commit-checks.py --mode fast
cd /tmp/tmp-opus47-decentdb-net-tests
dotnet run -c Release --project src/Runner/Runner.csproj -- --scale smoke --only MicroOrm
```
Expected smoke target: MicroOrm `seed_songs r/s` ≥ 200 000.

### Acceptance criteria

- [ ] All five new tests pass.
- [ ] Existing `DecentDB.Tests` (MicroOrm portion) pass.
- [ ] `seed_songs r/s` ≥ 200 000 in the comparison benchmark smoke run.
- [ ] No change to the public signature of `InsertManyAsync`.
- [ ] XML doc on `InsertManyAsync` mentions the 256-row chunk size and
      the lack of `RETURNING`.

---

## Slice N3 — MicroOrm POCO portability: ignore unmapped reference/collection properties

**Priority:** P1.
**Depends on:** none.
**Estimated diff size:** ~30 lines + tests.

### Problem

When a benchmark/app shares POCOs between EF Core (which auto-discovers
navigation properties like `public Artist Artist { get; set; }`) and
MicroOrm (which has no concept of navigations), MicroOrm's
`EntityMap` *includes* the navigation property as if it were a column.
This produces silent breakage: an `INSERT` tries to bind a value of type
`Artist` and the parameter conversion fails with a confusing message.
Workaround used by the benchmark was making navigation properties
`internal`, which is hostile to library design.

### Verified evidence

- `bindings/dotnet/src/DecentDB.MicroOrm/EntityMap.cs:34-61` — the
  property loop in `EntityMap`'s constructor accepts every `public`
  read/write instance property without filtering by type. Reference types
  and `IEnumerable<T>` properties go straight into `Properties`.

### Design decisions already made (do not relitigate)

1. The new rule: a property is **excluded** from `EntityMap.Properties`
   unless one of the following holds:
   - It has `[Column]` (explicit opt-in).
   - It has `[PrimaryKey]` (explicit opt-in).
   - Its `PropertyType` is one of the *natively-bindable* CLR types listed
     in step 2 below, **or** it is `Nullable<T>` of one of those, **or**
     it is an `enum`, **or** it is `byte[]`.
2. The natively-bindable CLR types (canonical, frozen by this slice):
   `bool`, `byte`, `sbyte`, `short`, `ushort`, `int`, `uint`, `long`,
   `ulong`, `float`, `double`, `decimal`, `string`, `Guid`, `DateTime`,
   `DateTimeOffset`, `DateOnly`, `TimeOnly`, `TimeSpan`. Source these
   from a single `static readonly HashSet<Type>` in
   `EntityMap` (or a new `BindableTypes.cs` if the file is added — single
   small file).
3. `[Ignore]` (`IgnoreAttribute`, see `Attributes.cs`) continues to work
   exactly as it does today and overrides everything.
4. This is a **non-breaking** change for any POCO whose properties were
   already all primitive/`string`/etc. — those still map. It *does* break
   any POCO where a user was relying on MicroOrm to auto-map a complex
   property type as JSON or as a converted blob; document this in the
   `MicroOrm/README.md` and surface it as a "POCO portability" note.
5. We do **not** add a `[NotMapped]`-attribute compatibility shim in this
   slice. (`[Ignore]` is the MicroOrm equivalent.)

### Implementation steps

All edits in `bindings/dotnet/src/DecentDB.MicroOrm/EntityMap.cs`.

1. Add a private static `IsBindable(Type)` helper:
   ```csharp
   private static readonly HashSet<Type> BindableTypes = new()
   {
       typeof(bool), typeof(byte), typeof(sbyte),
       typeof(short), typeof(ushort), typeof(int), typeof(uint),
       typeof(long), typeof(ulong), typeof(float), typeof(double),
       typeof(decimal), typeof(string), typeof(Guid),
       typeof(DateTime), typeof(DateTimeOffset),
       typeof(DateOnly), typeof(TimeOnly), typeof(TimeSpan),
       typeof(byte[]),
   };

   private static bool IsBindable(Type t)
   {
       var underlying = Nullable.GetUnderlyingType(t) ?? t;
       if (underlying.IsEnum) return true;
       return BindableTypes.Contains(underlying);
   }
   ```
2. In the property loop (currently lines 39-61) add a filter immediately
   after the indexer/CanRead/CanWrite checks:
   ```csharp
   var hasExplicitMap =
       prop.GetCustomAttribute<ColumnAttribute>() != null ||
       prop.GetCustomAttribute<PrimaryKeyAttribute>() != null;

   if (!hasExplicitMap && !IsBindable(prop.PropertyType))
   {
       continue; // navigation/collection/complex property — ignored
   }
   ```
3. Add an XML doc comment above the `EntityMap` class explaining the new
   filter rule (link to N3 in the comment).
4. Update `bindings/dotnet/src/DecentDB.MicroOrm/README.md` "Conventions"
   section with a short paragraph titled "POCO portability with EF Core"
   that explains the auto-skip rule and how to opt in via `[Column]`.

### Tests to add

In `bindings/dotnet/tests/DecentDB.Tests/MicroOrm/`:

1. `EntityMap_IgnoresReferenceNavigationByDefault` — define an entity
   `class Album { public int Id; public Artist Artist; public string Title; }`,
   call `EntityMap.For<Album>()`, assert `Properties` contains only `Id`
   and `Title`.
2. `EntityMap_IgnoresCollectionNavigationByDefault` — entity with
   `public List<Song> Songs { get; set; }`. Assert excluded.
3. `EntityMap_IncludesReferenceWhenColumnAttributePresent` — entity with
   `[Column("artist_blob")] public Artist Artist`. Assert included.
4. `EntityMap_IncludesEnums` — entity with `public Genre Genre { get; set; }`.
   Assert included (mapped as `long`, today's behavior).
5. `EntityMap_IncludesNullablePrimitives` — entity with `public int? Year`.
   Assert included.
6. `InsertMany_OnPocoWithNavigations_DoesNotBindNavigation` — full
   round-trip: build POCO `Album` with both an `Artist` reference *and*
   `ArtistId int`; insert it via `InsertManyAsync`; assert no exception
   and the row contains the right `ArtistId`.

### Validation

```bash
dotnet test bindings/dotnet/tests/DecentDB.Tests/DecentDB.Tests.csproj -c Release
python3 scripts/do-pre-commit-checks.py --mode fast
```

### Acceptance criteria

- [ ] All six tests pass.
- [ ] `EntityMap.For<T>()` no longer throws on a POCO whose only
      "extra" properties are navigations.
- [ ] No public-API breakage: `EntityMap` itself stays `internal`; only
      runtime mapping behavior changes.
- [ ] README updated with the "POCO portability with EF Core" paragraph.

---

## Slice N4 — MicroOrm: `QueryRawAsync<T>` for keyless DTOs

**Priority:** P1.
**Depends on:** N3 is *not* required (independent), but landing N3 first
gives a cleaner test surface.
**Estimated diff size:** ~80 lines.

### Problem

`DecentDBContext.QueryAsync<T>` requires `T` to have a `[PrimaryKey]` or
a property literally named `Id` (case-sensitive), because `EntityMap.For<T>`
throws if no PK is found. This is correct for write-side mapping but
wrong for read-only DTOs used to materialize view rows or aggregate
results.

### Verified evidence

- `bindings/dotnet/src/DecentDB.MicroOrm/EntityMap.cs:66-69` —
  `if (PrimaryKey == null) throw new InvalidOperationException(...)`.
- `bindings/dotnet/src/DecentDB.MicroOrm/DecentDBContext.cs:210-223` —
  `QueryAsync<T>` calls `EntityMap.For<T>()`, hitting the throw for
  PK-less DTOs.
- `FastMaterializer<T>.Bind` (`FastMaterializer.cs:14-41`) does *not*
  itself need a PK; it iterates `template.MappedProperties` from
  `EntityMap` but never references `PrimaryKey`. So the materializer is
  ready; only the `EntityMap` constructor blocks us.

### Design decisions already made (do not relitigate)

1. Add a new `EntityMap.ForReadOnly<T>()` static method that returns an
   `EntityMap` whose `PrimaryKey` is `null` and whose `Properties` are
   computed using the *same* filtering rules as N3 (or, if N3 is not yet
   merged, the existing rules with the extra `if (PrimaryKey == null)`
   branch skipped).
2. The read-only `EntityMap` is cached in a *separate* dictionary from
   the write-side cache (so the same `T` can have one of each).
3. Add a new method `Task<List<T>> QueryRawAsync<T>(string sql, params object?[] args)`
   on `DecentDBContext` (no `where T : class, new()` constraint relaxation
   — keep `where T : class, new()` because `FastMaterializer` requires
   parameterless construction).
4. Existing `QueryAsync<T>` keeps its current PK-required semantics — do
   **not** silently relax it. Callers who want keyless materialization
   must explicitly switch to `QueryRawAsync<T>`. This is the safer choice
   and avoids surprising behavior shifts for existing users.

### Implementation steps

All edits in
`bindings/dotnet/src/DecentDB.MicroOrm/EntityMap.cs` and
`bindings/dotnet/src/DecentDB.MicroOrm/DecentDBContext.cs`.

1. In `EntityMap.cs`:
   - Add `private static readonly ConcurrentDictionary<Type, EntityMap> ReadOnlyCache = new();`.
   - Refactor the constructor to take a private `bool requirePrimaryKey`
     parameter (default `true`). When `false`, omit the throw on
     lines 66-69.
   - Add `public static EntityMap ForReadOnly<T>() => ForReadOnly(typeof(T));`.
   - Add `public static EntityMap ForReadOnly(Type t) =>
     ReadOnlyCache.GetOrAdd(t, static t => new EntityMap(t, requirePrimaryKey: false));`.
2. In `DecentDBContext.cs`, add directly after `QueryAsync<T>`:
   ```csharp
   public async Task<List<T>> QueryRawAsync<T>(string sql, params object?[] args)
       where T : class, new()
   {
       var map = EntityMap.ForReadOnly<T>();
       using var scope = AcquireConnectionScope();
       using var cmd = BuildRawCommand(scope.Connection, sql, args);
       using var reader = await cmd.ExecuteReaderAsync();
       var mapper = FastMaterializer<T>.Bind(map, reader);
       var list = new List<T>();
       while (await reader.ReadAsync())
       {
           list.Add(mapper(reader));
       }
       return list;
   }
   ```
3. Add an XML doc comment on `QueryRawAsync<T>` clearly stating: "Use
   this for read-only DTOs that have no primary key (view rows, aggregate
   projections). For write-side mapped entities use `Set<T>()` or
   `QueryAsync<T>`."

### Tests to add

In `bindings/dotnet/tests/DecentDB.Tests/MicroOrm/`:

1. `QueryRawAsync_MapsViewRowsWithoutPK` — define a DTO
   `class TopArtist { public string ArtistName; public long SongCount; }`.
   Run `await ctx.QueryRawAsync<TopArtist>("SELECT artist_name, song_count
   FROM v_top_artists")` against a seeded test DB. Assert results are
   populated correctly.
2. `QueryRawAsync_RespectsColumnAttribute` — DTO has
   `[Column("song_count")] public long Songs;`. Assert mapping works.
3. `QueryAsync_StillRequiresPrimaryKey` — DTO with no PK. Assert
   `InvalidOperationException` is still thrown by `QueryAsync<T>` (proves
   N4 is opt-in via the new method, not a behavior shift).
4. `QueryRawAsync_IgnoresNavigationProperties` — only meaningful if N3
   is merged; if so, DTO with a navigation property still works.

### Validation

```bash
dotnet test bindings/dotnet/tests/DecentDB.Tests/DecentDB.Tests.csproj -c Release
python3 scripts/do-pre-commit-checks.py --mode fast
```

### Acceptance criteria

- [ ] All four tests pass.
- [ ] `QueryAsync<T>` behavior is unchanged.
- [ ] `EntityMap.For<T>()` behavior is unchanged.
- [ ] `EntityMap.ForReadOnly<T>()` is documented as the keyless-DTO
      entry point.
- [ ] `DecentDBContext.QueryRawAsync<T>` has a clear XML doc.

---

## Slice N5 — MicroOrm: `InsertManyAsync` `RETURNING` parity with `InsertAsync`

**Priority:** P1.
**Depends on:** N2 (multi-row `VALUES` path).
**Estimated diff size:** ~80 lines.

### Problem

`InsertAsync` already handles auto-generated integer PKs by appending
`RETURNING <pk_col>` and reading the value back into the entity (see
`DbSet.cs:182-225`). `InsertManyAsync` has no such path; users must
either pre-assign PKs or fall back to per-row `InsertAsync`, losing the
N2 batching win.

### Verified evidence

- Single-row path: `bindings/dotnet/src/DecentDB.MicroOrm/DbSet.cs:176-231`.
- Multi-row path after N2 lands: `DbSet.cs:245-…`.
- Engine `RETURNING` works on multi-row `INSERT` (the executor returns
  one row per inserted row in declaration order; this is already covered
  by engine tests for `RETURNING`).

### Design decisions already made (do not relitigate)

1. Add a new public method
   `Task InsertManyReturningAsync(IList<T> entities, CancellationToken ct = default)`.
   Behavior:
   - Requires `_map.PrimaryKey != null && IsDefaultIntegerPk(...)` for
     **every** entity in the list. If any entity has a non-default PK,
     throw `InvalidOperationException` with a message naming the index
     of the first offending entity.
   - Emits `INSERT INTO … (cols-without-pk) VALUES (…),(…)… RETURNING <pk_col>`.
   - Reads the returned PKs back in row order and assigns them to the
     entities.
2. The original `InsertManyAsync(IEnumerable<T>)` keeps its behavior
   (no `RETURNING`, requires pre-assigned PKs). It is the fast path; the
   new `InsertManyReturningAsync(IList<T>)` is the convenience path.
3. The new method takes `IList<T>` (not `IEnumerable<T>`) because we need
   ordinal row→entity matching after `RETURNING`, which is harder to do
   safely with a lazy enumerable.

### Implementation steps

All edits in `bindings/dotnet/src/DecentDB.MicroOrm/DbSet.cs`.

1. Refactor the multi-row SQL builder from N2 into a helper
   `BuildMultiRowInsertSql(IReadOnlyList<PropertyMap> bindProps, int rowCount,
   string? returningColumn)` that emits an optional `RETURNING <col>`
   suffix when `returningColumn` is non-null.
2. Add the new `InsertManyReturningAsync` method:
   - Validate PK status of every entity up front.
   - Slice into chunks of `MaxRowsPerInsert`.
   - For each chunk:
     - Build SQL with `returningColumn = _map.PrimaryKeyColumnName`.
     - Use the bind-props list **excluding** the PK property.
     - Execute via `cmd.ExecuteReaderAsync(...)`.
     - Walk the reader; for each row, read column 0, assign to the
       corresponding entity's PK property via reflection (matching the
       single-row path's `Convert.ChangeType(returnedId, pk.PropertyType)`).
     - Throw `InvalidOperationException` if the reader produced fewer
       rows than the chunk size.
3. Preserve the same transaction semantics as `InsertManyAsync`.

### Tests to add

1. `InsertManyReturning_AssignsPKsInOrder` — 5 entities with PK==0;
   after the call, PKs are populated and strictly increasing.
2. `InsertManyReturning_RejectsPreAssignedPK` — 3 entities; entity[1] has
   PK==42 pre-assigned. Expectation: `InvalidOperationException` whose
   message names index 1; no rows inserted.
3. `InsertManyReturning_TwoChunks_PKsContiguous` — 257 entities; PKs are
   strictly increasing across the chunk boundary.
4. `InsertManyReturning_RollsBackOnNotNullViolation` — same as N2 test
   #4 but using the returning method; verify 0 rows in DB after.

### Validation

```bash
dotnet test bindings/dotnet/tests/DecentDB.Tests/DecentDB.Tests.csproj -c Release
python3 scripts/do-pre-commit-checks.py --mode fast
```

### Acceptance criteria

- [ ] All four tests pass.
- [ ] `InsertManyAsync` behavior is unchanged.
- [ ] XML doc on `InsertManyReturningAsync` cross-references
      `InsertManyAsync` and explains when to choose each.

---

## Slice N6 — EF Core + MicroOrm: handle non-deterministic CLR calls in LINQ predicates

**Priority:** P1.
**Depends on:** none.
**Estimated diff size:** ~100 lines.

### Problem

`.Where(x => x.Id == random.Next(0, 1000))` (and similar uses of
`DateTime.Now`, `Guid.NewGuid()`, etc.) inside a LINQ predicate produces
either a confusing translator error in `DecentDB.EntityFrameworkCore`
or a flat-out misleading error in `DecentDB.MicroOrm.LinqProvider`. EF
Core's SQLite/SQL Server providers handle this by *evaluating the call
once on the client* and inlining the constant; ours does not.

### Verified evidence

- EF Core provider has no method-call translator for `System.Random`:
  - `bindings/dotnet/src/DecentDB.EntityFrameworkCore/Query/Internal/DecentDBMethodCallTranslatorProvider.cs:1-15`
    only registers `DecentDBStringMethodTranslator`, `DecentDBMathTranslator`,
    `DecentDBWindowFunctionTranslator`.
- MicroOrm: `bindings/dotnet/src/DecentDB.MicroOrm/ExpressionSqlBuilder.cs`
  walks the expression tree and falls through to a generic
  `NotSupportedException` when it sees a `MethodCallExpression` it
  doesn't recognize.

### Design decisions already made (do not relitigate)

1. **Both bindings adopt the same rule:** any `MethodCallExpression` (or
   `MemberExpression` accessing a non-static field/property of a captured
   variable) whose target is **not** a parameter of the lambda **and**
   whose result type is one of the natively-bindable types (see N3, step
   2) is **eagerly evaluated on the client** at translation time, and
   the result is inlined as a parameter (`@p<n>` for MicroOrm,
   `SqlConstantExpression`/`SqlParameterExpression` for EF Core).
2. If a method call cannot be evaluated (it depends on the lambda
   parameter) **and** there is no registered translator, throw
   `NotSupportedException` with a message of the form:
   `"DecentDB cannot translate '<full method signature>' to SQL. Either
   call this method outside the LINQ expression and capture the result,
   or use a server-side equivalent."`
3. The eager-evaluation is performed via
   `Expression.Lambda(expression).Compile().DynamicInvoke()`. This is
   the same approach EF Core's `SqliteEvaluatableExpressionFilter` uses.
4. We do **not** add a new translator for `System.Random` (random in SQL
   would tie to engine RNG semantics that we don't want to pin yet).
   The fix is purely "evaluate-once and inline."

### Implementation steps

#### EF Core side

1. Create
   `bindings/dotnet/src/DecentDB.EntityFrameworkCore/Query/Internal/DecentDBEvaluatableExpressionFilter.cs`:
   ```csharp
   using System.Linq.Expressions;
   using Microsoft.EntityFrameworkCore.Query;

   namespace DecentDB.EntityFrameworkCore.Query.Internal;

   public sealed class DecentDBEvaluatableExpressionFilter : RelationalEvaluatableExpressionFilter
   {
       public DecentDBEvaluatableExpressionFilter(
           EvaluatableExpressionFilterDependencies deps,
           RelationalEvaluatableExpressionFilterDependencies relDeps)
           : base(deps, relDeps) { }

       public override bool IsEvaluatableExpression(
           Expression expression, IModel model)
       {
           if (expression is MethodCallExpression mc)
           {
               // System.Random is non-deterministic, but evaluating once
               // on the client matches SQLite/SQL Server provider behavior.
               if (mc.Object is not null &&
                   mc.Object.Type == typeof(System.Random))
               {
                   return true;
               }
           }
           return base.IsEvaluatableExpression(expression, model);
       }
   }
   ```
2. Register the filter in
   `bindings/dotnet/src/DecentDB.EntityFrameworkCore/Extensions/DecentDBServiceCollectionExtensions.cs`
   alongside the other `TryAdd` calls. Pattern:
   ```csharp
   builder.TryAdd<IEvaluatableExpressionFilter, DecentDBEvaluatableExpressionFilter>();
   ```
   Locate the closest existing `TryAdd<I…, …>()` block to keep the diff
   small.

#### MicroOrm side

1. In `bindings/dotnet/src/DecentDB.MicroOrm/ExpressionSqlBuilder.cs`,
   in the `MethodCallExpression` visitor branch:
   - Before falling through to "not supported", check whether **none of**
     the call's `Object` and `Arguments` reference any
     `ParameterExpression` from the outer lambda. If true, evaluate the
     call via `Expression.Lambda(expression).Compile().DynamicInvoke()`
     and emit it as a parameter.
   - If the eager evaluation throws, wrap and rethrow as
     `NotSupportedException` with the message from step 2 of "Design
     decisions" above.
2. In the unsupported fall-through, change the existing
   `NotSupportedException` message to match the canonical text from step 2.

### Tests to add

#### EF Core

In `bindings/dotnet/tests/DecentDB.EntityFrameworkCore.Tests/`:

1. `Where_WithRandomNext_EvaluatesOnceClientSide` — the predicate
   `.Where(a => a.Id == _random.Next(0, 100))` translates without
   exception; the same `Random` instance produces a reproducible result
   across two calls when seeded with a constant (proves it was evaluated
   once).
2. `Where_WithDateTimeNow_EvaluatesOnceClientSide` — predicate
   `.Where(a => a.CreatedAt < DateTime.UtcNow)` translates and runs.
3. `Where_WithUnsupportedInstanceMethod_ThrowsHelpfulMessage` — predicate
   `.Where(a => a.Title.IndexOfAny(new[] { 'a' }) > 0)`. Expectation:
   `NotSupportedException` whose message contains `"IndexOfAny"` and the
   guidance text from design decision #2.

#### MicroOrm

In `bindings/dotnet/tests/DecentDB.Tests/MicroOrm/`:

1. `Where_WithRandomNext_EvaluatesOnceClientSide` — analogous.
2. `Where_WithUnsupportedMethod_ThrowsHelpfulMessage` — analogous.

### Validation

```bash
dotnet test bindings/dotnet/tests/ -c Release
python3 scripts/do-pre-commit-checks.py --mode fast
```

### Acceptance criteria

- [ ] All five tests pass.
- [ ] No regression in existing translator tests.
- [ ] The unsupported-method exception message contains the *exact*
      guidance text from design decision #2 (so it is greppable in user
      bug reports).

---

## Slice N7 — EF Core: `UseDecentDB` accepts bare paths consistently with ADO.NET

**Priority:** P1.
**Depends on:** none.
**Estimated diff size:** ~30 lines.

### Problem

`DecentDB.AdoNet.DecentDBConnection("/tmp/foo.ddb")` opens
`/tmp/foo.ddb`. But
`UseDecentDB("/tmp/foo.ddb")` silently treats `"/tmp/foo.ddb"` as a
connection string with no `Data Source` key, and EF Core ends up opening
a *different* file (or failing). The benchmark's debugging trail for this
took hours.

### Verified evidence

- `bindings/dotnet/src/DecentDB.EntityFrameworkCore/DecentDBDbContextOptionsBuilderExtensions.cs:11-27`
  passes the raw string straight into the extension via
  `extension.WithConnectionString(connectionString)`.
- `bindings/dotnet/src/DecentDB.MicroOrm/DecentDBContext.cs:60-74` already
  has a `LooksLikeConnectionString` heuristic ("a `=` means it's a
  connection string; otherwise it's a path") and prepends `Data Source=`
  if needed. **This same heuristic must be applied in EF Core's
  `UseDecentDB`.**

### Design decisions already made (do not relitigate)

1. The heuristic is the **same** as MicroOrm's: if the input string does
   not contain `=`, treat it as a bare path and rewrite it to
   `"Data Source=<input>"`.
2. The rewrite happens **only** in the string-overload of `UseDecentDB`.
   The `DecentDBConnectionStringBuilder` overload and the `DbConnection`
   overload are unaffected.
3. The heuristic is implemented in a shared internal helper in
   `DecentDB.AdoNet` (so MicroOrm can also use it instead of duplicating
   the logic). New file:
   `bindings/dotnet/src/DecentDB.AdoNet/Internal/ConnectionStringHelper.cs`.

### Implementation steps

1. Add
   `bindings/dotnet/src/DecentDB.AdoNet/Internal/ConnectionStringHelper.cs`:
   ```csharp
   namespace DecentDB.AdoNet.Internal;

   internal static class ConnectionStringHelper
   {
       /// <summary>
       /// If <paramref name="input"/> looks like a path (contains no '='),
       /// returns <c>"Data Source=&lt;input&gt;"</c>. Otherwise returns
       /// the input unchanged.
       /// </summary>
       public static string NormalizeToConnectionString(string input)
       {
           if (string.IsNullOrWhiteSpace(input))
               throw new System.ArgumentException(
                   "Connection string or data source path must be provided.",
                   nameof(input));
           return input.Contains('=')
               ? input
               : "Data Source=" + input;
       }
   }
   ```
2. In
   `bindings/dotnet/src/DecentDB.EntityFrameworkCore/DecentDBDbContextOptionsBuilderExtensions.cs`,
   change line 21 from
   `extension = extension.WithConnectionString(connectionString);`
   to
   ```csharp
   extension = extension.WithConnectionString(
       ConnectionStringHelper.NormalizeToConnectionString(connectionString));
   ```
   Add `using DecentDB.AdoNet.Internal;` at the top.
3. In `bindings/dotnet/src/DecentDB.MicroOrm/DecentDBContext.cs`, replace
   the inline `LooksLikeConnectionString`/`Data Source=` logic on lines
   60-62 with a call to the new helper. Delete the now-unused
   `LooksLikeConnectionString` method **only if** no other code in the
   file references it (verify with `grep`).
4. The helper is `internal`. To use it from `DecentDB.MicroOrm` and
   `DecentDB.EntityFrameworkCore`, add `[assembly: InternalsVisibleTo]`
   entries to `DecentDB.AdoNet.csproj`:
   ```xml
   <ItemGroup>
     <InternalsVisibleTo Include="DecentDB.MicroOrm" />
     <InternalsVisibleTo Include="DecentDB.EntityFrameworkCore" />
   </ItemGroup>
   ```
   If those entries already exist for other reasons, do nothing.

### Tests to add

In `bindings/dotnet/tests/DecentDB.EntityFrameworkCore.Tests/`:

1. `UseDecentDB_AcceptsBarePath` — call `UseDecentDB("/tmp/<unique>.ddb")`,
   open context, insert a row, dispose, reopen, read it back. Assert the
   row is present.
2. `UseDecentDB_AcceptsDataSourceConnectionString` —
   `UseDecentDB("Data Source=/tmp/<unique>.ddb")`, same round-trip;
   asserts behavior is identical.
3. `UseDecentDB_BothFormsTargetSameFile` — open via bare path, insert,
   close; open via `Data Source=` form pointing at the *same* path,
   read; assert the row is visible. (This is the regression-detection
   test for the original silent-different-file bug.)

In `bindings/dotnet/tests/DecentDB.Tests/MicroOrm/`:

1. `DecentDBContext_AcceptsBarePathStillWorks` — explicit regression
   test for the helper extraction.

### Validation

```bash
dotnet test bindings/dotnet/tests/ -c Release
python3 scripts/do-pre-commit-checks.py --mode fast
```

### Acceptance criteria

- [ ] All four tests pass.
- [ ] `LooksLikeConnectionString` is removed from `DecentDBContext.cs`
      (or kept only as a thin forwarder to the helper).
- [ ] `ConnectionStringHelper.NormalizeToConnectionString` has an XML
      doc comment.
- [ ] No new public API surface added.

---

## Slice N8 — AdoNet: strongly-typed `Pooling` and tighter `CommandTimeout` docs on `DecentDBConnectionStringBuilder`

**Priority:** P2.
**Depends on:** N7 (uses the same internal helper file location).
**Estimated diff size:** ~50 lines.

### Problem

`Pooling=true|false` is honored by `DecentDB.MicroOrm` (see
`DecentDBContext.cs:63-65`), but `DecentDB.AdoNet.DecentDBConnectionStringBuilder`
exposes no strongly-typed `Pooling` property. Users discover the keyword
only by reading source. The same builder also under-documents
`CommandTimeout`.

### Verified evidence

- Builder today: `bindings/dotnet/src/DecentDB.AdoNet/DecentDBConnectionStringBuilder.cs`
  has no `Pooling` member. The `CommandTimeout` getter on line 57 silently
  defaults to `30` with no XML doc.
- MicroOrm consumes `Pooling`:
  `bindings/dotnet/src/DecentDB.MicroOrm/DecentDBContext.cs:63-65`,
  case-insensitive parse via `TryGetBoolOption`.

### Design decisions already made (do not relitigate)

1. Add a public `bool Pooling { get; set; }` property; default value
   when the key is absent is `true` (matches MicroOrm's
   `bool pooling = true` default).
2. The setter writes the literal string `"True"` or `"False"` (matching
   `DbConnectionStringBuilder` conventions for bool values). The getter
   accepts `True`, `False`, `1`, `0` (case-insensitive) — same parsing
   rules as MicroOrm's `TryGetBoolOption`.
3. The `Pooling` keyword's behavior in `DecentDB.AdoNet.DecentDBConnection`
   itself is **not** added in this slice (today the AdoNet binding has
   no pool; only MicroOrm interprets the keyword). The builder simply
   makes the keyword discoverable. Add an XML doc on the property
   explicitly noting "currently consumed by `DecentDB.MicroOrm` only;
   ADO.NET ignores this key. Tracked for a future ADO.NET pool
   implementation."
4. Add XML docs to all existing properties (`DataSource`, `CacheSize`,
   `Logging`, `LogLevel`, `CommandTimeout`).

### Implementation steps

1. In
   `bindings/dotnet/src/DecentDB.AdoNet/DecentDBConnectionStringBuilder.cs`:
   - Add `private const string PoolingKey = "Pooling";`.
   - Add the new property after `CommandTimeout`:
     ```csharp
     /// <summary>
     /// When true, MicroOrm reuses a single open connection across
     /// operations. ADO.NET-only callers currently ignore this key.
     /// Default: true.
     /// </summary>
     public bool Pooling
     {
         get
         {
             if (!TryGetValue(PoolingKey, out var v) || v is not string s)
                 return true;
             if (bool.TryParse(s, out var b)) return b;
             if (s == "1") return true;
             if (s == "0") return false;
             return true;
         }
         set => this[PoolingKey] = value ? "True" : "False";
     }
     ```
   - Add XML doc comments above every existing property.
2. Update `bindings/dotnet/src/DecentDB.AdoNet/README.md` (or equivalent
   docs file) "Connection-string keys" section to enumerate every key
   the builder exposes, including `Pooling`.

### Tests to add

In `bindings/dotnet/tests/DecentDB.Tests/AdoNet/`:

1. `ConnectionStringBuilder_Pooling_DefaultsTrue` — new builder, no key
   set; assert `Pooling == true`.
2. `ConnectionStringBuilder_Pooling_RoundTripsTrue` — `b.Pooling = true;
   var cs = b.ConnectionString;` then parse `cs` into a new builder and
   assert `Pooling == true`.
3. `ConnectionStringBuilder_Pooling_AcceptsZeroAndOne` — direct connection
   string `"Pooling=0;Data Source=x"`; assert `Pooling == false`. Same
   for `"Pooling=1"`.
4. `ConnectionStringBuilder_Pooling_FalseFlowsToMicroOrm` — construct
   builder with `Pooling = false`, hand its `ConnectionString` to
   `new DecentDBContext(cs)`, exercise an operation that takes the
   non-pooled branch (e.g., asserting `_pooling == false` via
   reflection if necessary, or by observing two `Open` calls in
   `SqlExecuting` events).

### Validation

```bash
dotnet test bindings/dotnet/tests/DecentDB.Tests/DecentDB.Tests.csproj -c Release
python3 scripts/do-pre-commit-checks.py --mode fast
```

### Acceptance criteria

- [ ] All four tests pass.
- [ ] Every property on `DecentDBConnectionStringBuilder` has an XML
      doc comment.
- [ ] README enumerates every supported connection-string key.

---

## Slice N9 — Engine: accept `CREATE VIEW IF NOT EXISTS` (parser-only addition)

**Priority:** P2.
**Depends on:** none.
**Estimated diff size:** ~25 lines + tests.

### Problem

PostgreSQL itself does not accept `CREATE VIEW IF NOT EXISTS`; it only
accepts `CREATE OR REPLACE VIEW`. The engine inherits this from
`pg_query` and surfaces a confusing parse error to .NET callers issuing
idempotent DDL. We support `CREATE TABLE IF NOT EXISTS` and
`CREATE INDEX IF NOT EXISTS`, so closing the gap for views is a small
ergonomic win.

### Verified evidence

- `crates/decentdb/src/sql/normalize.rs:757-779` — `normalize_create_view`
  reads `statement.replace` (so `CREATE OR REPLACE VIEW` works) but
  there is no `if_not_exists` field on `CreateViewStatement`.
- For `CREATE TABLE IF NOT EXISTS` precedent see `normalize.rs:3446`.

### Design decisions already made (do not relitigate)

1. Implementation strategy: **pre-parser textual rewrite**. Before
   handing the SQL string to `pg_query`, detect the leading pattern
   `^\s*CREATE\s+VIEW\s+IF\s+NOT\s+EXISTS\s+` (case-insensitive) and:
   - Strip the `IF NOT EXISTS` tokens.
   - Set a `if_not_exists: bool` flag on the resulting `CreateViewStatement`.
   - At execution time, if the view already exists, return success
     (matching the `CREATE TABLE IF NOT EXISTS` semantics).
2. The pre-parser rewrite goes in the same module that already does the
   `CREATE … IF NOT EXISTS` rewrites for the `pg_query` workaround, if
   any exists. Otherwise it goes immediately above
   `normalize_create_view` as a small standalone helper called from
   `db.rs` before `pg_query::parse(...)`. **Investigate first** — if a
   centralized pre-parser shim already exists, use it; if not, place the
   rewrite in a new helper `crates/decentdb/src/sql/preparse.rs` and
   call it from the SQL entry point.
3. Add `pub if_not_exists: bool` to `CreateViewStatement`.
4. The execution path in `crates/decentdb/src/exec/ddl.rs` (or wherever
   `CreateView` is dispatched) checks `if_not_exists` and on
   "view already exists" returns `Ok(())` instead of erroring. Look for
   the existing `CreateTable` / `if_not_exists` precedent in the same
   file and mirror it exactly.

### Implementation steps

1. Locate the `CREATE TABLE IF NOT EXISTS` short-circuit in the executor.
   Use `grep -n "if_not_exists" crates/decentdb/src/exec/`. Mirror the
   structure for `CreateView`.
2. Add the field on `CreateViewStatement` in
   `crates/decentdb/src/sql/ast.rs` (or wherever the struct is
   defined; `grep -n "struct CreateViewStatement" crates/decentdb/src/`).
3. Add the pre-parser rewrite. The simplest correct form:
   ```rust
   fn rewrite_create_view_if_not_exists(sql: &str) -> (String, bool) {
       // Match a leading "CREATE VIEW IF NOT EXISTS" ignoring leading
       // whitespace and ASCII case. Only rewrite the first occurrence;
       // we do not attempt to handle multi-statement scripts here.
       let trimmed = sql.trim_start();
       let prefix_len = sql.len() - trimmed.len();
       const NEEDLE: &str = "CREATE VIEW IF NOT EXISTS ";
       if trimmed.len() >= NEEDLE.len()
           && trimmed[..NEEDLE.len()].eq_ignore_ascii_case(NEEDLE)
       {
           let rest = &trimmed[NEEDLE.len()..];
           let rewritten = format!("{}CREATE VIEW {}", &sql[..prefix_len], rest);
           return (rewritten, true);
       }
       (sql.to_string(), false)
   }
   ```
   Apply it at the top of the SQL parsing entry point. Plumb the `bool`
   into the resulting `CreateViewStatement` field.
4. Update `normalize_create_view` to honor the new field (just propagate
   it).
5. In the executor, on duplicate-view error and `if_not_exists == true`,
   short-circuit to success.

### Tests to add

In `crates/decentdb/src/sql/normalize_more_tests.rs` (or wherever
`CREATE VIEW` parser tests live):

1. `create_view_if_not_exists_parses` — `"CREATE VIEW IF NOT EXISTS v
   AS SELECT 1"` parses to `CreateViewStatement { if_not_exists: true,
   view_name: "v", … }`.
2. `create_view_if_not_exists_case_insensitive` — same with
   `"create view IF not exists v AS SELECT 1"`.
3. `create_view_without_if_not_exists_unchanged` — confirms
   `if_not_exists: false` on the bare form.

In `crates/decentdb/tests/` (integration):

4. `create_view_if_not_exists_idempotent` — issue the same `CREATE VIEW
   IF NOT EXISTS v AS SELECT 1` twice; both succeed; `v` exists.
5. `create_view_if_not_exists_does_not_replace` — create `v AS SELECT 1`,
   then issue `CREATE VIEW IF NOT EXISTS v AS SELECT 2`; `v` still
   selects 1 (we do *not* replace, matching `CREATE TABLE IF NOT EXISTS`
   semantics).

### Validation

```bash
cd /home/steven/source/decentdb
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
python3 scripts/do-pre-commit-checks.py --mode fast
```

### Acceptance criteria

- [ ] All five tests pass.
- [ ] `CREATE OR REPLACE VIEW` behavior is unchanged.
- [ ] `cargo clippy` is clean.
- [ ] No on-disk format change. No WAL change. No C ABI change.
      (This slice is parser+executor only; no ADR is required.)

---

## Slice N10 — Engine + AdoNet: improve subquery-in-FROM error context

**Priority:** P2.
**Depends on:** none.
**Estimated diff size:** ~30 lines.

### Problem

`SELECT AVG(cnt) FROM (SELECT COUNT(*) AS cnt FROM t)` fails. The engine
already returns an informative message (`"subqueries in FROM require an
alias"`), but it surfaces in .NET as a `DecentDBException` with no
position information. New users blame the binding; the message is
correct.

### Verified evidence

- Engine error already informative:
  `crates/decentdb/src/sql/normalize.rs:1134` —
  `unsupported("subqueries in FROM require an alias")`.

### Design decisions already made (do not relitigate)

1. The engine error message is fine; do **not** rewrite it.
2. Add the **column or row position** of the offending `RangeSubselect`
   to the error message (extracted from `range.location` if present in
   the `pg_query` `RangeSubselect` proto). Format:
   `"subqueries in FROM require an alias (use ` ` ) AS some_name`); near
   character N"`.
3. In `bindings/dotnet/src/DecentDB.AdoNet/DecentDBCommand.cs` (or
   wherever the engine error is wrapped into `DecentDBException`),
   ensure the `Message` property exposes the engine message verbatim so
   the position info reaches users.

### Implementation steps

1. In `crates/decentdb/src/sql/normalize.rs:1123-1138`, capture
   `range.location` (an `i32` position into the SQL text). Pass it to a
   helper that formats the error with `near character N` if `location >= 0`.
2. Mirror the same change for `range.subquery is missing its SELECT`
   (line 1128) and the parallel error on line 1518/1523 if trivially
   doable; otherwise leave those alone in this slice.
3. In `bindings/dotnet/src/DecentDB.AdoNet/DecentDBCommand.cs`, find the
   point where engine errors are mapped into `DecentDBException` and
   verify the engine `Message` is forwarded unchanged. (No code change
   if it already is; add a regression test instead.)

### Tests to add

1. `crates/decentdb/src/sql/normalize.rs` test
   `subquery_in_from_without_alias_includes_position` — assert the error
   string contains `"AS some_name"` example and `"near character"`.
2. `bindings/dotnet/tests/DecentDB.Tests/AdoNet/SqlErrorMessageTests.cs`:
   `MissingSubqueryAlias_PropagatesEngineMessage` — execute the failing
   SQL via `DecentDBCommand`, assert `DecentDBException.Message`
   contains `"alias"`.

### Validation

```bash
cd /home/steven/source/decentdb
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
dotnet test bindings/dotnet/tests/DecentDB.Tests/DecentDB.Tests.csproj -c Release
python3 scripts/do-pre-commit-checks.py --mode fast
```

### Acceptance criteria

- [ ] Both tests pass.
- [ ] No other engine error message is altered.
- [ ] No on-disk format change. No WAL change. No C ABI change.

---

## Slice N11 — Bindings docs: feature-parity matrix and POCO portability notes

**Priority:** P3.
**Depends on:** N1, N2, N3, N4, N5, N7 — anything that lands here should
be reflected in the matrix immediately.
**Estimated diff size:** ~150 lines of markdown.

### Problem

Engineers picking a binding cannot tell at a glance which features each
.NET surface supports. The benchmark log is the de facto reference today.

### Implementation steps

1. Create `bindings/dotnet/README.md` (top-level for the .NET surface).
   It already might exist as a stub; if so, edit. If not, create.
2. Sections to include, with **exact** headings:
   - `# DecentDB for .NET`
   - `## Choosing a binding` — one paragraph each on AdoNet, MicroOrm,
     EntityFrameworkCore, with one-line "use this when…" recommendations.
   - `## Feature parity matrix` — table with columns
     `[ Feature | AdoNet | MicroOrm | EF Core ]`. Rows (initial set):
     - `Single-row INSERT`
     - `Bulk INSERT (multi-row VALUES)`
     - `RETURNING (single row)`
     - `RETURNING (bulk)`
     - `Async transactions`
     - `IDbBatch`
     - `LINQ where/order/skip/take`
     - `LINQ aggregates`
     - `Streaming reads`
     - `Connection pooling`
     - `View querying (keyless DTO)`
     - `Diagnostic events (`SqlExecuting`/`SqlExecuted`)`
     Use `✅`, `⚠️ partial`, `❌` consistently.
   - `## Connection strings` — paragraph noting that bare paths are
     accepted by **all three** bindings (AdoNet via the connection
     constructor, MicroOrm and EF Core after N7), and that the
     canonical form is `"Data Source=<path>;Pooling=true;…"`. Enumerate
     every supported key.
   - `## POCO portability with EF Core` — paragraph explaining N3's
     auto-skip rule and how to opt back in via `[Column]`. Show a
     before/after code snippet for a typical artist/album/song POCO
     used in both EF Core and MicroOrm.
   - `## Per-database vs per-binding views` — short note: views are
     per-database (CREATE VIEW persists in the file). Apps that share a
     file across bindings only need to issue the DDL once. Apps that
     use *separate* files per binding (as the benchmark does) must
     re-create each view per file.
   - `## Performance characteristics` — table summarizing the
     `/tmp/tmp-opus47-decentdb-net-tests/` benchmark numbers post-N1/N2.
     Mark numbers as "as of <YYYY-MM-DD>, engine vX.Y.Z" so they have a
     visible expiry.
3. Cross-link from each binding's existing per-package README
   (`bindings/dotnet/src/DecentDB.AdoNet/README.md`,
   `…/DecentDB.MicroOrm/README.md`,
   `…/DecentDB.EntityFrameworkCore/README.md`) to the new top-level
   matrix.

### Validation

- Visual review of rendered Markdown in a viewer.
- `python3 scripts/do-pre-commit-checks.py --mode fast` (catches
  malformed code fences and broken Markdown links if a checker exists).

### Acceptance criteria

- [ ] Top-level `bindings/dotnet/README.md` exists with all sections
      above.
- [ ] Each per-package README links to the top-level matrix.
- [ ] Matrix rows reflect *current* code state at the time the slice
      lands, not aspirational state.

---

## Slice N12 — AdoNet: accept multi-statement SQL via the existing `SqlStatementSplitter`

**Priority:** P1.
**Depends on:** none.
**Estimated diff size:** ~80 lines added, ~10 lines modified across 2 files.

### Problem

A second agent reported that `DecentDBCommand.Prepare()` and
`ExecuteReader()` reject any `CommandText` containing more than one
top-level SQL statement, throwing `expected exactly one SQL statement`.
This forces every consumer to split DDL into separate commands manually
even though `SqlStatementSplitter` already exists in the binding.

This is unfriendly for tooling that ships migrations as a single SQL
script (EF Core migrations, Dapper-style `.sql` resources, hand-written
schema files). All major ADO.NET providers (SqlClient, Npgsql,
SQLite-net) accept multi-statement command text in `ExecuteNonQuery`.

### Verified evidence

- `bindings/dotnet/src/DecentDB.AdoNet/DecentDBCommand.cs` —
  `ExecuteNonQuery` calls `Prepare()` (single-statement only) for the
  fast path, and only goes through `SqlStatementSplitter` in a
  fallback path that itself rejects splitter-produced multi-statement
  for `ExecuteReader`.
- `bindings/dotnet/src/DecentDB.AdoNet/SqlStatementSplitter.cs`
  exists and produces a `List<string>` of statements but is not wired
  into `ExecuteReader` or `Prepare`.

### Design decisions already made (do not relitigate)

1. `ExecuteNonQuery` and `ExecuteNonQueryAsync` accept multi-statement
   command text. They split via `SqlStatementSplitter`, then for each
   produced statement run the existing prepare-and-execute path. The
   return value (rows affected) is the **sum** across all statements.
2. `ExecuteReader` and `ExecuteReaderAsync` accept multi-statement
   command text **only** when at most one of the produced statements
   is a `SELECT`/`RETURNING`-bearing statement. Any non-result-set
   statement (DDL, plain `INSERT`/`UPDATE`/`DELETE`) executes
   immediately; the single result-set statement is exposed through the
   `DbDataReader`. If two or more statements would produce result
   sets, throw `NotSupportedException` with message
   `"DecentDBCommand.ExecuteReader cannot return more than one result set; use ExecuteReader once per SELECT statement, or call NextResult() (not yet supported)."`
3. `Prepare()` is a no-op when the command contains multiple
   statements — preparation only happens at execution time per
   sub-statement. (Document this explicitly in the XML doc.)
4. Parameter binding is by **name across all sub-statements**: a
   `@p0` referenced in statement 2 binds to the same value as `@p0`
   referenced in statement 1. This matches user expectations and
   mirrors SQLite-net behavior.
5. Transactions explicitly demarcated by the user (`BeginTransaction`)
   wrap **all** sub-statements; if no transaction is active and the
   command contains DML, the existing implicit-transaction behavior
   is preserved per sub-statement.

### Implementation steps

All edits in `bindings/dotnet/src/DecentDB.AdoNet/DecentDBCommand.cs`
and `bindings/dotnet/src/DecentDB.AdoNet/SqlStatementSplitter.cs`.

1. In `SqlStatementSplitter`, add a public method
   `IReadOnlyList<string> Split(string sql)` if it does not already
   return a list. Strip trailing whitespace and skip empty fragments.
2. In `DecentDBCommand.ExecuteNonQuery`, if `CommandText` contains a
   non-string, non-comment `;` outside of parentheses, route through
   `SqlStatementSplitter.Split` and execute each fragment via the
   existing single-statement code path. Sum the affected-row counts.
3. In `DecentDBCommand.ExecuteReader`, do the same split, but:
   - Pre-scan each fragment with the lightweight existing
     "starts-with `SELECT` after optional whitespace and `WITH`" check
     used by the binding to detect result-set statements.
   - If more than one fragment is a result-set statement, throw the
     `NotSupportedException` from §2 above before executing anything.
   - Execute non-result-set fragments immediately, then return the
     reader for the single result-set fragment.
4. In `DecentDBCommand.Prepare()`, if multiple statements are
   detected, set an internal `_isMultiStatement = true` flag, do not
   pre-prepare, and document the no-op behavior in the method's XML
   comment.
5. Update existing XML doc comments on `ExecuteNonQuery`,
   `ExecuteReader`, and `Prepare` to describe the new behavior.

### Tests to add

Add to `bindings/dotnet/test/DecentDB.AdoNet.Tests/`:

- `Test_ExecuteNonQuery_MultiStatement_DDL` — runs
  `"CREATE TABLE a (id INTEGER PRIMARY KEY); CREATE INDEX i_a ON a(id);"`
  in one command; asserts both objects exist via `GetSchema`.
- `Test_ExecuteNonQuery_MultiStatement_ReturnsRowCountSum` — runs two
  `INSERT` statements in one command; asserts the returned count
  equals the sum.
- `Test_ExecuteReader_MultiStatement_OneSelect` — runs
  `"INSERT INTO t VALUES (1); SELECT * FROM t;"`; asserts the reader
  yields the inserted row.
- `Test_ExecuteReader_MultiStatement_TwoSelects_Throws` — asserts
  `NotSupportedException` with the documented message.
- `Test_Prepare_MultiStatement_IsNoOp` — asserts no exception is
  thrown and a subsequent `ExecuteNonQuery` succeeds.
- `Test_MultiStatement_SharesParameters` — runs
  `"INSERT INTO t VALUES (@id); INSERT INTO u VALUES (@id);"` with
  one `@id` parameter; asserts both inserts used the same value.

### Validation

```bash
cd /home/steven/source/decentdb
dotnet test bindings/dotnet/test/DecentDB.AdoNet.Tests/DecentDB.AdoNet.Tests.csproj
python3 scripts/do-pre-commit-checks.py --mode fast
```

### Acceptance criteria

- [ ] All six new tests pass.
- [ ] Existing AdoNet tests unaffected.
- [ ] The benchmark project's `Schema.Apply` can be simplified to a
      single multi-statement `ExecuteNonQuery` call without errors.
- [ ] XML docs explicitly state the multi-statement behavior and the
      single-result-set restriction for `ExecuteReader`.

---

## Slice N13 — AdoNet `SqlStatementSplitter`: handle `CREATE TRIGGER ... BEGIN ... END` bodies

**Priority:** P2.
**Depends on:** none.
**Estimated diff size:** ~40 lines added in one file, plus tests.

### Problem

A second agent reported that running
`CREATE TRIGGER trg AFTER INSERT ON t BEGIN UPDATE u SET ...; END;`
through the binding fails with
`syntax error at or near 'BEGIN'` because `SqlStatementSplitter` treats
the `;` inside the trigger body as a statement boundary and feeds the
engine a half-statement.

DecentDB's parser accepts the full `CREATE TRIGGER … BEGIN … END;`
form when given the *whole* statement; the bug is in the binding's
splitter.

### Verified evidence

- `bindings/dotnet/src/DecentDB.AdoNet/SqlStatementSplitter.cs` — the
  current splitter tracks string literals and parentheses but has no
  awareness of `BEGIN ... END` blocks.
- The engine's parser handles trigger bodies; verify by feeding the
  full statement directly through the C ABI in a one-off test.

### Design decisions already made

1. The splitter recognizes `BEGIN` (case-insensitive, as a whole word)
   in trigger and procedure-like positions (after `CREATE TRIGGER ...`
   or `CREATE PROCEDURE ...`) and treats the matching `END` as the
   end of the compound body. `;` inside the body do **not** terminate
   the outer statement.
2. Nested `BEGIN ... END` blocks are tracked with an integer depth
   counter.
3. Comments (`-- ...`, `/* ... */`) inside the body are skipped using
   the existing comment-skipping helpers.
4. If `BEGIN` is encountered but no matching `END` is found before
   end-of-input, the splitter throws
   `FormatException("CREATE TRIGGER body is missing END;")` with the
   approximate column.

### Implementation steps

1. Add a private state to the splitter's tokenizer loop tracking
   `inCompoundBody` (bool) and `compoundDepth` (int).
2. After emitting the `CREATE` keyword, peek the next non-whitespace
   word; if it is `TRIGGER` or `PROCEDURE`, set a flag
   `expectingCompoundBody = true`.
3. When `expectingCompoundBody && lastEmittedWord == "BEGIN"`, set
   `inCompoundBody = true; compoundDepth = 1`.
4. While `inCompoundBody`, increment `compoundDepth` on each `BEGIN`
   and decrement on each `END`. When `compoundDepth == 0`, exit the
   compound body and resume normal `;`-based splitting.
5. A `;` encountered while `inCompoundBody && compoundDepth > 0` is
   treated as a literal character; do not split.

### Tests to add

- `Test_Split_CreateTrigger_PreservesBody` — input is a
  trigger with two `;` inside the body; asserts the splitter returns
  exactly one fragment containing the full `CREATE TRIGGER ... END;`.
- `Test_Split_CreateTrigger_NestedBeginEnd` — trigger body contains a
  nested `BEGIN ... END`; asserts one fragment.
- `Test_Split_CreateTrigger_MissingEnd_Throws` — asserts the
  `FormatException` from design decision §4.
- `Test_Split_CreateTriggerThenSelect` — trigger immediately followed
  by `SELECT 1;`; asserts two fragments.

### Validation

```bash
cd /home/steven/source/decentdb
dotnet test bindings/dotnet/test/DecentDB.AdoNet.Tests/DecentDB.AdoNet.Tests.csproj --filter FullyQualifiedName~SqlStatementSplitter
```

### Acceptance criteria

- [ ] All four new tests pass.
- [ ] Existing splitter tests unaffected.
- [ ] When N12 is also in, a benchmark consumer can run
      `CREATE TRIGGER ... BEGIN ... END;` followed by a `SELECT` in a
      single `ExecuteNonQuery` call.

---

## Slice N14 — EF Core: rewrite correlated `Count` subqueries as `LEFT JOIN ... GROUP BY`

**Priority:** P1.
**Depends on:** none.
**Estimated diff size:** ~120 lines added in one new file, ~20 lines
modified to wire it up.

### Problem

A second agent reported that LINQ aggregate queries such as
`ctx.Artists.Select(a => new { a.Name, SongCount = ctx.Songs.Count(s => s.ArtistId == a.Id) })`
or `SelectMany` chains with nested `Count`/`Sum` translate to SQL
shaped like:

```sql
SELECT a.id, a.name,
       (SELECT COUNT(*) FROM songs s WHERE s.artist_id = a.id) AS song_count
FROM artists a
```

DecentDB's planner currently executes the correlated subquery once per
outer row, which on 50K artists × full table scans is prohibitively
slow. Rewriting the same intent to:

```sql
SELECT a.id, a.name, COALESCE(g.cnt, 0) AS song_count
FROM artists a
LEFT JOIN (SELECT artist_id, COUNT(*) AS cnt FROM songs GROUP BY artist_id) g
       ON g.artist_id = a.id
```

is asymptotically faster and is what every comparable ORM (Dapper-EF,
linq2db) emits as the default for DecentDB-class engines that lack
late-materialization optimizations.

### Verified evidence

- `bindings/dotnet/src/DecentDB.EntityFrameworkCore/Query/Internal/`
  contains existing translator providers
  (`DecentDBMethodCallTranslatorProvider.cs`,
  `DecentDBSqlTranslatingExpressionVisitor.cs` if present) but no
  query-level rewriter.
- The EF Core extensibility point is
  `IQueryTranslationPostprocessor` (registered via
  `IQueryTranslationPostprocessorFactory`); a custom one can rewrite
  the SQL expression tree before the SQL is materialized.

### Design decisions already made

1. The post-processor visits `SelectExpression` nodes and looks for
   projections of the form `(SELECT COUNT(...) FROM <T> WHERE
   <T>.<fk> = <outer>.<pk>) AS <alias>`.
2. When found, it lifts the subquery into a top-level
   `LEFT JOIN (SELECT <fk>, COUNT(...) AS <alias> FROM <T> GROUP BY
   <fk>) <g> ON <g>.<fk> = <outer>.<pk>` and replaces the original
   projection with `COALESCE(<g>.<alias>, 0)`.
3. Only `COUNT(*)` and `COUNT(<col>)` are rewritten in this slice.
   `SUM`/`AVG`/`MIN`/`MAX` follow the same pattern and may be added
   later; out of scope here.
4. The rewriter is **opt-out** via a context option
   `optionsBuilder.UseDecentDB(o => o.DisableCorrelatedAggregateRewrite())`
   for the rare case where a user wants the literal correlated form.
5. If the original subquery has additional predicates beyond the
   `outer.pk = inner.fk` join key, the rewriter **bails out** and
   leaves the subquery intact. (Folding arbitrary predicates into
   `GROUP BY` requires careful semantics work; out of scope.)

### Implementation steps

1. Create
   `bindings/dotnet/src/DecentDB.EntityFrameworkCore/Query/Internal/DecentDBCorrelatedAggregateRewriter.cs`
   implementing `RelationalQueryTranslationPostprocessor`. Override
   `Process(Expression query)` and walk for `SelectExpression`s.
2. Create
   `bindings/dotnet/src/DecentDB.EntityFrameworkCore/Query/Internal/DecentDBQueryTranslationPostprocessorFactory.cs`
   implementing `IQueryTranslationPostprocessorFactory` returning the
   new rewriter (chained after the default `RelationalQueryTranslationPostprocessor`).
3. In
   `bindings/dotnet/src/DecentDB.EntityFrameworkCore/Infrastructure/Internal/DecentDBOptionsExtension.cs`
   (or wherever `ApplyServices` lives), replace the registration of
   `IQueryTranslationPostprocessorFactory` with the new factory.
4. Add `DisableCorrelatedAggregateRewrite()` to
   `DecentDBDbContextOptionsBuilder` (the inner options builder
   exposed by `UseDecentDB(o => ...)`).
5. The rewriter implementation:
   - Recognize the projection pattern
     `ScalarSubqueryExpression(SelectExpression { Projection: [ Sql.Function "COUNT"(...) ], Tables: [ TableExpression t ], Predicate: SqlBinaryExpression Equal(ColumnExpression(outerPk), ColumnExpression(innerFk)) })`.
   - Construct the equivalent `JoinExpression(LeftJoin)` with a
     subquery `SelectExpression` that groups by `innerFk` and projects
     `COUNT(...) AS gN_alias`.
   - Replace the projection with
     `SqlFunctionExpression("COALESCE", [ColumnExpression(group.alias), Constant(0L)])`.

### Tests to add

In `bindings/dotnet/test/DecentDB.EntityFrameworkCore.Tests/`:

- `Test_CorrelatedCount_RewrittenToGroupByJoin` — uses
  `EnableSensitiveDataLogging()` + a captured-SQL fixture; asserts
  the executed SQL contains `LEFT JOIN` and not
  `(SELECT COUNT(`.
- `Test_CorrelatedCount_ProducesSameRows` — runs the LINQ query
  against a small dataset and asserts identical results before/after
  the rewrite (compare against `DisableCorrelatedAggregateRewrite()`).
- `Test_CorrelatedCount_WithExtraPredicate_BailsOut` — asserts the
  rewriter leaves a query with a `WHERE s.year > 2000` predicate
  unmodified.
- `Test_DisableCorrelatedAggregateRewrite_OptOut` — asserts the
  generated SQL is the original correlated form.

### Validation

```bash
cd /home/steven/source/decentdb
dotnet test bindings/dotnet/test/DecentDB.EntityFrameworkCore.Tests/DecentDB.EntityFrameworkCore.Tests.csproj
python3 scripts/do-pre-commit-checks.py --mode fast
```

### Acceptance criteria

- [ ] All four new tests pass.
- [ ] The benchmark's `query_top10_artists_by_songs` LINQ query (in
      the EF Core runner) executes in under **2 s** at full scale
      after this slice (currently 1.7 s post-N1; rewrite should keep
      or improve that and unblock more complex aggregates).
- [ ] Generated SQL is captured in the test output for the four cases
      above and stored as snapshot files under
      `bindings/dotnet/test/DecentDB.EntityFrameworkCore.Tests/Snapshots/`.

---

## Slice N15 — EF Core: trim and document the first-`DbContext` startup cost

**Priority:** P2.
**Depends on:** none.
**Estimated diff size:** ~30 lines of code touch-ups, ~80 lines of
docs.

### Problem

A second agent measured ~7–8 s of overhead the first time
`new BenchmarkDbContext()` is constructed, before any query runs. Most
of this is EF Core's model-build and provider-service compilation, but
some of it is in the DecentDB binding's service-collection wiring
(model conventions, type mappings).

For long-running services this is a one-time cost, but for CLI tools
and tests it dominates wall time. The slice does two things:

1. Pre-build a model cache so subsequent `DbContext`s using the same
   `OnModelCreating` reuse it.
2. Document the cost prominently with the recommended mitigations.

### Verified evidence

- A second agent's runtime trace shows ~7–8 s spent in
  `IModelCustomizer`/`ConventionSetBuilder` on first construction.
- EF Core's `DbContextOptionsBuilder.UseModel()` accepts a pre-built
  `IModel`; the binding does not currently expose a helper to create
  one.

### Design decisions already made

1. Add `DecentDBModelBuilder.BuildModel<TContext>()` (static helper)
   that constructs the model exactly as a real
   `TContext` would, then returns the frozen `IModel`. The first call
   does the work; subsequent calls return a cached instance keyed by
   `typeof(TContext)`.
2. The cache is `ConcurrentDictionary<Type, IModel>` and is process-wide.
3. No public API change to existing `UseDecentDB(...)` overloads — the
   helper is opt-in. Document it in `bindings/dotnet/README.md` (slice N11).
4. Do not pre-emptively call this helper from any existing code path —
   that would defer cost without removing it. Users opt in.

### Implementation steps

1. Create
   `bindings/dotnet/src/DecentDB.EntityFrameworkCore/DecentDBModelBuilder.cs`:
   ```csharp
   public static class DecentDBModelBuilder
   {
       private static readonly ConcurrentDictionary<Type, IModel> Cache = new();
       public static IModel BuildModel<TContext>() where TContext : DbContext, new()
       {
           return Cache.GetOrAdd(typeof(TContext), _ =>
           {
               using var ctx = new TContext();
               return ctx.Model;
           });
       }
   }
   ```
2. Add an overload `UseDecentDB(this DbContextOptionsBuilder b, string connStr, IModel model)` that calls `b.UseModel(model).UseDecentDB(connStr)`.
3. Document in
   `bindings/dotnet/src/DecentDB.EntityFrameworkCore/README.md` (or a
   new "Performance tips" section in N11) the recommended pattern:
   ```csharp
   var model = DecentDBModelBuilder.BuildModel<MyContext>();
   var options = new DbContextOptionsBuilder<MyContext>()
       .UseDecentDB(connStr, model)
       .Options;
   ```

### Tests to add

- `Test_BuildModel_CachesPerContextType` — calls `BuildModel<X>()`
  twice; asserts the second call is < 50 ms.
- `Test_UseDecentDB_WithPrebuiltModel_AvoidsModelBuild` — measures
  `new MyContext()` cold-start with and without the helper; asserts
  the helper variant is at least 5× faster on the second invocation.

### Validation

```bash
cd /home/steven/source/decentdb
dotnet test bindings/dotnet/test/DecentDB.EntityFrameworkCore.Tests/DecentDB.EntityFrameworkCore.Tests.csproj --filter FullyQualifiedName~ModelBuilder
```

### Acceptance criteria

- [ ] Both new tests pass.
- [ ] Documentation block exists with the example pattern.
- [ ] Model cache is thread-safe (verified by a stress test that
      calls `BuildModel<TContext>()` from 16 parallel threads).

---

## Slice N16 — AdoNet: unify WAL filename convention and expose `DeleteDatabaseFiles` helper

**Priority:** P2.
**Depends on:** none.
**Estimated diff size:** ~40 lines added in 1 file, plus tests.

### Problem

A second agent and the consumer-side EF Core experiment both tripped
on the same issue: the engine's WAL file is named `<db>.ddb.wal`
(the suffix is `.wal`, not `-wal`). Cleanup scripts that delete
`<db>.ddb-wal` (the convention some other databases use) leave the
real WAL behind. On a subsequent open the engine attempts to replay
the stale WAL against the (recreated) data file and reports the
misleading
`DecentDB error 2: database corruption: catalog root page magic is
invalid` error. (The corresponding engine improvement is in §6
backlog.)

The binding can defuse this footgun by shipping a small helper
`DecentDBConnection.DeleteDatabaseFiles(string path)` that deletes the
data file and **all** known sidecar files in one call.

### Verified evidence

- During the consumer-side benchmark refactor the runner used
  `rm -f run-*-full.ddb-wal` (wrong suffix); the leftover
  `run-efcore-full.ddb.wal` (67 MB) caused the open of a freshly
  re-created DB to fail. Replacing the cleanup with
  `rm -f run-*.ddb run-*.ddb.wal run-*.ddb-wal` fixed the issue
  and the EF Core run then completed in 27 s.
- Engine source in `crates/decentdb/src/wal/**` confirms the suffix
  is `.wal`. There is no `-wal` form.

### Design decisions already made

1. The helper is named
   `DecentDBConnection.DeleteDatabaseFiles(string databasePath)`.
2. It deletes (in this order, ignoring `FileNotFoundException` for
   each one): `<path>.wal`, `<path>-wal` (defensive against an
   imagined-future rename), `<path>-shm` (likewise), and finally
   `<path>` itself. Other `IOException`s are rethrown.
3. The helper is `static` and lives on `DecentDBConnection`. It
   does not require an open connection; it does **not** call any
   native code; it must not throw if the file does not exist.
4. XML doc explicitly notes that the data file is deleted **last**
   so that an interruption mid-call leaves the database openable
   (the WAL will simply replay over the original file).

### Implementation steps

1. Add the static method to
   `bindings/dotnet/src/DecentDB.AdoNet/DecentDBConnection.cs`.
2. Add an XML doc block describing the order and the
   non-throwing-on-missing semantics.
3. Update any internal cleanup helper used by the
   binding's tests to call `DeleteDatabaseFiles`.

### Tests to add

- `Test_DeleteDatabaseFiles_RemovesAllSidecars` — creates a fake
  `.ddb`, `.ddb.wal`, and `.ddb-shm` in a temp dir; asserts all
  three are gone after the call.
- `Test_DeleteDatabaseFiles_NoOp_WhenMissing` — asserts no exception
  on a path that does not exist.
- `Test_DeleteDatabaseFiles_DeletesDataFileLast` — uses a custom
  delete-trace; asserts `<path>.wal` is deleted before `<path>`.

### Validation

```bash
cd /home/steven/source/decentdb
dotnet test bindings/dotnet/test/DecentDB.AdoNet.Tests/DecentDB.AdoNet.Tests.csproj --filter FullyQualifiedName~DeleteDatabaseFiles
```

### Acceptance criteria

- [ ] All three new tests pass.
- [ ] Doc explicitly calls out `.wal` as the suffix.
- [ ] N11 docs slice references the helper as the recommended cleanup
      pattern.

---

## Slice N17 — AdoNet `DecentDBConnection`: fire `StateChange` on `Open()` and `Close()`

**Priority:** P1.
**Depends on:** none.
**Estimated diff size:** ~25 lines added in one file, plus 2 tests.

### Problem

A second agent reported that `DecentDBConnection.Open()` and `Close()`
do not raise the inherited `DbConnection.StateChange` event. This
breaks any consumer that depends on the standard ADO.NET state-change
contract — including diagnostics frameworks
(`Microsoft.Extensions.Diagnostics`), connection pools, and Dapper
extensions.

### Verified evidence

- `bindings/dotnet/src/DecentDB.AdoNet/DecentDBConnection.cs` —
  `Open()` and `Close()` mutate `_state` directly without invoking
  the protected `OnStateChange` helper.
- `System.Data.Common.DbConnection.OnStateChange` is the standard
  hook for raising `StateChange`.

### Design decisions already made

1. Both `Open()`/`OpenAsync` and `Close()` capture the previous
   state, mutate state, and then call
   `OnStateChange(new StateChangeEventArgs(previous, current))`
   exactly once.
2. If the open or close throws after state mutation but before
   `OnStateChange`, restore the previous state and re-throw without
   firing the event. (Matches SqlConnection behavior.)
3. `Dispose` calling `Close()` triggers exactly one `StateChange`
   event, not two.

### Implementation steps

1. In `DecentDBConnection.Open`:
   ```csharp
   var previous = _state;
   try { /* existing native open code */ }
   catch { _state = previous; throw; }
   _state = ConnectionState.Open;
   OnStateChange(new StateChangeEventArgs(previous, _state));
   ```
2. Mirror this in `OpenAsync` and `Close`.
3. Verify `Dispose` does not double-fire by adding a guard that
   `Close` is a no-op when already closed.

### Tests to add

- `Test_Open_FiresStateChange_ClosedToOpen` — subscribes to
  `StateChange`; asserts a single event with
  `OriginalState=Closed, CurrentState=Open`.
- `Test_Close_FiresStateChange_OpenToClosed` — symmetric.
- `Test_Dispose_DoesNotDoubleFireStateChange` — asserts exactly one
  event with `Open→Closed`.

### Validation

```bash
cd /home/steven/source/decentdb
dotnet test bindings/dotnet/test/DecentDB.AdoNet.Tests/DecentDB.AdoNet.Tests.csproj --filter FullyQualifiedName~StateChange
```

### Acceptance criteria

- [ ] All three new tests pass.
- [ ] No regression in existing connection lifecycle tests.

---

## Slice N18 — AdoNet `GetSchema("Indexes")`: distinguish auto-PK indexes from user indexes

**Priority:** P2.
**Depends on:** none.
**Estimated diff size:** ~15 lines added (one new column), ~10 lines
modified, plus tests.

### Problem

A second agent reported that
`connection.GetSchema("Indexes", new[] { tableName })` returns both
the user-created secondary indexes **and** the implicit primary-key
index that the engine creates per table with a PK. There is no way
in the returned `DataTable` to distinguish them, which forces
consumer test code to filter by name pattern.

### Verified evidence

- The engine creates one implicit index per declared `PRIMARY KEY`
  (visible in `crates/decentdb/src/catalog/**`).
- `bindings/dotnet/src/DecentDB.AdoNet/DecentDBConnection.cs` —
  `GetSchema` returns rows with columns `INDEX_CATALOG`,
  `INDEX_SCHEMA`, `INDEX_NAME`, `TABLE_NAME`, `IS_UNIQUE`, etc., but
  no `IS_PRIMARY_KEY`-style column.

### Design decisions already made

1. Add a new `bool` column `IS_PRIMARY_KEY` to the `Indexes` schema
   collection. `true` when the index was auto-created for a
   `PRIMARY KEY` constraint, `false` otherwise.
2. Detection rule: an index is considered the auto-PK index when its
   name matches the engine's auto-PK convention **or** when its
   indexed column set equals the table's PK column set and no
   explicit `CREATE INDEX` statement was issued. The simplest
   implementation reads the catalog flag if available; otherwise
   uses the name-prefix heuristic and documents it.
3. The `Indexes` schema metadata table (returned by
   `GetSchema(DbMetaDataCollectionNames.MetaDataCollections)`) is
   updated to advertise the new column.

### Implementation steps

1. In `DecentDBConnection.GetSchema("Indexes", restrictions)`,
   add the new column to the `DataTable` definition.
2. Populate it for each row using whatever signal the catalog query
   provides (preferred) or the name-prefix heuristic (fallback).
3. Update the `MetaDataCollections` schema definition to include
   the new column with type `System.Boolean`.

### Tests to add

- `Test_GetSchema_Indexes_FlagsAutoPkIndex` — creates a table with a
  PK and one user index; asserts the auto-PK row has
  `IS_PRIMARY_KEY = true` and the user index has
  `IS_PRIMARY_KEY = false`.
- `Test_GetSchema_Indexes_NoPk_NoAutoFlagged` — creates a keyless
  table with one user index; asserts no row has
  `IS_PRIMARY_KEY = true`.

### Validation

```bash
cd /home/steven/source/decentdb
dotnet test bindings/dotnet/test/DecentDB.AdoNet.Tests/DecentDB.AdoNet.Tests.csproj --filter FullyQualifiedName~GetSchema_Indexes
```

### Acceptance criteria

- [ ] Both new tests pass.
- [ ] `IS_PRIMARY_KEY` is documented in the binding's schema-metadata
      doc table.
- [ ] N11 docs slice mentions the new column in the feature-parity
      matrix.

---

## Slice N19 — EFCore: cache compiled query plans across DbContext lifetime

**Priority:** P2 — surfaced by the raw-Rust baseline (see §0). On its
own it does not move the needle on bulk seeding, but it eliminates
EFCore's measurable per-call LINQ-translation tax on small queries
(e.g. `COUNT(*)` 0 ms → 243 ms; by-id lookup 1 ms → 47 ms).
**Depends on:** none (independent of N1–N18).
**Estimated diff size:** medium.
**Engine ABI / on-disk impact:** none (EF Core binding only).

### Problem

The raw-Rust baseline measured these latencies on the same database:

| query                              | RustRaw | AdoNet  | EFCore (refactored) |
|------------------------------------|--------:|--------:|--------------------:|
| `SELECT COUNT(*) FROM songs`       |   0 ms  |   3 ms  |             243 ms  |
| `SELECT … FROM artists WHERE id=?` |   1 ms  |   1 ms  |              47 ms  |

The 240 ms / 46 ms gap on EFCore is **not engine work** — those
queries finish in microseconds at the engine layer. It is per-call
LINQ → SQL translation cost paid by EF Core's
`IQueryCompiler` every time the consumer reuses the same LINQ
expression with different parameter values. EF Core ships
`EF.CompileQuery` / `EF.CompileAsyncQuery` as the standard mitigation
but the DecentDB provider does not currently document or test the
fast path, and provider-side `IQuerySqlGenerator` allocations may
defeat reuse even when the consumer compiles once.

### Verified evidence

- `bindings/dotnet/src/DecentDB.EntityFrameworkCore/Query/Internal/`
  contains the SQL generator and translator. They run unchanged on
  every `ToList()` call.
- The .NET benchmark consumer at
  `/tmp/tmp-opus47-decentdb-net-tests/src/EfCore.Bench/EfCoreRunner.cs`
  reuses the same context and the same LINQ shapes; the per-call
  cost is measured even after warm-up.

### Design decisions already made

1. **No new public API on the binding.** The fix is a provider-side
   internal cache, plus documentation pointing consumers at
   `EF.CompileQuery` for the strong path.
2. **Do not invent a parallel cache to EF Core's own `IQueryCache`.**
   Instead, audit the DecentDB provider's
   `IRelationalParameterBasedSqlProcessorFactory` /
   `IQuerySqlGeneratorFactory` implementations to ensure they honor
   EF Core's existing reuse contract (return cached singletons where
   the framework expects singletons; key cache by parameter shape,
   not parameter values).
3. **Acceptance metric is the raw-Rust baseline numbers.** The two
   queries above must drop to ≤ 5 ms and ≤ 3 ms respectively (a
   ~50× improvement) on the second-and-subsequent invocation.

### Implementation steps

1. Add a micro-benchmark fixture under
   `bindings/dotnet/test/DecentDB.EntityFrameworkCore.Tests/` that
   warms a `DbContext`, then runs `Set<T>().Count()` and
   `Set<T>().FirstOrDefault(x => x.Id == 1)` 1 000 times and asserts
   the median latency.
2. Run it once to capture the regression baseline (expected ~50 ms /
   ~5 ms median).
3. Audit `Query/Internal/` for places where the provider returns a
   *new* generator/processor on every call. Convert them to cached
   singletons keyed by query shape. Use EF Core's
   `IDiagnosticsLogger<DbLoggerCategory.Query>` to verify cache hits
   in test mode.
4. Add a section to
   `bindings/dotnet/src/DecentDB.EntityFrameworkCore/README.md`
   titled "Compiled queries for tight loops" with a worked example
   using `EF.CompileQuery`.
5. Re-run the micro-benchmark and assert ≤ 5 ms / ≤ 3 ms median.
6. Re-run the comparison suite at
   `/tmp/tmp-opus47-decentdb-net-tests` and update the
   "EFCore micro-overhead on tiny queries" row in the §0 raw-Rust
   baseline table.

### Tests to add

- `EfCore_RepeatedCount_StaysUnderFiveMs` (xUnit, in the
  micro-benchmark fixture).
- `EfCore_RepeatedPointLookup_StaysUnderThreeMs`.
- `EfCore_CompiledQuery_ExampleCompiles_AndExecutes` — sanity test
  that the README example compiles and returns the expected row
  count.

### Validation commands

```bash
# Provider unit tests + new micro-benchmark fixture
dotnet test bindings/dotnet/test/DecentDB.EntityFrameworkCore.Tests \
  -c Release --filter "Category=micro-bench"

# Smoke at the comparison suite
cd /tmp/tmp-opus47-decentdb-net-tests
dotnet run --project src/Runner -c Release -- --scale smoke --binding efcore
```

### Acceptance criteria

- [ ] `query_count_songs` median latency on the EFCore binding is
      ≤ 5 ms at smoke scale (down from ~243 ms).
- [ ] `query_artist_by_id` median latency is ≤ 3 ms at smoke scale
      (down from ~47 ms).
- [ ] No regression in `seed_songs` r/s (the slice does not touch
      the write path).
- [ ] README example for `EF.CompileQuery` is present and compiles.
- [ ] N11 docs slice picks up the compiled-query guidance.

---

## Slice N-REL — Release: version bump, regenerate benchmark numbers, smoke

**Priority:** Last.
**Depends on:** all preceding slices that are scheduled for the release.
**Estimated diff size:** small.

### Implementation steps

1. Bump the .NET package versions (consistently across
   `DecentDB.AdoNet.csproj`, `DecentDB.MicroOrm.csproj`,
   `DecentDB.EntityFrameworkCore.csproj`,
   `DecentDB.EntityFrameworkCore.Design.csproj`,
   `DecentDB.EntityFrameworkCore.NodaTime.csproj`,
   `DecentDB.Native.csproj`). Use the project's existing version-bump
   convention (consult prior commit history; do **not** invent one).
2. Re-run the comparison benchmark at full scale and copy the numbers
   into:
   - The §0 "Motivation" table at the top of *this* document, marked
     `(post-N1/N2)`.
   - The "Performance characteristics" section of the new top-level
     `bindings/dotnet/README.md` (N11).
3. Run the full validation suite:
   ```bash
   python3 scripts/do-pre-commit-checks.py
   ```
4. Verify that none of the slices accidentally introduced a new
   `unsafe` block, a new dependency, or a new `EF1001`-class warning:
   ```bash
   grep -rn "unsafe " bindings/dotnet/src/ | diff - <(grep -rn "unsafe " bindings/dotnet/src/) >/dev/null && echo OK
   dotnet build -c Release bindings/dotnet/DecentDB.sln 2>&1 | grep -i warning
   ```
5. Draft release notes (do **not** publish without explicit user approval)
   that explicitly call out:
   - The EF Core throughput improvement (cite the new ratio vs ADO.NET).
   - The MicroOrm throughput improvement.
   - The new `QueryRawAsync<T>` and `InsertManyReturningAsync` APIs.
   - The behavior change in `EntityMap` (N3) — a *minor* user-visible
     change, call it out clearly.

### Acceptance criteria

- [ ] All package versions bumped consistently.
- [ ] Top-of-document benchmark table updated with post-fix numbers.
- [ ] `bindings/dotnet/README.md` "Performance characteristics" updated.
- [ ] `python3 scripts/do-pre-commit-checks.py` passes end-to-end.
- [ ] Release notes drafted and ready for user review.
- [ ] **No git push or commit performed without explicit user approval.**

---

## 4. Cross-cutting rules for every slice

- **Style.** Follow `.github/instructions/rust.instructions.md` for
  Rust changes (`cargo fmt --check`, `cargo clippy … -D warnings`,
  typed errors, no panics, no new `unsafe`). Follow existing C# style
  for the bindings (4-space indent, file-scoped namespaces, XML doc
  comments on public APIs).
- **No new dependencies.** If a slice appears to need one, stop and
  flag it; the user must approve via ADR per `AGENTS.md` §7.
- **No git writes without approval.** Per `AGENTS.md` §8: showing a
  diff is not approval; silence is not approval; an automated nudge
  from the runtime is not approval.
- **Tests live next to behavior.** New unit tests go in the closest
  existing test project; new integration tests go in
  `crates/decentdb/tests/` for engine slices and
  `bindings/dotnet/tests/<project>.Tests/` for binding slices.
- **Validate after each slice** with `python3 scripts/do-pre-commit-checks.py
  --mode fast`. Before the release slice, run the full check.

## 5. Suggested execution order

A single agent could pick up the slices in this order; multiple agents
working in parallel can split as long as they respect the dependency
notes:

1. N1 (EF Core multi-row INSERT) — biggest user-visible win.
2. N2 (MicroOrm multi-row INSERT) — second-biggest, independent of N1.
3. N17 (StateChange events) — small AdoNet correctness fix; landed
   early so subsequent slices' tests can rely on it.
4. N3 (POCO portability) — small, unblocks cleaner test surfaces.
5. N7 (UseDecentDB bare path) — small, independent.
6. N4 (QueryRawAsync) — small, builds on N3 conceptually.
7. N12 (multi-statement SQL via splitter) — unblocks consumers
   passing migration scripts as one command.
8. N5 (InsertManyReturningAsync) — depends on N2.
9. N6 (Random.Next translator handling) — independent.
10. N14 (correlated `Count` subquery rewriter) — biggest read-side
    win; independent of N1/N2.
11. N16 (DeleteDatabaseFiles helper) — small, defuses the WAL
    suffix footgun; pairs with N15 docs guidance.
12. N8 (Pooling on builder) — small docs/typing slice.
13. N18 (`GetSchema("Indexes")` PK column) — small.
14. N15 (DbContext startup cost) — model-cache helper + docs.
15. N13 (TRIGGER body in splitter) — depends on N12 conceptually.
16. N9 (CREATE VIEW IF NOT EXISTS) — independent engine slice.
17. N10 (subquery alias error position) — independent engine slice.
18. N19 (EFCore compiled-query plan cache) — independent EFCore slice;
    fixes the 240 ms / 47 ms small-query tax measured against the
    raw-Rust baseline in §0.
19. N11 (binding docs) — gathers the truth from all preceding slices.
20. N-REL (release) — last.

## 6. Deferred — needs ADR before any work begins

These items came out of the same review but are intentionally **out of
scope** for this plan because they touch the C ABI or on-disk surface
and `AGENTS.md` §7 requires an ADR first:

- **`ddb_stmt_bind_batch` C-ABI entry point.** Would let bindings skip
  the SQL-text round-trip entirely for bulk INSERT. Adds a new exported
  symbol → ABI change → ADR. Open this only after measuring whether
  N1+N2 close the gap sufficiently.
- **Public WAL stats (`uncheckpointed_bytes`, `last_checkpoint_lsn`).**
  Adds new exported symbols → ABI change → ADR. The benchmark observed
  that `WAL size = 0` is reported because the connection close
  triggers a checkpoint; exposing live stats requires a stable contract.
- **`ddb_explain` / structured query-plan ABI.** Same shape: useful, but
  any stable schema for the returned plan must be ADR-discussed first
  (because consumers would pin to it).
- **Stale-WAL detection at open time.** Engine should detect when the
  WAL references pages from a different generation than the data
  file (e.g. checksum/LSN mismatch on the catalog root) and emit a
  dedicated error (`"stale WAL detected at <path>; rerun recovery or
  delete the WAL"`) instead of the misleading
  `"catalog root page magic is invalid"`. Touches WAL-recovery code
  path semantics and error vocabulary; ADR per `AGENTS.md` §7.
- **`COUNT(*)` cold-start latency.** A second agent measured
  `SELECT COUNT(*) FROM artists` (50K rows) at ~8 s on first
  execution. Eliminating this requires either a cached row-count in
  the table catalog (on-disk format change → ADR) or a covering
  optimization in the planner that reads only the leaf-page row
  counts (planner change but no format change). Either way the
  approach must be ADR-decided before implementation.
- **Query-time intermediate buffers retained until `Db` drop.**
  Surfaced by the raw-Rust baseline: peak RSS climbs from ~25 MB
  (post-seed) to ~2.2 GB during read evaluation on a 145 MB
  database, and stays at 2.2 GB until the `Db` is dropped. The same
  shape appears in every .NET binding — proving it is engine-side.
  Suspected cause is group-by hash tables, ORDER BY sort buffers,
  and view-materialization scratch space being parked on a
  per-`Db` arena rather than freed at end-of-statement. Fix likely
  touches the executor's allocator strategy and may interact with
  the page-cache eviction policy → ADR per `AGENTS.md` §7. Until
  fixed, all bindings inherit the symptom and `peak_working_set`
  metrics in benchmarks should be interpreted accordingly.

If/when these are approved, each gets its own slice plan and ADR in
`design/adr/`.

## 7. What was deliberately *not* changed

- **`EF1001` suppression.** Verified the EF Core binding builds with
  zero warnings without consumer-side suppression (rebuilt
  `/tmp/tmp-opus47-decentdb-net-tests/src/EfCore.Bench` after removing
  `<NoWarn>EF1001</NoWarn>`). No slice; the original challenge log
  entry was retracted.
- **MicroOrm pluralizer behavior.** The "`MyAddress` → `my_addresses`"
  surprise is real (`Conventions.Pluralize`), but it's a low-traffic
  ergonomics issue and any change is a behavior break. Move to the
  general backlog rather than this plan.
- **`DbSet<T>` namespace collision** with EF Core's `DbSet<T>`. Renaming
  is a breaking change and deserves its own deprecation cycle, not a
  slice in this plan.
- **`ExecuteUpdate` / `ExecuteDelete` / `BulkInsert` parity audit.** A
  separate, larger workstream; not appropriate to fold into this plan.
- **Engine memory ceiling itself.** The raw-Rust baseline added in
  §0 demonstrates that the 2.2 GB peak RSS observed at full scale
  is reached even with zero binding overhead. That makes it an
  engine concern, not a binding concern; it is captured in §6
  ("Query-time intermediate buffers retained until `Db` drop") as
  an ADR-required item. None of the slices N1–N19 are expected to
  meaningfully reduce peak RSS; gains will be dominated by the
  engine fix.
- **Pursuing AdoNet beyond 1.27× of the engine ceiling.** The
  baseline showed AdoNet is already within 27 % of raw Rust on the
  heaviest write step and within 9 % on every read. Squeezing out
  the remaining gap (FFI marshalling, parameter rewriter,
  `IDataReader` materialization) is not in scope for this plan
  because the cost/benefit no longer justifies a slice; document
  it in N11 as "future micro-optimization" instead.

---
*End of plan.*
