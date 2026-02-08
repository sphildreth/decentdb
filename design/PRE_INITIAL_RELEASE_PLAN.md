# Pre-Initial Release Plan (1.0.0)

**Date:** 2026-02-08
**Target release:** v1.0.0

This document captures all findings from a comprehensive codebase review performed prior
to tagging the first stable release. Items are grouped by severity and ordered by
priority within each group. Every item has a checkbox so progress can be tracked
incrementally.

---

## 1 — Release Blockers

These items **must** be resolved before the v1.0.0 tag is created. Shipping with any of
these unresolved would either break users on day one or undermine the credibility of a
"1.0" label.

### 1.1 Version numbers are wrong in multiple places

The package version and README still reflect pre-release identifiers that will confuse
every user who installs or reads about the project.

| File | Current value | Required value |
|------|--------------|----------------|
| `decentdb.nimble` line 1 | `version = "0.1.0"` | `version = "1.0.0"` |
| `README.md` line 33 | `Current version: **0.0.1** (beta)` | `Current version: **1.0.0**` |

- [x] Update `decentdb.nimble` version to `1.0.0`
- [x] Update `README.md` line 33 to `1.0.0` and remove "(beta)" qualifier

### 1.2 CHANGELOG Known Limitations section is stale

The "Known Limitations" section at the bottom of `CHANGELOG.md` lists multiple features
as unsupported that **are already implemented and tested**. Shipping this text with 1.0
will mislead users into thinking the database is less capable than it actually is.

| Limitation listed | Actual status | Evidence |
|-------------------|--------------|----------|
| "No window functions" | **ROW_NUMBER implemented** | `src/exec/exec.nim` — `ROW_NUMBER` window expression support |
| "No Common Table Expressions (WITH clauses)" | **Non-recursive CTEs implemented** | `src/sql/binder.nim` — CTE binding and expansion |
| "No views" | **CREATE/DROP VIEW implemented** | `src/sql/binder.nim` `skCreateView`; `src/engine.nim` — view creation/query |
| "No subqueries in SELECT list" | **Scalar subqueries supported** | `src/exec/exec.nim` — subselect evaluation in expressions |

The following limitations **are still accurate** and should remain:

- Single writer only (no concurrent write transactions)
- Single process access (no multi-process concurrency)
- No stored procedures
- Statement-time foreign key enforcement (not deferred)
- No full-text search with ranking (trigram substring matching only)
- No replication
- No built-in encryption

Additionally, new limitations should be documented to set accurate expectations:

- ROW_NUMBER is the only window function; RANK, DENSE_RANK, LAG, LEAD, etc. are not
  supported
- Only non-recursive CTEs are supported (no `WITH RECURSIVE`)
- `UPDATE ... RETURNING` and `DELETE ... RETURNING` are not yet supported
  (only `INSERT ... RETURNING`)
- `ADD CONSTRAINT` (post-creation) is not supported
- Targetless `ON CONFLICT DO UPDATE` is not supported

- [x] Remove the four stale limitation entries from `CHANGELOG.md`
- [x] Add the new accurate limitation entries listed above
- [x] Review and update any other documentation that references these features as
      missing (especially `docs/about/changelog.md` if it mirrors `CHANGELOG.md`)

### 1.3 No CI workflow for tests on push/PR

The repository has three GitHub Actions workflows:

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `release.yml` | Tag push (`v0.1.*`, `v1.0.0`, `v1.0.0-rc.*`) | Build release artifacts |
| `nuget.yml` | (tag-based) | Publish .NET NuGet package |
| `docs.yml` | (push-based) | Deploy documentation site |

**There is no workflow that runs the test suite on pushes to `main` or on pull requests.**
This means broken code can be merged and even tagged as v1.0.0 without any automated
test gate. For a database engine where correctness is the north star, this is a critical
gap.

The CI workflow should:

1. Run on push to `main` and on all pull requests
2. Execute `nimble test_nim` (the 88 Nim unit tests via testament)
3. Run on at least Ubuntu; ideally on the same three-platform matrix used by
   `release.yml` (ubuntu-latest, macos-latest, windows-latest)
4. Optionally run `nimble test_py` for the Python crash/differential/property harness

- [x] Create `.github/workflows/ci.yml` with test jobs on push and PR
- [ ] Verify the workflow passes on all three platforms before tagging

### 1.4 Node.js binding is incomplete

The `bindings/node/decentdb/` directory contains scaffolding (N-API addon source,
`binding.gyp`, `index.js` wrapper) but the binding is not in a shippable state:

- The native addon (`.node` file) is not pre-built; users must compile from source
  with `node-gyp`, which requires a working Nim + C toolchain
- `index.js` has no fallback or helpful error when the compiled addon is missing
- Test coverage is minimal — only `test_debug.js` exists
- No example code demonstrating usage
- The companion `knex-decentdb` dialect adapter has no tests and contains a stale
  WAL file (`test_knex.db-wal`) committed to the repository

Including this in a 1.0 release without qualification would set false expectations.

- [x] ~~**Option A (recommended):** Remove Node.js binding from the 1.0 release entirely
      and re-introduce it in a later release when it is complete; OR~~
- [x] **Option B:** ~~Keep it but clearly mark it as **experimental / unsupported**~~ —
      Node.js binding is now production-ready with 26 tests (decentdb-native) + 9 tests
      (knex-decentdb) covering all data types, error handling, constraints, transactions,
      async iteration, schema introspection, and statement lifecycle
- [x] Remove committed test artifacts (`bindings/node/knex-decentdb/test_knex.db`,
      `bindings/node/knex-decentdb/test_knex.db-wal`)

---

## 2 — High Priority

These items are not strict blockers but will cause friction or erode trust if left
unaddressed. They should be completed before or shortly after the release tag.

### 2.1 Stale / uncommitted artifacts in repository root

The working directory contains several files that should not be present in a clean
checkout. While most match `.gitignore` patterns and are not tracked, some may slip
through or confuse contributors:

| File(s) | Type | Action |
|---------|------|--------|
| `debug_ast` | Debug artifact | Delete and add to `.gitignore` if not already covered |
| `snippet.nim` | Scratch file | Delete and add to `.gitignore` |
| `test_pk_fk.ddb-wal`, `test_pk_fk_restrict.db-wal`, `test_pk_opt.db-wal`, `test_pk_prepared_update.db-wal`, `test_pk_text.db-wal` | Stale test WAL files | Delete; add `*.db-wal` and `*.ddb-wal` patterns to `.gitignore` |
| `testresults.html` | Testament HTML report | Delete; already covered by `testresults/` in `.gitignore` but the HTML file is separate |
| `libdecentdb.so` | Built shared library | Already in `.gitignore` (`*.so`); delete from working directory |

- [x] Delete stale files from working directory
- [x] Verify `.gitignore` covers `debug_ast`, `snippet.nim`, `*.db-wal`, `*.ddb-wal`,
      and `testresults.html`
- [x] Confirm none of these files are tracked by git (`git ls-files` check)

### 2.2 C API input validation gaps

The C API (`src/c_api.nim`) is the foundation for all language bindings. Several
functions accept untrusted input from C callers without sufficient validation. In a
1.0 release these are the API contract that third-party code depends on.

#### 2.2.1 `decentdb_bind_text()` and `decentdb_bind_blob()` (lines 537–554)

```nim
proc decentdb_bind_text*(p: pointer, col: cint, utf8: cstring, byte_len: cint): cint =
  var bytes = newSeq[byte](byte_len)          # byte_len could be negative
  if byte_len > 0: copyMem(addr bytes[0], utf8, byte_len)
```

- `byte_len` is a `cint` (signed 32-bit). A negative value passed from C will be
  implicitly converted to a very large `Natural` when calling `newSeq`, causing either
  an out-of-memory crash or undefined behavior.
- No null-check on the `data`/`utf8` pointer when `byte_len > 0`.
- No upper bound on size (a caller could pass `INT32_MAX` and allocate 2 GB).

**Fix:** Validate `byte_len >= 0` and `byte_len <= MAX_BIND_SIZE` (e.g., 1 GB) before
allocation. Return error code `-1` and set last error on failure. Check that the source
pointer is not nil when `byte_len > 0`.

#### 2.2.2 `decentdb_column_text()` and `decentdb_column_blob()` (lines ~748–758)

These return `unsafeAddr h.currentValues[col].bytes[0]` as a raw `cstring`/`ptr uint8`
to the caller. The pointer becomes invalid if:

- The statement is stepped again (new row overwrites `currentValues`)
- The statement is finalized
- The database is closed

This is standard SQLite-style behavior but **must be documented** in the C API header
and binding READMEs so users understand the lifetime contract.

**Fix:** Add clear documentation (not a code change) about pointer lifetime rules.

#### 2.2.3 `decentdb_prepare()` — no SQL length limit (line ~368)

There is no check on the length of the SQL string before passing it to libpg_query.
A malicious or accidental multi-megabyte SQL string could cause excessive memory
allocation in the parser.

**Fix:** Add a configurable max SQL length (e.g., 1 MB default) and reject with
`ERR_SQL` if exceeded.

- [x] Add `byte_len >= 0` guard and upper-bound check in `decentdb_bind_text`
- [x] Add `byte_len >= 0` guard and null-pointer check in `decentdb_bind_blob`
- [x] Add SQL length limit in `decentdb_prepare`
- [x] Document pointer lifetime rules for `decentdb_column_text/blob` return values

### 2.3 WAL frames lack checksums

The database header uses CRC32C checksums (ADR-0016, implemented in
`src/pager/db_header.nim`), but individual WAL frames have **no integrity check**.
A corrupted or bit-rotted frame will be silently replayed during WAL recovery,
potentially corrupting the database.

This is particularly concerning because DecentDB's north star is "Priority #1: Durable
ACID writes." WAL replay is the primary recovery mechanism after a crash, and applying
a corrupted frame silently undermines that guarantee.

SQLite uses a 32-bit checksum on every WAL frame. Adding CRC32C (which is already
implemented in the codebase for the DB header) to each frame would provide the same
protection.

Additionally, in `src/wal/wal.nim` line ~225, the frame type byte is cast directly to
`WalFrameType` without validating it is within the enum range:

```nim
let frameType = WalFrameType(rawFrameType)  # no range check
```

A corrupted byte here could produce an invalid enum value.

- [ ] Add CRC32C checksum to WAL frame format (requires ADR since this changes the
      WAL frame format — see `AGENTS.md` section 5)
- [x] Add range validation before `WalFrameType` cast
- [ ] Update WAL format version if frame layout changes

### 2.4 File permissions not hardened

`src/vfs/os_vfs.nim` creates database and WAL files using Nim's default `open()` call,
which inherits the process umask. On a typical Linux system with umask `022`, database
files will be world-readable (`0644`).

For a database engine, the secure default should be `0600` (owner read/write only) on
Unix systems. This prevents other users on the same machine from reading the database.

- [x] Set file permissions to `0600` after creation on POSIX systems
- [x] Document the default permissions and how to override them

### 2.5 Missing examples for Go and Python bindings

The .NET binding has two polished example projects (`examples/dotnet/dapper-basic/` and
`examples/dotnet/microorm-linq/`). The Go and Python bindings have comprehensive test
suites but **no standalone example programs** that a new user could copy and run.

Users evaluating DecentDB for Go or Python will look for an `examples/` directory first.

- [x] Create `examples/go/` with a basic create-table / insert / query example
- [x] Create `examples/python/` with a basic DB-API 2.0 usage example
- [x] Verify examples compile and run against the current `libdecentdb.so`

---

## 3 — Medium Priority

These items represent rough edges that are acceptable for a 1.0.0 release but should be
tracked for follow-up in 1.0.x or 1.1.0 patches.

### 3.1 Proposed/Draft ADRs should be explicitly deferred

Fifteen ADRs are currently in **Proposed** or **Draft** status. Several of these describe
foundational changes (freelist atomicity, WAL locking, cache contention strategies) that
could be misread as unresolved design questions blocking the release.

| ADR | Title | Status |
|-----|-------|--------|
| 0044 | .NET NuGet Packaging | Proposed |
| 0051 | Freelist Atomicity | Proposed |
| 0052 | Trigram Durability | Proposed |
| 0053 | Fine-Grained WAL Locking | Draft |
| 0054 | Lock Contention Improvements | Draft |
| 0055 | Thread Safety and Snapshot Context | Proposed |
| 0056 | WAL Index Pruning on Checkpoint | Proposed |
| 0057 | Transactional Freelist Header Updates | Proposed |
| 0058 | Background Incremental Checkpoint Worker | Proposed |
| 0059 | Page Cache Contention Strategy | Proposed |
| 0060 | WAL-Safe Page Cache Eviction/Flush Pipeline | Proposed |
| 0061 | Typed Index Key Encoding (Text/Blob) | Proposed |
| 0062 | B+Tree Prefix Compression | Proposed |
| 0063 | Trigram Postings Paging Format | Proposed |
| 0091 | Decimal/UUID Implementation | Draft |

None of these block the 1.0 release — they describe future optimizations or alternative
approaches. However, leaving them in Proposed/Draft status creates ambiguity.

- [x] Update each ADR's status to either **Accepted** (if implemented) or **Deferred**
      (if the decision is explicitly postponed). See `design/ADR_PENDING_PLAN.md` for
      the full deferred ADR tracking document.
- [x] For ADR-0091 (Decimal/UUID): updated to **Accepted** — implementation is complete

### 3.2 SQL reference documentation contains "future release" language

`docs/user-guide/sql-reference.md` contains three forward-looking statements that should
be clarified for a 1.0 release:

| Line | Text | Action |
|------|------|--------|
| 145 | "`ADD CONSTRAINT` is planned for future releases" | Reword to "not supported in this release" or move to a Known Limitations section |
| 238 | "Targetless `ON CONFLICT DO UPDATE` is not yet supported" | Reword to "is not supported" (remove "yet") |
| 242 | "`UPDATE ... RETURNING` and `DELETE ... RETURNING` are not yet supported" | Same — remove "yet" for definitive 1.0 scoping |

- [x] Update SQL reference to use definitive language for 1.0 scope
- [ ] Add a "Planned for future releases" subsection if desired, separated from the
      current feature documentation

### 3.3 Missing resource limits and DoS protections

The engine has some complexity limits (max 10,000 expanded AST nodes, max 16 CTE/view
expansion depth, max 16 trigger recursion depth) but lacks limits in other areas:

| Resource | Current limit | Risk |
|----------|--------------|------|
| Query result set size | None | OOM on large `SELECT *` without `LIMIT` |
| JOIN cardinality | None | Cartesian product can exhaust memory |
| SQL text length | None | Parser may allocate unbounded memory |
| String/BLOB bind size | None | `decentdb_bind_text/blob` accepts up to `INT32_MAX` bytes |
| Subquery nesting depth | None | Deep nesting could exhaust stack |

For an embedded single-process database these are lower risk than for a networked server,
but they should be documented as known limitations at minimum.

- [x] Document resource limit expectations in the user guide or FAQ
- [ ] Consider adding a configurable max result set size (optional, can be post-1.0)
- [x] Consider adding a SQL text length limit (see item 2.2.3)

### 3.4 Compiler warnings in build output

The Nim compiler emits two `XDeclaredButNotUsed` hints during compilation:

```
src/sql/binder.nim(655, 6) Hint: 'viewColumnExprMap' is declared but not used
src/engine.nim(1446, 6) Hint: 'evalInsertExprs' is declared but not used
```

While these are hints rather than warnings and do not affect correctness, they suggest
either dead code that should be removed or incomplete features. A clean build with zero
warnings/hints is a good signal for a 1.0 release.

- [x] Remove or use `viewColumnExprMap` in `src/sql/binder.nim`
- [x] Remove or use `evalInsertExprs` in `src/engine.nim`

### 3.5 Shared library versioning

The release workflow builds `libdecentdb.so` / `libdecentdb.dylib` / `decentdb.dll` but
these artifacts do not carry version metadata:

- **Linux:** No SONAME set (should be `libdecentdb.so.1` with `libdecentdb.so.1.0.0`
  actual file and `libdecentdb.so` → `libdecentdb.so.1` symlink)
- **macOS:** No `install_name` set via `-install_name @rpath/libdecentdb.1.dylib`
- **Windows:** No version resource embedded in the DLL

This makes it difficult for downstream packagers (dpkg, rpm, brew, vcpkg) to handle
library upgrades and ABI compatibility correctly.

- [ ] Add SONAME / version suffix to Linux shared library build
- [ ] Add install_name to macOS dylib build
- [ ] Consider embedding a version resource in Windows DLL (optional)

---

## 4 — Low Priority (Post-1.0)

These items are informational and suitable for the 1.1.0+ roadmap. They are included
here for completeness and to capture findings from the review.

### 4.1 No fuzzing infrastructure

The project has excellent property-based testing (`tests/harness/property_runner.py`)
with random data generation and seed reproducibility, but no coverage-guided fuzzing
(libfuzzer, AFL, or similar). Coverage-guided fuzzing is particularly effective at
finding edge cases in parsers and binary format decoders (SQL parser, record encoding,
WAL frame decoding, B+tree node parsing).

- [ ] Evaluate adding a libfuzzer harness for the SQL parser (via libpg_query)
- [ ] Evaluate adding a fuzzer for WAL frame decoding
- [ ] Evaluate adding a fuzzer for B+tree record encoding/decoding

### 4.2 Concurrent reader stress testing

The codebase has two concurrency-related test files (`test_checkpoint_reader_race.nim`,
`test_wal_checkpoint_race.nim`) but no large-scale multi-threaded stress test that
exercises the one-writer / many-readers model under sustained load. The property-based
test runner covers some concurrent scenarios but is Python-based and limited in
parallelism.

- [ ] Create a Nim-based concurrent stress test (N reader threads + 1 writer thread,
      sustained operations, snapshot isolation verification)
- [ ] Add to CI as a longer-running nightly test if too slow for every push

### 4.3 Large dataset scalability validation

The test suite exercises correctness at small to medium scale but does not validate
behavior with millions of rows. Specific concerns include B+tree depth and rebalancing
at scale, page cache eviction under memory pressure with large working sets, and
checkpoint duration with large WAL files.

- [ ] Create a 1M+ row integration test
- [ ] Validate B+tree depth stays within expected bounds
- [ ] Measure checkpoint latency at scale

### 4.4 Advanced SQL features gap documentation

Several SQL features are partially implemented but not comprehensively tested or
documented. These should be tracked for future work:

| Feature | Current state | Gap |
|---------|--------------|-----|
| Window functions | ROW_NUMBER only | No RANK, DENSE_RANK, LAG, LEAD, NTILE, etc. |
| Set operations | Basic UNION | INTERSECT, EXCEPT not tested/verified |
| Aggregates | COUNT(*), SUM, AVG, MIN, MAX | COUNT(DISTINCT), GROUP_CONCAT not available |
| NULL semantics | Basic support | Edge cases in ORDER BY, GROUP BY, three-valued logic not comprehensively tested |
| Collation | Default byte ordering | No locale-aware or case-insensitive collation |

- [ ] Document supported vs unsupported SQL features in a compatibility matrix
- [ ] Prioritize window function expansion for 1.1

### 4.5 Replace unsafe pointer patterns (long-term)

The codebase uses `cast[ptr UncheckedArray[...]]` in approximately 8+ locations across
`src/c_api.nim`, `src/wal/wal.nim`, `src/btree/btree.nim`, and `src/exec/exec.nim`.
While these are necessary for FFI and performance-critical paths, each one is a potential
source of memory safety bugs. Over time, these should be audited and replaced with
bounds-checked alternatives where the performance cost is acceptable.

- [ ] Audit all `cast[ptr UncheckedArray[...]]` usage and document why each is necessary
- [ ] Replace with bounds-checked alternatives where possible without performance impact

---

## Summary

| Priority | Item count | Status |
|----------|-----------|--------|
| 🔴 **Blockers** | 4 items | Must resolve before tagging v1.0.0 |
| 🟡 **High** | 5 items | Should resolve before or shortly after release |
| 🟠 **Medium** | 5 items | Track for 1.0.x or 1.1.0 |
| 🟢 **Low** | 5 items | Roadmap for post-1.0 |

### Estimated effort for blockers only

| Item | Effort |
|------|--------|
| 1.1 Version numbers | Minutes |
| 1.2 CHANGELOG Known Limitations | Under an hour |
| 1.3 CI workflow | A few hours (write + verify on 3 platforms) |
| 1.4 Node.js binding decision | Decision + small README/doc edit |

The core engine, storage layer, WAL, B+tree, SQL execution, constraint enforcement,
and ACID transaction support are all production-quality with zero TODO/FIXME markers,
consistent error handling, and comprehensive test coverage (88 unit tests + crash
injection + property testing + differential testing). **The engine itself is ready.**
The work remaining is packaging, documentation accuracy, and CI hygiene.
