# CLI Improvements Design Document
**Date:** 2026-01-28  
**Status:** Proposed  
**Type:** Enhancement (User Interface)

## 1. Summary

This document outlines the plan to enhance the `decentdb_cli` tool with comprehensive CLI features including:
- Standard flags (`--help`, `--version`)
- Schema introspection (`--list-tables`, `--describe`, `--list-indexes`)
- Data import/export (`--import`, `--export`, `--bulk-load`, `--dump-sql`)

The implementation will upgrade from manual `parseopt` parsing to the modern `cligen` framework with subcommands.

## 2. Motivation

### 2.1 Current Limitations

The `decentdb_cli` tool (src/decentdb_cli.nim:1-56) currently uses Nim's basic `parseopt` library with these limitations:

1. **No help text** - `--help` flag not implemented; users cannot discover available options
2. **No version information** - `--version` flag missing; cannot verify CLI version
3. **Silent unknown flags** - Invalid options are discarded without warning or error
4. **Poor error messages** - Missing required arguments produce only JSON output
5. **Manual maintenance** - Adding new options requires updating case statements
6. **No short flags** - Only long-form flags supported (e.g., `--db` but not `-d`)

### 2.2 User Impact

**Current experience:**
```bash
$ decentdb_cli --help
{"ok":false,"error":{"code":"ERR_IO","message":"Missing --db argument","context":""},"rows":[]}

$ decentdb_cli --version
{"ok":false,"error":{"code":"ERR_IO","message":"Missing --db argument","context":""},"rows":[]}

$ decentdb_cli --typo-in-flag-name
# Silently ignored, no warning or error - BAD!
```

**Problem:** Users can't discover what flags exist, verify CLI version, or get feedback on typos.

### 2.3 Goals

1. Provide standard Unix-style `--help` / `-h` and `--version` / `-v` flags
2. Generate formatted help text automatically from code documentation
3. **Improve error messages for invalid or missing arguments** (detect typos, suggest correct flags)
4. Support short flags for common options (`-d` for `--db`, `-s` for `--sql`)
5. Enable future extensibility (subcommands, additional options)

## 3. Current Implementation Analysis

### 3.1 Architecture

**Main entry point:** `src/decentdb_cli.nim`

**Current parsing approach:**
```nim
for kind, key, val in getOpt():  # parseopt iterator
  case key
  of "db":
    dbPath = val
  of "sql":
    sql = val
  of "open-close":
    openClose = true
  else:
    discard  # Unknown flags silently ignored
```

**Current flags:**
- `--db=PATH` - Database file path (required)
- `--sql=STATEMENT` - SQL statement to execute (optional)
- `--open-close` - Test mode: open and close DB without running SQL (optional)

**Output format:**
- JSON-only for test harness integration
- Schema: `{"ok": bool, "error": DbError|null, "rows": string[]}`

**Exit codes:**
- `0` - Success
- `1` - Error

### 3.2 Test Integration

The CLI is invoked by Python test harness (`tests/harness/runner.py:20-29`):

```python
def build_engine_command(engine_path: str, db_path: str, sql: str | None, open_close: bool) -> list[str]:
    cmd = [engine_path]
    if engine_path.endswith(".py"):
        cmd = [sys.executable, engine_path]
    cmd += [f"--db={db_path}"]
    if open_close:
        cmd += ["--open-close"]
    elif sql:
        cmd += ["--sql", sql]
    return cmd
```

**Critical constraint:** Output must remain JSON for test harness to parse results.

## 4. Proposed Solution: Upgrade to cligen

### 4.1 Framework Selection

**Chosen framework:** `cligen` (https://github.com/c-blake/cligen)

**Rationale:**

| Criterion | Assessment |
|-----------|------------|
| **Zero boilerplate** | Generates CLI from function signatures automatically |
| **Automatic help** | Generates help from doc comments and type signatures |
| **Built-in flags** | `--help`, `--version` provided out-of-the-box |
| **Type safety** | Compile-time validation of all arguments |
| **Backward compatible** | Accepts same `--flag=value` syntax as parseopt |
| **Popular** | Most widely used CLI framework in Nim ecosystem |
| **Extensible** | Supports subcommands via `dispatchMulti` for future growth |
| **Dependencies** | Pure Nim, no C library dependencies |

### 4.2 Alternatives Considered

#### Option A: Manual parseopt Enhancement
- **Approach:** Keep parseopt, manually implement `--help` and `--version`
- **Pros:** No new dependencies
- **Cons:** Tedious maintenance, error-prone, reinventing wheel
- **Verdict:** ❌ Not recommended - doesn't meet "modern CLI" standard

#### Option B: argparse Library
- **Approach:** Use Nim's argparse library (Python-like DSL)
- **Pros:** Explicit control, familiar to Python developers
- **Cons:** More verbose than cligen, less idiomatic Nim
- **Verdict:** ⚠️ Viable alternative, but cligen is more Nim-native

#### Option C: docopt Library
- **Approach:** Help-first design (write help text, parser generated)
- **Pros:** Very readable specification
- **Cons:** Less type-safe, less actively maintained
- **Verdict:** ❌ Not recommended for production tool

### 4.3 Expected User Experience

**After implementation:**

```bash
$ decentdb_cli --help
DecentDb CLI - ACID-first embedded relational database

Execute SQL statements against a DecentDb database file.
All output is JSON formatted for programmatic use.

Usage:
  decentdb_cli [optional-params] 
Options:
  -h, --help                      print this cligen-erated help
  --help-syntax                   advanced: prepend,plurals,..
  -v, --version    bool   false   print version
  -d, --db=        string ""      Path to database file (required)
  -s, --sql=       string ""      SQL statement to execute
  --open-close     bool   false   Open and close without executing SQL (testing mode)

$ decentdb_cli --version
0.0.1

$ decentdb_cli --typo-in-flag-name
Error: Unknown option: "typo-in-flag-name"
Try 'decentdb_cli --help' for more information.

$ decentdb_cli -d test.db -s "SELECT * FROM users"
{"ok":true,"error":null,"rows":["id|name|email",...]}
```

**Key improvement:** Invalid flags now produce helpful error messages instead of being silently ignored.

## 5. Implementation Plan

### Phase 1: Add Dependency

**File:** `decentdb.nimble`

**Changes:**
```nim
requires "nim >= 1.6.0"
requires "cligen >= 1.7.0"  # Add this line
```

**Justification:**
- cligen 1.7.0+ is stable and mature
- Pure Nim dependency (no C library complications)
- No impact on core database engine

### Phase 2: Refactor CLI Implementation

**File:** `src/decentdb_cli.nim`

**Changes:**

1. **Add version constant**
   ```nim
   const Version = "0.0.1"  # Matches decentdb.nimble
   ```

2. **Rename and refactor main()**
   ```nim
   proc cliMain(db: string, sql: string = "", openClose: bool = false): int =
     ## DecentDb CLI - ACID-first embedded relational database
     ## 
     ## Execute SQL statements against a DecentDb database file.
     ## All output is JSON formatted for programmatic use.
     
     # Existing logic unchanged, but returns int exit code
     # ... (same validation and execution logic)
   ```

3. **Replace parseopt with cligen dispatch**
   ```nim
   when isMainModule:
     import cligen
     dispatch cliMain,
       version = Version,
       help = {
         "db": "Path to database file (required)",
         "sql": "SQL statement to execute",
         "openClose": "Open and close database without executing SQL (testing mode)"
       },
       short = {
         "db": 'd',
         "sql": 's'
       }
   ```

**Key preservation:**
- ✅ JSON output format unchanged
- ✅ Exit codes unchanged (0 = success, 1 = error)
- ✅ Same core logic for opening DB and executing SQL

### Phase 3: Validation Testing

**No test harness changes required** - cligen accepts same syntax as parseopt.

**New tests to add:**

1. **CLI help/version tests** - New file: `tests/nim/test_cli.nim`
   ```nim
   import std/[unittest, osproc]
   
   suite "CLI interface tests":
     test "help flag shows usage":
       let (output, exitCode) = execCmdEx("./decentdb_cli --help")
       check exitCode == 0
       check "Usage:" in output
       check "--db" in output
     
     test "version flag shows version":
       let (output, exitCode) = execCmdEx("./decentdb_cli --version")
       check exitCode == 0
       check "0.0.1" in output
     
     test "short flags work":
       # Would require test database setup
       discard
   ```

2. **Python harness compatibility test** - Add to `tests/harness/test_runner.py`
   ```python
   def test_cli_version_flag(self):
       """Test --version flag returns version info"""
       result = subprocess.run(
           [self.engine_path, "--version"],
           capture_output=True,
           text=True
       )
       self.assertEqual(result.returncode, 0)
       self.assertIn("0.0.1", result.stdout)
   
   def test_cli_help_flag(self):
       """Test --help flag shows usage"""
       result = subprocess.run(
           [self.engine_path, "--help"],
           capture_output=True,
           text=True
       )
       self.assertEqual(result.returncode, 0)
       self.assertIn("Usage:", result.stdout)
       
### Phase 4: Documentation Updates

**Files to update:**

1. **design/PRD.md** - Add CLI interface section
   ```markdown
   ## CLI Interface
   
   DecentDb provides a command-line tool for direct database interaction:
   
   ```bash
   decentdb_cli --db=path/to/db.db --sql="SELECT * FROM users"
   ```
   
   All output is JSON formatted for programmatic integration with test harnesses.
   ```

2. **README.md** (if exists) - Add usage examples

3. **Create docs/CLI_USAGE.md** (optional) - Detailed CLI reference

### Phase 5: Build System Integration

**File:** `decentdb.nimble`

**Update test task** to include CLI tests:
```nim
task test, "Run Nim + Python unit tests":
  # ... existing tests ...
  exec "nim c -r tests/nim/test_cli.nim"  # Add this line
  exec "python -m unittest tests/harness/test_runner.py"
```

## 6. Testing Strategy

### 6.1 Unit Tests (Nim)

**Coverage:**
- `--help` flag produces formatted output
- `--version` flag shows version number
- `-h` short flag works (alias for `--help`)
- `-v` short flag works (alias for `--version`)
- Unknown flags trigger error message with help hint
- Required `--db` flag validation

### 6.2 Integration Tests (Python Harness)

**Coverage:**
- Existing scenarios continue to work unchanged
- Version check via subprocess
- Help text parsing
- JSON output format validation

### 6.3 Regression Tests

**Critical checks:**
- All existing `tests/harness/test_runner.py` tests pass
- JSON schema unchanged
- Exit codes unchanged
- Flag syntax backward compatible

Tests will need to be modified to use the new CLI interface.

## 7. Risks & Mitigations

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| Test harness breaks | High | Low | Keep JSON output identical; extensive regression testing |
| cligen dependency issues | Medium | Low | Pin version in nimble; pure Nim (no C deps) |
| Help text maintenance | Low | Medium | Doc comments live in code; CI lint checks |
| Performance regression | Low | Very Low | cligen parsing is compile-time optimized |

## 8. Success Criteria (Extended Phase 1)

Implementation is **done** when:

### Basic CLI Features:
✅ `decentdb_cli --help` shows formatted usage with all subcommands  
✅ `decentdb_cli --version` outputs `0.0.1`  
✅ **Invalid flags trigger error** (e.g., `--typo` → "Unknown option" with help hint)  
✅ Short flags work: `-d database.db -s "SELECT 1"`  

### Schema Introspection:
✅ `decentdb schema list-tables -d test.db` lists all tables  
✅ `decentdb schema describe users -d test.db` shows table structure  
✅ `decentdb schema list-indexes -d test.db` shows all indexes  

### Import Functionality:
✅ `decentdb import users data.csv -d test.db` imports CSV  
✅ `decentdb bulk-load users data.csv -d test.db --batch-size=10000` fast bulk import  
✅ Support for both CSV and JSON input formats  

### Export Functionality:
✅ `decentdb export users output.csv -d test.db` exports to CSV  
✅ `decentdb export users output.json -d test.db --format=json` exports to JSON  
✅ `decentdb dump-sql -d test.db > backup.sql` full database dump  

### Testing & Compatibility:
✅ All existing Python harness tests pass without modification  
✅ New CLI tests added to `tests/nim/test_cli.nim`  
✅ Test task in nimble file includes CLI tests  
✅ CI passes on Linux/macOS/Windows  

### Documentation:
✅ CLI-IMPROVEMENTS.md updated with final implementation  
✅ PRD.md updated with CLI interface section  
✅ Usage examples for all new commands  

## 9. ADR Assessment

**Question:** Does this change require an ADR (Architecture Decision Record)?

**Answer:** **No** ✅

**Reasoning:**

Per AGENTS.md Section 5, ADRs are required for:
- ❌ File format layout or versioning strategy - **Not affected**
- ❌ WAL frame format, checksums, commit markers - **Not affected**
- ❌ Checkpoint strategy and truncation rules - **Not affected**
- ❌ Concurrency/locking semantics - **Not affected**
- ❌ SQL dialect decisions - **Not affected**
- ❌ Trigram canonicalization/storage - **Not affected**
- ❌ Major dependencies affecting core engine - **cligen is CLI-only, no engine impact**

This is a **user interface enhancement** that:
- Does not change database file formats, durability, or ACID guarantees
- Does not affect SQL semantics or query execution
- Adds a UI-only dependency (cligen) with no core engine coupling

## 10. Implementation Scope (Extended Phase 1)

### Tasks:
- Add cligen dependency
- Refactor CLI with subcommands structure
- Schema introspection commands
- CSV/JSON parser implementation
- Import and bulk-load functionality
- Export functionality (CSV/JSON/SQL)
- Dump-sql for full database backup
- Comprehensive test coverage
- Documentation updates
- CI validation

## 11. Additional Parameters: Hidden Engine Capabilities

### 11.1 Capabilities Already in Engine

Through exploration of the codebase, these capabilities exist in the engine API but are **not exposed** through the CLI:

#### A. **Bulk Load API** (src/engine.nim) 🌟 **HIGH PRIORITY**
```nim
proc bulkLoad*(db: Db, tableName: string, rows: seq[seq[Value]], 
               options: BulkLoadOptions, wal: Wal): Result[Void]
```

**Proposed CLI options:**
- `--bulk-load <file>` - Load data from CSV/JSON file
- `--batch-size <n>` - Rows per batch (default: 10000)
- `--sync-interval <n>` - Batches between fsync (default: 10)
- `--durability [full|deferred|none]` - Durability mode (default: full)
- `--no-checkpoint` - Skip checkpoint after bulk load

**Use case:** Fast import of large datasets (PRD target: 100k records < 20s)

#### B. **Cache Configuration** (src/pager/pager.nim) 🌟 **HIGH PRIORITY**
```nim
proc newPager*(vfs: Vfs, file: VfsFile, cachePages: int = 64): Result[Pager]
```

Currently **hardcoded to 64 pages** (256KB).

**Proposed CLI options:**
- `--cache-pages <n>` - Set page cache size (default: 64)
- `--cache-mb <n>` - Set cache in megabytes (alternative to --cache-pages)

**Use case:** Performance tuning for different workloads

#### C. **Schema Introspection** (src/catalog/catalog.nim) 🌟 **HIGH PRIORITY**
```nim
type Catalog = ref object
  tables*: Table[string, TableMeta]
  indexes*: Table[string, IndexMeta]
```

**Proposed CLI options:**
- `--list-tables` - List all tables in database
- `--describe <table>` - Show table schema (columns, types, constraints)
- `--list-indexes [table]` - List all indexes (or for specific table)
- `--show-schema` - Dump full database schema as SQL

**Use case:** Schema discovery without writing SQL queries

#### D. **Query Timing & Statistics** 📊 **MEDIUM PRIORITY**
```nim
# Currently only in benchmark suite (tests/bench/bench.nim)
```

**Proposed CLI options:**
- `--timing` - Show query execution time
- `--stats` - Show statistics (cache hits, pages read, etc.)
- `--explain` - Show query execution plan (future)

**Use case:** Performance diagnostics

#### E. **Checkpoint Control** (src/wal/wal.nim) ⚙️ **MEDIUM PRIORITY**
```nim
proc checkpoint*(wal: Wal, pager: Pager): Result[uint64]
proc setCheckpointConfig*(wal: Wal, everyBytes, everyMs, ...): void
```

**Proposed CLI options:**
- `--checkpoint` - Force checkpoint immediately
- `--checkpoint-bytes <n>` - Auto checkpoint when WAL reaches size
- `--checkpoint-ms <n>` - Auto checkpoint after elapsed time
- `--reader-warn-ms <n>` - Warn about long-running readers

**Use case:** Manual WAL management and debugging

#### F. **Transaction Control** (src/wal/wal.nim) 🔄 **MEDIUM PRIORITY**
```nim
proc beginWrite*(wal: Wal): Result[WalWriter]
proc commit*(writer: WalWriter): Result[uint64]
proc rollback*(writer: WalWriter): Result[Void]
```

**Proposed CLI options:**
- `--begin` - Start transaction (with interactive mode)
- `--commit` - Commit transaction
- `--rollback` - Rollback transaction
- `--no-auto-commit` - Manual transaction control

**Use case:** Multi-statement transactions

#### G. **Export/Import** (src/storage/storage.nim) 💾 **MEDIUM PRIORITY**
```nim
proc scanTable*(pager: Pager, table: TableMeta): Result[seq[StoredRow]]
```

**Proposed CLI options:**
- `--export <table> <file>` - Export table to CSV/JSON/SQL
- `--import <table> <file>` - Import from CSV/JSON
- `--dump-sql` - Export entire database as SQL dump

**Use case:** Data portability (aligns with PRD Section 6)

#### H. **Index Maintenance** (src/storage/storage.nim) 🔧 **LOW PRIORITY**
```nim
proc rebuildIndex*(pager, catalog, index): Result[Void]
```

**Proposed CLI options:**
- `--rebuild-index <name>` - Rebuild specific index
- `--rebuild-all-indexes` - Rebuild all indexes
- `--verify-index <name>` - Verify index integrity

**Use case:** Maintenance and corruption recovery

#### I. **Reader Diagnostics** (src/wal/wal.nim) 🔍 **LOW PRIORITY**
```nim
proc readerCount*(wal: Wal): int
proc readersOverThreshold*(wal: Wal, thresholdMs): seq[...]
proc takeWarnings*(wal: Wal): seq[string]
```

**Proposed CLI options:**
- `--reader-count` - Show active reader count
- `--long-readers <ms>` - List readers over threshold
- `--warnings` - Show accumulated WAL warnings
- `--verbose` - Enable verbose output with warnings

**Use case:** Concurrency debugging

#### J. **Database Header Inspection** (src/pager/db_header.nim) 🔍 **LOW PRIORITY**
```nim
type DbHeader = object
  formatVersion*, pageSize*, schemaCookie*, ...
```

**Proposed CLI options:**
- `--dump-header` - Show database header metadata
- `--verify-header` - Verify header checksum
- `--db-info` - Show format version, page size, cache config

**Use case:** Forensics and debugging

### 11.2 Proposed Parameter Tiers

**Phase 1 (Extended - This Document):** 🎯 **IMPLEMENTING NOW**
- ✅ `--help` / `-h`, `--version` / `-v` (standard CLI)
- ✅ `--db` / `-d`, `--sql` / `-s` (existing functionality)
- ✅ `--list-tables`, `--describe`, `--list-indexes` (schema introspection)
- ✅ `--import`, `--bulk-load` (CSV/JSON import - PRD requirement)
- ✅ `--export`, `--dump-sql` (data portability - PRD Section 6)
- ✅ Subcommand structure using `dispatchMulti`

**Phase 2 (Future - Performance & Diagnostics):**
- ⏭️ `--cache-pages`, `--cache-mb` (performance tuning)
- ⏭️ `--timing`, `--stats` (diagnostics)
- ⏭️ `--checkpoint`, `--checkpoint-bytes` (WAL control)
- ⏭️ `--begin`, `--commit`, `--rollback` (transaction control)

**Phase 3 (Future - Advanced/Debugging):**
- ⏭️ `--rebuild-index`, `--verify-index` (maintenance)
- ⏭️ `--reader-count`, `--long-readers` (concurrency diagnostics)
- ⏭️ `--dump-header`, `--db-info` (forensics)
- ⏭️ `--warnings`, `--verbose` (debugging)

### 11.3 Subcommand Architecture (Future)

For Phase 2+, recommend **subcommand structure** using cligen's `dispatchMulti`:

```bash
# Query execution (current default)
decentdb exec --db=test.db --sql="SELECT * FROM users"

# Schema introspection
decentdb schema list-tables --db=test.db
decentdb schema describe users --db=test.db

# Data operations
decentdb bulk-load users data.csv --batch-size=50000 --durability=deferred
decentdb export users users.csv --format=csv
decentdb import users users.csv

# Maintenance
decentdb checkpoint --db=test.db
decentdb rebuild-index users_name_idx --db=test.db

# Diagnostics
decentdb stats --db=test.db
decentdb info --db=test.db  # Header + config
```

This keeps the CLI organized as features grow.

## 12. Future Enhancements (Out of Scope for Phase 1)

These are **not** part of the initial `--help`/`--version` implementation:

1. **Interactive REPL mode** - `decentdb repl --db=path.db`
2. **Configuration file support** - `~/.decentdb/config`
3. **Output format options** - `--format=[json|csv|table]`
4. **Color output** - Syntax highlighting for REPL mode
5. **Shell completion** - Bash/Zsh completion scripts
6. **Parameterized queries** - `--param <value>` for `$1, $2` placeholders
7. **Fault injection** - `--inject-fault` (testing only)

These would require separate design documents and may be considered post-MVP.

## 13. References

- Current CLI implementation: `src/decentdb_cli.nim`
- Test harness integration: `tests/harness/runner.py`
- Project PRD: `design/PRD.md`
- Agent guidelines: `AGENTS.md`
- cligen documentation: https://github.com/c-blake/cligen

## 14. Approval & Next Steps

**Status:** Pending review

**Next steps:**
1. Review this design document
2. If approved, proceed with Phase 1 implementation
3. Iterate through phases 2-5 with testing at each step
4. Mark as **Implemented** when all success criteria met
