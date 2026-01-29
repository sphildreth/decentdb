# ADR 003: CLI Engine Enhancements for Performance Tuning and Transaction Control

**Status:** Accepted  
**Date:** 2026-01-28  
**Deciders:** Engineering Team  
**Related:** [CLI-IMPROVEMENTS.md](../CLI-IMPROVEMENTS.md), [ADR-001](001-mvp-cli-tool.md)

---

## Context

The CLI improvements implementation (Phase 1 complete) established modern CLI interfaces for schema introspection and data import/export. Phase 2 requires deeper engine integration to support:

1. **Cache Configuration** - Allow users to tune page cache size for performance
2. **WAL Checkpoint Control** - Enable manual checkpoint triggering and configuration
3. **Transaction Control** - Support explicit BEGIN/COMMIT/ROLLBACK commands

Currently, these features are either hardcoded (cache), not exposed (WAL), or stubbed out (transactions) in the engine layer.

### Current Limitations

**1. Cache Size Hardcoded:**
```nim
# engine.nim:109
let pagerRes = newPager(vfs, file, cachePages = 64)  # Hardcoded!
```

**2. WAL Not Exposed:**
```nim
type Db* = ref object
  # ... fields ...
  # No WAL reference! WAL is created/destroyed per operation
```

**3. Transaction Stubs:**
```nim
# engine.nim:494-511
of skBegin, skCommit, skRollback:
  discard  # Not implemented!
```

---

## Decision

We will enhance the `Db` object and `openDb` API to support:

### 1. Configurable Cache Size

**API Changes:**
```nim
proc openDb*(path: string, cachePages: int = 64): Result[Db]
```

**CLI Interface:**
```bash
./decentdb exec --db=test.db --sql="SELECT ..." --cache-pages=256
./decentdb exec --db=test.db --sql="SELECT ..." --cache-mb=1  # Alternative
```

**Rationale:**
- Small databases (< 1MB): Use default 64 pages (256KB)
- Medium databases (1-100MB): Use 256-1024 pages (1-4MB cache)
- Large databases (> 100MB): Use 2048+ pages (8MB+ cache)

### 2. WAL Handle Exposure

**API Changes:**
```nim
type Db* = ref object
  # ... existing fields ...
  wal*: Wal              # Add WAL reference
  activeWriter*: Option[WalWriter]  # Track active transaction

proc getWal*(db: Db): Wal  # Accessor for checkpoint operations
```

**CLI Interface:**
```bash
./decentdb exec --db=test.db --checkpoint  # Force checkpoint
```

**Rationale:**
- WAL lifetime should match Db lifetime for session consistency
- Enables checkpoint control without breaking encapsulation
- Allows future enhancement of checkpoint policies

### 3. Transaction State Management

**API Changes:**
```nim
proc beginTransaction*(db: Db): Result[Void]
proc commitTransaction*(db: Db): Result[Void]
proc rollbackTransaction*(db: Db): Result[Void]

# execSql checks activeWriter and uses it if present
proc execSql*(db: Db, sqlText: string, params: seq[Value] = @[]): Result[seq[string]]
```

**CLI Interface:**
```bash
# Multi-statement transaction (requires engine support for statement batching)
./decentdb exec --db=test.db --sql="BEGIN; INSERT ...; COMMIT;"

# Or explicit transaction mode (future enhancement)
./decentdb exec --db=test.db --begin
./decentdb exec --db=test.db --sql="INSERT ..." --no-auto-commit
./decentdb exec --db=test.db --commit
```

**Rationale:**
- Db object is the natural place for transaction lifecycle
- Enables multi-statement transactions
- Maintains ACID guarantees across CLI calls (if used programmatically)

---

## Implementation Strategy

### Phase 2A: Cache Configuration (Low Risk)

1. Add `cachePages` parameter to `openDb()`
2. Thread parameter through to `newPager()`
3. Add `--cache-pages` and `--cache-mb` CLI flags
4. Update tests to verify cache sizing

**Risk Level:** Low (pure parameter passing)  
**Breaking Change:** No (default parameter maintains compatibility)

### Phase 2B: WAL Exposure (Medium Risk)

1. Add `wal: Wal` field to `Db` object
2. Initialize WAL in `openDb()`, close in `closeDb()`
3. Expose `checkpoint()` via new proc
4. Add `--checkpoint` CLI flag

**Risk Level:** Medium (changes Db lifecycle)  
**Breaking Change:** No (additive only)

### Phase 2C: Transaction State (Medium-High Risk)

1. Add `activeWriter: Option[WalWriter]` to `Db`
2. Implement `beginTransaction/commit/rollback` procs
3. Modify `execSql` to check/use activeWriter
4. Handle BEGIN/COMMIT/ROLLBACK statements

**Risk Level:** Medium-High (changes execution semantics)  
**Breaking Change:** No (changes are opt-in via SQL commands)

---

## Consequences

### Positive

✅ **Tunable Performance** - Users can optimize cache for their workload  
✅ **Manual Checkpoint Control** - Enables WAL management strategies  
✅ **Explicit Transactions** - Supports multi-statement ACID transactions  
✅ **Future-Proof** - Lays groundwork for connection pooling, sessions  
✅ **Backward Compatible** - All changes use defaults or opt-in

### Negative

⚠️ **API Surface Growth** - More parameters and state to manage  
⚠️ **Complexity** - Transaction state adds lifecycle concerns  
⚠️ **Testing Burden** - More edge cases (active tx during close, etc.)

### Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| Memory usage with large cache | Document cache sizing guidelines |
| Forgotten transactions (no auto-rollback) | Add transaction timeout warning |
| WAL handle leaks | Ensure proper cleanup in closeDb() |
| Concurrent access issues | Document single-writer limitation |

---

## Testing Strategy

### Unit Tests

```nim
# Test cache configuration
test "openDb with custom cache size":
  let db = openDb("test.db", cachePages = 256)
  check db.ok
  check db.value.pager.cacheSize == 256

# Test WAL checkpoint
test "manual checkpoint":
  let db = openDb("test.db")
  # ... insert data ...
  let ckRes = checkpoint(db.value.wal, db.value.pager)
  check ckRes.ok

# Test explicit transactions
test "explicit transaction commit":
  let db = openDb("test.db")
  check db.value.beginTransaction().ok
  # ... SQL operations ...
  check db.value.commitTransaction().ok

test "explicit transaction rollback":
  let db = openDb("test.db")
  check db.value.beginTransaction().ok
  # ... SQL operations ...
  check db.value.rollbackTransaction().ok
```

### Integration Tests

- Verify cache sizing affects performance (benchmark)
- Test checkpoint reduces WAL size
- Verify transaction isolation (concurrent readers)
- Test transaction rollback undoes changes

---

## Alternatives Considered

### Alternative 1: Keep Cache Hardcoded
**Rejected:** Limits performance tuning for diverse workloads

### Alternative 2: Environment Variables for Config
```bash
DECENTDB_CACHE_PAGES=256 ./decentdb exec --db=test.db
```
**Rejected:** Less discoverable, harder to script

### Alternative 3: Configuration File
```ini
[performance]
cache_pages = 256
```
**Rejected:** Overkill for MVP, can add later

### Alternative 4: Separate Transaction CLI Tool
```bash
./decentdb exec --db=test.db --begin
./decentdb exec --db=test.db --sql="..."
./decentdb exec --db=test.db --commit
```
**Rejected:** Awkward UX, requires shared lock management

---

## Implementation Checklist

- [ ] Update `Db` type with `wal` and `activeWriter` fields
- [ ] Modify `openDb()` signature to accept `cachePages`
- [ ] Initialize/cleanup WAL in `openDb()`/`closeDb()`
- [ ] Implement `beginTransaction/commit/rollback` procs
- [ ] Update `execSql` to handle transaction state
- [ ] Add cache configuration CLI flags
- [ ] Add checkpoint CLI flag
- [ ] Handle BEGIN/COMMIT/ROLLBACK SQL statements
- [ ] Write unit tests for new functionality
- [ ] Update documentation
- [ ] Run full test suite

---

## References

- [CLI Improvements Design](../CLI-IMPROVEMENTS.md)
- [SQLite WAL Mode](https://www.sqlite.org/wal.html) - Inspiration for checkpoint semantics
- [PostgreSQL Configuration](https://www.postgresql.org/docs/current/runtime-config.html) - Cache sizing examples

---

## Notes

This ADR focuses on **exposing existing engine capabilities** rather than creating new ones. The WAL and transaction infrastructure already exists; we're making it accessible via CLI.

Future enhancements could include:
- Auto-checkpoint policies (`--checkpoint-bytes`, `--checkpoint-ms`)
- Read-only transaction mode
- Savepoints (nested transactions)
- Connection pooling for concurrent CLI invocations
