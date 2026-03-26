# Performance Improvement: Beat SQLite on All Benchmark Metrics

## Objective

Improve DecentDB engine core performance until all metrics in the Python benchmark exceed SQLite performance.

## Baseline Benchmark First

**Command:**
```bash
PYTHONPATH=bindings/python DECENTDB_NATIVE_LIB=/home/steven/source/decentdb/target/release/libdecentdb.so python bindings/python/benchmarks/bench_complex.py --users 1000 --items 50 --orders 100
```

**Important:** Do not trust embedded numbers in this prompt. Run the benchmark at the start of the task and treat that output as the source of truth for the current baseline.

**What to record from the fresh run:**
- the full DecentDB section
- the full SQLite section
- the comparison summary
- the exact command, library path, and any environment variables used

Use those fresh numbers to decide which metrics are currently furthest behind and to measure progress after each optimization.
Keep the comparison fair: if you change benchmark inputs or harness settings for DecentDB, apply the equivalent change for SQLite too.

**Target:** All DecentDB metrics must be better than or equal to SQLite.

## The 7 Tenets (from PRD.md)

All changes must adhere to these priorities:

1. **ACID Compliance is Forefront** - Data integrity is non-negotiable. Must survive sudden power loss, kernel panics, and process crashes without corruption. WAL and fsync policies must be mathematically sound and verified via crash-injection testing.

2. **Uncompromising Performance** - Performance must beat SQLite on all fronts. Zero-copy deserialization and lock-free snapshot isolation for concurrent readers. Optimized WAL appending and background checkpointing. Absolute control over the buffer pool.

3. **Minimal Disk Footprint** - Smaller is better, provided it does not compromise ACID guarantees or performance. Use explicit byte-aligned memory layouts.

4. **World-Class Documentation** - Documentation must be accurate, continuously updated, and contain helpful examples.

5. **Best-in-Class Tooling & Bindings** - DecentDB must feel like a native citizen in modern tech stacks.

6. **Fantastic CLI Experience** - The CLI must provide best-in-class UX.

7. **Fast Developer Feedback Loop** - CI/CD must respect developer time. PR checks under 10 minutes.

## Constraints

- **DO NOT** compromise ACID guarantees (Tenet #1) for performance gains
- **DO NOT** add major dependencies without ADR and user approval
- **DO NOT** break existing tests
- **DO NOT** change C ABI or on-disk format without proper ADR
- **DO NOT** use `unsafe` unless strictly required and fully documented
- **DO NOT** commit changes without explicit user approval

## Architecture Context

The engine is organized into these key modules (see `crates/decentdb/src/`):

- `vfs/` - Virtual file system layer
- `storage/` - Pager and page cache
- `wal/` - Write-ahead logging
- `btree/` - B+Tree implementation
- `record/` - Record encoding/decoding
- `catalog/` - Schema management
- `sql/` - SQL parsing
- `planner/` - Query planning
- `exec/` - Query execution
- `search/` - Search/indexing

Key ADRs to reference:
- `design/adr/0001-page-size.md`
- `design/adr/0002-wal-commit-record-format.md`
- `design/adr/0032-btree-page-layout.md`
- `design/adr/0033-wal-frame-format.md`

## Investigation Approach

### Phase 1: Profile and Identify Bottlenecks

1. **Run benchmark with profiling:**
   ```bash
   # CPU profiling with perf
   perf record -g -- env PYTHONPATH=bindings/python DECENTDB_NATIVE_LIB=/path/to/libdecentdb.so python bindings/python/benchmarks/bench_complex.py --users 1000 --items 50 --orders 100
   perf report
   
   # Or with flamegraph
   cargo install flamegraph
   flamegraph -o flamegraph.svg -- env PYTHONPATH=bindings/python DECENTDB_NATIVE_LIB=/path/to/libdecentdb.so python bindings/python/benchmarks/bench_complex.py --users 1000 --items 50 --orders 100
   ```

2. **Analyze each benchmark component:**
   - Catalog Insert: Bulk insert of users and items
   - Orders Insert: Transaction with multiple table inserts
   - Complex Report Query: Multi-table JOIN with aggregation
   - User History Joins: Point lookups with JOINs
   - Any other metric currently losing to SQLite in the fresh comparison output

3. **Key areas to investigate:**
   - WAL write path (fsync frequency, frame format, checkpointing)
   - B+Tree insertion (page splits, node traversal)
   - Query execution (join algorithms, index usage)
   - Record serialization/deserialization overhead
   - Memory allocation patterns
   - Index lookup efficiency

### Phase 2: Implement Optimizations

For each identified bottleneck, implement targeted optimizations:

1. **Write Path (Insert Performance):**
   - WAL frame batching
   - Checkpoint optimization
   - Page cache write coalescing
   - B+Tree bulk load optimization

2. **Read Path (Query Performance):**
   - Index scan optimization
   - Join algorithm improvements
   - Zero-copy record access
   - Query plan caching

3. **Memory and Allocation:**
   - Reduce allocations in hot paths
   - Pre-allocation strategies
   - Buffer pool tuning

### Phase 3: Validate and Test

After each optimization:

1. **Run unit tests:**
   ```bash
   cargo test -p decentdb
   ```

2. **Run binding tests:**
   ```bash
   # Python
   cd bindings/python && pytest
   
   # .NET
   cd bindings/dotnet && dotnet test
   ```

3. **Run benchmark to verify improvement:**
   ```bash
   PYTHONPATH=bindings/python DECENTDB_NATIVE_LIB=/path/to/libdecentdb.so python bindings/python/benchmarks/bench_complex.py --users 1000 --items 50 --orders 100
   ```

4. **Run clippy:**
   ```bash
   cargo clippy --all-targets --all-features -- -D warnings
   ```

5. **Verify ACID guarantees:**
   - Run crash-injection tests if available
   - Verify WAL recovery still works

## Expected Output

For each optimization:

1. **Document the bottleneck** with profiling evidence
2. **Propose the fix** with rationale
3. **Implement the change** with tests
4. **Show benchmark improvement** with before/after numbers
5. **Verify no regressions** in other tests

## Success Criteria

- [ ] Catalog Insert: DecentDB faster than or equal to SQLite
- [ ] Orders Insert: DecentDB faster than or equal to SQLite
- [ ] Complex Report Query: DecentDB faster than or equal to SQLite
- [ ] User History Join p50: DecentDB faster than or equal to SQLite
- [ ] User History Join p95: DecentDB faster than or equal to SQLite
- [ ] All existing tests pass
- [ ] No clippy warnings
- [ ] ACID guarantees preserved

## Reference Files

- `design/PRD.md` - Product requirements
- `design/SPEC.md` - Specification
- `design/TESTING_STRATEGY.md` - Testing approach
- `AGENTS.md` - Agent workflow guidelines
- `.github/copilot-instructions.md` - Repository rules
- `.github/instructions/rust.instructions.md` - Rust coding rules
- `.github/skills/rust-code-generation/SKILL.md` - Rust generation skill
- `bindings/python/benchmarks/bench_complex.py` - Benchmark source

## Notes

- Always run the benchmark first and capture fresh baseline numbers before making optimization claims.
- Start with profiling to identify actual bottlenecks, not assumptions
- Make incremental changes and validate after each
- Prefer boring, explicit implementations over clever ones
- Keep changes small and focused
- Document any ADR-worthy decisions
