# Performance Improvement: Beat SQLite on All Benchmark Metrics

## Objective

Improve DecentDB engine core performance until all metrics in the Python benchmark exceed SQLite performance.

## Current Benchmark Results

**Command:**
```bash
DECENTDB_NATIVE_LIB=/home/steven/source/decentdb/target/release/libdecentdb.so python benchmarks/bench_complex.py --users 1000 --items 50 --orders 100
```

**Current Results:**
```
=== decentdb ===
Generating memory dataset...
Setting up schema...
DecentDB native library: /home/steven/source/decentdb/target/release/libdecentdb.so
Catalog Insert (1050 rows): 0.008955118s
Orders Insert (524 rows): 0.022067484s
Simple Point Lookup (5000 lookups): p50=0.024977ms p95=0.035517ms
Range Scan (5000 scans): p50=0.020719ms p95=0.032461ms
Join Query (5000 joins): p50=0.141866ms p95=0.171593ms
Aggregate Query (5000 aggregates): p50=0.041427ms p95=0.064060ms
Complex Sales Report Query: 0.000453342s
User History Joins (5000 lookups): p50=0.012513ms p95=0.063950ms
Update Operations (5000 updates): p50=0.029676ms p95=0.055504ms
Delete Operations (100 deletes): p50=0.073268ms p95=0.105929ms
Full Table Scan (500 scans): p50=0.016151ms p95=0.024626ms

=== sqlite ===
Generating memory dataset...
Setting up schema...
Catalog Insert (1050 rows): 0.003046582s
Orders Insert (524 rows): 0.003031944s
Simple Point Lookup (5000 lookups): p50=0.003697ms p95=0.003848ms
Range Scan (5000 scans): p50=0.008065ms p95=0.029565ms
Join Query (5000 joins): p50=0.026389ms p95=0.031860ms
Aggregate Query (5000 aggregates): p50=0.016470ms p95=0.024857ms
Complex Sales Report Query: 0.000101812s
User History Joins (5000 lookups): p50=0.003737ms p95=0.008276ms
Update Operations (5000 updates): p50=0.003457ms p95=0.010931ms
Delete Operations (100 deletes): p50=0.015750ms p95=0.021911ms
Full Table Scan (500 scans): p50=0.003005ms p95=0.003767ms

=== Comparison (DecentDB vs SQLite) ===
DecentDB better at:
- none
SQLite better at:
- Catalog Insert Time: 0.003046582s vs 0.008955118s (2.939x faster/lower)
- Orders Insert throughput: 172826.41 rows/s vs 23745.34 rows/s (7.278x higher)
- Point Lookup p50: 0.003697ms vs 0.024977ms (6.756x faster/lower)
- Point Lookup p95: 0.003848ms vs 0.035517ms (9.230x faster/lower)
- Range Scan p50: 0.008065ms vs 0.020719ms (2.569x faster/lower)
- Range Scan p95: 0.029565ms vs 0.032461ms (1.098x faster/lower)
- Join Query p50: 0.026389ms vs 0.141866ms (5.376x faster/lower)
- Join Query p95: 0.031860ms vs 0.171593ms (5.386x faster/lower)
- Aggregate Query p50: 0.016470ms vs 0.041427ms (2.515x faster/lower)
- Aggregate Query p95: 0.024857ms vs 0.064060ms (2.577x faster/lower)
- Complex Report Query: 0.000101812s vs 0.000453342s (4.453x faster/lower)
- User History Join p50: 0.003737ms vs 0.012513ms (3.348x faster/lower)
- User History Join p95: 0.008276ms vs 0.063950ms (7.727x faster/lower)
- Update p50: 0.003457ms vs 0.029676ms (8.584x faster/lower)
- Update p95: 0.010931ms vs 0.055504ms (5.078x faster/lower)
- Delete p50: 0.015750ms vs 0.073268ms (4.652x faster/lower)
- Delete p95: 0.021911ms vs 0.105929ms (4.835x faster/lower)
- Full Table Scan p50: 0.003005ms vs 0.016151ms (5.375x faster/lower)
- Full Table Scan p95: 0.003767ms vs 0.024626ms (6.537x faster/lower)
```

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
   perf record -g -- DECENTDB_NATIVE_LIB=/path/to/libdecentdb.so python benchmarks/bench_complex.py --users 1000 --items 50 --orders 100
   perf report
   
   # Or with flamegraph
   cargo install flamegraph
   flamegraph -o flamegraph.svg -- DECENTDB_NATIVE_LIB=/path/to/libdecentdb.so python benchmarks/bench_complex.py --users 1000 --items 50 --orders 100
   ```

2. **Analyze each benchmark component:**
   - **Catalog Insert (~2.9x slower):** Bulk insert of users and items
   - **Orders Insert (~7.3x slower):** Transaction with multiple table inserts
   - **Complex Report Query (~4.5x slower):** Multi-table JOIN with aggregation
   - **User History Joins (~3.3x-7.7x slower):** Point lookups with JOINs

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
   DECENTDB_NATIVE_LIB=/path/to/libdecentdb.so python benchmarks/bench_complex.py --users 1000 --items 50 --orders 100
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

- Start with profiling to identify actual bottlenecks, not assumptions
- Make incremental changes and validate after each
- Prefer boring, explicit implementations over clever ones
- Keep changes small and focused
- Document any ADR-worthy decisions
