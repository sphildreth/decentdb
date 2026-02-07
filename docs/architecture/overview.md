# Architecture Overview

DecentDB is designed with a modular architecture emphasizing correctness, performance, and simplicity.

## Design Principles

1. **Correctness First**: ACID guarantees are non-negotiable
2. **Fast Reads**: Optimized for read-heavy workloads
3. **Simple Model**: Single writer, many readers
4. **Testability**: Comprehensive testing at all layers
5. **Portability**: Single file database, cross-platform

## High-Level Architecture

```
┌─────────────────────────────────────────┐
│              SQL Interface              │
│           (Parser, Binder)              │
├─────────────────────────────────────────┤
│           Query Planner                 │
│      (Index selection, Joins)           │
├─────────────────────────────────────────┤
│           Execution Engine              │
│    (Scan, Filter, Join, Sort)          │
├─────────────────────────────────────────┤
│          Storage Manager                │
│   (Tables, Indexes, Constraints)        │
├─────────────────────────────────────────┤
│            B+Tree Layer                 │
│   (Tables, Indexes ordered storage)     │
├─────────────────────────────────────────┤
│             Pager Layer                 │
│   (Page cache, Free space)              │
├─────────────────────────────────────────┤
│               WAL Layer                 │
│   (Write-ahead log, Recovery)           │
├─────────────────────────────────────────┤
│                VFS                      │
│   (File I/O abstraction)                │
└─────────────────────────────────────────┘
```

## Module Responsibilities

### SQL Module (`sql/`)

- **Parser**: Converts SQL text to AST using libpg_query
- **Binder**: Resolves table/column names, validates types
- **SQL Types**: Expression trees, statement representations

### Planner (`planner/`)

- Query optimization
- Index selection
- Join ordering
- Access path selection

### Execution Engine (`exec/`)

- Iterator-based (Volcano) model
- Operators: Scan, Filter, Project, Join, Sort, Limit
- Expression evaluation
- Row materialization

### Storage (`storage/`)

- Table operations (scan, insert, update, delete)
- Index operations (seek, range scan)
- Constraint enforcement (PK, FK, UNIQUE, NOT NULL)
- Trigram index management
- Bulk load operations

### B+Tree (`btree/`)

- Ordered key-value storage
- Page-based tree structure
- Cursor iteration
- Node split/merge
- Overflow handling

### Pager (`pager/`)

- Page cache management
- Page allocation/deallocation
- Free list management
- Database header

### WAL (`wal/`)

- Write-ahead logging
- Transaction commit markers
- Crash recovery
- Checkpointing
- Snapshot isolation for readers

### VFS (`vfs/`)

- OS file abstraction
- Error injection for testing
- Platform independence

## Data Flow

### Query Execution Flow

1. **Parse**: SQL text → AST
2. **Bind**: Resolve names, validate
3. **Plan**: Choose access paths
4. **Execute**: Run operators
5. **Return**: Results to client

Example:
```sql
SELECT * FROM users WHERE id = 1
```

1. Parser creates SELECT statement with WHERE clause
2. Binder resolves "users" table and "id" column
3. Planner sees PK lookup, chooses IndexSeek
4. Executor runs IndexSeek on users PK index
5. Returns row to client

### Write Flow

1. **Begin Transaction**: Acquire write lock
2. **Modify Pages**: Write to page cache
3. **Log Changes**: Append to WAL
4. **Commit**: Write commit marker, fsync WAL
5. **Optional Checkpoint**: Copy pages to main file

### Read Flow

1. **Begin Read**: Capture snapshot LSN
2. **Query**: Read pages from cache or disk
3. **Check WAL**: Overlay WAL frames if newer than snapshot
4. **Return**: Consistent snapshot view

## Concurrency Model

### Single Writer

- Only one write transaction at a time
- Implemented via WAL write mutex
- Simple, no deadlocks
- Writers don't block readers

### Multiple Readers

- Many concurrent read transactions
- Each sees snapshot from transaction start
- Readers use snapshot LSN for consistency
- Readers don't block each other

### Snapshot Isolation

Readers capture `snapshot_lsn` at start:
- See all changes committed before snapshot_lsn
- Don't see changes committed after
- Uncommitted changes never visible

## Storage Format

### Database File Structure

```
Page 1: Header (128 bytes) + Page 1 data
Page 2-N: B+Tree pages, overflow pages, etc.
```

### Header Contents

- Magic bytes: "DECENTDB"
- Format version
- Page size
- Schema cookie
- Root page IDs
- Freelist head
- Last checkpoint LSN
- Checksum

### WAL File Structure

```
Frame 1: [header][payload][checksum]
Frame 2: [header][payload][checksum]
...
Frame N: [commit marker]
```

Frame types:
- **PAGE**: Modified page data
- **COMMIT**: Transaction commit
- **CHECKPOINT**: Checkpoint completed

## Memory Management

### Page Cache

- Fixed-size pool of page buffers
- LRU eviction policy
- Pin/unpin for active pages
- Write-through for dirty pages

### Sort Buffers

- External merge sort for large ORDER BY
- Spills to disk when memory limit exceeded
- Default: 16MB buffer, 64 spill files max

### Row Buffers

- Reusable per-operator buffers
- Avoids per-row heap allocation
- Bounded memory usage

## Testing Strategy

Each layer has comprehensive tests:

- **Unit Tests**: Individual module correctness
- **Property Tests**: Invariants hold under random operations
- **Crash Tests**: WAL recovery verified
- **Differential Tests**: Match PostgreSQL behavior
- **Benchmarks**: Performance regression detection

See [Testing Guide](../development/testing.md) for details.

## Performance Characteristics

### Read Performance

- Point lookup: O(log n) via B+Tree
- Range scan: O(log n + m) where m = results
- Index seek: Direct page access
- Table scan: Sequential page reads

### Write Performance

- Insert: O(log n) for index + O(1) for append
- Update: O(log n) for lookup + O(log n) for re-index
- Delete: O(log n) for lookup + O(log n) for index update
- WAL append: O(1) amortized

### Space Overhead

- B+Tree internal fragmentation: ~20-30%
- WAL: 0-2x database size (depending on checkpoint frequency)
- Indexes: ~20-100% overhead depending on key size

## Extension Points

### Custom VFS

Implement `Vfs` interface for:
- Custom storage backends
- Encryption
- Compression
- Network storage

### Custom Operators

Add to execution engine:
- New join algorithms
- Custom aggregations
- Domain-specific operations

## Further Reading

- [Storage Engine](storage.md) - Page format, B+Trees
- [WAL & Recovery](wal.md) - Durability, checkpoints
- [B+Tree Details](btree.md) - Node structure, splits
- [Query Execution](query-execution.md) - Operators, planning
