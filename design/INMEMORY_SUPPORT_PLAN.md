# In-Memory Database Support Plan

## 1. Overview
The goal is to support in-memory databases in DecentDB, similar to SQLite's `:memory:`.
The most performant yet simplest way to achieve this is to implement an `InMemoryVfs` (Virtual File System) that stores file contents in memory.
This approach requires zero architectural changes to the core engine (`Pager`, `Wal`, `BTree`), preserves Snapshot Isolation (concurrent readers), and bounds memory usage perfectly.

## 2. Architecture
- **`MemVfs`**: A new VFS implementation that stores files in memory.
- **`MemVfsFile`**: A subclass of `VfsFile` that holds a `string` or `seq[byte]` buffer.
- **`Vfs` Interface Expansion**: Add `getFileSize`, `fileExists`, and `removeFile` to the `Vfs` interface to remove direct `os` module dependencies in the engine.
- **`openDb`**: Modified to detect `:memory:` and instantiate a `MemVfs` instead of `OsVfs`.

## 3. Detailed Design

### 3.1 Refactoring `VfsFile`
Currently, `VfsFile` is a concrete type containing a `File` handle:
```nim
type VfsFile* = ref object
  path*: string
  file*: File        # <-- This must move to a subclass
  lock*: Lock
  bufferedDirty*: Atomic[bool]
```
We will refactor it to be an extensible base class:
```nim
type VfsFile* = ref object of RootObj
  path*: string
  lock*: Lock
  bufferedDirty*: Atomic[bool]
```
`OsVfs` will define `OsVfsFile` inheriting from `VfsFile` and adding the `file: File` field.
`MemVfs` will define `MemVfsFile` inheriting from `VfsFile` and adding the `data: string` field.

#### 3.1.1 Affected Files
The following files directly access `VfsFile.file` and must be updated to cast to `OsVfsFile`:
- `src/vfs/os_vfs.nim` - All VFS methods that access `file.file`
- `src/engine.nim` - Line 34 defines `file*: VfsFile` in `Db` object
- `src/pager/pager.nim` - Line 45 defines `file*: VfsFile` in `Pager` object
- `src/pager/db_header.nim` - Lines 235, 244 use `VfsFile` in `readHeader`/`writeHeader`
- `src/wal/wal.nim` - Line 70 defines `file*: VfsFile` in `Wal` object

**Migration Strategy**: Since `VfsFile` methods receive the file as a parameter, the VFS implementation (OsVfs or MemVfs) will cast the `VfsFile` to its concrete type internally. No changes required in callers.

### 3.2 Expanding the `Vfs` Interface
Currently, `engine.nim`, `pager.nim`, and `wal.nim` use `os.getFileInfo`, `os.fileExists`, and `os.removeFile` directly. **Run `grep -r 'os\\.(getFileInfo|fileExists|removeFile|fileSize)' src/` before implementation to confirm all locations.**

These must be abstracted into the `Vfs` interface:
```nim
method getFileSize*(vfs: Vfs, path: string): Result[int64] {.base.}
method fileExists*(vfs: Vfs, path: string): bool {.base.}
method removeFile*(vfs: Vfs, path: string): Result[Void] {.base.}
```
`OsVfs` will implement these using the `os` module. `MemVfs` will implement these by checking its internal `files` table.

### 3.3 Implementing `MemVfs`
Create `src/vfs/mem_vfs.nim`:
```nim
type MemVfsFile* = ref object of VfsFile
  data*: seq[byte]

type MemVfs* = ref object of Vfs
  files*: Table[string, MemVfsFile]
  vfsLock*: Lock
```
- `open`: If `create` is true, create a new `MemVfsFile` and add it to `files`. If false, look it up.
- `read` / `readStr`: Acquire the file's lock, `copyMem` from `data` to the buffer.
- `write` / `writeStr`: Acquire the file's lock, resize `data` if `offset + len > data.len`, `copyMem` from the buffer to `data`.
- `fsync`: No-op (return `okVoid()`).
- `truncate`: Acquire the file's lock, resize `data`.
- `close`: Remove the file from the `files` table and release memory. This ensures prompt cleanup when `Db` is closed.
- `getFileSize`: Return `data.len` as `int64`. Returns `0` if file doesn't exist or `data` is empty.
- `fileExists`: Check if path exists in `files` table.
- `removeFile`: Remove the file from `files` table, releasing memory.
- `supportsMmap`: Return `false`. Memory-mapped I/O is not supported for in-memory files.
- `mapWritable` / `unmap`: Return error `ERR_INTERNAL` - not supported.

#### 3.3.1 Memory Management
- Files are owned by the `MemVfs` instance.
- When `close()` is called, the file is removed from `files` table immediately (not deferred to GC).
- When the `Db` object is closed, it should call `vfs.close()` on all open files, then the `MemVfs` itself can be GC'd.
- This ensures deterministic memory release, important for test scenarios that create/destroy many in-memory databases.

### 3.4 Modifying `openDb`
In `src/engine.nim`:
```nim
proc openDb*(path: string, cachePages: int = 1024): Result[Db] =
  let isMemory = path.endsWith(":memory:")
  let vfs: Vfs = if isMemory: newMemVfs() else: newOsVfs()
  ...
```
Replace all direct `os` calls (`getFileInfo`, `fileExists`, `removeFile`) with `vfs.getFileSize`, `vfs.fileExists`, and `vfs.removeFile`.

### 3.5 WAL Handling for `:memory:`
**Decision**: WAL will NOT be bypassed for `:memory:` databases in v1.

**Rationale**:
1. **Simplicity**: No changes required to WAL, Pager, or recovery code paths.
2. **Correctness**: Preserves Snapshot Isolation semantics exactly as they work for disk-based databases.
3. **Test coverage**: Existing WAL tests will automatically cover in-memory databases.

The `MemVfs` simulates the filesystem safely, so the WAL will function correctly in memory. The overhead of writing to a memory-backed WAL and checkpointing to a memory-backed DB is minimal (just `memcpy` operations).

**Future Optimization**: A `DurabilityMode.dmNone` fast-path could be added later to bypass WAL entirely for `:memory:` databases, reducing memory overhead by ~50% (no double-buffering of data in both DB and WAL). This would require:
- ADR documenting the trade-offs
- Changes to `Wal.nim` to skip WAL writes when `dmNone` is set
- Changes to `Pager.nim` to write directly to the DB file
- Testing to ensure Snapshot Isolation still works correctly

## 4. Performance and Memory Considerations
- **Memory Usage**: The memory usage is bounded to `Size of DB in MemVfs` + `Size of WAL in MemVfs` + `Pager Cache Size (default 4MB)`. The WAL file size is bounded by the auto-checkpoint interval (default 64MB). This means the overhead of double-buffering is minimal and perfectly acceptable for an in-memory database.
- **CPU Overhead**: Reading and writing to `MemVfs` involves `memcpy`, which is extremely fast (microseconds per page).
- **Concurrency**: Because the WAL is still used, Snapshot Isolation and concurrent readers work exactly as they do for disk-based databases.

## 4.1 Risks and Mitigations
| Risk | Impact | Mitigation |
|------|--------|------------|
| Memory exhaustion | Process OOM if in-memory DB grows unbounded | Document that `:memory:` is for ephemeral workloads; consider adding optional memory limits in future |
| GC pressure | Large in-memory DBs cause GC pauses | Use `seq[byte]` instead of `string` for binary data; pre-allocate buffers where possible |
| VfsFile refactoring breaks existing code | Compilation errors in dependent modules | Incremental migration: first add base class, then update OsVfs, then add MemVfs |
| mmap not supported | Performance regression if engine relies on mmap | Engine already handles `supportsMmap() == false` gracefully (used for fault-injection tests) |

## 4.2 ADR Requirement
Per `design/adr/README.md`, an ADR is required for decisions that "have meaningful trade-offs that future contributors will need to understand."

**ADR Required**: Yes. Create `design/adr/NNNN-inmemory-vfs-design.md` documenting:
1. The decision to use inheritance-based VFS extensibility vs. composition
2. The decision to keep WAL enabled for `:memory:` in v1
3. The trade-offs of memory overhead vs. correctness guarantees
4. The mmap support decision (not supported for MemVfs)

## 5. Testing Strategy
- Add unit tests for `MemVfs` in `tests/vfs/test_mem_vfs.nim`.
- Add a test in `tests/test_engine.nim` that opens `:memory:`, creates tables, inserts data, and verifies concurrent readers work.
- Add a test in `tests/test_engine.nim` that verifies multiple independent `:memory:` databases do not share state.RSS drops via `getrusage` or equivalent
- Add a test in `tests/test_engine.nim` that verifies transaction rollback works correctly in `:memory:` (insert data, rollback, verify data is gone).
- Add a test in `tests/test_engine.nim` that verifies memory is released when `Db` is closed (open `:memory:`, insert large data, close, open again, verify memory usage dropped).
- Run core benchmark smoke tests against `:memory:`.
- Ensure that closing the `Db` object properly frees the `MemVfs` memory (no memory leaks).

## 5.1 Connection String Handling
The following connection string patterns will be supported:
- `:memory:` - Standard in-memory database (each connection gets a new isolated instance)

**Not supported in v1** (deferred to future):
- `file::memory:?cache=shared` - Shared in-memory database across connections (requires global registry)
- Named in-memory databases (e.g., `file:mydb?mode=memory`) - Requires additional state management

The `:memory:` pattern matches SQLite's behavior for simplicity and familiarity.

## 6. Documentation Updates
- Update `docs/getting-started/README.md` and `docs/api/README.md` to explain the `:memory:` connection string.
- Clearly state that in-memory databases are process-bound, destroyed when closed, ideal for tests/ephemeral workloads, and durability guarantees apply only to the process lifetime.
- Document that `:memory:` databases do not support mmap (no performance impact expected).
- Add example code showing how to use `:memory:` for unit testing.

## 7. Implementation Checklist
- [ ] Create ADR for in-memory VFS design decisions
- [ ] Refactor `VfsFile` to be a base class
- [ ] Create `OsVfsFile` inheriting from `VfsFile`
- [ ] Update `OsVfs` to use `OsVfsFile`
- [ ] Add `getFileSize`, `fileExists`, `removeFile` to `Vfs` interface
- [ ] Implement `OsVfs` methods for new interface
- [ ] Create `MemVfs` and `MemVfsFile`
- [ ] Update `openDb` to detect `:memory:`
- [ ] Replace direct `os` calls with VFS methods in engine/pager/wal
- [ ] Add unit tests for `MemVfs`
- [ ] Add integration tests for `:memory:` databases
- [ ] Update documentation
- [ ] Run full test suite to verify no regressions
