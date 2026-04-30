# DecentDB Writer Self-Stale-Read Investigation

**Date:** 2026-04-23  
**Branch:** `sph-2026-04-21.02`  
**Flaky Test:** `bindings/python/tests/test_concurrency_stress.py::TestWriterReaderInterleave::test_writer_reader_interleave`  
**Baseline:** `cargo test -p decentdb --lib` — 753/753 passed

---

## 1. Problem Statement

The Python stress test starts 1 writer thread (continuous autocommit INSERT) and 3 reader threads (continuous `SELECT MAX(id) FROM t`). After 2 seconds, the writer's own connection (never closed) runs `SELECT COUNT(*) FROM t` and asserts it equals the number of inserts it committed.

**Result:** ~2–5 failures per 30 iterations. The final count is **hundreds of rows lower** than the committed write count. Example failure:

```
AssertionError: Final count 22368 should match writes 22746
```

This is a **writer self-stale-read**: the connection that produced the commits under-counts its own work.

---

## 2. Key Observations

### 2.1 The failure is NOT a reopen bug
- The writer never closes its connection during the 2-second run.
- The under-count is observed via a pre-close `SELECT COUNT(*) FROM t`.
- Therefore the bug is inside the writer's own `Db` handle, not a cross-connection visibility problem.

### 2.2 Auto-checkpoint mitigates rather than causes the flake
- Disabling thresholds (`wal_checkpoint_threshold_pages = 0, wal_checkpoint_threshold_bytes = 0`) **increased** failure rate to ~10/30.
- With default thresholds (~4k pages / 64 MiB) failure rate stays at ~2/30.
- This tells us checkpoints are part of the path, but the default auto-checkpoint path is *reducing* the window.

### 2.3 The checkpoint itself is not silently discarding commits
- Recovery (`wal::recovery`) replays every commit frame from WAL and rebuilds an in-memory `WalIndex`.
- `wal::index` retains all versions unless `prune_at_or_below` or `clear` is called.
- A `SnapshotPageStore` that WAL-falls-back-to-pager should therefore still see every version.

### 2.4 The real corruption happens in `refresh_engine_from_storage`
This is the **primary root cause**.

`refresh_engine_from_storage` (db.rs:2639) rebuilds the in-memory `EngineRuntime` whenever:
- `latest_lsn > last_runtime_lsn` (new WAL content exists), OR
- `latest_checkpoint_epoch != last_seen_checkpoint_epoch` (a checkpoint happened somewhere).

The **checkpoint-epoch branch** is unconditionally destructive:

```rust
if latest_checkpoint_epoch != last_seen_checkpoint_epoch {
    // Refreshes pager cache from disk (clears page cache!)
    self.inner.pager.refresh_from_disk(on_disk_header)?;
}

// Then unconditionally rebuilds the runtime from storage:
let (mut runtime, runtime_lsn) = EngineRuntime::load_from_storage(...)?;
// Overwrites the writer's in-memory engine with this reload:
*guard = runtime;
```

When a checkpoint fires on **another** connection:
1. It copies back WAL versions to disk pages.
2. It advances `checkpoint_epoch`.
3. It may truncate the WAL (setting `wal_end_lsn` → 0).

Now the **writer's next read** sees `epoch_changed == true`. It calls `refresh_from_disk`, which **clears the pager's page cache**, then calls `load_from_storage`. `load_from_storage` takes a `begin_reader()` guard at the *current* `wal_end_lsn` (which may be 0 or very low after truncation). `SnapshotPageStore::read_page` WAL-falls-back-to-pager. Because the pager cache was just cleared, the pager reads stale pages from disk that reflect the checkpointed state — **missing all commits made after the checkpoint**.

The writer's in-memory `EngineRuntime` was **ahead** of disk. The reload **overwrites it with stale data**. The writer never recovers because `last_runtime_lsn` is now set to the (stale) snapshot LSN.

---

## 3. Structural Gap: Why the Existing Fast-path Fails

The existing fast-path in `refresh_engine_from_storage`:

```rust
if latest_lsn <= last_runtime_lsn && latest_checkpoint_epoch == last_seen_checkpoint_epoch {
    return Ok(());
}
```

This only skips reload when **both** LSN and epoch are unchanged. But when a checkpoint fires:
- `latest_lsn` drops to 0 (WAL truncated).
- `latest_checkpoint_epoch` increments.
- `last_runtime_lsn` is still the writer's high watermark.

The fast-path is **not taken** because epoch changed, so the code proceeds to reload — even though the writer's in-memory state is authoritative.

---

## 4. Checkpoint Truncation Guard (Already Implemented)

The prior agent introduced a guard in `checkpoint.rs`:

```rust
if wal.inner.reader_registry.active_reader_count()? == 0 && safe_lsn >= current_lsn {
    index.clear();
    drop(index);
    writer::truncate_to_header(wal)?;
}
```

**What this fixes:** When the last active reader drops *after* `safe_lsn` was computed but *before* the truncation check, the old code would have truncated away post-`safe_lsn` commits. The new guard prevents that race.

**What this does NOT fix:** Even when truncation is correct, the epoch still increments. The writer still sees `epoch_changed` and still reloads from stale disk. So **this guard alone does not eliminate the flake** (confirmed: ~28/30 passes, 2/30 still fail).

---

## 5. Attempted Fixes That Did Not Work

### 5.1 Naïve `wal_truncated && last_runtime_lsn > 0` guard
Tried skipping `load_from_storage` when `latest_lsn < last_runtime_lsn`. This prevented readers from ever seeing newly-checkpointed data in tests like `sql_transaction_reads_see_committed_snapshot_at_start`, where a **reader** connection (with `last_runtime_lsn == 0`) must reload after a writer checkpointed.

### 5.2 `has_uncheckpointed_writes` flag

Added an `AtomicBool` to track whether *this* Db handle had performed uncommitted writes. The idea was: only skip reload if *we* are the ones ahead. Implementation was abandoned because:

- `commit()` has multiple call-sites (autocommit, explicit txn, batch).
- Keeping the flag in sync across all paths is fragile.
- It still wouldn't handle the case where the writer *did* uncommitted writes, then another connection checkpointed, then the writer refreshes — the flag is `true`, but the reload is still needed to pick up schema changes made by the other connection.
- **Critical regression:** Adding the `has_uncheckpointed_writes` field to `DbInner` without proper initialization in `open_with_vfs` left it uninitialized, causing undefined behavior. When combined with the `wal_truncated && has_uncheckpointed_writes` guard in `refresh_engine_from_storage`, the test `sql_transaction_reads_see_committed_snapshot_at_start` failed. The field was reverted.

### 5.3 Why `restore_runtime_from_storage` complicates the LSN-based guard

`restore_runtime_from_storage` is called on txn rollback or commit-persist failure. It sets `last_runtime_lsn` to the snapshot LSN from the newly loaded `EngineRuntime`. If a rollback happens after a checkpoint:

- `last_runtime_lsn` is reset to the checkpoint's snapshot LSN.
- `latest_lsn` from `wal.latest_snapshot()` may still show the *new* WAL end (if the checkpoint didn't truncate).
- Or `latest_lsn` may be 0 (if the checkpoint truncated).

This means `latest_lsn < last_runtime_lsn` is NOT a reliable signal of "writer is ahead" in all cases. After rollback, `last_runtime_lsn` may legitimately be ahead of `latest_lsn` even though the writer hasn't committed anything.

---

## 6. What We Know the Correct Fix Must Do

1. **When any connection's epoch changes** → still refresh pager header / page cache (necessary for reader correctness).
2. **When the writer's `last_runtime_lsn > latest_lsn`** → the writer is ahead. Do NOT clobber `engine` with a stale reload.
3. **When another connection checkpointed and the local `engine` is NOT ahead** → must reload so readers see the checkpointed data.
4. **When no engine exists yet** (`last_runtime_lsn == 0`) → must reload regardless.

The missing piece is a way for `refresh_engine_from_storage` to distinguish:
> "Is the `engine` I currently hold the one that produced `last_runtime_lsn`?"

Today `last_runtime_lsn` is set at `commit()` but also at `restore_runtime_from_storage()`. After a rollback, `last_runtime_lsn` may legitimately lag behind `latest_lsn`. A future commit on this handle will then push it forward again.

---

## 7. Next Steps (Outstanding)

1. **Implement a robust writer-authority check in `refresh_engine_from_storage`**  
   Options:
   - Add a `writer_last_commit_lsn: AtomicU64` field to `DbInner` that is **only** updated by `commit()` (not by restore/reload). Use it to decide: if `latest_lsn <= writer_last_commit_lsn`, skip reload.
   - Or, change `refresh_engine_from_storage` to compare `last_runtime_lsn` against the snapshot LSN used during the most recent successful `load_from_storage`. Only reload if the disk header's `last_checkpoint_lsn` moved AND `wal_end_lsn > last_runtime_lsn`.

2. **Validate with the full checklist:**
   - `cargo fmt --check`
   - `cargo clippy --workspace --all-targets --all-features -- -D warnings`
   - `cargo test -p decentdb --lib` (753/753)
   - `cargo test --workspace`
   - Python flake test 30+ iterations → must be 30/30 passes
   - `git stash pop` → re-run flake test 30+ iterations

3. **Remove remaining diagnostic `eprintln!`** (one remains in `db.rs:1463` inside `open_with_vfs`).

---

## 8. Files Touched So Far

| File | Change | Status |
|------|--------|--------|
| `crates/decentdb/src/wal/checkpoint.rs` | Added `safe_lsn >= current_lsn` truncation guard; removed `eprintln!` | ✅ Clean, tests pass |
| `crates/decentdb/src/db.rs` | Reverted (was broken by `has_uncheckpointed_writes` attempt) | 🔄 Needs correct fix |

---

## 9. Reproduction Script

```python
# .tmp/probe_flake3.py  (already present in working tree)
import os, sys, tempfile, threading, time
sys.path.insert(0, "/home/steven/source/decentdb/bindings/python")
import decentdb

def run_once():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "writer_reader.ddb")
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER, val TEXT)")
        conn.commit()
        conn.close()

        stop_flag = threading.Event()
        write_count = [0]

        def writer():
            conn = decentdb.connect(db_path)
            cur = conn.cursor()
            i = 1
            while not stop_flag.is_set():
                cur.execute("INSERT INTO t VALUES (?, ?)", (i, f"row_{i}"))
                conn.commit()
                write_count[0] = i
                i += 1
            conn.close()

        def reader():
            conn = decentdb.connect(db_path)
            cur = conn.cursor()
            while not stop_flag.is_set():
                cur.execute("SELECT MAX(id) FROM t")
                cur.fetchone()
            conn.close()

        wt = threading.Thread(target=writer)
        rts = [threading.Thread(target=reader) for _ in range(3)]
        wt.start(); [t.start() for t in rts]
        time.sleep(2); stop_flag.set()
        wt.join(timeout=2); [t.join(timeout=2) for t in rts]

        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM t")
        final = cur.fetchone()[0]
        conn.close()
        return final == write_count[0], (final, write_count[0])

for _ in range(30):
    ok, info = run_once()
    if not ok:
        print(f"FAIL: {info}")
        break
else:
    print("All 30 passed")
```

**Current result with only `checkpoint.rs` fix:** ~28/30 passes.

---

*End of document.*
