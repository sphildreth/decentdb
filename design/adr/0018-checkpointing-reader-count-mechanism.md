# Checkpointing Reader Count Mechanism
**Date:** 2026-01-28
**Status:** Accepted

### Decision
Use an atomic reader reference counter with epoch-based cleanup to coordinate checkpoints with concurrent readers.

### Rationale
- The 0.x baseline requires "only checkpoint when no active readers" for simplicity
- Need atomic mechanism to track reader count without locks
- Must handle race conditions: reader starts after checkpoint begins
- Need fallback for forced checkpoint (timeout expires with active readers)

### Mechanism

#### 1. Atomic Reader Counter
```nim
type CheckpointState = object
  active_readers: AtomicU32   # Number of active read transactions
  checkpoint_epoch: AtomicU64  # Monotonically increasing epoch
  checkpoint_pending: AtomicBool  # True when checkpoint requested
```

#### 2. Reader Lifecycle
```nim
proc begin_read_txn(db: Database): ReadTransaction =
  # Increment counter atomically
  db.checkpoint_state.active_readers.fetch_add(1, Ordering.SeqCst)
  
  # Capture snapshot LSN
  let snapshot_lsn = db.wal.end_lsn.load(Ordering.Acquire)
  
  return ReadTransaction(
    snapshot_lsn: snapshot_lsn,
    start_epoch: db.checkpoint_state.checkpoint_epoch.load(Ordering.Acquire)
  )

proc end_read_txn(txn: ReadTransaction) =
  db.checkpoint_state.active_readers.fetch_sub(1, Ordering.SeqCst)
```

#### 3. Checkpoint Protocol
```nim
proc checkpoint(db: Database) =
  # 1. Set pending flag (blocks new write transactions)
  db.checkpoint_state.checkpoint_pending.store(true, Ordering.SeqCst)
  
  # 2. Wait for active readers to drain (up to timeout)
  let start_time = get_monotonic_time()
  while db.checkpoint_state.active_readers.load(Ordering.SeqCst) > 0:
    if get_monotonic_time() - start_time > checkpoint_timeout:
      # Timeout: force checkpoint with active readers
      force_checkpoint_with_readers(db)
      return
    sleep_ms(10)
  
  # 3. Safe to checkpoint - increment epoch
  db.checkpoint_state.checkpoint_epoch.fetch_add(1, Ordering.SeqCst)
  
  # 4. Copy WAL pages to main DB
  perform_checkpoint(db)
  
  # 5. Truncate WAL and reset state
  truncate_wal(db)
  db.checkpoint_state.checkpoint_pending.store(false, Ordering.SeqCst)
```

#### 4. Forced Checkpoint (Active Readers Present)
If timeout expires with active readers:
1. Increment epoch (readers will see new epoch on next access)
2. Copy pages atomically using copy-on-write semantics
3. Readers using old pages continue without disruption
4. New readers see checkpointed state

### Safety Guarantees
- **No data loss**: Checkpoint only copies committed pages (LSN <= committed_lsn)
- **Reader consistency**: Readers with snapshot_lsn < checkpoint_lsn use WAL overlay
- **No deadlocks**: Timeout ensures checkpoint eventually completes

### Alternatives Considered
- **RWLock**: Would block readers during checkpoint, violating "readers never block"
- **Epoch-based reclamation**: More complex, better for post-1.0 incremental checkpoint
- **Stop-the-world**: Simplest but violates concurrent reader requirement

### Trade-offs
- **Pros**: Readers never blocked, atomic counter is fast, timeout prevents deadlock
- **Cons**: Forced checkpoint has overhead (copy-on-write), counter adds memory barrier per txn

### References
- SPEC.md §4.3 (Checkpointing)
- SPEC.md §5.3 (Locks and latches)
