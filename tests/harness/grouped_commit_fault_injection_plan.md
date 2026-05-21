# Grouped Commit Fault-Injection Test Plan

The plan below defines how to validate grouped-commit behavior now that the
queue path emits explicit commit groups. The basic queue and metrics contract is
covered by unit and binding smoke tests; failpoint-backed crash windows remain
the deeper harness expansion point.

## Test intent

- Validate that grouped commits are either fully durable or rolled back cleanly.
- Validate behavior when failures occur at every durable publish boundary.
- Validate recovery/reopen invariants around partial group readiness.

## Harness model

- Use failpoint-backed storage scenarios already used by `tests/harness`:
  - `wal.write_commit`
  - `wal.sync_metadata`
  - `wal.sync_data`
- Capture reopen behavior with deterministic replay after each fault point.
- Use a small, fixed row set and explicit commit markers.

## Scenario matrix

1. `grouped_commit__all_commit` (reference/control)
   - 3 synthetic queued transactions in one process.
   - No failpoint injected.
   - Expected: all rows durable after successful reopen.

2. `grouped_commit__fail_during_second_commit`
   - Inject `wal.write_commit` error for the second queued transaction.
   - Expected: first commit durable, second and third either rejected or replay-safe,
     no durable state duplication.

3. `grouped_commit__fail_sync_before_group_flush`
   - Inject `wal.sync_data` error before group fsync.
   - Expected: no transaction in the group reports durable success if its data
     was not physically published.

4. `grouped_commit__crash_after_sync`
   - Simulate restart right after group sync returns but before completion ack path.
   - Expected: all transactions that were included in the sync recover as committed.

## Preconditions

- Queue API path present to submit at least 3 independent write requests.
- Deterministic seeds and fixed key assignment to detect duplicates and replay.
- Failpoint-enabled VFS or WAL hooks available for the specific crash window
  under test.

## Assertions

- Persisted row count matches recovered commit set.
- Reopen does not panic and returns a valid manifest.
- If a scenario fails before group durability point, partial rows are not visible
  as committed.
- If a scenario crashes after durability point, pre-planned committed rows are
  present after restart.

## Output artifacts

- A per-scenario JSON report with:
  - `scenario_name`
  - `failpoint`
  - `submitted_requests`
  - `expected_committed_ids`
  - `committed_ids_after_reopen`
  - `status`
  - `recovered_within_budget_ms`
