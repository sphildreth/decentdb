# ADR 0151: Sync Operational Hardening
**Date:** 2026-05-17
**Status:** Accepted

## Context

Slices 1-5 made local sync durable, inspectable, transportable, scoped, and
conflict-aware. Operators still needed first-class reporting for prune safety,
replica lag, recent sync sessions, and an end-to-end doctor view that combined
integrity with retention and operational context.

## Decision

1. **Safe prune remains the default.**
   - Pruning is blocked unless the requested sequence is at or below the safe
     prune watermark.
   - Safe prune is derived from known peer/session watermarks, including
     imported peer watermarks already persisted in metadata.
   - The error path must clearly say `cannot prune through ...` and identify the
     blocking watermark or the absence of known watermarks.

2. **Operators get explicit override modes.**
   - `--dry-run` reports what would be pruned without mutating the journal.
   - `--allow-data-loss` permits pruning beyond the safe watermark only when an
     operator explicitly accepts that tradeoff.
   - The prune summary reports the requested and effective cutoff, records
     removed, dry-run mode, override mode, and blocking labels.

3. **Lag and retention are first-class diagnostics.**
   - A peer lag report shows inbound and outbound watermarks, the local high
     watermark, and derived lag for each configured peer.
   - A retention report exposes total journal records, first/last sequence,
     safe prune watermark, prunable record count, blocking labels, and journal
     size.
   - The operational doctor report combines integrity, retention, peer lag,
     unresolved conflicts, recent sessions, severity, and guidance strings.

4. **v1 stays conservative.**
   - The doctor/reporting surface is informational and does not auto-heal or
     auto-prune.
   - Schema drift handling remains limited to existing integrity checks and
     compatibility warnings already supported by the v1 sync system.
   - More advanced recovery, remote coordination, and speculative repair remain
     future work.

## Consequences

- Operators can see why pruning is or is not safe before mutating the journal.
- CLI, SQL inspection, and engine APIs now expose the same operational state.
- The sync surface remains conservative and deterministic in v1.
- More advanced retention policies, remote watermarks, and automated repair are
  still intentionally out of scope.

## References

- `design/WIN_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md`
- `design/FUTURE_WINS.md`
- `crates/decentdb/src/sync.rs`
- `crates/decentdb/src/db.rs`
- `crates/decentdb-cli/src/commands/mod.rs`
