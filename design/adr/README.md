# Architecture Decision Records (ADRs)

This directory contains the historical and active ADRs for DecentDB.

> **Note on historical context:** Some earlier ADRs (0001 through 0117) reference
> older file paths or module names. The *architectural decisions* (for example
> WAL formats, B-Tree layouts, and SQL semantics) remain valid and binding for
> the current Rust engine.

### Recent Rust-Specific ADRs:
- **0118-rust-ffi-panic-safety.md**: Mandates `catch_unwind` on all C-ABI boundaries.
- **0119-rust-vfs-pread-pwrite.md**: Mandates standard file positional I/O over `unsafe mmap` for the Virtual File System.
- **0120-core-storage-engine-btree.md**: Formalizes the choice of an optimized B+Tree over an LSM-Tree for the core storage engine.
- **0165-production-browser-runtime-contract.md**: Defines browser support tiers, Dedicated Worker owner coordination through BroadcastChannel/Web Locks, service-worker exclusion, diagnostics, and browser sync deferral.
- **0164-reactive-query-subscriptions-and-change-streams.md**: Defines the in-process reactive event hub, watch kinds, post-commit delivery contract, row-diff bounds, and C ABI watch-handle model.
- **0163-operational-sys-metrics.md**: Defines the canonical read-only `sys.*` operational metrics surfaces and legacy `sys_sync_*` compatibility.

### Branch / Diff / Restore / Time-Travel ADRs:
- **0153-branch-metadata-identity-and-user-surface.md**: Defines branch/snapshot identity, default `main`, checkout scope, branch commit markers, CLI/API naming, inspection surfaces, and legacy compatibility.
- **0154-branch-root-manifest-and-copy-on-write-storage.md**: Defines root manifests, page-level copy-on-write branch writes, shared pages, B+Tree root interaction, and the no-reflink storage direction.
- **0155-branch-aware-wal-commit-records-and-recovery.md**: Defines branch-aware WAL commit metadata, atomic branch head updates, recovery order, checksums, and global `wal_end_lsn` behavior.
- **0156-branch-checkpoint-retention-and-garbage-collection.md**: Defines branch-aware checkpointing, root reachability, retention policy, branch garbage collection, and doctor diagnostics.
- **0157-branch-diff-restore-and-merge-semantics.md**: Defines schema/row diff, restore guardrails, constrained merge semantics, conflict policy, and rebase deferral.
- **0158-branch-sync-interaction.md**: Defines default-branch-only sync for v1, local-only branch metadata, preflight imports, merge-to-main sync capture, and restore guardrails.
- **0159-branch-workflow-logical-replay-v1.md**: Records the accepted v1 implementation choice to ship branch workflows through durable branch-head SQL replay before any page-level copy-on-write optimization.
