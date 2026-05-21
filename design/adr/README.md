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
- **0173-lua-extension-function-kind-phasing.md**: Defines the complete Lua extension function and persistence scope: scalar functions, table-valued functions, aggregates, collations, deterministic persisted schema expressions, dependency tracking, and binding/docs coverage.
- **0172-lua-extension-cli-c-abi-and-binding-contract.md**: Defines CLI lifecycle commands, C ABI JSON bridge shape, binding responsibilities, trust configuration, dependency/rebuild commands, and extension inspection surfaces.
- **0171-lua-extension-sql-type-and-planner-contract.md**: Defines SQL registration, strict manifest-declared signatures for all Lua extension objects, DecentDB-owned type conversion, NULL handling, planner limits, and deterministic persisted dependency rules.
- **0170-lua-extension-package-catalog-and-trust.md**: Defines package layout, manifest authority, SHA-256 package hashing, Ed25519 package signatures, database-owned internal package storage, enablement, purge, and connection-level trust.
- **0169-lua-extension-runtime-dependency-and-sandbox.md**: Defines the Lua 5.4 `mlua` vendored runtime direction, native/browser build policy, DecentDB-owned runtime abstraction, sandbox, and resource limits.
- **0168-sync-shape-streaming-subscriptions.md**: Defines production sync shapes as durable scoped subscriptions backed by sync scopes and public changesets, with ack/resume and retention behavior.
- **0167-public-changeset-api.md**: Defines the stable logical changeset envelope, source boundaries, transactional apply, idempotency, inspection, inversion limits, and JSON bridge baseline.
- **0166-production-sync-relay-boundary-and-identity.md**: Defines the self-hosted relay boundary, v2 protocol namespace, principal model, authorization split, transport security posture, and relay diagnostics.
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
