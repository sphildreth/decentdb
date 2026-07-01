# Architecture Decision Records (ADRs)

This directory contains the historical and active ADRs for DecentDB.

> **Note on historical context:** Some earlier ADRs (0001 through 0117) reference
> older file paths or module names. The *architectural decisions* (for example
> WAL formats, B-Tree layouts, and SQL semantics) remain valid and binding for
> the current Rust engine.

### Recent Rust-Specific ADRs:
- **0201-c-abi-typed-batch-bool-signature.md**: Extends the existing `ddb_stmt_execute_batch_typed` signature grammar with `b` for BOOLEAN values encoded through the existing `values_i64` array, preserving the C function shape while letting bindings keep boolean DML on the typed prepared-batch path.
- **0199-transaction-local-cascade-delete-batching.md**: Proposed transaction-local row-change delta design for making cascade deletes visible statement-by-statement while batching physical child-table compaction and index maintenance, targeting the MovieDB cascade SQLite gap without changing FK semantics or durability.
- **0198-vectorized-returning-dml-execution.md**: Proposed prepared-plan, direct-projection, and transaction-local vectorized execution design for closing `UPDATE RETURNING` and `INSERT RETURNING` SQLite gaps through ordinary repeated execute calls without weakening durability or changing benchmark lanes.
- **0197-fulltext-runtime-index-delta-overlays.md**: Proposed runtime fulltext base-plus-overlay design to remove whole-index copy-on-write clones during small DML, targeting the Showdown bulk delete and fulltext-index mutation gaps while preserving ADR 0175/0176 fulltext semantics.
- **0196-persisted-dml-and-cascade-delete-performance.md**: Proposed follow-on performance track for closing the remaining SQLite gaps through direct persisted paged-row DML and batched cascade deletes while preserving durable defaults, existing profile semantics, benchmark-result parsing discipline, and fallback correctness.
- **0195-embedded-fast-profile-and-resident-read-fast-path.md**: Adds the `DbConfig::embedded_fast()` preset (retain row sources across autocommit commits + legacy single-payload persist) and a resident read fast path (`try_resident_read_for_statement`) that skips the per-statement WAL-reader + row-source reload when base tables are already resident, closing the autocommit write/read reload cliff while preserving durable sync and the existing presets' memory bounds.
- **0194-query-plan-cache-prepared-plan-reuse.md**: Defines the Phase 1B prepared-plan bundle cache for `PreparedSimple*` read plans and simple DML plans, its first-miss admission behavior, shared invalidation contract, memory-budget split, and validation requirements.
- **0193-query-plan-cache-c-abi-surface-and-binding-contract.md**: Defines the additive `plan_cache_enabled` and `plan_cache_max_bytes` C ABI open options, the default-on behavior, the no-C-ABI-version-bump decision, the `ddb_plan_cache_summary` and `ddb_plan_cache_flush` accessors, and the maintained-binding contract for the connection-local plan cache.
- **0192-query-plan-cache-security-generation-and-tde.md**: Defines the `policy_mask_generation` cache-key counter, the audit-context-as-observable-but-not-cache-key decision, TDE's plan-cache non-interaction, and the round-trip test that enforces the audit-context exclusion.
- **0191-query-plan-cache-memory-accounting-and-eviction.md**: Defines the 256 KiB default, the fixed-overhead-plus-recursive-helper accounting method, the oversized-entry refusal path, the page-cache budget independence, and the lock-model contract for the connection-local plan cache.
- **0190-query-plan-cache-scope-key-and-lifecycle.md**: Defines the connection-local plan cache scope, the (SQL text, parameter shape, persistent schema cookie, temp schema cookie, policy/mask generation) cache key, the `PlanCacheInvalidator` trait, the cross-process lazy-validation pattern, and the four-phase rollout (1A AST cache, 1B simple-plan reuse, 2 process-global, 3 object-level invalidation).
- **0189-runtime-tracing-api-and-binding-contract.md**: Defines SQL-first runtime diagnostics access, Rust-owned typed tracing configuration, conservative C ABI expansion rules, binding responsibilities, and follow-up ADR triggers for broad telemetry APIs.
- **0188-runtime-advisors-and-fix-plan-policy.md**: Defines runtime advisor finding identity, severity/confidence/evidence requirements, conservative missing/unused index guidance, Doctor `--fix-plan`, and the no-automatic-schema-mutation boundary.
- **0187-runtime-sys-views-and-doctor-projection.md**: Defines `sys.sessions`, `sys.slow_queries`, `sys.lock_waits`, `sys.index_usage`, and `sys.doctor_findings` as read-only virtual inspection views with explicit Doctor refresh semantics.
- **0186-runtime-tracing-contract-and-redaction.md**: Defines opt-in bounded in-memory runtime tracing, disabled-overhead targets, redaction defaults, event-family scope, trace buffer behavior, and persistent/export telemetry ADR triggers.
- **0185-rich-structured-error-diagnostics-contract.md**: Defines the versioned structured diagnostic object, stable subcode contract, C ABI JSON accessor direction, binding projection requirements, redaction rules, retry/permanence classification, and Doctor handoff policy.
- **0184-default-fast-planner-and-runtime-contract.md**: Defines the default-fast performance boundary, durable-default guardrails, covering-index execution rules, statistics/plan-cache constraints, and ADR triggers for format, WAL, and broad binding changes.
- **0183-mobile-tde-key-provider-and-platform-keystore-boundary.md**: Defines the mobile TDE key-provider boundary, Keychain/Keystore reference-adapter scope, key-loss behavior, and the rule that Rust and pure Dart remain platform key-store agnostic.
- **0182-mobile-runtime-lifecycle-storage-sync-and-support-tiers.md**: Defines mobile app-process ownership, app-private storage, sidecar handling, best-effort background sync with apply-before-ack, support tiers, and device/simulator validation requirements.
- **0181-mobile-flutter-package-and-native-artifact-contract.md**: Defines Flutter-first mobile packaging, Android/iOS targets, XCFramework direction, separate mobile release workflow, and continued use of the stable C ABI boundary.
- **0180-database-identity-for-coordination-sidecars.md**: Defines the stable non-secret database header identity and sidecar fingerprint required for stale-sidecar detection, plus the format-bump and migration-parser obligations.
- **0179-cross-process-public-contract-bindings-and-diagnostics.md**: Defines the public process-coordination option, binding responsibilities, SQL/CLI diagnostics, and safe-by-default error behavior.
- **0178-cross-process-reader-retention-and-wal-refresh.md**: Defines cross-process reader slots, checkpoint retention across processes, WAL index refresh, and stale reader cleanup.
- **0177-cross-process-coordination-sidecar-and-locking.md**: Defines the coordination sidecar, byte-range file locking model, VFS process-lock capability, and no-mmap v1 direction.
- **0176-full-text-search-storage-durability-and-binding-contract.md**: Defines FTS as engine-owned derived secondary index state, with term/postings/document-stat storage, rebuild/verify behavior, stale-index handling, and binding responsibilities through ordinary SQL.
- **0175-native-full-text-search-query-surface-and-ranking.md**: Defines native `USING fulltext` indexes, `fulltext_match('index', query)`, `bm25('index')` ranking, portable analyzer/query behavior, and the non-virtual-table FTS user surface.
- **0118-rust-ffi-panic-safety.md**: Mandates `catch_unwind` on all C-ABI boundaries.
- **0119-rust-vfs-pread-pwrite.md**: Mandates standard file positional I/O over `unsafe mmap` for the Virtual File System.
- **0120-core-storage-engine-btree.md**: Formalizes the choice of an optimized B+Tree over an LSM-Tree for the core storage engine.
- **0174-local-data-security-tde-policies-masking-audit-context.md**: Defines TDE v1, durable row policies, column masks, audit context, C ABI key options, catalog boundaries, and follow-up security work.
- **0173-lua-extension-function-kind-phasing.md**: Defines the complete Lua extension function scope: scalar functions, table-valued functions, aggregates, query-time collations, persistence boundaries, dependency inspection, and docs/API coverage.
- **0172-lua-extension-cli-c-abi-and-binding-contract.md**: Defines CLI lifecycle commands, C ABI JSON bridge shape, binding responsibilities, trust configuration, dependency/rebuild commands, and extension inspection surfaces.
- **0171-lua-extension-sql-type-and-planner-contract.md**: Defines SQL registration, strict manifest-declared signatures for all Lua extension objects, DecentDB-owned type conversion, NULL handling, planner limits, and persisted-object boundaries.
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
