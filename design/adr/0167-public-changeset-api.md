# ADR 0167: Public Changeset API
**Date:** 2026-05-20
**Status:** Accepted

## Context

DecentDB already captures sync-relevant mutations in a durable local journal and
can exchange batches through file and HTTP workflows. Those batches are enough
for shipped sync slices, but roadmap priority #1 requires a public changeset
API that applications, relays, browser/mobile clients, bindings, and tooling can
use safely.

The public API must not expose raw sidecar journal lines as the stable contract.
The journal is an internal capture format with retention and implementation
constraints. A production changeset needs an explicit envelope, compatibility
metadata, conflict metadata, idempotency fields, stable error behavior, and
clear apply semantics.

## Decision

DecentDB will expose a stable logical **changeset** contract for production
sync and application tooling.

### 1. Public changesets are logical envelopes

A public changeset is a versioned logical envelope, not the raw sync journal.
The initial public envelope is JSON for debuggability and binding reach. A
future binary encoding may be added behind the same logical schema.

The envelope contains at least:

- `changeset_version`;
- `changeset_id`;
- `source_replica_id`;
- `source_kind`;
- `tenant_id` when created under relay context;
- `scope_name` or `shape_id` when scoped;
- `base_kind`;
- `base_checkpoint`, `base_branch`, or `base_snapshot`;
- `start_checkpoint`;
- `end_checkpoint`;
- `source_high_watermark`;
- `schema_fingerprint`;
- `schema_cookie`;
- `sync_contract_version`;
- optional `query_contract_fingerprint`;
- `records`;
- `conflict_policy_hint`;
- `created_at_micros`;
- `producer_capabilities`;
- `limits`;
- optional `signature` or `integrity_hash`.

The record shape contains at least:

- table name;
- operation;
- primary-key payload;
- origin replica and sequence;
- transaction boundary metadata;
- after image for inserts/updates;
- tombstone marker for deletes;
- before hash or version marker when available;
- before image only when the source operation supports it;
- schema cookie at capture or diff time;
- conflict metadata when the record was produced from a conflict-aware source.

### 2. Sources

Changesets can be created from three source boundaries:

1. **Checkpoint boundary**: journal-derived changes since a peer checkpoint.
2. **Branch boundary**: logical diff between two branch heads or branch states.
3. **Snapshot boundary**: logical diff between two retained snapshots or a
   snapshot and a branch head.

Checkpoint changesets are the primary production sync path. Branch and snapshot
changesets reuse the branch diff semantics from ADR 0157 and must reject
unsupported row-diff cases rather than guessing.

### 3. Transactional apply

Applying a changeset is a single transactional operation unless the caller
explicitly requests a documented non-default conflict mode.

Default apply behavior:

- validate envelope version and compatibility;
- validate scope/shape authorization context when present;
- reject malformed or unsupported records before mutating user data;
- apply records through normal logical write paths;
- suppress outbound sync capture for imported remote records;
- record conflicts using existing conflict workflows;
- advance watermarks only after durable success;
- record session metadata and diagnostics durably.

Reapplying the same changeset must be idempotent. The engine must detect an
already-applied `changeset_id` or already-applied source sequence range and
return a stable replay result instead of duplicating writes.

### 4. Inspection and inversion

Changeset inspection is side-effect-free and returns a structured summary:

- source and base information;
- record count and byte size;
- touched tables;
- operation counts;
- checkpoint range;
- schema/query compatibility metadata;
- unsupported features;
- potential retention or apply warnings.

Changeset inversion is supported only when the changeset carries enough before
state or when the source boundary can be re-read to construct a safe inverse.
If inversion is not safe, the API returns `CHANGESET_INVERSION_UNSUPPORTED`.
The engine must not synthesize inverse changes from incomplete state.

### 5. Compatibility checks

Changeset apply must check:

- changeset version;
- sync protocol version;
- engine compatibility range;
- schema fingerprint and schema cookie;
- sync contract version;
- scope/shape contract;
- primary-key shape for every changed table;
- query-contract fingerprint when the changeset was produced for a query-backed
  shape;
- feature flags such as compression, encryption, before images, and conflict
  policies.

Compatibility failure is a structured error and does not mutate user data.

### 6. API surfaces

The engine exposes public changeset operations through:

- Rust APIs on `Db`;
- C ABI JSON request/response entry points;
- CLI commands under `decentdb sync changeset`;
- binding-friendly JSON bridge methods.

Bindings may wrap the JSON bridge in typed language models, but the JSON
request/response contract is the stable cross-binding baseline.

## Rationale

A stable logical changeset lets sync relay, browser/mobile clients, branch
workflows, support bundles, and external tools use one contract instead of
reverse-engineering internal journal details.

Keeping checkpoint changesets as the primary path preserves the shipped sync
journal design. Allowing branch and snapshot sources makes changesets useful
for review, rehearsal, and controlled promotion workflows without replicating
branch metadata to peers.

Restricting inversion to sources with real before state prevents a dangerous
public API from implying undo semantics that the engine cannot prove.

## Consequences

- The sync journal remains an implementation detail.
- Changeset JSON becomes a stable public contract and must be versioned.
- C ABI additions affect all bindings and require binding smoke coverage.
- Branch/snapshot changesets are limited by existing branch diff constraints.
- Apply behavior is easier to test because it remains transactional and uses
  existing conflict/session metadata.

## Alternatives Considered

1. **Expose raw journal records.** Rejected because the sidecar journal is an
   internal capture format and does not carry enough public compatibility,
   identity, or idempotency metadata.
2. **Use only branch diff as the public changeset source.** Rejected because
   production sync needs efficient incremental checkpoint-based export.
3. **Make changesets binary-only.** Rejected for the first production surface.
   JSON is inspectable, easy for bindings, and aligned with the shipped sync
   batch direction.
4. **Always support inversion.** Rejected because many journal-derived records
   do not contain complete before images.

## Validation Requirements

Implementation is not complete until tests cover:

- changeset creation from checkpoints;
- branch/snapshot source rejection for unsupported tables;
- stable JSON schema and round-trip parsing;
- malformed envelope rejection before mutation;
- schema/query compatibility rejection before mutation;
- idempotent reapply;
- transactional rollback on apply failure;
- conflict recording and resolution hooks;
- inversion success only when sufficient before state exists;
- C ABI JSON allocation/free behavior and panic safety;
- binding smoke tests for the JSON bridge.

## References

- `design/FUTURE_WINS.md` priority #1
- `design/WIN_PRODUCTION_RELAY_SPEC.md`
- `design/WIN_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md`
- `design/adr/0147-local-sync-journal-foundation.md`
- `design/adr/0150-sync-conflict-resolution-workflows.md`
- `design/adr/0157-branch-diff-restore-and-merge-semantics.md`
- `design/adr/0158-branch-sync-interaction.md`
- `design/adr/0118-rust-ffi-panic-safety.md`
