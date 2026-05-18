# Schema Compatibility

Sync is schema-aware. It is not enough for the row shape to look similar; the
schema cookie must be compatible too.

## Safe Changes

These are the safest changes for rolling sync upgrades:

- additive columns with defaults or nullability that preserve old rows
- non-breaking application-level backfills
- scope/catalog changes that do not invalidate existing batches

## Risky Changes

These require more care:

- changing primary keys
- renaming tables that are part of active scopes
- dropping columns that appear in historical batches
- introducing incompatible row filters or scope bindings

## Incompatible Peers

When peers disagree on schema compatibility, sync should fail loudly rather
than silently corrupt data.

Practical guidance:

1. upgrade the schema on one side first if the change is backward-compatible
2. verify the schema cookie and journal health
3. re-run sync
4. if the batch still fails, inspect the conflict or import error text

## Schema Cookie Notes

Each journal record carries a schema cookie. During import, DecentDB checks that
the imported cookie matches the local schema state.

If the cookies differ, the import is rejected with a schema mismatch error.

## Rolling Upgrade Pattern

For a multi-replica deployment:

1. stop writes if the migration is breaking
2. upgrade one replica
3. validate the inspection views and batch exchange
4. upgrade the remaining replicas
5. resume normal sync

## What Not To Assume

- Do not assume arbitrary old batches will import after a breaking DDL change.
- Do not assume schema drift can be auto-merged.
- Do not assume the sync layer will rewrite incompatible history for you.

