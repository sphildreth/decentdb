# Public Changesets

Public changesets are the stable sync envelope for production relay, SDKs, and
tooling. They are not raw journal lines. A changeset carries versioning,
source/base information, compatibility metadata, records, and an integrity hash
so receivers can inspect it and apply it safely.

## Create

Checkpoint changesets are the normal production sync path:

```bash
decentdb sync changeset create \
  --db=app.ddb \
  --from-checkpoint=relay:42 \
  --output=.tmp/app.dcs.json
```

Branch and snapshot changesets use deterministic row diff semantics:

```bash
decentdb sync changeset create \
  --db=app.ddb \
  --from-branch=main \
  --to-branch=review \
  --output=.tmp/review.dcs.json
```

Unsupported table diffs, schema drift, or missing primary-key information fail
before a misleading changeset is produced.

## Inspect And Apply

```bash
decentdb sync changeset inspect --input=.tmp/app.dcs.json --check-local
decentdb sync changeset apply --db=replica.ddb --input=.tmp/app.dcs.json
```

Apply is transactional by default. Reapplying the same changeset is idempotent:
the receiver detects the changeset ID and integrity hash and reports
`already_applied` instead of duplicating writes.

## Invert

```bash
decentdb sync changeset invert \
  --input=.tmp/review.dcs.json \
  --output=.tmp/review-inverse.dcs.json
```

Inversion succeeds only when records carry enough before-state. The engine
returns `CHANGESET_INVERSION_UNSUPPORTED` instead of guessing undo data.

## C ABI And SDKs

The C ABI exposes `ddb_sync_changeset_create_json`,
`ddb_sync_changeset_apply_json`, `ddb_sync_changeset_inspect_json`, and
`ddb_sync_changeset_invert_json`. The general `ddb_db_sync_execute_json` bridge
also supports the `changeset_create`, `changeset_apply`,
`changeset_inspect`, and `changeset_invert` operations for bindings.
