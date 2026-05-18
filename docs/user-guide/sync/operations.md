# Operations

This page covers the day-to-day commands you use after sync is enabled.

## Status

```bash
decentdb sync status --db=app.ddb --format=table
```

Use this to confirm that the replica is enabled, the replica ID is set, and the
journal is growing.

## Doctor

```bash
decentdb sync doctor --db=app.ddb --format=table
```

`sync doctor` combines:

- journal integrity
- retention safety
- peer lag
- unresolved conflicts
- recent session summaries
- guidance text

Use it before prune or before upgrading a schema.

## Peer Lag

The `sync doctor` and `sys_sync_peer_lag` surfaces show inbound and outbound
lag per peer. If lag grows, inspect the latest sessions and the peer binding.

## Retention

Use dry run first:

```bash
decentdb sync prune --db=app.ddb --through=42 --dry-run --format=table
```

If the safe watermark is lower than the requested prune point, the command
fails unless `--allow-data-loss` is set.

`--allow-data-loss` is a deliberate override. It should be used only when you
have another copy of the data or you are intentionally discarding history.

## Maintenance Routine

Recommended order for a healthy replica:

1. `sync status`
2. `sync pending`
3. `sync conflicts`
4. `sync doctor`
5. `sync prune --dry-run`
6. `sync prune` only when the dry run is safe

## Output Shapes

- `sync status` is a compact key-value table or JSON object.
- `sync doctor` is a report object in JSON or a human table with extra sections.
- `sync prune` returns `requested_through`, `effective_through`, `pruned`,
  `dry_run`, `allow_data_loss`, and `blocked_by_json`.

