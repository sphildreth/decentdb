# Examples

This gallery collects the runnable sync samples and the manual workflows that
the docs reference.

## Runnable Scripts

- [manual-exchange.sh](../../examples/sync/manual-exchange.sh)
- [scoped-tenant.sh](../../examples/sync/scoped-tenant.sh)
- [conflict-demo.sh](../../examples/sync/conflict-demo.sh)

## Manual Exchange

The simplest demo is local export/import between two databases.

Use it when you want:

- an offline-only smoke test
- a deterministic batch file
- no network or server process

See the runnable script above and the [quickstart](quickstart.md).

## Scoped Tenant Sync

This demo shows how a peer bound to a scope only moves scoped rows.

See [scopes](scopes.md) for the filtering rules and the script for a runnable
localhost exchange.

## Conflict Demo

This demo creates a deterministic conflict, inspects it, resolves it, and then
reopens it.

See [conflicts](conflicts.md) and the sample script.

## Doctor and Retention Workflow

```bash
decentdb sync doctor --db=app.ddb --format=table
decentdb sync prune --db=app.ddb --through=100 --dry-run --format=table
```

Use this pair before any pruning decision.

