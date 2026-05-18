# Quickstart

This quickstart uses two local databases and no external network.

You will:

1. initialize two replicas
2. write a row on the source database
3. export the source journal as a JSON batch
4. import the batch into the target database
5. verify the copied row

## CLI Walkthrough

```bash
mkdir -p .tmp/sync-quickstart
cat > .tmp/sync-quickstart/items.sql <<'SQL'
CREATE TABLE IF NOT EXISTS items (
  id INT64 PRIMARY KEY,
  name TEXT NOT NULL,
  qty INT64 NOT NULL
);
SQL

decentdb exec --db=.tmp/sync-quickstart/source.ddb --sql="$(cat .tmp/sync-quickstart/items.sql)" --noRows
decentdb exec --db=.tmp/sync-quickstart/target.ddb --sql="$(cat .tmp/sync-quickstart/items.sql)" --noRows

decentdb sync init --db=.tmp/sync-quickstart/source.ddb --replica-id=node-a
decentdb sync init --db=.tmp/sync-quickstart/target.ddb --replica-id=node-b

decentdb exec --db=.tmp/sync-quickstart/source.ddb --sql="INSERT INTO items (id, name, qty) VALUES (1, 'widget', 3)" --noRows

decentdb sync status --db=.tmp/sync-quickstart/source.ddb --format=table
decentdb sync pending --db=.tmp/sync-quickstart/source.ddb --since=0 --limit=10 --format=table

decentdb sync export \
  --db=.tmp/sync-quickstart/source.ddb \
  --since=0 \
  --limit=100 \
  --output=.tmp/sync-quickstart/items.batch.json

decentdb sync import \
  --db=.tmp/sync-quickstart/target.ddb \
  --input=.tmp/sync-quickstart/items.batch.json

decentdb exec --db=.tmp/sync-quickstart/target.ddb --sql="SELECT id, name, qty FROM items ORDER BY id" --format=table
decentdb sync pending --db=.tmp/sync-quickstart/target.ddb --since=0 --limit=10 --format=table
```

Expected shapes:

- `sync status` prints `enabled`, `replica_id`, `next_sequence`,
  `journal_path`, and `journal_size`.
- `sync pending` shows a table with `sequence`, `transaction_lsn`, `table`,
  `op`, and `primary_key`.
- `sync export` writes a JSON batch file containing `batch_id`, `record_count`,
  and `records`.
- `sync import` prints a summary like `seen=1, applied=1, skipped=0,
  conflicted=0`.
- the target query returns the inserted row.

## .NET Walkthrough

The same flow is available through `DecentDBSyncClient`.

```csharp
using System.Collections.Generic;
using DecentDB.AdoNet;
using DecentDB.Native;

await using var source = new DecentDBConnection("Data Source=.tmp/sync-quickstart/source.ddb");
await using var target = new DecentDBConnection("Data Source=.tmp/sync-quickstart/target.ddb");

await source.OpenAsync();
await target.OpenAsync();

await source.Sync.InitializeReplicaAsync("node-a");
await target.Sync.InitializeReplicaAsync("node-b");

var batch = await source.Sync.ExportBatchAsync(since: 0, limit: 100);
var summary = await target.Sync.ImportBatchAsync(batch);
Console.WriteLine($"{summary.Applied} row(s) applied");
```

## Journal Flow

```mermaid
flowchart LR
  A[Local write] --> B[Sync journal append]
  B --> C[Export batch]
  C --> D[Import on peer]
  D --> E[Conflict check]
  E --> F[Watermark advance]
  F --> G[Retention/prune later]
```
