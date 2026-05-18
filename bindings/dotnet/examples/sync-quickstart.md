# .NET Sync Quickstart

This sample shows the flagship sync surface exposed through
`DecentDB.AdoNet.DecentDBConnection.Sync` or `DecentDB.Native.DecentDB.Sync`.
It uses the local engine JSON bridge, so applications can call sync operations
directly without shelling out to the CLI.

```csharp
using System.Collections.Generic;
using DecentDB.AdoNet;
using DecentDB.Native;

using var connection = new DecentDBConnection("Data Source=app.ddb");
await connection.OpenAsync();

await connection.Sync.InitializeReplicaAsync("node-a");
await connection.Sync.AddPeerAsync(new SyncPeer
{
    Name = "central",
    Endpoint = "https://sync.example",
    TokenEnv = "DECENTDB_SYNC_TOKEN"
});

await connection.Sync.CreateScopeAsync(new SyncScope
{
    Name = "tenant_42",
    IncludeTables = new List<string> { "items" },
    RowFilter = null
});

await connection.Sync.BindPeerScopeAsync("central", "tenant_42");

var doctor = await connection.Sync.GetDoctorReportAsync();
if (doctor.HighestSeverity != SyncDoctorSeverity.Info)
{
    Console.WriteLine($"Doctor severity: {doctor.HighestSeverity}");
}

var batch = await connection.Sync.ExportBatchAsync(since: 0, limit: 100, scope: "tenant_42");
var summary = await connection.Sync.ImportBatchAsync(batch, scope: "tenant_42");
Console.WriteLine($"Applied: {summary.Applied}");
```

Agent-friendly raw JSON access stays available for debugging and tooling:

```csharp
var doctorJson = await connection.Sync.ExecuteRawJsonAsync("{\"op\":\"doctor\"}");
Console.WriteLine(doctorJson);
```

Built-in HTTP transport parity for `sync run` is still future adapter work.
The SDK here is engine-local only.
