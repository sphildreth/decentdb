using System;
using System.Data.Common;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using DecentDB.AdoNet;
using DecentDB.Native;
using Xunit;

namespace DecentDB.Tests;

public sealed class SyncSdkTests
{
    [Fact]
    public async Task ConnectionSyncCoversReplicaPeerScopeAndDoctor()
    {
        var dbPath = UniqueDbPath("sync-sdk-replica-peer-scope");
        try
        {
            using var connection = new DecentDBConnection($"Data Source={dbPath}");
            connection.Open();
            ExecuteNonQuery(connection, "CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT)");

            var status = await connection.Sync.InitializeReplicaAsync("node-a");
            Assert.True(status.Enabled);
            Assert.Equal("node-a", status.ReplicaId);

            status = await connection.Sync.GetStatusAsync();
            Assert.True(status.Enabled);
            Assert.Equal("node-a", status.ReplicaId);

            var peer = await connection.Sync.AddPeerAsync(new SyncPeer
            {
                Name = "central",
                Endpoint = "https://sync.example",
                TokenEnv = null
            });
            Assert.Equal("central", peer.Name);

            var peers = await connection.Sync.ListPeersAsync();
            Assert.Single(peers);

            var scope = await connection.Sync.CreateScopeAsync(new SyncScope
            {
                Name = "tenant_42",
                IncludeTables = new() { "items" },
                RowFilter = null
            });
            Assert.Equal("tenant_42", scope.Name);

            var binding = await connection.Sync.BindPeerScopeAsync("central", "tenant_42");
            Assert.Equal("central", binding.PeerName);
            Assert.Equal("tenant_42", binding.ScopeName);

            var bindings = await connection.Sync.ListPeerScopeBindingsAsync();
            Assert.Single(bindings);
        }
        finally
        {
            DecentDBConnection.DeleteDatabaseFiles(dbPath);
        }
    }

    [Fact]
    public async Task ExecuteRawJsonParsesDoctorPayload()
    {
        var dbPath = UniqueDbPath("sync-sdk-doctor-json");
        try
        {
            using var connection = new DecentDBConnection($"Data Source={dbPath}");
            connection.Open();
            ExecuteNonQuery(connection, "CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT)");
            await connection.Sync.InitializeReplicaAsync("node-a");

            var doctorJson = await connection.Sync.ExecuteRawJsonAsync("{\"op\":\"doctor\"}");
            using var document = JsonDocument.Parse(doctorJson);
            Assert.True(document.RootElement.TryGetProperty("highest_severity", out var severity));
            Assert.True(severity.ValueKind == JsonValueKind.String || severity.ValueKind == JsonValueKind.Null);
        }
        finally
        {
            DecentDBConnection.DeleteDatabaseFiles(dbPath);
        }
    }

    [Fact]
    public async Task ExportImportRoundTripAppliesBatchAndPersistsTargetRow()
    {
        var sourcePath = UniqueDbPath("sync-sdk-export-source");
        var targetPath = UniqueDbPath("sync-sdk-export-target");
        try
        {
            using var source = new DecentDBConnection($"Data Source={sourcePath}");
            using var target = new DecentDBConnection($"Data Source={targetPath}");
            source.Open();
            target.Open();

            ExecuteNonQuery(source, "CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT)");
            ExecuteNonQuery(target, "CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT)");

            await source.Sync.InitializeReplicaAsync("node-a");
            await target.Sync.InitializeReplicaAsync("node-b");

            ExecuteNonQuery(source, "INSERT INTO items (id, name) VALUES (1, 'alpha')");

            var batch = await source.Sync.ExportBatchAsync(since: 0, limit: 100);
            var summary = await target.Sync.ImportBatchAsync(batch);
            Assert.Equal(1, summary.Applied);

            var count = ExecuteScalarLong(target, "SELECT COUNT(*) FROM items WHERE id = 1");
            Assert.Equal(1, count);
        }
        finally
        {
            DecentDBConnection.DeleteDatabaseFiles(sourcePath);
            DecentDBConnection.DeleteDatabaseFiles(targetPath);
        }
    }

    [Fact]
    public async Task DryRunPruneReportsDryRunAndLeavesPendingChangesIntact()
    {
        var dbPath = UniqueDbPath("sync-sdk-prune-dry-run");
        try
        {
            using var connection = new DecentDBConnection($"Data Source={dbPath}");
            connection.Open();
            ExecuteNonQuery(connection, "CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT)");
            await connection.Sync.InitializeReplicaAsync("node-a");
            ExecuteNonQuery(connection, "INSERT INTO items (id, name) VALUES (1, 'alpha')");

            var before = await connection.Sync.GetPendingChangesAsync();
            Assert.NotEmpty(before);

            var summary = await connection.Sync.PruneAsync(through: 1, dryRun: true, allowDataLoss: true);
            Assert.True(summary.DryRun);

            var after = await connection.Sync.GetPendingChangesAsync();
            Assert.Equal(before.Count, after.Count);
        }
        finally
        {
            DecentDBConnection.DeleteDatabaseFiles(dbPath);
        }
    }

    private static string UniqueDbPath(string label) =>
        Path.Combine(Path.GetTempPath(), $"{label}-{Guid.NewGuid():N}.ddb");

    private static void ExecuteNonQuery(DecentDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        _ = command.ExecuteNonQuery();
    }

    private static long ExecuteScalarLong(DecentDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(command.ExecuteScalar());
    }
}
