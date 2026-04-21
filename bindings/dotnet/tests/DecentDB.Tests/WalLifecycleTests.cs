using System.Data;
using System.Data.Common;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

[Collection(MemoryLeakCollectionDefinition.Name)]
public sealed class WalLifecycleTests : IDisposable
{
    private readonly string _dbPath = ReleaseGateTestHelpers.CreateDbPath("wal_lifecycle");

    public void Dispose()
    {
        ReleaseGateTestHelpers.DeleteDbArtifacts(_dbPath);
    }

    [Fact]
    public void LargeWrites_Checkpoint_LeavesNoLargeResidualJournalArtifact()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using (var command = connection.CreateCommand())
        {
            command.CommandText = "CREATE TABLE wal_probe (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)";
            command.ExecuteNonQuery();
        }

        using (var transaction = connection.BeginTransaction())
        using (var command = connection.CreateCommand())
        {
            command.Transaction = transaction;
            command.CommandText = "INSERT INTO wal_probe (id, payload) VALUES (@p0, @p1)";
            AddParameter(command, "@p0");
            AddParameter(command, "@p1");
            command.Prepare();

            for (var i = 0; i < 50_000; i++)
            {
                command.Parameters[0].Value = i;
                command.Parameters[1].Value = $"payload-{i:D8}";
                command.ExecuteNonQuery();
            }

            transaction.Commit();
        }

        connection.Checkpoint();

        var dashWalAfterCheckpoint = ReleaseGateTestHelpers.FileLengthOrZero(_dbPath + "-wal");
        var dottedWalAfterCheckpoint = ReleaseGateTestHelpers.FileLengthOrZero(_dbPath + ".wal");

        Assert.True(
            dashWalAfterCheckpoint < 1024 * 1024 && dottedWalAfterCheckpoint < 1024 * 1024,
            $"Checkpoint left large journal artifacts (-wal={ReleaseGateTestHelpers.FormatBytes(dashWalAfterCheckpoint)}, .wal={ReleaseGateTestHelpers.FormatBytes(dottedWalAfterCheckpoint)})");
    }

    [Fact]
    public void ReopenAfterCommittedWrites_PreservesDataWithoutExplicitCheckpoint()
    {
        using (var connection = new DecentDBConnection($"Data Source={_dbPath}"))
        {
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = """
                                  CREATE TABLE reopen_probe (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
                                  INSERT INTO reopen_probe (id, payload) VALUES (1, 'one'), (2, 'two'), (3, 'three');
                                  """;
            command.ExecuteNonQuery();
        }

        using var reopened = new DecentDBConnection($"Data Source={_dbPath}");
        reopened.Open();
        using var verify = reopened.CreateCommand();
        verify.CommandText = "SELECT COUNT(*) FROM reopen_probe";

        Assert.Equal(3L, Convert.ToInt64(verify.ExecuteScalar()));
    }

    [Fact]
    public void ReopenThenCheckpoint_LeavesWalBounded()
    {
        using (var connection = new DecentDBConnection($"Data Source={_dbPath}"))
        {
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = """
                                  CREATE TABLE checkpoint_probe (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
                                  INSERT INTO checkpoint_probe (id, payload) VALUES (1, 'one'), (2, 'two'), (3, 'three');
                                  """;
            command.ExecuteNonQuery();
        }

        using var reopened = new DecentDBConnection($"Data Source={_dbPath}");
        reopened.Open();
        reopened.Checkpoint();

        var walBytes = ReleaseGateTestHelpers.FileLengthOrZero(_dbPath + "-wal");
        Assert.True(
            walBytes < 1024 * 1024,
            $"WAL remained unexpectedly large after reopen+checkpoint ({ReleaseGateTestHelpers.FormatBytes(walBytes)})");
    }

    private static void AddParameter(DbCommand command, string name)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        command.Parameters.Add(parameter);
    }
}
