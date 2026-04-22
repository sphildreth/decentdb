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

    [Fact]
    public void WalAutoCheckpoint_DefaultThreshold_StaysBounded()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using (var command = connection.CreateCommand())
        {
            command.CommandText = "CREATE TABLE auto_checkpoint_test (id INTEGER PRIMARY KEY, data TEXT)";
            command.ExecuteNonQuery();
        }

        using (var transaction = connection.BeginTransaction())
        using (var command = connection.CreateCommand())
        {
            command.Transaction = transaction;
            command.CommandText = "INSERT INTO auto_checkpoint_test (id, data) VALUES (@p0, @p1)";
            AddParameter(command, "@p0");
            AddParameter(command, "@p1");
            command.Prepare();

            for (var i = 0; i < 10_000; i++)
            {
                command.Parameters[0].Value = i;
                command.Parameters[1].Value = $"data_{i}";
                command.ExecuteNonQuery();
            }

            transaction.Commit();
        }

        var walSizeAfterCommit = ReleaseGateTestHelpers.FileLengthOrZero(_dbPath + "-wal");
        Assert.True(
            walSizeAfterCommit < 5 * 1024 * 1024,
            $"WAL should auto-checkpoint and stay bounded, but was {ReleaseGateTestHelpers.FormatBytes(walSizeAfterCommit)}");
    }

    [Fact]
    public void WalManualCheckpoint_PragmaWalCheckpoint_PersistsData()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using (var command = connection.CreateCommand())
        {
            command.CommandText = "CREATE TABLE checkpoint_pragma_test (id INTEGER PRIMARY KEY, value INTEGER)";
            command.ExecuteNonQuery();

            for (int i = 0; i < 1000; i++)
            {
                command.CommandText = $"INSERT INTO checkpoint_pragma_test (id, value) VALUES ({i}, {i * 10})";
                command.ExecuteNonQuery();
            }

            command.CommandText = "PRAGMA wal_checkpoint(TRUNCATE)";
            command.ExecuteNonQuery();
        }

        using var reopened = new DecentDBConnection($"Data Source={_dbPath}");
        reopened.Open();
        using var verify = reopened.CreateCommand();
        verify.CommandText = "SELECT SUM(value) FROM checkpoint_pragma_test";
        var sum = Convert.ToInt64(verify.ExecuteScalar());
        Assert.Equal(4995000L, sum);
    }

    [Fact]
    public void WalFile_AfterMultipleTransactions_RemainsAccessible()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using var create = connection.CreateCommand();
        create.CommandText = "CREATE TABLE multi_tx_test (id INTEGER PRIMARY KEY, data TEXT)";
        create.ExecuteNonQuery();

        for (int tx = 0; tx < 5; tx++)
        {
            using var transaction = connection.BeginTransaction();
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "INSERT INTO multi_tx_test (id, data) VALUES (@p0, @p1)";
            AddParameter(command, "@p0");
            AddParameter(command, "@p1");
            command.Prepare();

            for (int i = 0; i < 100; i++)
            {
                command.Parameters[0].Value = tx * 100 + i;
                command.Parameters[1].Value = $"tx{tx}_item{i}";
                command.ExecuteNonQuery();
            }

            transaction.Commit();
        }

        using var verify = connection.CreateCommand();
        verify.CommandText = "SELECT COUNT(*) FROM multi_tx_test";
        Assert.Equal(500L, Convert.ToInt64(verify.ExecuteScalar()));

        var walSize = ReleaseGateTestHelpers.FileLengthOrZero(_dbPath + "-wal");
        Assert.True(walSize < 10 * 1024 * 1024, $"WAL too large: {ReleaseGateTestHelpers.FormatBytes(walSize)}");
    }

    [Fact]
    public void WalPragma_JournalMode_EnabledByDefault()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA journal_mode";
        var journalMode = command.ExecuteScalar()?.ToString()?.ToUpperInvariant();

        Assert.True(
            journalMode == "WAL" || journalMode == "DELETE",
            $"Expected WAL or DELETE journal mode, got: {journalMode}");
    }

    [Fact]
    public void ReopenDatabase_AfterWalFullCheckpoint_DataDurable()
    {
        string dbPath2 = ReleaseGateTestHelpers.CreateDbPath("wal_durable");

        try
        {
            using (var connection = new DecentDBConnection($"Data Source={dbPath2}"))
            {
                connection.Open();

                using var command = connection.CreateCommand();
                command.CommandText = "CREATE TABLE durable_test (id INTEGER PRIMARY KEY, payload TEXT)";
                command.ExecuteNonQuery();

                for (int i = 0; i < 500; i++)
                {
                    command.CommandText = $"INSERT INTO durable_test (id, payload) VALUES ({i}, 'payload_{i}')";
                    command.ExecuteNonQuery();
                }

                connection.Checkpoint();
            }

            using var reader = new DecentDBConnection($"Data Source={dbPath2}");
            reader.Open();
            using var verify = reader.CreateCommand();
            verify.CommandText = "SELECT COUNT(*) FROM durable_test";
            Assert.Equal(500L, Convert.ToInt64(verify.ExecuteScalar()));
        }
        finally
        {
            ReleaseGateTestHelpers.DeleteDbArtifacts(dbPath2);
        }
    }
}
