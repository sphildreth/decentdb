using System.Data;
using DecentDB.AdoNet;
using DecentDB.Native;
using Xunit;

namespace DecentDB.Tests;

public sealed class ConcurrencyAndVisibilityTests : IDisposable
{
    private readonly string _dbPath = ReleaseGateTestHelpers.CreateDbPath("concurrency_visibility");

    public void Dispose()
    {
        ReleaseGateTestHelpers.DeleteDbArtifacts(_dbPath);
    }

    [Fact]
    public void UncommittedRows_AreInvisibleAcrossConnections()
    {
        using var setupConnection = new DecentDBConnection($"Data Source={_dbPath}");
        setupConnection.Open();
        using (var setup = setupConnection.CreateCommand())
        {
            setup.CommandText = "CREATE TABLE visibility_probe (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)";
            setup.ExecuteNonQuery();
        }

        using var writerConnection = new DecentDBConnection($"Data Source={_dbPath}");
        using var readerConnection = new DecentDBConnection($"Data Source={_dbPath}");
        writerConnection.Open();
        readerConnection.Open();

        using var transaction = writerConnection.BeginTransaction(IsolationLevel.ReadCommitted);
        using (var writer = writerConnection.CreateCommand())
        {
            writer.Transaction = transaction;
            writer.CommandText = "INSERT INTO visibility_probe (id, payload) VALUES (1, 'pending')";
            writer.ExecuteNonQuery();
        }

        using var reader = readerConnection.CreateCommand();
        reader.CommandText = "SELECT COUNT(*) FROM visibility_probe";
        Assert.Equal(0L, Convert.ToInt64(reader.ExecuteScalar()));

        transaction.Commit();
    }

    [Fact]
    public void CommittedRows_BecomeVisibleToNewConnections()
    {
        using (var writerConnection = new DecentDBConnection($"Data Source={_dbPath}"))
        {
            writerConnection.Open();
            using var setup = writerConnection.CreateCommand();
            setup.CommandText = """
                               CREATE TABLE visibility_probe (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
                               INSERT INTO visibility_probe (id, payload) VALUES (1, 'committed');
                               """;
            setup.ExecuteNonQuery();
        }

        using var readerConnection = new DecentDBConnection($"Data Source={_dbPath}");
        readerConnection.Open();
        using var reader = readerConnection.CreateCommand();
        reader.CommandText = "SELECT payload FROM visibility_probe WHERE id = 1";

        Assert.Equal("committed", reader.ExecuteScalar());
    }

    [Fact]
    public void FirstWriter_CommitAfterLaterWriterCommitted_ThrowsConflict()
    {
        using var setupConnection = new DecentDBConnection($"Data Source={_dbPath}");
        setupConnection.Open();
        using (var setup = setupConnection.CreateCommand())
        {
            setup.CommandText = "CREATE TABLE writer_probe (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)";
            setup.ExecuteNonQuery();
        }

        using var writerOne = new DecentDBConnection($"Data Source={_dbPath}");
        using var writerTwo = new DecentDBConnection($"Data Source={_dbPath}");
        writerOne.Open();
        writerTwo.Open();

        using var transaction = writerOne.BeginTransaction(IsolationLevel.ReadCommitted);
        using (var command = writerOne.CreateCommand())
        {
            command.Transaction = transaction;
            command.CommandText = "INSERT INTO writer_probe (id, payload) VALUES (1, 'writer-one')";
            command.ExecuteNonQuery();
        }

        using (var commandTwo = writerTwo.CreateCommand())
        {
            using var transactionTwo = writerTwo.BeginTransaction(IsolationLevel.ReadCommitted);
            commandTwo.Transaction = transactionTwo;
            commandTwo.CommandText = "INSERT INTO writer_probe (id, payload) VALUES (2, 'writer-two')";
            commandTwo.ExecuteNonQuery();
            transactionTwo.Commit();
        }

        var exception = Assert.Throws<DecentDBException>(() => transaction.Commit());
        Assert.Contains("transaction conflict", exception.Message, StringComparison.OrdinalIgnoreCase);

        using var verify = writerTwo.CreateCommand();
        verify.CommandText = "SELECT COUNT(*) FROM writer_probe";
        Assert.Equal(1L, Convert.ToInt64(verify.ExecuteScalar()));
    }
}
