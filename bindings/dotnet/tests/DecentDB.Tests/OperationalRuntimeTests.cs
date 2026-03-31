using System.Data;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

public sealed class OperationalRuntimeTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_operational_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
        TryDelete(_dbPath + ".copy");
        TryDelete(_dbPath + ".copy-wal");
        TryDelete(_dbPath + ".bak");
    }

    [Fact]
    public void BeginTransaction_PreservesSupportedIsolationLevels()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var readCommitted = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        Assert.Equal(IsolationLevel.ReadCommitted, readCommitted.IsolationLevel);
        readCommitted.Rollback();

        using var readUncommitted = conn.BeginTransaction(IsolationLevel.ReadUncommitted);
        Assert.Equal(IsolationLevel.ReadUncommitted, readUncommitted.IsolationLevel);
        readUncommitted.Rollback();
    }

    [Fact]
    public void BeginTransaction_UnsupportedIsolationLevel_FallsBackToSnapshot()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var transaction = conn.BeginTransaction(IsolationLevel.Serializable);
        Assert.Equal(IsolationLevel.Snapshot, transaction.IsolationLevel);
        transaction.Rollback();
    }

    [Fact]
    public async Task OpenAsync_PreCancelledToken_ThrowsOperationCanceledException()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() => conn.OpenAsync(cts.Token));
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_PreCancelledToken_ThrowsOperationCanceledException()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE cancel_demo (id INTEGER PRIMARY KEY)";

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() => cmd.ExecuteNonQueryAsync(cts.Token));
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }
}
