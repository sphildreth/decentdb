using System;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Threading.Tasks;
using Xunit;
using DecentDb.AdoNet;
using DecentDb.Native;

namespace DecentDb.Tests;

public class TransactionTests : IDisposable
{
    private readonly string _dbPath;

    public TransactionTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
        var walPath = _dbPath + "-wal";
        if (File.Exists(walPath))
            File.Delete(walPath);
    }

    [Fact]
    public void CommitTransaction()
    {
        using var conn = new DecentDbConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)";
        cmd.ExecuteNonQuery();

        using var tx = conn.BeginTransaction();
        cmd.Transaction = tx;

        cmd.CommandText = "INSERT INTO accounts (id, balance) VALUES (1, 100)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO accounts (id, balance) VALUES (2, 200)";
        cmd.ExecuteNonQuery();

        tx.Commit();

        cmd.CommandText = "SELECT SUM(balance) FROM accounts";
        var total = cmd.ExecuteScalar();
        Assert.Equal(300L, Convert.ToInt64(total));
    }

    [Fact]
    public void RollbackTransaction()
    {
        using var conn = new DecentDbConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE logs (id INTEGER PRIMARY KEY, message TEXT)";
        cmd.ExecuteNonQuery();

        using var tx = conn.BeginTransaction();
        cmd.Transaction = tx;

        cmd.CommandText = "INSERT INTO logs (id, message) VALUES (1, 'Before rollback')";
        cmd.ExecuteNonQuery();

        tx.Rollback();

        cmd.CommandText = "SELECT COUNT(*) FROM logs";
        var count = cmd.ExecuteScalar();
        Assert.Equal(0L, count);
    }

    [Fact]
    public void NestedTransactionsNotSupported()
    {
        using var conn = new DecentDbConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)";
        cmd.ExecuteNonQuery();

        using var tx1 = conn.BeginTransaction();

        // MVP: one transaction per connection at a time.
        Assert.Throws<DecentDbException>(() => conn.BeginTransaction());

        tx1.Rollback();
    }

    [Fact]
    public void TransactionIsolationLevelSnapshot()
    {
        using var conn = new DecentDbConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO items (id, name) VALUES (1, 'Initial')";
        cmd.ExecuteNonQuery();

        using var tx = conn.BeginTransaction(IsolationLevel.Snapshot);
        cmd.Transaction = tx;

        cmd.CommandText = "SELECT name FROM items WHERE id = $1";
        var idParam = cmd.CreateParameter();
        idParam.ParameterName = "$1";
        idParam.Value = 1;
        cmd.Parameters.Add(idParam);

        var name = cmd.ExecuteScalar();
        Assert.Equal("Initial", name);

        tx.Commit();
    }

    [Fact]
    public void AutoRollbackOnDispose()
    {
        using var conn = new DecentDbConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE temp_data (id INTEGER PRIMARY KEY, value TEXT)";
        cmd.ExecuteNonQuery();

        DbTransaction? tx = null;
        try
        {
            tx = conn.BeginTransaction();
            cmd.Transaction = tx;

            cmd.CommandText = "INSERT INTO temp_data (id, value) VALUES (1, 'will be rolled back')";
            cmd.ExecuteNonQuery();
        }
        finally
        {
            tx?.Dispose();
        }

        cmd.CommandText = "SELECT COUNT(*) FROM temp_data";
        var count = cmd.ExecuteScalar();
        Assert.Equal(0L, count);
    }
}
