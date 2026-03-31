using DecentDB.AdoNet;
using DecentDB.Native;
using Xunit;

namespace DecentDB.Tests;

public sealed class FailureContractTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_failure_contract_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    [Fact]
    public void UniqueViolation_ThrowsDecentDBException_AndRollbackRemovesEarlierWrites()
    {
        EnsureSchema();

        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var tx = conn.BeginTransaction();
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = "INSERT INTO failure_users (id, email) VALUES (1, 'ada@example.com')";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO failure_users (id, email) VALUES (2, 'ada@example.com')";
        var ex = Assert.Throws<DecentDBException>(() => cmd.ExecuteNonQuery());
        Assert.NotEqual(0, ex.ErrorCode);
        Assert.False(string.IsNullOrWhiteSpace(ex.Message));
        Assert.Equal("INSERT INTO failure_users (id, email) VALUES (2, 'ada@example.com')", ex.Sql);

        tx.Rollback();
        cmd.Transaction = null;
        cmd.CommandText = "SELECT COUNT(*) FROM failure_users";
        Assert.Equal(0L, Convert.ToInt64(cmd.ExecuteScalar()));
    }

    [Fact]
    public void ForeignKeyViolation_ThrowsDecentDBException()
    {
        EnsureSchema();

        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO failure_children (id, parent_id, name) VALUES (1, 999, 'orphan')";
        var ex = Assert.Throws<DecentDBException>(() => cmd.ExecuteNonQuery());
        Assert.NotEqual(0, ex.ErrorCode);
        Assert.False(string.IsNullOrWhiteSpace(ex.Message));
        Assert.Equal("INSERT INTO failure_children (id, parent_id, name) VALUES (1, 999, 'orphan')", ex.Sql);
    }

    [Fact]
    public void CheckConstraintViolation_ThrowsDecentDBException()
    {
        EnsureSchema();

        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO failure_stock_checks (id, quantity) VALUES (1, -1)";
        var ex = Assert.Throws<DecentDBException>(() => cmd.ExecuteNonQuery());
        Assert.NotEqual(0, ex.ErrorCode);
        Assert.False(string.IsNullOrWhiteSpace(ex.Message));
        Assert.Equal("INSERT INTO failure_stock_checks (id, quantity) VALUES (1, -1)", ex.Sql);
    }

    private void EnsureSchema()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
                          DROP TABLE IF EXISTS failure_stock_checks;
                          DROP TABLE IF EXISTS failure_children;
                          DROP TABLE IF EXISTS failure_parents;
                          DROP TABLE IF EXISTS failure_users;
                          CREATE TABLE failure_users (
                              id INTEGER PRIMARY KEY,
                              email TEXT NOT NULL UNIQUE
                          );
                           CREATE TABLE failure_parents (
                               id INTEGER PRIMARY KEY,
                               name TEXT NOT NULL
                           );
                          CREATE TABLE failure_children (
                              id INTEGER PRIMARY KEY,
                              parent_id INTEGER NOT NULL,
                              name TEXT NOT NULL,
                              FOREIGN KEY (parent_id) REFERENCES failure_parents (id) ON DELETE RESTRICT
                          );
                          CREATE TABLE failure_stock_checks (
                              id INTEGER PRIMARY KEY,
                              quantity INTEGER NOT NULL CHECK (quantity >= 0)
                          );
                          """;
        cmd.ExecuteNonQuery();
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }
}
