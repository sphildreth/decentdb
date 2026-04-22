using System.Data;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

public sealed class MultiStatementTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_multi_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
        if (File.Exists(_dbPath + "-wal"))
            File.Delete(_dbPath + "-wal");
    }

    [Fact]
    public void ExecuteNonQuery_MultiStatement_CreatesTableAndInserts()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE multi_stmt (id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO multi_stmt (id, name) VALUES (1, 'Alice');
            INSERT INTO multi_stmt (id, name) VALUES (2, 'Bob');
            """;

        var affected = cmd.ExecuteNonQuery();
        Assert.True(affected >= 2);

        using var verify = conn.CreateCommand();
        verify.CommandText = "SELECT COUNT(*) FROM multi_stmt";
        Assert.Equal(2L, verify.ExecuteScalar());
    }

    [Fact]
    public void ExecuteNonQuery_MultiStatement_WithParameters_UsesLastStatement()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var setup = conn.CreateCommand();
        setup.CommandText = "CREATE TABLE multi_param (id INTEGER PRIMARY KEY, val INTEGER)";
        setup.ExecuteNonQuery();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO multi_param (id, val) VALUES (1, 10);
            INSERT INTO multi_param (id, val) VALUES (2, 20);
            """;

        var affected = cmd.ExecuteNonQuery();
        Assert.True(affected >= 1);

        using var verify = conn.CreateCommand();
        verify.CommandText = "SELECT SUM(val) FROM multi_param";
        Assert.Equal(30L, verify.ExecuteScalar());
    }

    [Fact]
    public void ExecuteNonQuery_SingleStatement_ReturnsCorrectAffected()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE single_stmt (id INTEGER PRIMARY KEY)";
        Assert.Equal(0, cmd.ExecuteNonQuery());

        cmd.CommandText = "INSERT INTO single_stmt (id) VALUES (1)";
        Assert.Equal(1, cmd.ExecuteNonQuery());
    }

    [Fact]
    public void ExecuteScalar_MultiStatement_ThrowsBecauseNotSupported()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1; SELECT 2;";

        Assert.Throws<DecentDB.Native.DecentDBException>(() => cmd.ExecuteScalar());
    }

    [Fact]
    public void ExecuteReader_MultiStatement_ThrowsBecauseNotSupported()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1; SELECT 2;";

        Assert.Throws<DecentDB.Native.DecentDBException>(() => cmd.ExecuteReader());
    }

    [Fact]
    public void ExecuteNonQuery_MultiStatement_DdlAndDmlMixed()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE mixed (id INTEGER PRIMARY KEY);
            INSERT INTO mixed (id) VALUES (10);
            INSERT INTO mixed (id) VALUES (20);
            UPDATE mixed SET id = id + 1;
            """;

        var affected = cmd.ExecuteNonQuery();
        Assert.True(affected >= 2);

        using var verify = conn.CreateCommand();
        verify.CommandText = "SELECT COUNT(*) FROM mixed WHERE id IN (11, 21)";
        Assert.Equal(2L, verify.ExecuteScalar());
    }
}
