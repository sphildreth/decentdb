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

    // ─── N13: CREATE TRIGGER … BEGIN … END splitting ───

    [Fact]
    public void SqlStatementSplitter_Split_CreateTrigger_PreservesBody()
    {
        // Trigger body contains semicolons; should produce exactly one statement.
        var sql = """
            CREATE TRIGGER trg_audit
            AFTER INSERT ON songs
            BEGIN
                INSERT INTO audit_log (msg) VALUES ('new song');
                UPDATE stats SET count = count + 1;
            END;
            """;

        var parts = SqlStatementSplitter.Split(sql);

        Assert.Single(parts);
        Assert.Contains("CREATE TRIGGER", parts[0]);
        Assert.Contains("BEGIN", parts[0]);
        Assert.Contains("INSERT INTO audit_log", parts[0]);
        Assert.Contains("UPDATE stats", parts[0]);
        Assert.Contains("END", parts[0]);
    }

    [Fact]
    public void SqlStatementSplitter_Split_CreateTrigger_NestedBeginEnd()
    {
        var sql = """
            CREATE TRIGGER trg_nested
            AFTER INSERT ON t
            BEGIN
                BEGIN
                    UPDATE a SET x = 1;
                END;
                INSERT INTO b VALUES (2);
            END;
            """;

        var parts = SqlStatementSplitter.Split(sql);

        Assert.Single(parts);
        var stmt = parts[0];
        Assert.Contains("CREATE TRIGGER", stmt);
        // Count BEGIN occurrences
        int beginCount = 0;
        int idx = 0;
        while ((idx = stmt.IndexOf("BEGIN", idx, StringComparison.OrdinalIgnoreCase)) >= 0)
        {
            beginCount++;
            idx += 5;
        }
        Assert.Equal(2, beginCount); // outer + 1 nested
    }

    [Fact]
    public void SqlStatementSplitter_Split_CreateTrigger_MissingEnd_ThrowsFormatException()
    {
        var sql = """
            CREATE TRIGGER broken
            AFTER INSERT ON t
            BEGIN
                INSERT INTO t VALUES (1);
            -- missing END
            """;

        Assert.Throws<FormatException>(() => SqlStatementSplitter.Split(sql));
    }

    [Fact]
    public void SqlStatementSplitter_Split_CreateTriggerThenSelect_ProducesTwoFragments()
    {
        var sql = """
            CREATE TRIGGER trg
            AFTER INSERT ON t
            BEGIN
                INSERT INTO log VALUES ('x');
            END;
            SELECT * FROM log;
            """;

        var parts = SqlStatementSplitter.Split(sql);

        Assert.Equal(2, parts.Count);
        Assert.Contains("CREATE TRIGGER", parts[0]);
        Assert.Contains("SELECT", parts[1]);
    }
}
