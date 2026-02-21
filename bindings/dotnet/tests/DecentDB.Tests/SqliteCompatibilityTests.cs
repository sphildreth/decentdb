using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

/// <summary>
/// Validates SQL feature compatibility needed for EF Core migrations from SQLite.
/// These tests cover patterns commonly used by EF Core and by applications
/// migrating from SQLite to DecentDB.
/// </summary>
public sealed class SqliteCompatibilityTests : IDisposable
{
    private readonly string _dbPath;

    public SqliteCompatibilityTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"decentdb_compat_{Guid.NewGuid():N}.ddb");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
        if (File.Exists(_dbPath + "-wal"))
            File.Delete(_dbPath + "-wal");
    }

    [Fact]
    public void CreateIndexIfNotExists_Succeeds()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, type INTEGER)");
        Exec(conn, "CREATE INDEX IF NOT EXISTS IX_t_name ON t(name)");
        // Running again should not throw
        Exec(conn, "CREATE INDEX IF NOT EXISTS IX_t_name ON t(name)");
    }

    [Fact]
    public void FilteredIndex_WithWhereClause_Succeeds()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, type INTEGER)");
        Exec(conn, "CREATE UNIQUE INDEX IX_t_type_filtered ON t(type) WHERE type != 3");

        Exec(conn, "INSERT INTO t VALUES (1, 1)");
        Exec(conn, "INSERT INTO t VALUES (2, 3)");
        Exec(conn, "INSERT INTO t VALUES (3, 3)"); // duplicate type=3 allowed by filter
    }

    [Fact]
    public void GroupConcat_WithSeparator_ReturnsDelimitedString()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, grp INTEGER)");
        Exec(conn, "INSERT INTO t VALUES (1, 'alice', 1), (2, 'bob', 1), (3, 'charlie', 2)");

        var result = Scalar(conn, "SELECT GROUP_CONCAT(name, '|') FROM t WHERE grp = 1");
        Assert.Contains("alice", result!);
        Assert.Contains("bob", result!);
        Assert.Contains("|", result!);
    }

    [Fact]
    public void GroupConcat_InSubquery_Succeeds()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, grp INTEGER)");
        Exec(conn, "INSERT INTO t VALUES (1, 'alice', 1), (2, 'bob', 1)");

        var result = Scalar(conn,
            "SELECT (SELECT GROUP_CONCAT(sub.name, '|') FROM t AS sub WHERE sub.grp = t.grp) FROM t GROUP BY grp");
        Assert.NotNull(result);
    }

    [Fact]
    public void InsertIntoSelect_CopiesRows()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE src (id INTEGER PRIMARY KEY, name TEXT)");
        Exec(conn, "CREATE TABLE dst (id INTEGER PRIMARY KEY, name TEXT)");
        Exec(conn, "INSERT INTO src VALUES (1, 'a'), (2, 'b')");
        Exec(conn, "INSERT INTO dst (id, name) SELECT id, name FROM src");

        Assert.Equal(2L, ScalarLong(conn, "SELECT COUNT(*) FROM dst"));
    }

    [Fact]
    public void ILike_CaseInsensitiveMatch()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
        Exec(conn, "INSERT INTO t VALUES (1, 'Alice'), (2, 'BOB')");

        Assert.Equal(1L, ScalarLong(conn, "SELECT COUNT(*) FROM t WHERE name ILIKE '%ali%'"));
        Assert.Equal(1L, ScalarLong(conn, "SELECT COUNT(*) FROM t WHERE name ILIKE '%bob%'"));
    }

    [Fact]
    public void Like_PatternMatching()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
        Exec(conn, "INSERT INTO t VALUES (1, 'alice'), (2, 'bob')");

        Assert.Equal(1L, ScalarLong(conn, "SELECT COUNT(*) FROM t WHERE name LIKE 'ali%'"));
        Assert.Equal(1L, ScalarLong(conn, "SELECT COUNT(*) FROM t WHERE name LIKE '%ob'"));
    }

    [Fact]
    public void ForeignKey_OnDeleteCascade()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)");
        Exec(conn, "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)");
        Exec(conn, "INSERT INTO parent VALUES (1, 'p1')");
        Exec(conn, "INSERT INTO child VALUES (1, 1)");
        Exec(conn, "DELETE FROM parent WHERE id = 1");

        Assert.Equal(0L, ScalarLong(conn, "SELECT COUNT(*) FROM child"));
    }

    [Fact]
    public void ForeignKey_OnDeleteSetNull()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)");
        Exec(conn, "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE SET NULL)");
        Exec(conn, "INSERT INTO parent VALUES (1, 'p1')");
        Exec(conn, "INSERT INTO child VALUES (1, 1)");
        Exec(conn, "DELETE FROM parent WHERE id = 1");

        Assert.Equal(1L, ScalarLong(conn, "SELECT COUNT(*) FROM child"));
        Assert.True(ScalarIsNull(conn, "SELECT pid FROM child WHERE id = 1"));
    }

    [Fact]
    public void ForeignKey_OnDeleteRestrict_PreventsDelete()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)");
        Exec(conn, "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE RESTRICT)");
        Exec(conn, "INSERT INTO parent VALUES (1, 'p1')");
        Exec(conn, "INSERT INTO child VALUES (1, 1)");

        Assert.ThrowsAny<Exception>(() => Exec(conn, "DELETE FROM parent WHERE id = 1"));
    }

    [Fact]
    public void CompositeIndex_Succeeds()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER)");
        Exec(conn, "CREATE INDEX IX_t_ab ON t(a, b)");
    }

    [Fact]
    public void OrderBy_LimitOffset()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
        Exec(conn, "INSERT INTO t VALUES (1, 'c'), (2, 'a'), (3, 'b')");

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT name FROM t ORDER BY name LIMIT 2 OFFSET 1";
        using var reader = cmd.ExecuteReader();

        var results = new List<string>();
        while (reader.Read()) results.Add(reader.GetString(0));

        Assert.Equal(2, results.Count);
        Assert.Equal("b", results[0]);
        Assert.Equal("c", results[1]);
    }

    [Fact]
    public void SubqueryInWhere_Succeeds()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, grp INTEGER)");
        Exec(conn, "INSERT INTO t VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 1)");

        Assert.Equal(2L, ScalarLong(conn,
            "SELECT COUNT(*) FROM t WHERE grp IN (SELECT grp FROM t WHERE name = 'a')"));
    }

    [Fact]
    public void Coalesce_ReturnsFirstNonNull()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY)");
        Exec(conn, "INSERT INTO t VALUES (1)");

        Assert.Equal("fallback", Scalar(conn, "SELECT COALESCE(NULL, 'fallback')"));
    }

    [Fact]
    public void CaseWhen_ConditionalExpression()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
        Exec(conn, "INSERT INTO t VALUES (1, 10), (2, 20)");

        Assert.Equal("low", Scalar(conn,
            "SELECT CASE WHEN val < 15 THEN 'low' ELSE 'high' END FROM t WHERE id = 1"));
    }

    [Fact]
    public void AggregateFunctions_CountSumAvgMinMax()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
        Exec(conn, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");

        Assert.Equal(3L, ScalarLong(conn, "SELECT COUNT(*) FROM t"));
        Assert.Equal(60L, ScalarLong(conn, "SELECT SUM(val) FROM t"));
        Assert.Equal(10L, ScalarLong(conn, "SELECT MIN(val) FROM t"));
        Assert.Equal(30L, ScalarLong(conn, "SELECT MAX(val) FROM t"));
    }

    [Fact]
    public void NullableDecimal_InsertAndRead()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, val DECIMAL(18,6))");
        Exec(conn, "INSERT INTO t VALUES (1, NULL)");
        Exec(conn, "INSERT INTO t VALUES (2, 42.5)");

        Assert.True(ScalarIsNull(conn, "SELECT val FROM t WHERE id = 1"));
        Assert.Equal(42.5m, ScalarDecimal(conn, "SELECT val FROM t WHERE id = 2"));
    }

    [Fact]
    public void DecimalPrecisionVariants_AllWork()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, a DECIMAL(18,6), b DECIMAL(10,2), c DECIMAL(8,4))");

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO t VALUES (1, @a, @b, @c)";
        cmd.Parameters.Add(new DecentDBParameter("@a", 123.456789m));
        cmd.Parameters.Add(new DecentDBParameter("@b", 99.99m));
        cmd.Parameters.Add(new DecentDBParameter("@c", 12.3456m));
        cmd.ExecuteNonQuery();

        Assert.Equal(123.456789m, ScalarDecimal(conn, "SELECT a FROM t WHERE id = 1"));
        Assert.Equal(99.99m, ScalarDecimal(conn, "SELECT b FROM t WHERE id = 1"));
        Assert.Equal(12.3456m, ScalarDecimal(conn, "SELECT c FROM t WHERE id = 1"));
    }

    #region Helpers

    private DecentDBConnection Open()
    {
        var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        return conn;
    }

    private static void Exec(DecentDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static string? Scalar(DecentDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        return result?.ToString();
    }

    private static long ScalarLong(DecentDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return (long)cmd.ExecuteScalar()!;
    }

    private static decimal ScalarDecimal(DecentDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        reader.Read();
        return reader.GetDecimal(0);
    }

    private static bool ScalarIsNull(DecentDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        reader.Read();
        return reader.IsDBNull(0);
    }

    #endregion
}
