using DecentDB.AdoNet;
using DecentDB.Native;
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

    #region EXISTS with derived-table JOINs

    [Fact]
    public void ExistsSubquery_WithDerivedTableJoin_ReturnsTrue()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        Exec(conn, """CREATE TABLE "AccessControls" ("Id" INTEGER NOT NULL, "LibraryId" INTEGER NOT NULL, "GroupId" INTEGER NOT NULL)""");
        Exec(conn, """CREATE TABLE "GroupMembers" ("UserId" INTEGER NOT NULL, "GroupId" INTEGER NOT NULL)""");
        Exec(conn, """INSERT INTO "AccessControls" ("Id", "LibraryId", "GroupId") VALUES (1, 100, 200)""");
        Exec(conn, """INSERT INTO "GroupMembers" ("UserId", "GroupId") VALUES (50, 200)""");

        var result = ScalarBool(conn, """
            SELECT EXISTS (
                SELECT 1
                FROM "AccessControls" AS "a"
                INNER JOIN (
                    SELECT "g"."GroupId"
                    FROM "GroupMembers" AS "g"
                    WHERE "g"."UserId" = 50
                ) AS "sub" ON "a"."GroupId" = "sub"."GroupId"
                WHERE "a"."LibraryId" = 100)
        """);
        Assert.True(result);
    }

    [Fact]
    public void ExistsSubquery_WithDerivedTableJoin_ReturnsFalse_WhenNoMatch()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        Exec(conn, """CREATE TABLE "AccessControls" ("Id" INTEGER NOT NULL, "LibraryId" INTEGER NOT NULL, "GroupId" INTEGER NOT NULL)""");
        Exec(conn, """CREATE TABLE "GroupMembers" ("UserId" INTEGER NOT NULL, "GroupId" INTEGER NOT NULL)""");
        Exec(conn, """INSERT INTO "AccessControls" ("Id", "LibraryId", "GroupId") VALUES (1, 100, 200)""");
        Exec(conn, """INSERT INTO "GroupMembers" ("UserId", "GroupId") VALUES (50, 999)""");

        var result = ScalarBool(conn, """
            SELECT EXISTS (
                SELECT 1
                FROM "AccessControls" AS "a"
                INNER JOIN (
                    SELECT "g"."GroupId"
                    FROM "GroupMembers" AS "g"
                    WHERE "g"."UserId" = 50
                ) AS "sub" ON "a"."GroupId" = "sub"."GroupId"
                WHERE "a"."LibraryId" = 100)
        """);
        Assert.False(result);
    }

    [Fact]
    public void ExistsSubquery_WithDerivedTableJoin_AndParameters()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        Exec(conn, """CREATE TABLE "AccessControls" ("Id" INTEGER NOT NULL, "LibraryId" INTEGER NOT NULL, "GroupId" INTEGER NOT NULL)""");
        Exec(conn, """CREATE TABLE "GroupMembers" ("UserId" INTEGER NOT NULL, "GroupId" INTEGER NOT NULL)""");
        Exec(conn, """INSERT INTO "AccessControls" ("Id", "LibraryId", "GroupId") VALUES (1, 100, 200)""");
        Exec(conn, """INSERT INTO "GroupMembers" ("UserId", "GroupId") VALUES (50, 200)""");

        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT EXISTS (
                SELECT 1
                FROM "AccessControls" AS "a"
                INNER JOIN (
                    SELECT "g"."GroupId"
                    FROM "GroupMembers" AS "g"
                    WHERE "g"."UserId" = @userId
                ) AS "sub" ON "a"."GroupId" = "sub"."GroupId"
                WHERE "a"."LibraryId" = @libId)
        """;
        cmd.Parameters.Add(new DecentDBParameter("@userId", 50));
        cmd.Parameters.Add(new DecentDBParameter("@libId", 100));
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.True(reader.GetBoolean(0));
    }

    private static bool ScalarBool(DecentDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        reader.Read();
        return reader.GetBoolean(0);
    }

    #endregion

    #region Type Affinity (ADR-0099)

    [Fact]
    public void IntegerEqualsTextNumeric_ReturnsTrue()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE ta (id INTEGER)");
        Exec(conn, "INSERT INTO ta VALUES (42)");
        Assert.Equal(1, ScalarInt(conn, "SELECT COUNT(*) FROM ta WHERE id = '42'"));
    }

    [Fact]
    public void IntegerEqualsTextNonNumeric_ReturnsFalse()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE tb (id INTEGER)");
        Exec(conn, "INSERT INTO tb VALUES (42)");
        Assert.Equal(0, ScalarInt(conn, "SELECT COUNT(*) FROM tb WHERE id = 'abc'"));
    }

    [Fact]
    public void RealEqualsTextNumeric_ReturnsTrue()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE tc (val REAL)");
        Exec(conn, "INSERT INTO tc VALUES (3.14)");
        Assert.Equal(1, ScalarInt(conn, "SELECT COUNT(*) FROM tc WHERE val = '3.14'"));
    }

    [Fact]
    public void TextEqualsInteger_ReturnsTrue()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE td (name TEXT)");
        Exec(conn, "INSERT INTO td VALUES ('100')");
        Assert.Equal(1, ScalarInt(conn, "SELECT COUNT(*) FROM td WHERE name = 100"));
    }

    [Fact]
    public void IntegerLessThanNonNumericText_ReturnsTrue()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE te (id INTEGER)");
        Exec(conn, "INSERT INTO te VALUES (42)");
        Assert.Equal(1, ScalarInt(conn, "SELECT COUNT(*) FROM te WHERE id < 'zzz'"));
    }

    [Fact]
    public void IntegerEqualsTextParam_ReturnsTrue()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE tf (id INTEGER)");
        Exec(conn, "INSERT INTO tf VALUES (42)");
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM tf WHERE id = @p0";
        cmd.Parameters.Add(new DecentDBParameter("@p0", "42"));
        using var reader = cmd.ExecuteReader();
        reader.Read();
        Assert.Equal(1L, reader.GetInt64(0));
    }

    private static int ScalarInt(DecentDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        reader.Read();
        return (int)reader.GetInt64(0);
    }

    #endregion

    #region Partial (Filtered) Unique Index

    [Fact]
    public void PartialUniqueIndex_AllowsDuplicatesExcludedByPredicate()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE Libraries (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Type INTEGER NOT NULL)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = """CREATE UNIQUE INDEX "IX_Libraries_Type" ON "Libraries" ("Type") WHERE "Type" != 3""";
        cmd.ExecuteNonQuery();

        // Type=3 is excluded from the unique index — duplicates should be allowed
        cmd.CommandText = "INSERT INTO Libraries (Id, Name, Type) VALUES (11, 'Storage One', 3)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO Libraries (Id, Name, Type) VALUES (12, 'Storage Two', 3)";
        cmd.ExecuteNonQuery(); // Should not throw

        Assert.Equal(2, ScalarInt(conn, "SELECT COUNT(*) FROM Libraries WHERE Type = 3"));
    }

    [Fact]
    public void PartialUniqueIndex_EnforcesDuplicatesMatchingPredicate()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE Libraries (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Type INTEGER NOT NULL)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = """CREATE UNIQUE INDEX "IX_Libraries_Type" ON "Libraries" ("Type") WHERE "Type" != 3""";
        cmd.ExecuteNonQuery();

        // Type=1 IS covered by the unique index — duplicates should fail
        cmd.CommandText = "INSERT INTO Libraries (Id, Name, Type) VALUES (21, 'Lib A', 1)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO Libraries (Id, Name, Type) VALUES (22, 'Lib B', 1)";
        Assert.Throws<DecentDBException>(() => cmd.ExecuteNonQuery());
    }

    [Fact]
    public void PartialUniqueIndex_MixedPredicateAndExcludedRows()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE Libraries (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Type INTEGER NOT NULL)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = """CREATE UNIQUE INDEX "IX_Libraries_Type" ON "Libraries" ("Type") WHERE "Type" != 3""";
        cmd.ExecuteNonQuery();

        // Insert multiple Type=3 (excluded from unique constraint)
        cmd.CommandText = "INSERT INTO Libraries (Id, Name, Type) VALUES (1, 'S1', 3)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO Libraries (Id, Name, Type) VALUES (2, 'S2', 3)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO Libraries (Id, Name, Type) VALUES (3, 'S3', 3)";
        cmd.ExecuteNonQuery();

        // Insert unique Type=1 and Type=2 (covered by unique constraint)
        cmd.CommandText = "INSERT INTO Libraries (Id, Name, Type) VALUES (4, 'A', 1)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO Libraries (Id, Name, Type) VALUES (5, 'B', 2)";
        cmd.ExecuteNonQuery();

        Assert.Equal(5, ScalarInt(conn, "SELECT COUNT(*) FROM Libraries"));
        Assert.Equal(3, ScalarInt(conn, "SELECT COUNT(*) FROM Libraries WHERE Type = 3"));
    }

    #endregion

    #region UNION subquery column resolution

    [Fact]
    public void UnionSubquery_NamedColumnProjection_ResolvesCorrectly()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE Items (Id INTEGER PRIMARY KEY, Name TEXT, Category TEXT)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO Items VALUES (1, 'Alpha', 'A')";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO Items VALUES (2, 'Beta', 'B')";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO Items VALUES (3, 'Gamma', 'A')";
        cmd.ExecuteNonQuery();

        // Qualified column references on UNION subquery alias
        cmd.CommandText = """
            SELECT "u"."Id", "u"."Name" FROM (
                SELECT "t"."Id", "t"."Name" FROM "Items" AS "t" WHERE "t"."Category" = 'A'
                UNION
                SELECT "t0"."Id", "t0"."Name" FROM "Items" AS "t0" WHERE "t0"."Category" = 'B'
            ) AS "u" ORDER BY "u"."Name"
            """;
        using var reader = cmd.ExecuteReader();
        var names = new List<string>();
        while (reader.Read())
            names.Add(reader.GetString(1));

        Assert.Equal(3, names.Count);
        Assert.Equal("Alpha", names[0]);
        Assert.Equal("Beta", names[1]);
        Assert.Equal("Gamma", names[2]);
    }

    [Fact]
    public void UnionSubquery_CountOverNamedColumns_Works()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE Settings (Id INTEGER PRIMARY KEY, Key TEXT, Value TEXT)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO Settings VALUES (1, 'validation.min', '5')";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO Settings VALUES (2, 'conversion.format', 'mp3')";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO Settings VALUES (3, 'other.setting', 'yes')";
        cmd.ExecuteNonQuery();

        // COUNT with LIKE parameters over UNION — pattern from EF Core OR filters
        cmd.CommandText = """
            SELECT COUNT(*) FROM (
                SELECT "s"."Id", "s"."Key", "s"."Value" FROM "Settings" AS "s" WHERE "s"."Key" LIKE @p0
                UNION
                SELECT "s0"."Id", "s0"."Key", "s0"."Value" FROM "Settings" AS "s0" WHERE "s0"."Key" LIKE @p1
            ) AS "u"
            """;
        cmd.Parameters.Add(new DecentDBParameter("@p0", "%validation%"));
        cmd.Parameters.Add(new DecentDBParameter("@p1", "%conversion%"));
        Assert.Equal(2L, cmd.ExecuteScalar());
    }

    [Fact]
    public void UnionSubquery_WithLimitOffset_ResolvesColumns()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE Records (Id INTEGER PRIMARY KEY, Tag TEXT)";
        cmd.ExecuteNonQuery();
        for (int i = 1; i <= 5; i++)
        {
            cmd.CommandText = $"INSERT INTO Records VALUES ({i}, 'tag{i}')";
            cmd.ExecuteNonQuery();
        }

        cmd.CommandText = """
            SELECT "u"."Id", "u"."Tag" FROM (
                SELECT "r"."Id", "r"."Tag" FROM "Records" AS "r" WHERE "r"."Id" <= 3
                UNION
                SELECT "r0"."Id", "r0"."Tag" FROM "Records" AS "r0" WHERE "r0"."Id" >= 4
            ) AS "u" ORDER BY "u"."Id" LIMIT @p0 OFFSET @p1
            """;
        cmd.Parameters.Add(new DecentDBParameter("@p0", 3));
        cmd.Parameters.Add(new DecentDBParameter("@p1", 1));
        using var reader = cmd.ExecuteReader();
        var ids = new List<long>();
        while (reader.Read())
            ids.Add(reader.GetInt64(0));

        Assert.Equal([2L, 3L, 4L], ids);
    }

    #endregion

    #region Composite Primary Key

    [Fact]
    public void CompositePrimaryKey_WhereOnFirstColumn_ReturnsAllMatches()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE cpk1 (a INTEGER NOT NULL, b INTEGER NOT NULL, name TEXT, PRIMARY KEY (a, b))");
        Exec(conn, "INSERT INTO cpk1 VALUES (1,1,'a1b1')");
        Exec(conn, "INSERT INTO cpk1 VALUES (1,2,'a1b2')");
        Exec(conn, "INSERT INTO cpk1 VALUES (2,1,'a2b1')");

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT name FROM cpk1 WHERE a = 1 ORDER BY b";
        var names = new List<string>();
        using (var r = cmd.ExecuteReader()) while (r.Read()) names.Add(r.GetString(0));
        Assert.Equal(["a1b1", "a1b2"], names);
    }

    [Fact]
    public void CompositePrimaryKey_WhereOnBothColumns_ReturnsSingleRow()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE cpk2 (a INTEGER NOT NULL, b INTEGER NOT NULL, name TEXT, PRIMARY KEY (a, b))");
        Exec(conn, "INSERT INTO cpk2 VALUES (1,1,'a1b1')");
        Exec(conn, "INSERT INTO cpk2 VALUES (1,2,'a1b2')");
        Exec(conn, "INSERT INTO cpk2 VALUES (2,1,'a2b1')");

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT name FROM cpk2 WHERE a = 1 AND b = 2";
        var names = new List<string>();
        using (var r = cmd.ExecuteReader()) while (r.Read()) names.Add(r.GetString(0));
        Assert.Equal(["a1b2"], names);
    }

    [Fact]
    public void CompositePrimaryKey_WhereOnSecondColumn_ReturnsCorrectRows()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE cpk3 (a INTEGER NOT NULL, b INTEGER NOT NULL, name TEXT, PRIMARY KEY (a, b))");
        Exec(conn, "INSERT INTO cpk3 VALUES (1,1,'a1b1')");
        Exec(conn, "INSERT INTO cpk3 VALUES (1,2,'a1b2')");
        Exec(conn, "INSERT INTO cpk3 VALUES (2,1,'a2b1')");

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT name FROM cpk3 WHERE b = 1 ORDER BY a";
        var names = new List<string>();
        using (var r = cmd.ExecuteReader()) while (r.Read()) names.Add(r.GetString(0));
        Assert.Equal(["a1b1", "a2b1"], names);
    }

    #endregion

    #region GROUP BY with ORDER BY on aggregate

    [Fact]
    public void GroupBy_OrderByCountDesc_ReturnsCorrectOrder()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE gb_plays (user_id INTEGER, song_id INTEGER)");
        Exec(conn, "INSERT INTO gb_plays VALUES (1, 1)");
        Exec(conn, "INSERT INTO gb_plays VALUES (1, 1)");
        Exec(conn, "INSERT INTO gb_plays VALUES (1, 2)");

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT song_id, COUNT(*) as cnt FROM gb_plays WHERE user_id = 1 GROUP BY song_id ORDER BY cnt DESC";
        using var reader = cmd.ExecuteReader();

        var rows = new List<(long songId, long count)>();
        while (reader.Read())
        {
            rows.Add((reader.GetInt64(0), reader.GetInt64(1)));
        }

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, rows[0].songId);
        Assert.Equal(2, rows[0].count);
        Assert.Equal(2, rows[1].songId);
        Assert.Equal(1, rows[1].count);
    }

    [Fact]
    public void GroupBy_OrderByRawCountDesc_ReturnsCorrectOrder()
    {
        using var conn = Open();
        Exec(conn, "CREATE TABLE gb_plays2 (user_id INTEGER, song_id INTEGER)");
        Exec(conn, "INSERT INTO gb_plays2 VALUES (1, 1)");
        Exec(conn, "INSERT INTO gb_plays2 VALUES (1, 1)");
        Exec(conn, "INSERT INTO gb_plays2 VALUES (1, 2)");

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT song_id, COUNT(*) as cnt FROM gb_plays2 WHERE user_id = 1 GROUP BY song_id ORDER BY COUNT(*) DESC";
        using var reader = cmd.ExecuteReader();

        var rows = new List<(long songId, long count)>();
        while (reader.Read())
        {
            rows.Add((reader.GetInt64(0), reader.GetInt64(1)));
        }

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, rows[0].songId);
        Assert.Equal(2, rows[0].count);
        Assert.Equal(2, rows[1].songId);
        Assert.Equal(1, rows[1].count);
    }

    #endregion

    #region DELETE with subquery parameters

    [Fact]
    public void Delete_WithExistsSubqueryParam_BindsCorrectly()
    {
        using var conn = Open();
        Exec(conn, @"
            CREATE TABLE del_artists (""Id"" INTEGER PRIMARY KEY, ""LibraryId"" INTEGER, ""Name"" TEXT);
            CREATE TABLE del_contributors (""Id"" INTEGER PRIMARY KEY, ""ArtistId"" INTEGER, ""Name"" TEXT);
            INSERT INTO del_artists VALUES (1, 10, 'Artist1');
            INSERT INTO del_artists VALUES (2, 20, 'Artist2');
            INSERT INTO del_contributors VALUES (1, 1, 'Contrib1');
            INSERT INTO del_contributors VALUES (2, 2, 'Contrib2');
        ");

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            DELETE FROM ""del_contributors""
            WHERE EXISTS (
                SELECT 1 FROM ""del_contributors"" AS ""c""
                INNER JOIN ""del_artists"" AS ""a"" ON ""c"".""ArtistId"" = ""a"".""Id""
                WHERE ""a"".""LibraryId"" = @p0
                AND ""del_contributors"".""Id"" = ""c"".""Id""
            )";
        var p = cmd.CreateParameter();
        p.ParameterName = "@p0";
        p.Value = 10L;
        cmd.Parameters.Add(p);
        var deleted = cmd.ExecuteNonQuery();

        Assert.Equal(1, deleted);

        // Verify only contrib for library 20 remains
        cmd.Parameters.Clear();
        cmd.CommandText = @"SELECT COUNT(*) FROM ""del_contributors""";
        var remaining = (long)cmd.ExecuteScalar()!;
        Assert.Equal(1, remaining);
    }

    [Fact]
    public void Delete_WithInSubqueryParam_BindsCorrectly()
    {
        using var conn = Open();
        Exec(conn, @"
            CREATE TABLE del2_artists (""Id"" INTEGER PRIMARY KEY, ""LibraryId"" INTEGER, ""Name"" TEXT);
            CREATE TABLE del2_contributors (""Id"" INTEGER PRIMARY KEY, ""ArtistId"" INTEGER, ""Name"" TEXT);
            INSERT INTO del2_artists VALUES (1, 10, 'Artist1');
            INSERT INTO del2_artists VALUES (2, 20, 'Artist2');
            INSERT INTO del2_contributors VALUES (1, 1, 'Contrib1');
            INSERT INTO del2_contributors VALUES (2, 2, 'Contrib2');
        ");

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            DELETE FROM ""del2_contributors""
            WHERE ""del2_contributors"".""Id"" IN (
                SELECT ""c0"".""Id""
                FROM ""del2_contributors"" AS ""c0""
                LEFT JOIN ""del2_artists"" AS ""a"" ON ""c0"".""ArtistId"" = ""a"".""Id""
                WHERE ""a"".""Id"" IS NOT NULL AND ""a"".""LibraryId"" = @p0
            )";
        var p = cmd.CreateParameter();
        p.ParameterName = "@p0";
        p.Value = 10L;
        cmd.Parameters.Add(p);
        var deleted = cmd.ExecuteNonQuery();

        Assert.Equal(1, deleted);

        cmd.Parameters.Clear();
        cmd.CommandText = @"SELECT COUNT(*) FROM ""del2_contributors""";
        var remaining = (long)cmd.ExecuteScalar()!;
        Assert.Equal(1, remaining);
    }
    #endregion

    #region JSON Functions

    [Fact]
    public void JsonArrayLength_ReturnsElementCount()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();

        cmd.CommandText = @"SELECT json_array_length('[1,2,3]')";
        Assert.Equal(3L, (long)cmd.ExecuteScalar()!);

        cmd.CommandText = @"SELECT json_array_length('[]')";
        Assert.Equal(0L, (long)cmd.ExecuteScalar()!);

        cmd.CommandText = @"SELECT json_array_length('[""a"",""b""]')";
        Assert.Equal(2L, (long)cmd.ExecuteScalar()!);
    }

    [Fact]
    public void JsonArrayLength_NullInput_ReturnsNull()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"SELECT json_array_length(NULL)";
        Assert.True(cmd.ExecuteScalar() is DBNull);
    }

    [Fact]
    public void JsonArrayLength_NonArrayInput_ReturnsZero()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"SELECT json_array_length('{""key"":""value""}')";
        Assert.Equal(0L, (long)cmd.ExecuteScalar()!);
    }

    [Fact]
    public void JsonArrayLength_WithPath_ReturnsNestedArrayCount()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"SELECT json_array_length('{""items"":[1,2,3,4]}', '$.items')";
        Assert.Equal(4L, (long)cmd.ExecuteScalar()!);
    }

    [Fact]
    public void JsonArrayLength_OnColumn_WorksCorrectly()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"CREATE TABLE json_test (id INTEGER PRIMARY KEY, data TEXT)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = @"INSERT INTO json_test (id, data) VALUES (1, '[""rock"",""pop""]')";
        cmd.ExecuteNonQuery();
        cmd.CommandText = @"INSERT INTO json_test (id, data) VALUES (2, '[""jazz""]')";
        cmd.ExecuteNonQuery();
        cmd.CommandText = @"INSERT INTO json_test (id, data) VALUES (3, NULL)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = @"SELECT id FROM json_test WHERE json_array_length(data) > 1";
        Assert.Equal(1L, (long)cmd.ExecuteScalar()!);
    }

    [Fact]
    public void JsonExtract_ReturnsValueAtPath()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();

        cmd.CommandText = @"SELECT json_extract('[""rock"",""pop"",""jazz""]', '$[0]')";
        Assert.Equal("rock", cmd.ExecuteScalar()!.ToString());

        cmd.CommandText = @"SELECT json_extract('[""rock"",""pop"",""jazz""]', '$[2]')";
        Assert.Equal("jazz", cmd.ExecuteScalar()!.ToString());
    }

    [Fact]
    public void JsonExtract_ObjectKey_ReturnsValue()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"SELECT json_extract('{""name"":""test"",""count"":42}', '$.name')";
        Assert.Equal("test", cmd.ExecuteScalar()!.ToString());

        cmd.CommandText = @"SELECT json_extract('{""name"":""test"",""count"":42}', '$.count')";
        Assert.Equal(42L, (long)cmd.ExecuteScalar()!);
    }

    [Fact]
    public void JsonExtract_NullInput_ReturnsNull()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"SELECT json_extract(NULL, '$[0]')";
        Assert.True(cmd.ExecuteScalar() is DBNull);
    }

    [Fact]
    public void JsonExtract_OutOfBounds_ReturnsNull()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"SELECT json_extract('[""a""]', '$[5]')";
        Assert.True(cmd.ExecuteScalar() is DBNull);
    }

    #endregion
}
