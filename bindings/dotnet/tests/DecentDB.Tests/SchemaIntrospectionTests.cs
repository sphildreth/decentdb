using System.Data;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

public sealed class SchemaIntrospectionTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_schema_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
        if (File.Exists(_dbPath + "-wal"))
            File.Delete(_dbPath + "-wal");
    }

    private DecentDBConnection OpenConnection()
    {
        var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        return conn;
    }

    private static void Execute(DecentDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    [Fact]
    public void ListTablesJson_ReturnsTables()
    {
        using var conn = OpenConnection();
        Execute(conn, "CREATE TABLE t1 (id INTEGER PRIMARY KEY)");
        Execute(conn, "CREATE TABLE t2 (id INTEGER PRIMARY KEY)");

        var json = conn.ListTablesJson();
        Assert.Contains("t1", json);
        Assert.Contains("t2", json);
    }

    [Fact]
    public void GetTableColumnsJson_ReturnsColumnMetadata()
    {
        using var conn = OpenConnection();
        Execute(conn, "CREATE TABLE cols_test (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL)");

        var json = conn.GetTableColumnsJson("cols_test");
        Assert.Contains("id", json);
        Assert.Contains("name", json);
        Assert.Contains("score", json);
        Assert.Contains("not_null", json);
    }

    [Fact]
    public void ListIndexesJson_ReturnsIndexes()
    {
        using var conn = OpenConnection();
        Execute(conn, "CREATE TABLE idx_test (id INTEGER PRIMARY KEY, name TEXT)");
        Execute(conn, "CREATE INDEX ix_name ON idx_test (name)");

        var json = conn.ListIndexesJson();
        Assert.Contains("ix_name", json);
        Assert.Contains("idx_test", json);
    }

    [Fact]
    public void ListViewsJson_ReturnsViews()
    {
        using var conn = OpenConnection();
        Execute(conn, "CREATE TABLE base_table (id INTEGER PRIMARY KEY, val INTEGER)");
        Execute(conn, "CREATE VIEW my_view AS SELECT id, val * 2 AS doubled FROM base_table");

        var json = conn.ListViewsJson();
        Assert.Contains("my_view", json);
    }

    [Fact]
    public void GetViewDdl_ReturnsCreateStatement()
    {
        using var conn = OpenConnection();
        Execute(conn, "CREATE TABLE base_table (id INTEGER PRIMARY KEY, val INTEGER)");
        Execute(conn, "CREATE VIEW ddl_view AS SELECT id FROM base_table");

        var ddl = conn.GetViewDdl("ddl_view");
        Assert.Contains("ddl_view", ddl);
        Assert.Contains("SELECT", ddl);
    }

    [Fact]
    public void ListTriggersJson_NoTriggers_ReturnsEmptyArray()
    {
        using var conn = OpenConnection();
        Execute(conn, "CREATE TABLE trg_table (id INTEGER PRIMARY KEY, val INTEGER)");

        var json = conn.ListTriggersJson();
        Assert.Contains("[]", json);
    }

    [Fact]
    public void GetTableDdl_ReturnsCreateStatement()
    {
        using var conn = OpenConnection();
        Execute(conn, "CREATE TABLE ddl_table (id INTEGER PRIMARY KEY, name TEXT NOT NULL)");

        var ddl = conn.GetTableDdl("ddl_table");
        Assert.Contains("ddl_table", ddl);
        Assert.Contains("id", ddl);
        Assert.Contains("name", ddl);
    }

    [Fact]
    public void GetSchema_Indexes_ReturnsIndexMetadata()
    {
        using var conn = OpenConnection();
        Execute(conn, "CREATE TABLE schema_idx_test (id INTEGER PRIMARY KEY, name TEXT, code INTEGER)");
        Execute(conn, "CREATE UNIQUE INDEX ux_code ON schema_idx_test (code)");
        Execute(conn, "CREATE INDEX ix_name ON schema_idx_test (name)");

        var dt = conn.GetSchema("Indexes");
        Assert.Equal("Indexes", dt.TableName);

        var names = new System.Collections.Generic.HashSet<string>();
        foreach (DataRow row in dt.Rows)
        {
            names.Add((string)row["INDEX_NAME"]);
        }

        Assert.Contains("ux_code", names);
        Assert.Contains("ix_name", names);
    }

    [Fact]
    public void GetSchema_Indexes_WithTableFilter()
    {
        using var conn = OpenConnection();
        Execute(conn, "CREATE TABLE filtered_idx_a (id INTEGER PRIMARY KEY, val TEXT)");
        Execute(conn, "CREATE TABLE filtered_idx_b (id INTEGER PRIMARY KEY, num INTEGER)");
        Execute(conn, "CREATE INDEX ix_a ON filtered_idx_a (val)");
        Execute(conn, "CREATE INDEX ix_b ON filtered_idx_b (num)");

        var dt = conn.GetSchema("Indexes", new[] { "filtered_idx_a" });

        foreach (DataRow row in dt.Rows)
        {
            Assert.Equal("filtered_idx_a", (string)row["TABLE_NAME"]);
        }

        // Primary key auto-index + ix_a
        Assert.Equal(2, dt.Rows.Count);
    }

    [Fact]
    public void AbiVersion_ReturnsNonZero()
    {
        var version = DecentDBConnection.AbiVersion();
        Assert.True(version > 0);
    }

    [Fact]
    public void EngineVersion_ReturnsNonEmpty()
    {
        var version = DecentDBConnection.EngineVersion();
        Assert.False(string.IsNullOrWhiteSpace(version));
    }
}
