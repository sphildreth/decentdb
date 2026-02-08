using System;
using System.IO;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

public sealed class ExplainAnalyzeTests : IDisposable
{
    private readonly string _dbPath;

    public ExplainAnalyzeTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_explain_analyze_{Guid.NewGuid():N}.ddb");
    }

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    private static void TryDelete(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); } catch { }
    }

    private DecentDBConnection OpenConnection()
    {
        var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        return conn;
    }

    [Fact]
    public void ExplainAnalyze_ReturnsActualMetrics()
    {
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INT, name TEXT)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO t VALUES (1, 'Alice')";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO t VALUES (2, 'Bob')";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO t VALUES (3, 'Charlie')";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "EXPLAIN ANALYZE SELECT * FROM t";
        using var reader = cmd.ExecuteReader();
        var lines = new System.Collections.Generic.List<string>();
        while (reader.Read())
        {
            lines.Add(reader.GetString(0));
        }

        var planText = string.Join("\n", lines);
        Assert.Contains("Project", planText);
        Assert.Contains("Actual Rows: 3", planText);
        Assert.Contains("Actual Time:", planText);
    }

    [Fact]
    public void ExplainAnalyze_WithFilter_CorrectRowCount()
    {
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INT)";
        cmd.ExecuteNonQuery();

        for (int i = 1; i <= 10; i++)
        {
            cmd.CommandText = $"INSERT INTO t VALUES ({i})";
            cmd.ExecuteNonQuery();
        }

        cmd.CommandText = "EXPLAIN ANALYZE SELECT * FROM t WHERE id > 5";
        using var reader = cmd.ExecuteReader();
        var lines = new System.Collections.Generic.List<string>();
        while (reader.Read())
        {
            lines.Add(reader.GetString(0));
        }

        var planText = string.Join("\n", lines);
        Assert.Contains("Actual Rows: 5", planText);
    }

    [Fact]
    public void ExplainAnalyze_EmptyTable_ZeroRows()
    {
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INT)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "EXPLAIN ANALYZE SELECT * FROM t";
        using var reader = cmd.ExecuteReader();
        var lines = new System.Collections.Generic.List<string>();
        while (reader.Read())
        {
            lines.Add(reader.GetString(0));
        }

        var planText = string.Join("\n", lines);
        Assert.Contains("Actual Rows: 0", planText);
    }

    [Fact]
    public void Explain_WithoutAnalyze_NoActualMetrics()
    {
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INT)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "EXPLAIN SELECT * FROM t";
        using var reader = cmd.ExecuteReader();
        var lines = new System.Collections.Generic.List<string>();
        while (reader.Read())
        {
            lines.Add(reader.GetString(0));
        }

        var planText = string.Join("\n", lines);
        Assert.Contains("Project", planText);
        Assert.DoesNotContain("Actual Rows:", planText);
        Assert.DoesNotContain("Actual Time:", planText);
    }
}
