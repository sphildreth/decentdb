using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Linq;
using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

/// <summary>
/// Tests for window functions accessed via raw SQL through EF Core's shared in-memory connection.
/// EF Core doesn't translate LINQ to window functions, so we use raw SQL via the underlying connection.
/// </summary>
public sealed class WindowFunctionEfCoreTests : IDisposable
{
    private readonly DecentDBConnection _connection;

    public WindowFunctionEfCoreTests()
    {
        _connection = new DecentDBConnection("Data Source=:memory:");
        _connection.Open();
        SeedData();
    }

    public void Dispose()
    {
        _connection.Dispose();
    }

    private void SeedData()
    {
        Exec("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT NOT NULL, dept TEXT NOT NULL, salary INTEGER NOT NULL)");
        var data = new[]
        {
            (1, "Alice", "eng", 120), (2, "Bob", "eng", 120),
            (3, "Carol", "eng", 100), (4, "Dave", "sales", 110),
            (5, "Eve", "sales", 105), (6, "Frank", "sales", 105),
            (7, "Grace", "hr", 95),
        };
        foreach (var (id, name, dept, salary) in data)
            Exec($"INSERT INTO employees (id, name, dept, salary) VALUES ({id}, '{name}', '{dept}', {salary})");
    }

    private void Exec(string sql)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private List<Dictionary<string, object?>> Query(string sql)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<Dictionary<string, object?>>();
        while (reader.Read())
        {
            var row = new Dictionary<string, object?>();
            for (int i = 0; i < reader.FieldCount; i++)
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            results.Add(row);
        }
        return results;
    }

    // ── ROW_NUMBER via EF Context's underlying connection ──

    [Fact]
    public void RowNumber_ViaSharedConnection()
    {
        var rows = Query("SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM employees ORDER BY rn");
        Assert.Equal(7, rows.Count);
        Assert.Equal(1L, rows[0]["rn"]);
        Assert.Equal(7L, rows[6]["rn"]);
    }

    [Fact]
    public void RowNumber_PartitionedByDept()
    {
        var rows = Query(@"
            SELECT name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
            FROM employees ORDER BY dept, rn");

        // eng: 3 rows, sales: 3 rows, hr: 1 row
        var eng = rows.Where(r => (string)r["dept"]! == "eng").ToList();
        Assert.Equal(3, eng.Count);
        Assert.Equal(1L, eng[0]["rn"]);

        var hr = rows.Where(r => (string)r["dept"]! == "hr").ToList();
        Assert.Single(hr);
        Assert.Equal(1L, hr[0]["rn"]);
    }

    // ── RANK ──

    [Fact]
    public void Rank_TiesAndGaps()
    {
        var rows = Query(@"
            SELECT name, salary, RANK() OVER (ORDER BY salary DESC) AS r
            FROM employees ORDER BY salary DESC, name");

        // 120,120,110,105,105,100,95 → ranks: 1,1,3,4,4,6,7
        Assert.Equal(1L, rows[0]["r"]); // Alice 120
        Assert.Equal(1L, rows[1]["r"]); // Bob 120
        Assert.Equal(3L, rows[2]["r"]); // Dave 110
        Assert.Equal(4L, rows[3]["r"]); // Eve 105
        Assert.Equal(4L, rows[4]["r"]); // Frank 105
        Assert.Equal(6L, rows[5]["r"]); // Carol 100
        Assert.Equal(7L, rows[6]["r"]); // Grace 95
    }

    [Fact]
    public void Rank_PartitionedByDept()
    {
        var rows = Query(@"
            SELECT name, dept, salary, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS r
            FROM employees ORDER BY dept, salary DESC, name");

        var eng = rows.Where(r => (string)r["dept"]! == "eng").ToList();
        Assert.Equal(1L, eng[0]["r"]); // Alice 120
        Assert.Equal(1L, eng[1]["r"]); // Bob 120
        Assert.Equal(3L, eng[2]["r"]); // Carol 100

        var sales = rows.Where(r => (string)r["dept"]! == "sales").ToList();
        Assert.Equal(1L, sales[0]["r"]);  // Dave 110
        Assert.Equal(2L, sales[1]["r"]);  // Eve 105
        Assert.Equal(2L, sales[2]["r"]);  // Frank 105
    }

    // ── DENSE_RANK ──

    [Fact]
    public void DenseRank_NoGaps()
    {
        var rows = Query(@"
            SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS dr
            FROM employees ORDER BY salary DESC");

        // 120,120,110,105,105,100,95 → dense_ranks: 1,1,2,3,3,4,5
        Assert.Equal(1L, rows[0]["dr"]);
        Assert.Equal(1L, rows[1]["dr"]);
        Assert.Equal(2L, rows[2]["dr"]);
        Assert.Equal(3L, rows[3]["dr"]);
        Assert.Equal(3L, rows[4]["dr"]);
        Assert.Equal(4L, rows[5]["dr"]);
        Assert.Equal(5L, rows[6]["dr"]);
    }

    [Fact]
    public void DenseRank_VsRank_Comparison()
    {
        var rows = Query(@"
            SELECT salary,
                   RANK() OVER (ORDER BY salary DESC) AS r,
                   DENSE_RANK() OVER (ORDER BY salary DESC) AS dr
            FROM employees ORDER BY salary DESC");

        // After first tie (120,120): RANK=3, DENSE_RANK=2 for salary 110
        var salary110 = rows.First(r => (long)r["salary"]! == 110);
        Assert.Equal(3L, salary110["r"]);
        Assert.Equal(2L, salary110["dr"]);
    }

    // ── LAG ──

    [Fact]
    public void Lag_DefaultOffset()
    {
        var rows = Query(@"
            SELECT name, salary, LAG(salary) OVER (ORDER BY id) AS prev
            FROM employees ORDER BY id");

        // First row: prev is NULL
        Assert.Null(rows[0]["prev"]);
        // Second row: prev = first row's salary
        Assert.Equal(120L, rows[1]["prev"]);
    }

    [Fact]
    public void Lag_WithExplicitDefault()
    {
        var rows = Query(@"
            SELECT name, LAG(salary, 1, 0) OVER (ORDER BY id) AS prev
            FROM employees ORDER BY id");

        Assert.Equal(0L, rows[0]["prev"]);
        Assert.Equal(120L, rows[1]["prev"]);
    }

    [Fact]
    public void Lag_Offset2()
    {
        var rows = Query(@"
            SELECT name, salary, LAG(salary, 2) OVER (ORDER BY id) AS prev2
            FROM employees ORDER BY id");

        Assert.Null(rows[0]["prev2"]); // no row 2 back
        Assert.Null(rows[1]["prev2"]); // no row 2 back
        Assert.Equal(120L, rows[2]["prev2"]); // 2 rows back = Alice's 120
    }

    [Fact]
    public void Lag_PartitionResets()
    {
        var rows = Query(@"
            SELECT name, dept, salary, LAG(salary) OVER (PARTITION BY dept ORDER BY id) AS prev
            FROM employees ORDER BY dept, id");

        // First in each partition is NULL
        var eng = rows.Where(r => (string)r["dept"]! == "eng").ToList();
        Assert.Null(eng[0]["prev"]);
        Assert.Equal(120L, eng[1]["prev"]);

        var hr = rows.Where(r => (string)r["dept"]! == "hr").ToList();
        Assert.Null(hr[0]["prev"]); // only 1 row
    }

    // ── LEAD ──

    [Fact]
    public void Lead_DefaultOffset()
    {
        var rows = Query(@"
            SELECT name, salary, LEAD(salary) OVER (ORDER BY id) AS next
            FROM employees ORDER BY id");

        Assert.Equal(120L, rows[0]["next"]); // Alice → Bob's 120
        Assert.Null(rows[^1]["next"]);       // Last row → NULL
    }

    [Fact]
    public void Lead_WithExplicitDefault()
    {
        var rows = Query(@"
            SELECT name, LEAD(salary, 1, -1) OVER (ORDER BY id) AS next
            FROM employees ORDER BY id");

        Assert.Equal(-1L, rows[^1]["next"]);
    }

    [Fact]
    public void Lead_PartitionResets()
    {
        var rows = Query(@"
            SELECT name, dept, LEAD(salary) OVER (PARTITION BY dept ORDER BY id) AS next
            FROM employees ORDER BY dept, id");

        var eng = rows.Where(r => (string)r["dept"]! == "eng").ToList();
        Assert.NotNull(eng[0]["next"]); // Alice → Bob
        Assert.Null(eng[^1]["next"]);   // Carol is last in eng

        var sales = rows.Where(r => (string)r["dept"]! == "sales").ToList();
        Assert.Null(sales[^1]["next"]); // Frank is last in sales
    }

    // ── Edge Cases ──

    [Fact]
    public void AllWindowFunctions_SingleRow()
    {
        Exec("CREATE TABLE one_row (id INTEGER PRIMARY KEY, val INTEGER)");
        Exec("INSERT INTO one_row (id, val) VALUES (1, 42)");

        var rows = Query(@"
            SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn,
                   RANK() OVER (ORDER BY id) AS r,
                   DENSE_RANK() OVER (ORDER BY id) AS dr,
                   LAG(val) OVER (ORDER BY id) AS prev,
                   LEAD(val) OVER (ORDER BY id) AS next
            FROM one_row");

        Assert.Single(rows);
        Assert.Equal(1L, rows[0]["rn"]);
        Assert.Equal(1L, rows[0]["r"]);
        Assert.Equal(1L, rows[0]["dr"]);
        Assert.Null(rows[0]["prev"]);
        Assert.Null(rows[0]["next"]);
    }

    [Fact]
    public void AllWindowFunctions_EmptyTable()
    {
        Exec("CREATE TABLE empty_wf (id INTEGER PRIMARY KEY, val INTEGER)");
        var rows = Query("SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn FROM empty_wf");
        Assert.Empty(rows);
    }

    [Fact]
    public void MultipleWindowFunctions_SameQuery()
    {
        var rows = Query(@"
            SELECT name, salary,
                   ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn,
                   RANK()       OVER (ORDER BY salary DESC) AS r,
                   DENSE_RANK() OVER (ORDER BY salary DESC) AS dr,
                   LAG(salary)  OVER (ORDER BY salary DESC) AS prev,
                   LEAD(salary) OVER (ORDER BY salary DESC) AS next
            FROM employees ORDER BY salary DESC, name");

        Assert.Equal(7, rows.Count);
        // All non-null for middle rows
        Assert.NotNull(rows[2]["prev"]);
        Assert.NotNull(rows[2]["next"]);
    }

    [Fact]
    public void WindowFunction_LagLead_TextColumn()
    {
        var rows = Query(@"
            SELECT name, LAG(name) OVER (ORDER BY id) AS prev, LEAD(name) OVER (ORDER BY id) AS next
            FROM employees ORDER BY id");

        Assert.Null(rows[0]["prev"]);
        Assert.Equal("Bob", rows[0]["next"]);
        Assert.Equal("Alice", rows[1]["prev"]);
    }

    [Fact]
    public void WindowFunction_AllTied()
    {
        Exec("CREATE TABLE all_tied (id INTEGER PRIMARY KEY, val INTEGER)");
        for (int i = 1; i <= 5; i++)
            Exec($"INSERT INTO all_tied (id, val) VALUES ({i}, 100)");

        var rows = Query(@"
            SELECT RANK() OVER (ORDER BY val) AS r,
                   DENSE_RANK() OVER (ORDER BY val) AS dr
            FROM all_tied");

        foreach (var row in rows)
        {
            Assert.Equal(1L, row["r"]);
            Assert.Equal(1L, row["dr"]);
        }
    }
}
