using System;
using System.Data;
using Xunit;
using DecentDB.AdoNet;

namespace DecentDB.Tests;

/// <summary>
/// Tests for SQL window functions: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD.
/// All tests use in-memory databases for isolation.
/// </summary>
public class WindowFunctionTests
{
    private const string MemoryConnectionString = "Data Source=:memory:";

    private static DecentDBConnection CreateSeededConnection()
    {
        var conn = new DecentDBConnection(MemoryConnectionString);
        conn.Open();

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE scores (
                    id    INTEGER PRIMARY KEY,
                    name  TEXT NOT NULL,
                    dept  TEXT NOT NULL,
                    score INTEGER NOT NULL
                )";
            cmd.ExecuteNonQuery();
        }

        var data = new[] {
            (1, "Alice", "eng", 95),
            (2, "Bob", "eng", 95),
            (3, "Carol", "eng", 80),
            (4, "Dave", "sales", 90),
            (5, "Eve", "sales", 85),
            (6, "Frank", "sales", 85),
        };

        foreach (var (id, name, dept, score) in data)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"INSERT INTO scores (id, name, dept, score) VALUES ({id}, '{name}', '{dept}', {score})";
            cmd.ExecuteNonQuery();
        }

        return conn;
    }

    // ── ROW_NUMBER ──

    [Fact]
    public void RowNumber_GlobalOrdering()
    {
        using var conn = CreateSeededConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT name, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM scores ORDER BY id";
        using var reader = cmd.ExecuteReader();

        var expected = new[] { ("Alice", 1L), ("Bob", 2L), ("Carol", 3L), ("Dave", 4L), ("Eve", 5L), ("Frank", 6L) };
        int i = 0;
        while (reader.Read())
        {
            Assert.Equal(expected[i].Item1, reader.GetString(0));
            Assert.Equal(expected[i].Item2, reader.GetInt64(1));
            i++;
        }
        Assert.Equal(6, i);
    }

    [Fact]
    public void RowNumber_PartitionByDept()
    {
        using var conn = CreateSeededConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score DESC) AS rn
            FROM scores ORDER BY dept, id";
        using var reader = cmd.ExecuteReader();

        var results = new System.Collections.Generic.List<(string name, string dept, long rn)>();
        while (reader.Read())
            results.Add((reader.GetString(0), reader.GetString(1), reader.GetInt64(2)));

        // eng partition: Alice(95), Bob(95), Carol(80)
        var eng = results.FindAll(r => r.dept == "eng");
        Assert.Equal(3, eng.Count);
        Assert.Contains(eng, r => r.rn == 1L);
        Assert.Contains(eng, r => r.rn == 2L);
        Assert.Contains(eng, r => r.rn == 3L);

        // sales partition: Dave(90), Eve(85), Frank(85)
        var sales = results.FindAll(r => r.dept == "sales");
        Assert.Equal(3, sales.Count);
        Assert.Contains(sales, r => r.rn == 1L);
        Assert.Contains(sales, r => r.rn == 2L);
        Assert.Contains(sales, r => r.rn == 3L);
    }

    [Fact]
    public void RowNumber_DescendingOrder()
    {
        using var conn = CreateSeededConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT name, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn FROM scores ORDER BY score DESC, id";
        using var reader = cmd.ExecuteReader();

        reader.Read();
        // Top score (95) gets rn=1
        Assert.Equal(1L, reader.GetInt64(1));
    }

    // ── RANK ──

    [Fact]
    public void Rank_TiesGetSameRank_WithGaps()
    {
        using var conn = CreateSeededConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT name, score, RANK() OVER (ORDER BY score DESC) AS r
            FROM scores ORDER BY score DESC, id";
        using var reader = cmd.ExecuteReader();

        var results = new System.Collections.Generic.List<(string name, long score, long rank)>();
        while (reader.Read())
            results.Add((reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2)));

        // Alice(95) and Bob(95) → rank 1
        Assert.Equal(1L, results[0].rank); // Alice
        Assert.Equal(1L, results[1].rank); // Bob
        // Carol(80) should be rank 5 (not 3!) — gap after 2 ties at rank 1, then 90, then 85, 85
        // Sorted: 95,95,90,85,85,80 → ranks: 1,1,3,4,4,6
        Assert.Equal(3L, results[2].rank); // Dave (90)
        Assert.Equal(4L, results[3].rank); // Eve (85)
        Assert.Equal(4L, results[4].rank); // Frank (85)
        Assert.Equal(6L, results[5].rank); // Carol (80)
    }

    [Fact]
    public void Rank_PartitionByDept()
    {
        using var conn = CreateSeededConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT name, dept, score, RANK() OVER (PARTITION BY dept ORDER BY score DESC) AS r
            FROM scores ORDER BY dept, score DESC, id";
        using var reader = cmd.ExecuteReader();

        var results = new System.Collections.Generic.List<(string name, string dept, long score, long rank)>();
        while (reader.Read())
            results.Add((reader.GetString(0), reader.GetString(1), reader.GetInt64(2), reader.GetInt64(3)));

        // eng: Alice(95)=1, Bob(95)=1, Carol(80)=3
        var eng = results.FindAll(r => r.dept == "eng");
        Assert.Equal(1L, eng[0].rank);
        Assert.Equal(1L, eng[1].rank);
        Assert.Equal(3L, eng[2].rank);

        // sales: Dave(90)=1, Eve(85)=2, Frank(85)=2
        var sales = results.FindAll(r => r.dept == "sales");
        Assert.Equal(1L, sales[0].rank);
        Assert.Equal(2L, sales[1].rank);
        Assert.Equal(2L, sales[2].rank);
    }

    [Fact]
    public void Rank_AllUnique_EqualsRowNumber()
    {
        using var conn = new DecentDBConnection(MemoryConnectionString);
        conn.Open();
        using (var c = conn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE unique_vals (id INTEGER PRIMARY KEY, val INTEGER)";
            c.ExecuteNonQuery();
        }
        for (int i = 1; i <= 5; i++)
        {
            using var c = conn.CreateCommand();
            c.CommandText = $"INSERT INTO unique_vals (id, val) VALUES ({i}, {i * 10})";
            c.ExecuteNonQuery();
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT ROW_NUMBER() OVER (ORDER BY val) AS rn,
                   RANK() OVER (ORDER BY val) AS r
            FROM unique_vals ORDER BY val";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            Assert.Equal(reader.GetInt64(0), reader.GetInt64(1));
        }
    }

    // ── DENSE_RANK ──

    [Fact]
    public void DenseRank_NoGaps()
    {
        using var conn = CreateSeededConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT name, score, DENSE_RANK() OVER (ORDER BY score DESC) AS dr
            FROM scores ORDER BY score DESC, id";
        using var reader = cmd.ExecuteReader();

        var results = new System.Collections.Generic.List<(string name, long score, long denseRank)>();
        while (reader.Read())
            results.Add((reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2)));

        // Sorted: 95,95,90,85,85,80 → dense_ranks: 1,1,2,3,3,4
        Assert.Equal(1L, results[0].denseRank); // Alice 95
        Assert.Equal(1L, results[1].denseRank); // Bob 95
        Assert.Equal(2L, results[2].denseRank); // Dave 90
        Assert.Equal(3L, results[3].denseRank); // Eve 85
        Assert.Equal(3L, results[4].denseRank); // Frank 85
        Assert.Equal(4L, results[5].denseRank); // Carol 80
    }

    [Fact]
    public void DenseRank_PartitionByDept()
    {
        using var conn = CreateSeededConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT name, dept, score, DENSE_RANK() OVER (PARTITION BY dept ORDER BY score DESC) AS dr
            FROM scores ORDER BY dept, score DESC, id";
        using var reader = cmd.ExecuteReader();

        var results = new System.Collections.Generic.List<(string name, string dept, long denseRank)>();
        while (reader.Read())
            results.Add((reader.GetString(0), reader.GetString(1), reader.GetInt64(3)));

        // eng: 95,95,80 → 1,1,2
        var eng = results.FindAll(r => r.dept == "eng");
        Assert.Equal(1L, eng[0].denseRank);
        Assert.Equal(1L, eng[1].denseRank);
        Assert.Equal(2L, eng[2].denseRank);

        // sales: 90,85,85 → 1,2,2
        var sales = results.FindAll(r => r.dept == "sales");
        Assert.Equal(1L, sales[0].denseRank);
        Assert.Equal(2L, sales[1].denseRank);
        Assert.Equal(2L, sales[2].denseRank);
    }

    [Fact]
    public void DenseRank_VsRank_DifferentGapBehavior()
    {
        using var conn = CreateSeededConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT score,
                   RANK() OVER (ORDER BY score DESC) AS r,
                   DENSE_RANK() OVER (ORDER BY score DESC) AS dr
            FROM scores ORDER BY score DESC, id";
        using var reader = cmd.ExecuteReader();

        var results = new System.Collections.Generic.List<(long score, long rank, long denseRank)>();
        while (reader.Read())
            results.Add((reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2)));

        // After ties at rank 1, RANK jumps to 3, DENSE_RANK goes to 2
        var afterTie = results.Find(r => r.score == 90);
        Assert.Equal(3L, afterTie.rank);
        Assert.Equal(2L, afterTie.denseRank);
    }

    // ── LAG ──

    [Fact]
    public void Lag_DefaultOffset1_DefaultNull()
    {
        using var conn = CreateSeededConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT name, score, LAG(score) OVER (ORDER BY score DESC) AS prev
            FROM scores ORDER BY score DESC";
        using var reader = cmd.ExecuteReader();

        reader.Read();
        // First row has no previous → NULL
        Assert.True(reader.IsDBNull(2));

        reader.Read();
        // Second row's prev = first row's score (95)
        Assert.Equal(95L, reader.GetInt64(2));
    }

    [Fact]
    public void Lag_ExplicitOffset2()
    {
        using var conn = CreateSeededConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT name, score, LAG(score, 2) OVER (ORDER BY id) AS prev2
            FROM scores ORDER BY id";
        using var reader = cmd.ExecuteReader();

        // Row 1 (id=1): offset 2 back → NULL
        reader.Read();
        Assert.True(reader.IsDBNull(2));

        // Row 2 (id=2): offset 2 back → NULL
        reader.Read();
        Assert.True(reader.IsDBNull(2));

        // Row 3 (id=3): offset 2 back → score of id=1 (95)
        reader.Read();
        Assert.Equal(95L, reader.GetInt64(2));
    }

    [Fact]
    public void Lag_ExplicitDefault()
    {
        using var conn = CreateSeededConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT name, score, LAG(score, 1, -1) OVER (ORDER BY id) AS prev
            FROM scores ORDER BY id";
        using var reader = cmd.ExecuteReader();

        // First row: no previous → default -1
        reader.Read();
        Assert.Equal(-1L, reader.GetInt64(2));

        // Second row: previous exists
        reader.Read();
        Assert.Equal(95L, reader.GetInt64(2));
    }

    [Fact]
    public void Lag_PartitionResetsPerGroup()
    {
        using var conn = CreateSeededConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT name, dept, score, LAG(score) OVER (PARTITION BY dept ORDER BY id) AS prev
            FROM scores ORDER BY dept, id";
        using var reader = cmd.ExecuteReader();

        var results = new System.Collections.Generic.List<(string name, string dept, long? prev)>();
        while (reader.Read())
            results.Add((reader.GetString(0), reader.GetString(1), reader.IsDBNull(3) ? null : reader.GetInt64(3)));

        // First in each partition has NULL prev
        var eng = results.FindAll(r => r.dept == "eng");
        Assert.Null(eng[0].prev);   // Alice is first in eng
        Assert.Equal(95L, eng[1].prev); // Bob sees Alice's 95

        var sales = results.FindAll(r => r.dept == "sales");
        Assert.Null(sales[0].prev); // Dave is first in sales
    }

    // ── LEAD ──

    [Fact]
    public void Lead_DefaultOffset1_DefaultNull()
    {
        using var conn = CreateSeededConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT name, score, LEAD(score) OVER (ORDER BY id) AS next
            FROM scores ORDER BY id";
        using var reader = cmd.ExecuteReader();

        // First row: next = second row's score (95)
        reader.Read();
        Assert.Equal(95L, reader.GetInt64(2));

        // Skip to last row
        while (reader.Read()) { }
        // (we can't easily get last row mid-stream, test differently)
    }

    [Fact]
    public void Lead_LastRowIsNull()
    {
        using var conn = CreateSeededConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT name, score, LEAD(score) OVER (ORDER BY id) AS next
            FROM scores ORDER BY id";
        using var reader = cmd.ExecuteReader();

        var results = new System.Collections.Generic.List<(string name, long? next)>();
        while (reader.Read())
            results.Add((reader.GetString(0), reader.IsDBNull(2) ? null : reader.GetInt64(2)));

        // Last row (Frank) has NULL next
        Assert.Null(results[^1].next);
        // First row (Alice) has Bob's score
        Assert.Equal(95L, results[0].next);
    }

    [Fact]
    public void Lead_ExplicitDefault()
    {
        using var conn = CreateSeededConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT name, LEAD(score, 1, 999) OVER (ORDER BY id) AS next
            FROM scores ORDER BY id";
        using var reader = cmd.ExecuteReader();

        var results = new System.Collections.Generic.List<long>();
        while (reader.Read())
            results.Add(reader.GetInt64(1));

        // Last row gets default 999
        Assert.Equal(999L, results[^1]);
    }

    [Fact]
    public void Lead_PartitionResetsPerGroup()
    {
        using var conn = CreateSeededConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT name, dept, score, LEAD(score) OVER (PARTITION BY dept ORDER BY id) AS next
            FROM scores ORDER BY dept, id";
        using var reader = cmd.ExecuteReader();

        var results = new System.Collections.Generic.List<(string name, string dept, long? next)>();
        while (reader.Read())
            results.Add((reader.GetString(0), reader.GetString(1), reader.IsDBNull(3) ? null : reader.GetInt64(3)));

        // Last in each partition has NULL next
        var eng = results.FindAll(r => r.dept == "eng");
        Assert.Null(eng[^1].next);   // Carol is last in eng
        Assert.NotNull(eng[0].next); // Alice has next

        var sales = results.FindAll(r => r.dept == "sales");
        Assert.Null(sales[^1].next); // Frank is last in sales
    }

    // ── Edge Cases ──

    [Fact]
    public void WindowFunction_SingleRow()
    {
        using var conn = new DecentDBConnection(MemoryConnectionString);
        conn.Open();
        using (var c = conn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE single (id INTEGER PRIMARY KEY, val INTEGER)";
            c.ExecuteNonQuery();
        }
        using (var c = conn.CreateCommand())
        {
            c.CommandText = "INSERT INTO single (id, val) VALUES (1, 42)";
            c.ExecuteNonQuery();
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn,
                   RANK() OVER (ORDER BY id) AS r,
                   DENSE_RANK() OVER (ORDER BY id) AS dr,
                   LAG(val) OVER (ORDER BY id) AS prev,
                   LEAD(val) OVER (ORDER BY id) AS next
            FROM single";
        using var reader = cmd.ExecuteReader();

        reader.Read();
        Assert.Equal(1L, reader.GetInt64(0)); // rn
        Assert.Equal(1L, reader.GetInt64(1)); // rank
        Assert.Equal(1L, reader.GetInt64(2)); // dense_rank
        Assert.True(reader.IsDBNull(3));      // lag = NULL
        Assert.True(reader.IsDBNull(4));      // lead = NULL
    }

    [Fact]
    public void WindowFunction_EmptyTable()
    {
        using var conn = new DecentDBConnection(MemoryConnectionString);
        conn.Open();
        using (var c = conn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE empty (id INTEGER PRIMARY KEY, val INTEGER)";
            c.ExecuteNonQuery();
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn FROM empty";
        using var reader = cmd.ExecuteReader();

        Assert.False(reader.Read());
    }

    [Fact]
    public void WindowFunction_NullValues_InOrderBy()
    {
        using var conn = new DecentDBConnection(MemoryConnectionString);
        conn.Open();
        using (var c = conn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE nullable (id INTEGER PRIMARY KEY, val INTEGER)";
            c.ExecuteNonQuery();
        }
        using (var c = conn.CreateCommand())
        {
            c.CommandText = "INSERT INTO nullable VALUES (1, NULL)";
            c.ExecuteNonQuery();
        }
        using (var c = conn.CreateCommand())
        {
            c.CommandText = "INSERT INTO nullable VALUES (2, NULL)";
            c.ExecuteNonQuery();
        }
        using (var c = conn.CreateCommand())
        {
            c.CommandText = "INSERT INTO nullable VALUES (3, 10)";
            c.ExecuteNonQuery();
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT id,
                   RANK() OVER (ORDER BY val) AS r,
                   DENSE_RANK() OVER (ORDER BY val) AS dr
            FROM nullable ORDER BY id";
        using var reader = cmd.ExecuteReader();

        var results = new System.Collections.Generic.List<(long id, long rank, long denseRank)>();
        while (reader.Read())
            results.Add((reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2)));

        // NULLs sort first and compare equal → same rank
        Assert.Equal(results[0].rank, results[1].rank);
        Assert.Equal(results[0].denseRank, results[1].denseRank);
        // Non-null value has different rank
        Assert.NotEqual(results[0].denseRank, results[2].denseRank);
    }

    [Fact]
    public void WindowFunction_MultipleWindowsInSameSelect()
    {
        using var conn = CreateSeededConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT name, score,
                   ROW_NUMBER() OVER (ORDER BY score DESC) AS rn,
                   RANK() OVER (ORDER BY score DESC) AS r,
                   DENSE_RANK() OVER (ORDER BY score DESC) AS dr
            FROM scores ORDER BY score DESC, name";
        using var reader = cmd.ExecuteReader();

        int count = 0;
        while (reader.Read())
        {
            count++;
            // All three window functions should return values (not NULL)
            Assert.False(reader.IsDBNull(2));
            Assert.False(reader.IsDBNull(3));
            Assert.False(reader.IsDBNull(4));
        }
        Assert.Equal(6, count);
    }

    [Fact]
    public void WindowFunction_WithAlias()
    {
        using var conn = CreateSeededConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT name, RANK() OVER (ORDER BY score DESC) AS my_ranking
            FROM scores ORDER BY score DESC, id LIMIT 3";
        using var reader = cmd.ExecuteReader();

        // Should execute without error and return 3 rows
        int count = 0;
        while (reader.Read()) count++;
        Assert.Equal(3, count);
    }

    [Fact]
    public void WindowFunction_LagLead_OnTextColumn()
    {
        using var conn = CreateSeededConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT name, LAG(name) OVER (ORDER BY id) AS prev_name,
                         LEAD(name) OVER (ORDER BY id) AS next_name
            FROM scores ORDER BY id";
        using var reader = cmd.ExecuteReader();

        // First row: prev is NULL, next is Bob
        reader.Read();
        Assert.True(reader.IsDBNull(1));
        Assert.Equal("Bob", reader.GetString(2));
    }

    [Fact]
    public void WindowFunction_AllTied()
    {
        using var conn = new DecentDBConnection(MemoryConnectionString);
        conn.Open();
        using (var c = conn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE ties (id INTEGER PRIMARY KEY, val INTEGER)";
            c.ExecuteNonQuery();
        }
        for (int i = 1; i <= 4; i++)
        {
            using var c = conn.CreateCommand();
            c.CommandText = $"INSERT INTO ties (id, val) VALUES ({i}, 100)";
            c.ExecuteNonQuery();
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT RANK() OVER (ORDER BY val) AS r,
                   DENSE_RANK() OVER (ORDER BY val) AS dr
            FROM ties";
        using var reader = cmd.ExecuteReader();

        while (reader.Read())
        {
            // All tied → all rank 1, all dense_rank 1
            Assert.Equal(1L, reader.GetInt64(0));
            Assert.Equal(1L, reader.GetInt64(1));
        }
    }
}
