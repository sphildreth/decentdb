using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

public sealed class ParameterCollectionTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_param_coll_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
        if (File.Exists(_dbPath + "-wal"))
            File.Delete(_dbPath + "-wal");
    }

    [Fact]
    public void Add_IncreasesCount()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        using var cmd = conn.CreateCommand();

        Assert.Empty(cmd.Parameters);

        var p1 = new DecentDBParameter("@a", 1);
        var idx = cmd.Parameters.Add(p1);

        Assert.Equal(0, idx);
        Assert.Single(cmd.Parameters);
    }

    [Fact]
    public void Add_WrongType_ThrowsArgumentException()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        using var cmd = conn.CreateCommand();

        Assert.Throws<ArgumentException>(() => cmd.Parameters.Add("not a parameter"));
    }

    [Fact]
    public void Insert_AtIndex_ShiftsElements()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        using var cmd = conn.CreateCommand();

        var p1 = new DecentDBParameter("@a", 1);
        var p2 = new DecentDBParameter("@b", 2);
        cmd.Parameters.Add(p1);
        cmd.Parameters.Insert(0, p2);

        Assert.Same(p2, cmd.Parameters[0]);
        Assert.Same(p1, cmd.Parameters[1]);
    }

    [Fact]
    public void Remove_ByObject_DecreasesCount()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        using var cmd = conn.CreateCommand();

        var p1 = new DecentDBParameter("@a", 1);
        cmd.Parameters.Add(p1);
        cmd.Parameters.Remove(p1);

        Assert.Empty(cmd.Parameters);
    }

    [Fact]
    public void RemoveAt_ByIndex_DecreasesCount()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        using var cmd = conn.CreateCommand();

        cmd.Parameters.Add(new DecentDBParameter("@a", 1));
        cmd.Parameters.Add(new DecentDBParameter("@b", 2));
        cmd.Parameters.RemoveAt(0);

        Assert.Single(cmd.Parameters);
        Assert.Equal("@b", cmd.Parameters[0].ParameterName);
    }

    [Fact]
    public void RemoveAt_ByName_RemovesMatchingParameter()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        using var cmd = conn.CreateCommand();

        cmd.Parameters.Add(new DecentDBParameter("@a", 1));
        cmd.Parameters.Add(new DecentDBParameter("@b", 2));
        cmd.Parameters.RemoveAt("@a");

        Assert.Single(cmd.Parameters);
        Assert.Equal("@b", ((DbParameter)cmd.Parameters[0]).ParameterName);
    }

    [Fact]
    public void Clear_EmptiesCollection()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        using var cmd = conn.CreateCommand();

        cmd.Parameters.Add(new DecentDBParameter("@a", 1));
        cmd.Parameters.Add(new DecentDBParameter("@b", 2));
        cmd.Parameters.Clear();

        Assert.Empty(cmd.Parameters);
    }

    [Fact]
    public void Contains_ByObject_ReturnsTrueWhenPresent()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        using var cmd = conn.CreateCommand();

        var p1 = new DecentDBParameter("@a", 1);
        cmd.Parameters.Add(p1);

        Assert.True(cmd.Parameters.Contains(p1));
        Assert.False(cmd.Parameters.Contains(new DecentDBParameter("@a", 1)));
    }

    [Fact]
    public void Contains_ByName_ReturnsTrueWhenPresent()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        using var cmd = conn.CreateCommand();

        cmd.Parameters.Add(new DecentDBParameter("@a", 1));

        Assert.True(cmd.Parameters.Contains("@a"));
        Assert.False(cmd.Parameters.Contains("@b"));
    }

    [Fact]
    public void IndexOf_ByObject_ReturnsIndex()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        using var cmd = conn.CreateCommand();

        var p1 = new DecentDBParameter("@a", 1);
        cmd.Parameters.Add(p1);

        Assert.Equal(0, cmd.Parameters.IndexOf(p1));
        Assert.Equal(-1, cmd.Parameters.IndexOf(new DecentDBParameter("@a", 1)));
    }

    [Fact]
    public void IndexOf_ByName_ReturnsIndex()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        using var cmd = conn.CreateCommand();

        cmd.Parameters.Add(new DecentDBParameter("@a", 1));
        cmd.Parameters.Add(new DecentDBParameter("@b", 2));

        Assert.Equal(0, cmd.Parameters.IndexOf("@a"));
        Assert.Equal(1, cmd.Parameters.IndexOf("@b"));
        Assert.Equal(-1, cmd.Parameters.IndexOf("@c"));
    }

    [Fact]
    public void GetParameter_ByName_ReturnsParameter()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        using var cmd = conn.CreateCommand();

        var p = new DecentDBParameter("@name", "value");
        cmd.Parameters.Add(p);

        Assert.Same(p, cmd.Parameters["@name"]);
    }

    [Fact]
    public void GetParameter_ByName_NotFound_ThrowsIndexOutOfRange()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        using var cmd = conn.CreateCommand();

        Assert.Throws<IndexOutOfRangeException>(() => cmd.Parameters["@missing"]);
    }

    [Fact]
    public void SetParameter_ByName_ReplacesExisting()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        using var cmd = conn.CreateCommand();

        var p1 = new DecentDBParameter("@name", "old");
        cmd.Parameters.Add(p1);

        var p2 = new DecentDBParameter("@name", "new");
        cmd.Parameters["@name"] = p2;

        Assert.Same(p2, cmd.Parameters["@name"]);
    }

    [Fact]
    public void SetParameter_ByName_AddsIfNotExisting()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        using var cmd = conn.CreateCommand();

        var p = new DecentDBParameter("@name", "value");
        cmd.Parameters["@name"] = p;

        Assert.Same(p, cmd.Parameters["@name"]);
    }

    [Fact]
    public void SetParameter_WrongType_ThrowsArgumentException()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        using var cmd = conn.CreateCommand();

        var fakeParam = new FakeDbParameter();
        Assert.Throws<ArgumentException>(() => cmd.Parameters[0] = fakeParam);
    }

    [Fact]
    public void AddRange_AddsMultipleParameters()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        using var cmd = conn.CreateCommand();

        var arr = new[]
        {
            new DecentDBParameter("@a", 1),
            new DecentDBParameter("@b", 2)
        };

        cmd.Parameters.AddRange(arr);
        Assert.Equal(2, cmd.Parameters.Count);
    }

    [Fact]
    public void CopyTo_CopiesToArray()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        using var cmd = conn.CreateCommand();

        var p = new DecentDBParameter("@a", 1);
        cmd.Parameters.Add(p);

        var array = new DbParameter[2];
        cmd.Parameters.CopyTo(array, 1);

        Assert.Same(p, array[1]);
    }

    [Fact]
    public void GetEnumerator_ReturnsAllParameters()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        using var cmd = conn.CreateCommand();

        cmd.Parameters.Add(new DecentDBParameter("@a", 1));
        cmd.Parameters.Add(new DecentDBParameter("@b", 2));

        var count = 0;
        foreach (DbParameter p in cmd.Parameters)
        {
            count++;
        }

        Assert.Equal(2, count);
    }

    private class FakeDbParameter : DbParameter
    {
        public override DbType DbType { get; set; }
        public override ParameterDirection Direction { get; set; }
        public override bool IsNullable { get; set; }
        [AllowNull]
        public override string ParameterName { get; set; } = "";
        public override int Size { get; set; }
        [AllowNull]
        public override string SourceColumn { get; set; } = "";
        public override bool SourceColumnNullMapping { get; set; }
        public override object? Value { get; set; }
        public override void ResetDbType() { }
    }
}
