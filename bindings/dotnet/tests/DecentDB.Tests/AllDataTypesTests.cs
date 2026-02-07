using System;
using System.Data.Common;
using System.IO;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

/// <summary>
/// Comprehensive round-trip tests for every DecentDB data type
/// through the ADO.NET layer using actual typed columns.
/// </summary>
public sealed class AllDataTypesTests : IDisposable
{
    private readonly string _dbPath;

    public AllDataTypesTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_alltypes_{Guid.NewGuid():N}.ddb");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
        var walPath = _dbPath + "-wal";
        if (File.Exists(walPath))
            File.Delete(walPath);
    }

    [Fact]
    public void AllTypes_InsertAndSelect_RoundTrip()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"CREATE TABLE all_types (
            id INTEGER PRIMARY KEY,
            i INT,
            t TEXT,
            bl BLOB,
            b BOOL,
            f FLOAT,
            d DECIMAL(18,4),
            u UUID
        )";
        cmd.ExecuteNonQuery();

        var guid = Guid.NewGuid();
        cmd.CommandText = "INSERT INTO all_types (id, i, t, bl, b, f, d, u) VALUES (@id, @i, @t, @bl, @b, @f, @d, @u)";
        cmd.Parameters.Clear();

        AddParam(cmd, "@id", 1);
        AddParam(cmd, "@i", 42L);
        AddParam(cmd, "@t", "hello world");
        AddParam(cmd, "@bl", new byte[] { 0xDE, 0xAD, 0xBE, 0xEF });
        AddParam(cmd, "@b", true);
        AddParam(cmd, "@f", 3.14159);
        AddParam(cmd, "@d", 123.4567m);
        AddParam(cmd, "@u", guid);

        Assert.Equal(1, cmd.ExecuteNonQuery());

        // Select and verify
        cmd.CommandText = "SELECT id, i, t, bl, b, f, d, u FROM all_types WHERE id = 1";
        cmd.Parameters.Clear();

        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        // INT64
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.Equal(42L, reader.GetInt64(1));

        // TEXT
        Assert.Equal("hello world", reader.GetString(2));

        // BLOB
        Assert.Equal(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF }, reader.GetFieldValue<byte[]>(3));

        // BOOL
        Assert.True(reader.GetBoolean(4));

        // FLOAT64
        Assert.Equal(3.14159, reader.GetDouble(5), 5);

        // DECIMAL
        Assert.Equal(123.4567m, reader.GetDecimal(6));

        // UUID (stored as BLOB, retrieved as Guid)
        Assert.Equal(guid, reader.GetGuid(7));

        Assert.False(reader.Read());
    }

    [Fact]
    public void AllTypes_NullValues_RoundTrip()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"CREATE TABLE null_types (
            id INTEGER PRIMARY KEY,
            i INT,
            t TEXT,
            bl BLOB,
            b BOOL,
            f FLOAT,
            d DECIMAL(18,2),
            u UUID
        )";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO null_types (id, i, t, bl, b, f, d, u) VALUES (@id, @i, @t, @bl, @b, @f, @d, @u)";
        cmd.Parameters.Clear();

        AddParam(cmd, "@id", 1);
        AddParam(cmd, "@i", DBNull.Value);
        AddParam(cmd, "@t", DBNull.Value);
        AddParam(cmd, "@bl", DBNull.Value);
        AddParam(cmd, "@b", DBNull.Value);
        AddParam(cmd, "@f", DBNull.Value);
        AddParam(cmd, "@d", DBNull.Value);
        AddParam(cmd, "@u", DBNull.Value);

        Assert.Equal(1, cmd.ExecuteNonQuery());

        cmd.CommandText = "SELECT i, t, bl, b, f, d, u FROM null_types WHERE id = 1";
        cmd.Parameters.Clear();

        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        for (int i = 0; i < 7; i++)
        {
            Assert.True(reader.IsDBNull(i), $"Column {i} should be NULL");
        }

        Assert.False(reader.Read());
    }

    [Fact]
    public void Int64_BoundaryValues()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t_int (id INTEGER PRIMARY KEY, v INT)";
        cmd.ExecuteNonQuery();

        long[] values = { 0, 1, -1, long.MaxValue, long.MinValue, 42, -42 };
        int id = 1;
        foreach (var val in values)
        {
            cmd.CommandText = "INSERT INTO t_int (id, v) VALUES (@id, @v)";
            cmd.Parameters.Clear();
            AddParam(cmd, "@id", id++);
            AddParam(cmd, "@v", val);
            cmd.ExecuteNonQuery();
        }

        cmd.CommandText = "SELECT v FROM t_int ORDER BY id";
        cmd.Parameters.Clear();
        using var reader = cmd.ExecuteReader();

        foreach (var expected in values)
        {
            Assert.True(reader.Read());
            Assert.Equal(expected, reader.GetInt64(0));
        }
    }

    [Fact]
    public void Bool_TrueAndFalse()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t_bool (id INTEGER PRIMARY KEY, v BOOL)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO t_bool (id, v) VALUES (1, @v)";
        cmd.Parameters.Clear();
        AddParam(cmd, "@v", true);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO t_bool (id, v) VALUES (2, @v)";
        cmd.Parameters.Clear();
        AddParam(cmd, "@v", false);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT v FROM t_bool ORDER BY id";
        cmd.Parameters.Clear();
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.True(reader.GetBoolean(0));
        Assert.True(reader.Read());
        Assert.False(reader.GetBoolean(0));
    }

    [Fact]
    public void Float64_Precision()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t_float (id INTEGER PRIMARY KEY, v FLOAT)";
        cmd.ExecuteNonQuery();

        double[] values = { 0.0, 1.0, -1.0, double.Epsilon, double.MaxValue, double.MinValue, 3.141592653589793 };
        int id = 1;
        foreach (var val in values)
        {
            cmd.CommandText = "INSERT INTO t_float (id, v) VALUES (@id, @v)";
            cmd.Parameters.Clear();
            AddParam(cmd, "@id", id++);
            AddParam(cmd, "@v", val);
            cmd.ExecuteNonQuery();
        }

        cmd.CommandText = "SELECT v FROM t_float ORDER BY id";
        cmd.Parameters.Clear();
        using var reader = cmd.ExecuteReader();

        foreach (var expected in values)
        {
            Assert.True(reader.Read());
            Assert.Equal(expected, reader.GetDouble(0));
        }
    }

    [Fact]
    public void Text_Unicode()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t_text (id INTEGER PRIMARY KEY, v TEXT)";
        cmd.ExecuteNonQuery();

        string[] values = { "", "hello", "你好世界", "🎉🚀", "café", "line1\nline2" };
        int id = 1;
        foreach (var val in values)
        {
            cmd.CommandText = "INSERT INTO t_text (id, v) VALUES (@id, @v)";
            cmd.Parameters.Clear();
            AddParam(cmd, "@id", id++);
            AddParam(cmd, "@v", val);
            cmd.ExecuteNonQuery();
        }

        cmd.CommandText = "SELECT v FROM t_text ORDER BY id";
        cmd.Parameters.Clear();
        using var reader = cmd.ExecuteReader();

        foreach (var expected in values)
        {
            Assert.True(reader.Read());
            Assert.Equal(expected, reader.GetString(0));
        }
    }

    [Fact]
    public void Blob_VariousSizes()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t_blob (id INTEGER PRIMARY KEY, v BLOB)";
        cmd.ExecuteNonQuery();

        byte[][] values = {
            Array.Empty<byte>(),
            new byte[] { 0x00 },
            new byte[] { 0xFF, 0xFE, 0xFD },
            new byte[256]
        };
        // Fill the 256-byte array with pattern
        for (int i = 0; i < 256; i++) values[3][i] = (byte)i;

        int id = 1;
        foreach (var val in values)
        {
            cmd.CommandText = "INSERT INTO t_blob (id, v) VALUES (@id, @v)";
            cmd.Parameters.Clear();
            AddParam(cmd, "@id", id++);
            AddParam(cmd, "@v", val);
            cmd.ExecuteNonQuery();
        }

        cmd.CommandText = "SELECT v FROM t_blob ORDER BY id";
        cmd.Parameters.Clear();
        using var reader = cmd.ExecuteReader();

        foreach (var expected in values)
        {
            Assert.True(reader.Read());
            Assert.Equal(expected, reader.GetFieldValue<byte[]>(0));
        }
    }

    [Fact]
    public void Uuid_MultipleValues()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t_uuid (id INTEGER PRIMARY KEY, v UUID)";
        cmd.ExecuteNonQuery();

        var guids = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.Empty };
        int id = 1;
        foreach (var g in guids)
        {
            cmd.CommandText = "INSERT INTO t_uuid (id, v) VALUES (@id, @v)";
            cmd.Parameters.Clear();
            AddParam(cmd, "@id", id++);
            AddParam(cmd, "@v", g);
            cmd.ExecuteNonQuery();
        }

        cmd.CommandText = "SELECT v FROM t_uuid ORDER BY id";
        cmd.Parameters.Clear();
        using var reader = cmd.ExecuteReader();

        foreach (var expected in guids)
        {
            Assert.True(reader.Read());
            Assert.Equal(expected, reader.GetGuid(0));
            Assert.Equal(expected, reader.GetFieldValue<Guid>(0));
        }
    }

    [Fact]
    public void GetValue_ReturnsCorrectBoxedTypes()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"CREATE TABLE t_getvalue (
            id INTEGER PRIMARY KEY,
            i INT,
            t TEXT,
            b BOOL,
            f FLOAT,
            d DECIMAL(10,2)
        )";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO t_getvalue (id, i, t, b, f, d) VALUES (@id, @i, @t, @b, @f, @d)";
        cmd.Parameters.Clear();
        AddParam(cmd, "@id", 1);
        AddParam(cmd, "@i", 99L);
        AddParam(cmd, "@t", "test");
        AddParam(cmd, "@b", true);
        AddParam(cmd, "@f", 2.5);
        AddParam(cmd, "@d", 10.50m);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT i, t, b, f, d FROM t_getvalue WHERE id = 1";
        cmd.Parameters.Clear();
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        Assert.IsType<long>(reader.GetValue(0));
        Assert.IsType<string>(reader.GetValue(1));
        Assert.IsType<bool>(reader.GetValue(2));
        Assert.IsType<double>(reader.GetValue(3));
        Assert.IsType<decimal>(reader.GetValue(4));
    }

    private static void AddParam(DbCommand cmd, string name, object value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = name;
        p.Value = value;
        cmd.Parameters.Add(p);
    }
}
