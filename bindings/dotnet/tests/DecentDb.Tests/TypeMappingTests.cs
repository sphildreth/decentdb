using System;
using System.IO;
using DecentDb.AdoNet;
using Xunit;

namespace DecentDb.Tests;

public sealed class TypeMappingTests : IDisposable
{
    private readonly string _dbPath;

    public TypeMappingTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid():N}.db");
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
    public void DateOnly_TimeOnly_TimeSpan_Guid_RoundTrip()
    {
        using var conn = new DecentDbConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, d INTEGER, t INTEGER, s INTEGER, g BLOB)";
        cmd.ExecuteNonQuery();

        var date = new DateOnly(2020, 2, 29);
        var time = new TimeOnly(9, 30, 15, 123);
        var span = TimeSpan.FromDays(3) + TimeSpan.FromMilliseconds(7);
        var guid = Guid.NewGuid();

        cmd.CommandText = "INSERT INTO t (id, d, t, s, g) VALUES (@id, @d, @t, @s, @g)";
        cmd.Parameters.Clear();

        var pId = cmd.CreateParameter();
        pId.ParameterName = "@id";
        pId.Value = 1;
        cmd.Parameters.Add(pId);

        var pD = cmd.CreateParameter();
        pD.ParameterName = "@d";
        pD.Value = date;
        cmd.Parameters.Add(pD);

        var pT = cmd.CreateParameter();
        pT.ParameterName = "@t";
        pT.Value = time;
        cmd.Parameters.Add(pT);

        var pS = cmd.CreateParameter();
        pS.ParameterName = "@s";
        pS.Value = span;
        cmd.Parameters.Add(pS);

        var pG = cmd.CreateParameter();
        pG.ParameterName = "@g";
        pG.Value = guid;
        cmd.Parameters.Add(pG);

        Assert.Equal(1, cmd.ExecuteNonQuery());

        cmd.CommandText = "SELECT d, t, s, g FROM t WHERE id = 1";
        cmd.Parameters.Clear();

        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        Assert.Equal(date, reader.GetFieldValue<DateOnly>(0));
        Assert.Equal(time, reader.GetFieldValue<TimeOnly>(1));
        Assert.Equal(span, reader.GetFieldValue<TimeSpan>(2));
        Assert.Equal(guid, reader.GetFieldValue<Guid>(3));

        Assert.False(reader.Read());
    }

    [Fact]
    public void DateTimeAndDateTimeOffset_AreUnixEpochMilliseconds()
    {
        using var conn = new DecentDbConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, dt INTEGER, dto INTEGER)";
        cmd.ExecuteNonQuery();

        var dt = new DateTime(2024, 01, 02, 03, 04, 05, DateTimeKind.Utc);
        var dto = new DateTimeOffset(2024, 01, 02, 03, 04, 05, TimeSpan.Zero);

        cmd.CommandText = "INSERT INTO t (id, dt, dto) VALUES (@id, @dt, @dto)";
        cmd.Parameters.Clear();

        var pId = cmd.CreateParameter();
        pId.ParameterName = "@id";
        pId.Value = 1;
        cmd.Parameters.Add(pId);

        var pDt = cmd.CreateParameter();
        pDt.ParameterName = "@dt";
        pDt.Value = dt;
        cmd.Parameters.Add(pDt);

        var pDto = cmd.CreateParameter();
        pDto.ParameterName = "@dto";
        pDto.Value = dto;
        cmd.Parameters.Add(pDto);

        Assert.Equal(1, cmd.ExecuteNonQuery());

        cmd.CommandText = "SELECT dt, dto FROM t WHERE id = 1";
        cmd.Parameters.Clear();

        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        Assert.Equal(dt, reader.GetFieldValue<DateTime>(0));
        Assert.Equal(dto, reader.GetFieldValue<DateTimeOffset>(1));
    }

    [Fact]
    public void MaxLength_UsesUtf8Bytes()
    {
        using var conn = new DecentDbConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO t (id, name) VALUES (@id, @name)";
        cmd.Parameters.Clear();

        var pId = cmd.CreateParameter();
        pId.ParameterName = "@id";
        pId.Value = 1;
        cmd.Parameters.Add(pId);

        // "🎉" is 4 bytes in UTF-8.
        var pName = cmd.CreateParameter();
        pName.ParameterName = "@name";
        pName.Value = "🎉";
        pName.Size = 3;
        cmd.Parameters.Add(pName);

        Assert.Throws<ArgumentException>(() => cmd.ExecuteNonQuery());
    }
}
