using System;
using System.IO;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

public sealed class DecimalTests : IDisposable
{
    private readonly string _dbPath;

    public DecimalTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_decimal_{Guid.NewGuid():N}.ddb");
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
    public void Decimal_RoundTrip()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        // Use DECIMAL(18,9) to support high precision and large numbers (up to 18 digits total)
        // Max integer part: 18-9 = 9 digits.
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, v DECIMAL(18,9))";
        cmd.ExecuteNonQuery();

        decimal[] values = {
            0m,
            1m,
            -1m,
            123.456m,
            -123.456m,
            0.000001m,
            123456789.987654321m, // 9 int digits, 9 frac digits = 18 total. Fits.
            // 99999999999999999m // This is 17 digits. If scale is 9, we need 17+9 = 26 digits capacity? No.
            // With DECIMAL(18,9), we have fixed scale 9.
            // So we can only store numbers with <= 9 integer digits.
            // 999999999 (9 digits) is max integer.
             999999999m,
            -999999999m
        };

        int id = 1;
        foreach (var val in values)
        {
            cmd.CommandText = "INSERT INTO t (id, v) VALUES (@id, @v)";
            cmd.Parameters.Clear();
            
            var pId = cmd.CreateParameter();
            pId.ParameterName = "@id";
            pId.Value = id++;
            cmd.Parameters.Add(pId);
            
            var p = cmd.CreateParameter();
            p.ParameterName = "@v";
            p.Value = val;
            cmd.Parameters.Add(p);
            cmd.ExecuteNonQuery();
        }

        cmd.CommandText = "SELECT v FROM t ORDER BY id";
        using var reader = cmd.ExecuteReader();
        
        foreach (var expected in values)
        {
            Assert.True(reader.Read());
            var actual = reader.GetDecimal(0);
            Assert.Equal(expected, actual);
            
            // Also check GetFieldValue
            Assert.Equal(expected, reader.GetFieldValue<decimal>(0));
            
            // Check GetValue
            Assert.Equal(expected, (decimal)reader.GetValue(0));
            
            // Check GetDataTypeName
            Assert.Equal("DECIMAL", reader.GetDataTypeName(0));
            Assert.Equal(typeof(decimal), reader.GetFieldType(0));
        }
    }

    [Fact]
    public void Decimal_Overflow_Throws()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, v DECIMAL(18,0))";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO t (id, v) VALUES (1, @v)";
        var p = cmd.CreateParameter();
        p.ParameterName = "@v";
        // Decimal.MaxValue is way too large for 64-bit backing
        p.Value = decimal.MaxValue;
        cmd.Parameters.Add(p);

        Assert.Throws<OverflowException>(() => cmd.ExecuteNonQuery());
    }

    [Fact]
    public void Decimal_Text_Interop()
    {
        // Verify that we can still read Text as Decimal if needed (via GetFieldValue parsing logic fallback)
        
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO t (id, v) VALUES (1, '12.34')";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT v FROM t";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        // GetValue returns string because column is TEXT
        Assert.IsType<string>(reader.GetValue(0));
        Assert.Equal("12.34", reader.GetString(0));

        // GetFieldValue<decimal> should parse it (legacy/fallback behavior)
        Assert.Equal(12.34m, reader.GetFieldValue<decimal>(0));
        
        // GetDecimal should parse it (legacy/fallback behavior)
        Assert.Equal(12.34m, reader.GetDecimal(0));
    }
}
