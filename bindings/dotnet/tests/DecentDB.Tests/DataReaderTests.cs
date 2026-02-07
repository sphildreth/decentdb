using System;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using Xunit;
using DecentDb.AdoNet;

namespace DecentDb.Tests;

public class DataReaderTests : IDisposable
{
    private readonly string _dbPath;

    public DataReaderTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid():N}.ddb");
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
    public void StreamingBehaviorForwardOnly()
    {
        using var conn = new DecentDbConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE streaming_test (id INTEGER, value INTEGER)";
        cmd.ExecuteNonQuery();
        for (int i = 0; i < 100; i++)
        {
            cmd.CommandText = $"INSERT INTO streaming_test (id, value) VALUES ({i}, {i * 10})";
            cmd.ExecuteNonQuery();
        }

        cmd.CommandText = "SELECT id, value FROM streaming_test ORDER BY id";
        using var reader = cmd.ExecuteReader();

        for (int i = 0; i < 100; i++)
        {
            Assert.True(reader.Read());
            Assert.Equal(i, reader.GetInt32(0));
            Assert.Equal(i * 10, reader.GetInt32(1));
        }

        Assert.False(reader.Read());
    }

    [Fact]
    public void FieldCountAndNames()
    {
        using var conn = new DecentDbConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE fields_test (id INTEGER PRIMARY KEY, name TEXT, price REAL, in_stock BOOLEAN)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO fields_test (id, name, price, in_stock) VALUES (1, 'Test', 9.99, TRUE)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT id, name, price, in_stock FROM fields_test";
        using var reader = cmd.ExecuteReader();

        Assert.Equal(4, reader.FieldCount);
        Assert.Equal("id", reader.GetName(0));
        Assert.Equal("name", reader.GetName(1));
        Assert.Equal("price", reader.GetName(2));
        Assert.Equal("in_stock", reader.GetName(3));
    }

    [Fact]
    public void GetDataTypeName()
    {
        using var conn = new DecentDbConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE types_test (int_val INTEGER, text_val TEXT, real_val REAL, bool_val BOOLEAN, blob_val BLOB)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO types_test (int_val, text_val, real_val, bool_val, blob_val) VALUES (1, 't', 1.0, TRUE, @blob)";
        var blobParam = cmd.CreateParameter();
        blobParam.ParameterName = "@blob";
        blobParam.Value = new byte[] { 0 };
        cmd.Parameters.Add(blobParam);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT * FROM types_test";
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());

        Assert.Equal("BIGINT", reader.GetDataTypeName(0));
        Assert.Equal("TEXT", reader.GetDataTypeName(1));
        Assert.Equal("DOUBLE", reader.GetDataTypeName(2));
        Assert.Equal("BOOLEAN", reader.GetDataTypeName(3));
        Assert.Equal("BLOB", reader.GetDataTypeName(4));
    }

    [Fact]
    public void GetFieldType()
    {
        using var conn = new DecentDbConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE type_mapping_test (int_col INTEGER, text_col TEXT, real_col REAL, bool_col BOOLEAN)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO type_mapping_test (int_col, text_col, real_col, bool_col) VALUES (1, 't', 1.0, TRUE)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT * FROM type_mapping_test";
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());

        Assert.Equal(typeof(long), reader.GetFieldType(0));
        Assert.Equal(typeof(string), reader.GetFieldType(1));
        Assert.Equal(typeof(double), reader.GetFieldType(2));
        Assert.Equal(typeof(bool), reader.GetFieldType(3));
    }

    [Fact]
    public void GetOrdinal()
    {
        using var conn = new DecentDbConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE ordinal_test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT id, name, value FROM ordinal_test";
        using var reader = cmd.ExecuteReader();

        Assert.Equal(0, reader.GetOrdinal("id"));
        Assert.Equal(1, reader.GetOrdinal("name"));
        Assert.Equal(2, reader.GetOrdinal("value"));
    }

    [Fact]
    public void GetFieldValue()
    {
        using var conn = new DecentDbConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE field_value_test (id INTEGER PRIMARY KEY, name TEXT, amount INTEGER)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO field_value_test (id, name, amount) VALUES (1, 'Test', 42)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT id, name, amount FROM field_value_test";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        Assert.Equal(1L, reader.GetFieldValue<long>(0));
        Assert.Equal("Test", reader.GetFieldValue<string>(1));
        Assert.Equal(42L, reader.GetFieldValue<int>(2));
    }

    [Fact]
    public void IndexerAccess()
    {
        using var conn = new DecentDbConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE indexer_test (a INTEGER, b TEXT, c REAL)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO indexer_test (a, b, c) VALUES (1, 'hello', 3.14)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT a, b, c FROM indexer_test";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        Assert.Equal(1L, reader[0]);
        Assert.Equal("hello", reader[1]);
        Assert.Equal(3.14, reader[2]);

        Assert.Equal("hello", reader["b"]);
        Assert.Equal(3.14, reader["c"]);
    }

    [Fact]
    public void HasRowsAndDepth()
    {
        using var conn = new DecentDbConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE has_rows_test (id INTEGER PRIMARY KEY)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT * FROM has_rows_test";
        using (var reader = cmd.ExecuteReader())
        {
            Assert.False(reader.HasRows);
        }

        cmd.CommandText = "INSERT INTO has_rows_test VALUES (1)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT * FROM has_rows_test";
        using var reader2 = cmd.ExecuteReader();
        Assert.True(reader2.HasRows);
        Assert.Equal(0, reader2.Depth);
    }

    [Fact]
    public void RecordsAffected()
    {
        using var conn = new DecentDbConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE records_test (id INTEGER PRIMARY KEY)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO records_test VALUES (1)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO records_test VALUES (2)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO records_test VALUES (3)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT COUNT(*) FROM records_test";
        var count = cmd.ExecuteScalar();
        Assert.Equal(3L, count);
    }

    [Fact]
    public void GetBytes()
    {
        using var conn = new DecentDbConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE blob_test (id INTEGER PRIMARY KEY, data BLOB)";
        cmd.ExecuteNonQuery();

        var bytes = new byte[] { 1, 2, 3, 4, 5 };
        cmd.CommandText = "INSERT INTO blob_test (id, data) VALUES (1, @data)";
        var param = cmd.CreateParameter();
        param.ParameterName = "@data";
        param.Value = bytes;
        cmd.Parameters.Add(param);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT data FROM blob_test WHERE id = $1";
        var idParam = cmd.CreateParameter();
        idParam.ParameterName = "$1";
        idParam.Value = 1;
        cmd.Parameters.Add(idParam);

        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        var result = reader.GetBytes(0, 0, null, 0, 0);
        Assert.Equal(bytes.Length, result);

        var buffer = new byte[5];
        reader.GetBytes(0, 0, buffer, 0, 5);
        Assert.Equal(bytes, buffer);
    }

    [Fact]
    public void GetGuid()
    {
        using var conn = new DecentDbConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE guid_test (id INTEGER PRIMARY KEY, guid_val BLOB)";
        cmd.ExecuteNonQuery();

        var guid = Guid.NewGuid();
        cmd.CommandText = "INSERT INTO guid_test (id, guid_val) VALUES (1, @guid)";
        var param = cmd.CreateParameter();
        param.ParameterName = "@guid";
        param.Value = guid.ToByteArray();
        cmd.Parameters.Add(param);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT guid_val FROM guid_test WHERE id = $1";
        var idParam = cmd.CreateParameter();
        idParam.ParameterName = "$1";
        idParam.Value = 1;
        cmd.Parameters.Add(idParam);

        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        var result = reader.GetGuid(0);
        Assert.Equal(guid, result);
    }
}
