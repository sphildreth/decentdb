using System;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using Xunit;
using DecentDB.AdoNet;

namespace DecentDB.Tests;

public class DmlTests : IDisposable
{
    private readonly string _dbPath;

    public DmlTests()
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
    public void CreateTableAndInsert()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)";
        var rowsAffected = cmd.ExecuteNonQuery();
        Assert.Equal(1, rowsAffected);

        cmd.CommandText = "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)";
        rowsAffected = cmd.ExecuteNonQuery();
        Assert.Equal(1, rowsAffected);
    }

    [Fact]
    public void SelectWithPositionalParameters()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO products (id, name, price) VALUES (1, 'Widget', 9.99)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO products (id, name, price) VALUES (2, 'Gadget', 19.99)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT * FROM products WHERE price < $1";
        var param = cmd.CreateParameter();
        param.ParameterName = "$1";
        param.Value = 25.0;
        cmd.Parameters.Add(param);

        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Widget", reader.GetString(1));
        Assert.Equal(9.99, reader.GetDouble(2));

        Assert.True(reader.Read());
        Assert.Equal("Gadget", reader.GetString(1));
        Assert.Equal(19.99, reader.GetDouble(2));

        Assert.False(reader.Read());
    }

    [Fact]
    public void SelectWithNamedParameters()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, salary INTEGER)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO employees (id, name, salary) VALUES (1, 'John', 50000)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO employees (id, name, salary) VALUES (2, 'Jane', 60000)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT * FROM employees WHERE salary > @minSalary";
        var param = cmd.CreateParameter();
        param.ParameterName = "@minSalary";
        param.Value = 55000;
        cmd.Parameters.Add(param);

        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Jane", reader.GetString(1));
        Assert.Equal(60000, reader.GetInt32(2));

        Assert.False(reader.Read());
    }

    [Fact]
    public void SelectWithP0Parameters()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO orders (id, amount) VALUES (1, 100)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO orders (id, amount) VALUES (2, 200)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT * FROM orders WHERE amount > @p0";
        var param = cmd.CreateParameter();
        param.ParameterName = "@p0";
        param.Value = 150;
        cmd.Parameters.Add(param);

        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(200, reader.GetInt32(1));
        Assert.False(reader.Read());
    }

    [Fact]
    public void SelectWithMultipleParameters()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO items (id, name, category, price) VALUES (1, 'Apple', 'Fruit', 1.0)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO items (id, name, category, price) VALUES (2, 'Banana', 'Fruit', 0.5)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO items (id, name, category, price) VALUES (3, 'Carrot', 'Vegetable', 0.75)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT * FROM items WHERE category = @category AND price < @maxPrice ORDER BY id";
        var catParam = cmd.CreateParameter();
        catParam.ParameterName = "@category";
        catParam.Value = "Fruit";
        cmd.Parameters.Add(catParam);

        var priceParam = cmd.CreateParameter();
        priceParam.ParameterName = "@maxPrice";
        priceParam.Value = 1.01;
        cmd.Parameters.Add(priceParam);

        using var reader = cmd.ExecuteReader();

        var seenApple = false;
        var seenBanana = false;
        while (reader.Read())
        {
            var name = reader.GetString(1);
            if (name == "Apple") seenApple = true;
            if (name == "Banana") seenBanana = true;
        }

        Assert.True(seenApple);
        Assert.True(seenBanana);
    }

    [Fact]
    public void UpdateAndDelete()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE counters (id INTEGER PRIMARY KEY, value INTEGER)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO counters (id, value) VALUES (1, 0)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "UPDATE counters SET value = value + 1 WHERE id = $1";
        var idParam = cmd.CreateParameter();
        idParam.ParameterName = "$1";
        idParam.Value = 1;
        cmd.Parameters.Add(idParam);

        var rowsAffected = cmd.ExecuteNonQuery();
        Assert.Equal(1, rowsAffected);

        cmd.CommandText = "SELECT value FROM counters WHERE id = $1";
        var value = cmd.ExecuteScalar();
        Assert.Equal(1L, value);

        cmd.CommandText = "DELETE FROM counters WHERE id = $1";
        rowsAffected = cmd.ExecuteNonQuery();
        Assert.Equal(1, rowsAffected);
    }

    [Fact]
    public void ExecuteScalar()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE config (key TEXT PRIMARY KEY, value TEXT)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO config (key, value) VALUES ('version', '1.0')";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT value FROM config WHERE key = @key";
        var param = cmd.CreateParameter();
        param.ParameterName = "@key";
        param.Value = "version";
        cmd.Parameters.Add(param);

        var value = cmd.ExecuteScalar();
        Assert.Equal("1.0", value);
    }

    [Fact]
    public void NullParameterHandling()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE nullable_test (id INTEGER PRIMARY KEY, value TEXT)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO nullable_test (id, value) VALUES (1, @val)";
        var param = cmd.CreateParameter();
        param.ParameterName = "@val";
        param.Value = DBNull.Value;
        cmd.Parameters.Add(param);

        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT value FROM nullable_test WHERE id = $1";
        var idParam = cmd.CreateParameter();
        idParam.ParameterName = "$1";
        idParam.Value = 1;
        cmd.Parameters.Add(idParam);

        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.True(reader.IsDBNull(1));
    }
}
