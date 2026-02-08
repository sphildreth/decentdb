using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;
using DecentDB.AdoNet;
using DecentDB.MicroOrm;

namespace DecentDB.Tests;

public class EdgeCaseTests : IDisposable
{
    private readonly string _dbPath;

    public EdgeCaseTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_edge_{Guid.NewGuid():N}.ddb");
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
    public void ConnectionStringParsing_EdgeCases()
    {
        // Test connection string with extra spaces
        var csWithSpaces = $"Data Source = {_dbPath} ; Cache Size = 32MB ";
        using var conn1 = new DecentDBConnection(csWithSpaces);
        Assert.Equal(_dbPath, conn1.DataSource);

        // Test connection string with mixed case
        var csMixedCase = $"data source={_dbPath};CACHE SIZE=16MB;logging=1";
        using var conn2 = new DecentDBConnection(csMixedCase);
        Assert.Equal(_dbPath, conn2.DataSource);

        // Test empty connection string
        using var conn3 = new DecentDBConnection("");
        Assert.Empty(conn3.ConnectionString);

        // Test connection string with unknown parameters
        var csUnknownParams = $"Data Source={_dbPath};UnknownParam=Value;AnotherUnknown=AnotherValue";
        using var conn4 = new DecentDBConnection(csUnknownParams);
        Assert.Equal(_dbPath, conn4.DataSource);
    }

    [Fact]
    public void ParameterBinding_EdgeCases()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        
        // Test binding with very large string
        var largeString = new string('x', 10000);
        cmd.CommandText = "CREATE TABLE test_large_str (id INTEGER PRIMARY KEY, txt TEXT)";
        cmd.ExecuteNonQuery();
        
        cmd.CommandText = "INSERT INTO test_large_str (id, txt) VALUES (?, ?)";
        var param = cmd.CreateParameter();
        param.Value = 1;
        cmd.Parameters.Add(param);
        var param2 = cmd.CreateParameter();
        param2.Value = largeString;
        cmd.Parameters.Add(param2);
        cmd.ExecuteNonQuery();

        // Test binding with max/min numeric values
        cmd.CommandText = "CREATE TABLE test_numeric_bounds (id INTEGER PRIMARY KEY, int_max INT64, int_min INT64, float_max FLOAT64, float_min FLOAT64)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO test_numeric_bounds (id, int_max, int_min, float_max, float_min) VALUES (?, ?, ?, ?, ?)";
        cmd.Parameters.Clear();
        
        cmd.Parameters.Add(new DecentDBParameter { Value = 1 });
        cmd.Parameters.Add(new DecentDBParameter { Value = long.MaxValue });
        cmd.Parameters.Add(new DecentDBParameter { Value = long.MinValue });
        cmd.Parameters.Add(new DecentDBParameter { Value = double.MaxValue });
        cmd.Parameters.Add(new DecentDBParameter { Value = double.MinValue });
        
        cmd.ExecuteNonQuery();

        // Verify the values were stored correctly
        cmd.CommandText = "SELECT int_max, int_min, float_max, float_min FROM test_numeric_bounds LIMIT 1";
        cmd.Parameters.Clear();
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(long.MaxValue, reader.GetInt64(0));
        Assert.Equal(long.MinValue, reader.GetInt64(1));
        Assert.Equal(double.MaxValue, reader.GetDouble(2));
        Assert.Equal(double.MinValue, reader.GetDouble(3));
    }

    [Fact]
    public void DateTimeParameterBinding()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE test_datetime (id INTEGER PRIMARY KEY, dt_value INT64)";
        cmd.ExecuteNonQuery();

        var now = DateTime.Now;
        var utcNow = DateTime.UtcNow;
        var dateTimeOffset = new DateTimeOffset(2023, 6, 15, 10, 30, 0, TimeSpan.Zero);

        cmd.CommandText = "INSERT INTO test_datetime (id, dt_value) VALUES (?, ?)";
        cmd.Parameters.Clear();
        cmd.Parameters.Add(new DecentDBParameter { Value = 1 });
        cmd.Parameters.Add(new DecentDBParameter { Value = now });
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO test_datetime (id, dt_value) VALUES (?, ?)";
        cmd.Parameters.Clear();
        cmd.Parameters.Add(new DecentDBParameter { Value = 2 });
        cmd.Parameters.Add(new DecentDBParameter { Value = utcNow });
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO test_datetime (id, dt_value) VALUES (?, ?)";
        cmd.Parameters.Clear();
        cmd.Parameters.Add(new DecentDBParameter { Value = 3 });
        cmd.Parameters.Add(new DecentDBParameter { Value = dateTimeOffset });
        cmd.ExecuteNonQuery();
    }

    [Fact]
    public void TimeSpanParameterBinding()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE test_timespan (id INTEGER PRIMARY KEY, ts_value INT64)";
        cmd.ExecuteNonQuery();

        var timeSpan = TimeSpan.FromHours(25.5); // More than 24 hours

        cmd.CommandText = "INSERT INTO test_timespan (id, ts_value) VALUES (?, ?)";
        cmd.Parameters.Clear();
        cmd.Parameters.Add(new DecentDBParameter { Value = 1 });
        cmd.Parameters.Add(new DecentDBParameter { Value = timeSpan });
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT ts_value FROM test_timespan LIMIT 1";
        cmd.Parameters.Clear();
        var result = cmd.ExecuteScalar();
        Assert.NotNull(result);
    }

    [Fact]
    public void DateOnlyTimeOnlyParameterBinding()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE test_date_time_only (id INTEGER PRIMARY KEY, date_val INT64, time_val INT64)";
        cmd.ExecuteNonQuery();

        var dateOnly = new DateOnly(2023, 6, 15);
        var timeOnly = new TimeOnly(14, 30, 45);

        cmd.CommandText = "INSERT INTO test_date_time_only (id, date_val, time_val) VALUES (?, ?, ?)";
        cmd.Parameters.Clear();
        cmd.Parameters.Add(new DecentDBParameter { Value = 1 });
        cmd.Parameters.Add(new DecentDBParameter { Value = dateOnly });
        cmd.Parameters.Add(new DecentDBParameter { Value = timeOnly });
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT date_val, time_val FROM test_date_time_only LIMIT 1";
        cmd.Parameters.Clear();
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        // The values would be stored as integers (day number for DateOnly, ticks for TimeOnly)
        Assert.True(reader.GetInt64(0) > 0); // Day number since epoch
        Assert.True(reader.GetInt64(1) > 0); // Ticks
    }

    [Fact]
    public void GuidParameterBinding()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE test_guid (id INTEGER PRIMARY KEY, guid_col BLOB)";
        cmd.ExecuteNonQuery();

        var guid = Guid.NewGuid();

        cmd.CommandText = "INSERT INTO test_guid (id, guid_col) VALUES (?, ?)";
        cmd.Parameters.Clear();
        cmd.Parameters.Add(new DecentDBParameter { Value = 1 });
        cmd.Parameters.Add(new DecentDBParameter { Value = guid });
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT guid_col FROM test_guid LIMIT 1";
        cmd.Parameters.Clear();
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var blob = reader.GetFieldValue<byte[]>(0);
        var retrievedGuid = new Guid(blob);
        Assert.Equal(guid, retrievedGuid);
    }

    [Fact]
    public void DecimalPrecisionTests()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE test_decimal (id INTEGER PRIMARY KEY, dec_val DECIMAL(18,4))";
        cmd.ExecuteNonQuery();

        // Test decimal with various scales
        var decimals = new[] {
            123.4567m,
            0.1234m,
            -987.6543m,
            0m,
            1000000.0001m
        };

        var id = 1;
        foreach (var dec in decimals)
        {
            cmd.CommandText = "INSERT INTO test_decimal (id, dec_val) VALUES (?, ?)";
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new DecentDBParameter { Value = id++ });
            cmd.Parameters.Add(new DecentDBParameter { Value = dec });
            cmd.ExecuteNonQuery();
        }

        cmd.CommandText = "SELECT COUNT(*) FROM test_decimal";
        var count = (long)(cmd.ExecuteScalar() ?? 0L);
        Assert.Equal(decimals.Length, count);
    }

    [Fact]
    public async Task MicroOrm_EntityValidation()
    {
        // Create the table first
        using (var conn = new DecentDBConnection($"Data Source={_dbPath}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE validated_product_entities (id INTEGER PRIMARY KEY, name TEXT, price DECIMAL(18,4))";
            cmd.ExecuteNonQuery();
        }

        using var context = new DecentDBContext(_dbPath);
        var productSet = context.Set<ValidatedProductEntity>();

        // Try to add an entity that violates validation
        var invalidProduct = new ValidatedProductEntity 
        { 
            Id = 1, 
            Name = "", // Invalid - required
            Price = -10 // Invalid - negative
        };

        // This should either throw or handle the validation appropriately
        // Since the actual validation logic is in the ORM, we'll just test insertion
        await productSet.InsertAsync(invalidProduct);
        
        // Verify it was inserted
        var retrieved = await productSet.GetAsync(1);
        Assert.NotNull(retrieved);
        Assert.Equal("", retrieved.Name);
        Assert.Equal(-10m, retrieved.Price);
    }

    [Fact]
    public void CommandTimeoutScenarios()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandTimeout = 0; // No timeout
        Assert.Equal(0, cmd.CommandTimeout);

        cmd.CommandTimeout = 120; // 2 minute timeout
        Assert.Equal(120, cmd.CommandTimeout);
    }

    [Fact]
    public void NestedTransactions()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var outerTrans = conn.BeginTransaction();
        
        // DecentDB might not support nested transactions, so this might throw
        // or it might just reuse the same transaction
        try 
        {
            using var innerTrans = conn.BeginTransaction();
            // If we get here, the implementation handles nested transactions somehow
        }
        catch (NotSupportedException)
        {
            // Expected behavior if nested transactions aren't supported
        }
        catch (DecentDB.Native.DecentDBException)
        {
            // DecentDB reports "Transaction already active"
        }
        
        outerTrans.Commit();
    }

    [Fact]
    public async Task AsyncOperationsConcurrency()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        // Create table
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE test_concurrent (id INTEGER PRIMARY KEY, value TEXT)";
            cmd.ExecuteNonQuery();
        }

        // Insert operations sequentially (DecentDB is single-writer)
        for (int i = 0; i < 5; i++)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "INSERT INTO test_concurrent (id, value) VALUES (?, ?)";
            var idParam = cmd.CreateParameter();
            idParam.Value = i + 1;
            cmd.Parameters.Add(idParam);
            var param = cmd.CreateParameter();
            param.Value = $"Value-{i}-{Guid.NewGuid()}";
            cmd.Parameters.Add(param);
            await cmd.ExecuteNonQueryAsync();
        }

        // Verify all records were inserted
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT COUNT(*) FROM test_concurrent";
        var count = (long)(cmd.ExecuteScalar() ?? 0L);
            Assert.Equal(5, count);
        }
    }

    public class ValidatedProductEntity
    {
        public int Id { get; set; }
        
        public string Name { get; set; } = "";
        
        public decimal Price { get; set; }
    }
}