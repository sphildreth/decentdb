using System.Data;
using DecentDB.AdoNet;
using Xunit;
using DecentDB.MicroOrm;

namespace DecentDB.Tests;

public sealed class ConnectionStringBuilderTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_csbuilder_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
    }

    [Fact]
    public void DefaultConstructor_InitializesEmpty()
    {
        var builder = new DecentDBConnectionStringBuilder();
        Assert.Empty(builder.ConnectionString);
        Assert.Empty(builder.DataSource);
        Assert.Null(builder.PerformanceProfile);
        Assert.Null(builder.CacheSize);
        Assert.Null(builder.ProcessCoordination);
        Assert.Null(builder.ProcessCoordinationTimeoutMs);
        Assert.False(builder.Logging);
        Assert.Null(builder.LogLevel);
        Assert.Equal(30, builder.CommandTimeout);
    }

    [Fact]
    public void Constructor_WithConnectionString_ParsesValues()
    {
        var builder = new DecentDBConnectionStringBuilder($"Data Source={_dbPath};Performance Profile=embedded_fast;Cache Size=64MB;Process Coordination=required;Process Coordination Timeout Ms=250;Logging=True;LogLevel=Info;Command Timeout=60");

        Assert.Equal(_dbPath, builder.DataSource);
        Assert.Equal("embedded_fast", builder.PerformanceProfile);
        Assert.Equal("64MB", builder.CacheSize);
        Assert.Equal("required", builder.ProcessCoordination);
        Assert.Equal(250, builder.ProcessCoordinationTimeoutMs);
        Assert.True(builder.Logging);
        Assert.Equal("Info", builder.LogLevel);
        Assert.Equal(60, builder.CommandTimeout);
    }

    [Fact]
    public void PerformanceProfile_SetAndGet_RoundTrips()
    {
        var builder = new DecentDBConnectionStringBuilder();
        builder.PerformanceProfile = "embedded_fast";
        Assert.Equal("embedded_fast", builder.PerformanceProfile);

        builder.PerformanceProfile = null;
        Assert.Null(builder.PerformanceProfile);
        Assert.DoesNotContain("Performance Profile", builder.ConnectionString);
    }

    [Fact]
    public void PerformanceProfile_ProfileAlias_Parses()
    {
        var builder = new DecentDBConnectionStringBuilder($"Data Source={_dbPath};Profile=tuned_durable");
        Assert.Equal("tuned_durable", builder.PerformanceProfile);
    }

    [Fact]
    public void DataSource_SetAndGet_RoundTrips()
    {
        var builder = new DecentDBConnectionStringBuilder();
        builder.DataSource = _dbPath;
        Assert.Equal(_dbPath, builder.DataSource);
        Assert.Contains($"Data Source={_dbPath}", builder.ConnectionString);
    }

    [Fact]
    public void CacheSize_SetAndGet_RoundTrips()
    {
        var builder = new DecentDBConnectionStringBuilder();
        builder.CacheSize = "32MB";
        Assert.Equal("32MB", builder.CacheSize);

        builder.CacheSize = null;
        Assert.Null(builder.CacheSize);
        Assert.DoesNotContain("Cache Size", builder.ConnectionString);
    }

    [Fact]
    public void Logging_SetAndGet_RoundTrips()
    {
        var builder = new DecentDBConnectionStringBuilder();
        builder.Logging = true;
        Assert.True(builder.Logging);

        builder.Logging = false;
        Assert.False(builder.Logging);
    }

    [Fact]
    public void ProcessCoordination_SetAndGet_RoundTrips()
    {
        var builder = new DecentDBConnectionStringBuilder
        {
            ProcessCoordination = "required",
            ProcessCoordinationTimeoutMs = 250
        };

        Assert.Equal("required", builder.ProcessCoordination);
        Assert.Equal(250, builder.ProcessCoordinationTimeoutMs);

        builder.ProcessCoordination = null;
        builder.ProcessCoordinationTimeoutMs = null;
        Assert.Null(builder.ProcessCoordination);
        Assert.Null(builder.ProcessCoordinationTimeoutMs);
    }

    [Fact]
    public void LogLevel_SetAndGet_RoundTrips()
    {
        var builder = new DecentDBConnectionStringBuilder();
        builder.LogLevel = "Debug";
        Assert.Equal("Debug", builder.LogLevel);

        builder.LogLevel = null;
        Assert.Null(builder.LogLevel);
    }

    [Fact]
    public void CommandTimeout_SetAndGet_RoundTrips()
    {
        var builder = new DecentDBConnectionStringBuilder();
        builder.CommandTimeout = 120;
        Assert.Equal(120, builder.CommandTimeout);
    }

    [Fact]
    public void CommandTimeout_NegativeValue_ThrowsArgumentOutOfRange()
    {
        var builder = new DecentDBConnectionStringBuilder();
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.CommandTimeout = -1);
    }

    [Fact]
    public void ConnectionString_RebuiltFromProperties_MatchesExpected()
    {
        var builder = new DecentDBConnectionStringBuilder
        {
            DataSource = _dbPath,
            PerformanceProfile = "embedded_fast",
            CacheSize = "128MB",
            ProcessCoordination = "single_process_unsafe",
            ProcessCoordinationTimeoutMs = 125,
            Logging = true,
            LogLevel = "Error",
            CommandTimeout = 15
        };

        var rebuilt = new DecentDBConnectionStringBuilder(builder.ConnectionString);
        Assert.Equal(_dbPath, rebuilt.DataSource);
        Assert.Equal("embedded_fast", rebuilt.PerformanceProfile);
        Assert.Equal("128MB", rebuilt.CacheSize);
        Assert.Equal("single_process_unsafe", rebuilt.ProcessCoordination);
        Assert.Equal(125, rebuilt.ProcessCoordinationTimeoutMs);
        Assert.True(rebuilt.Logging);
        Assert.Equal("Error", rebuilt.LogLevel);
        Assert.Equal(15, rebuilt.CommandTimeout);
    }

    [Fact]
    public void ConnectionString_UsedWithDecentDBConnection_OpensSuccessfully()
    {
        var builder = new DecentDBConnectionStringBuilder
        {
            DataSource = _dbPath,
            PerformanceProfile = "embedded_fast",
            CommandTimeout = 45
        };

        using var conn = new DecentDBConnection(builder.ConnectionString);
        conn.Open();
        Assert.Equal(ConnectionState.Open, conn.State);
    }

    [Fact]
    public void Pooling_DefaultsTrue()
    {
        var builder = new DecentDBConnectionStringBuilder
        {
            DataSource = _dbPath
        };
        Assert.True(builder.Pooling);
    }

    [Fact]
    public void Pooling_RoundTripsTrue()
    {
        var builder = new DecentDBConnectionStringBuilder();
        builder.Pooling = true;
        var cs = builder.ConnectionString;
        var parsed = new DecentDBConnectionStringBuilder(cs);
        Assert.True(parsed.Pooling);
    }

    [Fact]
    public void Pooling_AcceptsZeroAndOne()
    {
        var builder1 = new DecentDBConnectionStringBuilder($"Data Source={_dbPath};Pooling=0");
        Assert.False(builder1.Pooling);

        var builder2 = new DecentDBConnectionStringBuilder($"Data Source={_dbPath};Pooling=1");
        Assert.True(builder2.Pooling);
    }

    [Fact]
    public void Pooling_FalseFlowsToMicroOrm()
    {
        var builder = new DecentDBConnectionStringBuilder
        {
            DataSource = _dbPath,
            Pooling = false
        };

        using var ctx = new DecentDBContext(builder.ConnectionString);
        // MicroOrm's DecentDBContext has a private field `_pooling` that's set from the connection string.
        // We can't directly access it, so we infer by checking that the connection is not pooled.
        // The simplest observable: check that the context creates a fresh connection per operation if pooling=false.
        // Instead, we'll just verify construction doesn't throw; the MicroOrm code path reads Pooling via TryGetBoolOption.
        // For a stronger test, we can check the internal state via reflection.
        var field = typeof(DecentDBContext).GetField("_pooling", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        Assert.NotNull(field);
        var value = (bool)field.GetValue(ctx)!;
        Assert.False(value);
    }
}
