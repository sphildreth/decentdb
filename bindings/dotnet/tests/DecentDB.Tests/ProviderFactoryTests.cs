using System.Data;
using System.Data.Common;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

public sealed class ProviderFactoryTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_factory_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
        if (File.Exists(_dbPath + "-wal"))
            File.Delete(_dbPath + "-wal");
    }

    [Fact]
    public void Instance_IsSingleton()
    {
        var a = DecentDBFactory.Instance;
        var b = DecentDBFactory.Instance;
        Assert.Same(a, b);
    }

    [Fact]
    public void CreateConnection_ReturnsDecentDBConnection()
    {
        using var conn = DecentDBFactory.Instance.CreateConnection();
        Assert.IsType<DecentDBConnection>(conn);
    }

    [Fact]
    public void CreateCommand_ReturnsDecentDBCommand()
    {
        using var cmd = DecentDBFactory.Instance.CreateCommand();
        Assert.IsType<DecentDBCommand>(cmd);
    }

    [Fact]
    public void CreateParameter_ReturnsDecentDBParameter()
    {
        var param = DecentDBFactory.Instance.CreateParameter();
        Assert.IsType<DecentDBParameter>(param);
    }

    [Fact]
    public void CreateConnectionStringBuilder_ReturnsDecentDBConnectionStringBuilder()
    {
        var builder = DecentDBFactory.Instance.CreateConnectionStringBuilder();
        Assert.IsType<DecentDBConnectionStringBuilder>(builder);
    }

    [Fact]
    public void CanCreateDataSourceEnumerator_IsFalse()
    {
        Assert.False(DecentDBFactory.Instance.CanCreateDataSourceEnumerator);
    }

    [Fact]
    public void CreateConnection_CanOpenAndQuery()
    {
        using var conn = DecentDBFactory.Instance.CreateConnection();
        conn.ConnectionString = $"Data Source={_dbPath}";
        conn.Open();

        Assert.Equal(ConnectionState.Open, conn.State);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE factory_test (id INTEGER PRIMARY KEY, name TEXT)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO factory_test (id, name) VALUES (1, 'factory')";
        Assert.Equal(1, cmd.ExecuteNonQuery());
    }

    [Fact]
    public void CreateCommand_WithFactoryConnection_ExecutesQuery()
    {
        using var conn = DecentDBFactory.Instance.CreateConnection();
        conn.ConnectionString = $"Data Source={_dbPath}";
        conn.Open();

        using var cmd = DecentDBFactory.Instance.CreateCommand();
        cmd.Connection = conn;
        cmd.CommandText = "SELECT 42";
        Assert.Equal(42L, cmd.ExecuteScalar());
    }

    [Fact]
    public void CreateParameter_WithFactoryCommand_BindsCorrectly()
    {
        using var conn = DecentDBFactory.Instance.CreateConnection();
        conn.ConnectionString = $"Data Source={_dbPath}";
        conn.Open();

        using var cmd = DecentDBFactory.Instance.CreateCommand();
        cmd.Connection = conn;
        cmd.CommandText = "CREATE TABLE param_factory (id INTEGER PRIMARY KEY, val TEXT)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO param_factory (id, val) VALUES (@id, @val)";

        var idParam = DecentDBFactory.Instance.CreateParameter();
        idParam.ParameterName = "@id";
        idParam.Value = 1;
        cmd.Parameters.Add(idParam);

        var valParam = DecentDBFactory.Instance.CreateParameter();
        valParam.ParameterName = "@val";
        valParam.Value = "hello";
        cmd.Parameters.Add(valParam);

        Assert.Equal(1, cmd.ExecuteNonQuery());
    }
}
