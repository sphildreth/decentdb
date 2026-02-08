using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using DecentDB.AdoNet;

namespace DecentDB.Tests;

public class AdoNetLayerTests : IDisposable
{
    private readonly string _dbPath;

    public AdoNetLayerTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_adonet_{Guid.NewGuid():N}.ddb");
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
    public void DecentDBConnection_DefaultConstructor_CreatesInstance()
    {
        using var conn = new DecentDBConnection();
        Assert.NotNull(conn);
        Assert.Equal(ConnectionState.Closed, conn.State);
        Assert.Empty(conn.ConnectionString);
    }

    [Fact]
    public void DecentDBConnection_ConnectionString_SetterGetter()
    {
        using var conn = new DecentDBConnection();
        var connectionString = $"Data Source={_dbPath}";
        
        conn.ConnectionString = connectionString;
        Assert.Equal(connectionString, conn.ConnectionString);
        Assert.Equal(_dbPath, conn.DataSource);
    }

    [Fact]
    public void DecentDBConnection_ConnectionString_WhileOpen_Throws()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        
        Assert.Throws<InvalidOperationException>(() => conn.ConnectionString = "Data Source=test.db");
    }

    [Fact]
    public void DecentDBConnection_Database_Property()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        Assert.Equal(Path.GetFileNameWithoutExtension(_dbPath), conn.Database);
    }

    [Fact]
    public void DecentDBConnection_ServerVersion_Property()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        Assert.Equal("1.0.0", conn.ServerVersion);
    }

    [Fact]
    public void DecentDBConnection_ChangeDatabase_NotSupported()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        Assert.Throws<NotSupportedException>(() => conn.ChangeDatabase("other_db"));
    }

    [Fact]
    public void DecentDBConnection_Open_Close_Sequence()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        Assert.Equal(ConnectionState.Closed, conn.State);
        
        conn.Open();
        Assert.Equal(ConnectionState.Open, conn.State);
        
        conn.Close();
        Assert.Equal(ConnectionState.Closed, conn.State);
    }

    [Fact]
    public void DecentDBConnection_BeginTransaction_IsolationLevels()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        
        // Test supported isolation levels (one at a time — DecentDB is single-transaction)
        using (var trans1 = conn.BeginTransaction(IsolationLevel.Snapshot))
        {
            trans1.Commit();
        }
        using (var trans2 = conn.BeginTransaction(IsolationLevel.ReadCommitted))
        {
            trans2.Commit();
        }
        using (var trans3 = conn.BeginTransaction(IsolationLevel.ReadUncommitted))
        {
            trans3.Commit();
        }
        
        // Unsupported isolation level should default to Snapshot
        using (var trans4 = conn.BeginTransaction(IsolationLevel.Serializable))
        {
            trans4.Commit();
        }
    }

    [Fact]
    public void DecentDBConnection_BeginTransaction_WhenClosed_Throws()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        Assert.Throws<InvalidOperationException>(() => conn.BeginTransaction());
    }

    [Fact]
    public void DecentDBConnection_Checkpoint_WhenOpen()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        
        // This should not throw
        conn.Checkpoint();
    }

    [Fact]
    public void DecentDBConnection_Checkpoint_WhenClosed_Throws()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        Assert.Throws<InvalidOperationException>(() => conn.Checkpoint());
    }

    [Fact]
    public void DecentDBConnection_ParseConnectionString_WithVariousOptions()
    {
        var connStr = $"Data Source={_dbPath};Cache Size=64MB;Logging=1;LogLevel=Info;Command Timeout=60";
        using var conn = new DecentDBConnection(connStr);
        
        Assert.Equal(_dbPath, conn.DataSource);
        // Note: DefaultCommandTimeoutSeconds is internal, so we can't test it directly
    }

    [Fact]
    public void DecentDBConnection_ParseConnectionString_WithDifferentDataSourceKeys()
    {
        // Test different variations of data source key
        using var conn1 = new DecentDBConnection($"Filename={_dbPath}");
        using var conn2 = new DecentDBConnection($"Database={_dbPath}");
        
        Assert.Equal(_dbPath, conn1.DataSource);
        Assert.Equal(_dbPath, conn2.DataSource);
    }

    [Fact]
    public void DecentDBCommand_DefaultConstructor_CreatesInstance()
    {
        using var cmd = new DecentDBCommand();
        Assert.NotNull(cmd);
        Assert.Equal(CommandType.Text, cmd.CommandType);
        Assert.Equal(30, cmd.CommandTimeout);
    }

    [Fact]
    public void DecentDBCommand_ConstructorWithConnection()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        using var cmd = new DecentDBCommand(conn);
        
        Assert.NotNull(cmd);
        Assert.Same(conn, cmd.Connection);
    }

    [Fact]
    public void DecentDBCommand_ConstructorWithConnectionAndCommandText()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        using var cmd = new DecentDBCommand(conn, "SELECT 1");
        
        Assert.NotNull(cmd);
        Assert.Same(conn, cmd.Connection);
        Assert.Equal("SELECT 1", cmd.CommandText);
    }

    [Fact]
    public void DecentDBCommand_CommandType_SetterGetter()
    {
        using var cmd = new DecentDBCommand();
        Assert.Equal(CommandType.Text, cmd.CommandType);
        
        // Setting to Text should work
        cmd.CommandType = CommandType.Text;
        Assert.Equal(CommandType.Text, cmd.CommandType);
        
        // Setting to non-Text should throw
        Assert.Throws<NotSupportedException>(() => cmd.CommandType = CommandType.StoredProcedure);
        Assert.Throws<NotSupportedException>(() => cmd.CommandType = CommandType.TableDirect);
    }

    [Fact]
    public void DecentDBCommand_CommandTimeout_SetterGetter()
    {
        using var cmd = new DecentDBCommand();
        
        cmd.CommandTimeout = 60;
        Assert.Equal(60, cmd.CommandTimeout);
        
        // Negative timeout should throw
        Assert.Throws<ArgumentException>(() => cmd.CommandTimeout = -1);
    }

    [Fact]
    public void DecentDBCommand_CommandText_SetterGetter()
    {
        using var cmd = new DecentDBCommand();
        cmd.CommandText = "SELECT 1";
        Assert.Equal("SELECT 1", cmd.CommandText);
    }

    [Fact]
    public void DecentDBCommand_DbConnection_SetterGetter()
    {
        using var conn1 = new DecentDBConnection($"Data Source={_dbPath}1");
        using var conn2 = new DecentDBConnection($"Data Source={_dbPath}2");
        using var cmd = new DecentDBCommand();
        
        cmd.Connection = conn1;
        Assert.Same(conn1, cmd.Connection);
        
        cmd.Connection = conn2;
        Assert.Same(conn2, cmd.Connection);
        
        // Setting to non-DecentDBConnection should throw
        using var fakeConn = new FakeDbConnection();
        Assert.Throws<ArgumentException>(() => cmd.Connection = fakeConn);
    }

    [Fact]
    public void DecentDBCommand_DbTransaction_SetterGetter()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        using var trans = conn.BeginTransaction();
        using var cmd = new DecentDBCommand(conn);
        
        cmd.Transaction = trans;
        Assert.Same(trans, cmd.Transaction);
    }

    [Fact]
    public void DecentDBCommand_Cancel_WhenNotExecuting()
    {
        using var cmd = new DecentDBCommand();
        // Should not throw when no statement is executing
        cmd.Cancel();
    }

    [Fact]
    public void DecentDBCommand_ExecuteNonQuery_WhenConnectionIsNull()
    {
        using var cmd = new DecentDBCommand();
        cmd.CommandText = "SELECT 1";
        
        Assert.Throws<InvalidOperationException>(() => cmd.ExecuteNonQuery());
    }

    [Fact]
    public void DecentDBCommand_ExecuteScalar_WhenConnectionIsNull()
    {
        using var cmd = new DecentDBCommand();
        cmd.CommandText = "SELECT 1";
        
        Assert.Throws<InvalidOperationException>(() => cmd.ExecuteScalar());
    }

    [Fact]
    public void DecentDBCommand_ExecuteReader_WhenConnectionIsNull()
    {
        using var cmd = new DecentDBCommand();
        cmd.CommandText = "SELECT 1";
        
        Assert.Throws<InvalidOperationException>(() => cmd.ExecuteReader());
    }

    [Fact]
    public void DecentDBCommand_Prepare_WhenConnectionClosed_Throws()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        using var cmd = new DecentDBCommand(conn);
        cmd.CommandText = "SELECT 1";
        
        Assert.Throws<InvalidOperationException>(() => cmd.Prepare());
    }

    [Fact]
    public void DecentDBCommand_CreateParameter_CreatesInstance()
    {
        using var cmd = new DecentDBCommand();
        var param = cmd.CreateParameter();
        
        Assert.NotNull(param);
        Assert.IsType<DecentDBParameter>(param);
    }

    [Fact]
    public void DecentDBParameter_Properties()
    {
        var param = new DecentDBParameter();
        
        // Test default values
        Assert.Equal(ParameterDirection.Input, param.Direction);
        Assert.False(param.IsNullable);
        Assert.Equal("", param.ParameterName);
        Assert.Equal(0, param.Precision);
        Assert.Equal(0, param.Scale);
        Assert.Equal(0, param.Size);
        Assert.Equal(DBNull.Value, param.Value);
        
        // Test setting values
        param.Direction = ParameterDirection.InputOutput;
        param.IsNullable = true;
        param.ParameterName = "testParam";
        param.Precision = 10;
        param.Scale = 2;
        param.Size = 100;
        param.Value = 42;
        
        Assert.Equal(ParameterDirection.InputOutput, param.Direction);
        Assert.True(param.IsNullable);
        Assert.Equal("testParam", param.ParameterName);
        Assert.Equal(10, param.Precision);
        Assert.Equal(2, param.Scale);
        Assert.Equal(100, param.Size);
        Assert.Equal(42, param.Value);
    }

    [Fact]
    public void DecentDBParameter_Constructors()
    {
        // Default constructor
        var param1 = new DecentDBParameter();
        Assert.Equal(DBNull.Value, param1.Value);
        
        // Constructor with name and value
        var param2 = new DecentDBParameter("name", 42);
        Assert.Equal("name", param2.ParameterName);
        Assert.Equal(42, param2.Value);
        
        // Constructor with name, type, and size
        var param3 = new DecentDBParameter("name", DbType.String, 50);
        Assert.Equal("name", param3.ParameterName);
        Assert.Equal(DbType.String, param3.DbType);
        Assert.Equal(50, param3.Size);
        
        // The DecentDBParameter class doesn't have a constructor with 4 parameters
        // So removing this test
    }

    [Fact]
    public void DecentDBParameterCollection_UsageThroughCommand()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        
        using var cmd = conn.CreateCommand();
        var param1 = new DecentDBParameter("param1", 1);
        var param2 = new DecentDBParameter("param2", 2);
        
        // Add parameters through command's parameter collection
        cmd.Parameters.Add(param1);
        cmd.Parameters.Add(param2);
        
        Assert.Equal(2, cmd.Parameters.Count);
        Assert.Same(param1, cmd.Parameters[0]);
        Assert.Same(param2, cmd.Parameters[1]);
        Assert.Same(param1, cmd.Parameters["param1"]);
        Assert.Same(param2, cmd.Parameters["param2"]);
    }

    [Fact]
    public void DecentDBTransaction_Dispose_MultipleTimes()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        var trans = conn.BeginTransaction();
        
        trans.Dispose(); // First dispose
        trans.Dispose(); // Second dispose - should not throw
    }

    [Fact]
    public void DecentDBTransaction_Commit_ThenDispose()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        
        using var trans = conn.BeginTransaction();
        trans.Commit();
        // Disposing after commit should not throw
    }

    [Fact]
    public void DecentDBTransaction_Rollback_ThenDispose()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        
        using var trans = conn.BeginTransaction();
        trans.Rollback();
        // Disposing after rollback should not throw
    }

    [Fact]
    public async Task DecentDBCommand_ExecuteNonQueryAsync()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        
        using var cmd = conn.CreateCommand() as DecentDBCommand;
        Assert.NotNull(cmd);
        cmd.CommandText = "CREATE TABLE IF NOT EXISTS test_async (id INTEGER PRIMARY KEY, name TEXT)";
        
        var result = await cmd.ExecuteNonQueryAsync(CancellationToken.None);
        Assert.True(result >= 0);
    }

    [Fact]
    public async Task DecentDBCommand_ExecuteScalarAsync()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        
        using var cmd = conn.CreateCommand() as DecentDBCommand;
        Assert.NotNull(cmd);
        cmd.CommandText = "SELECT 42";
        
        var result = await cmd.ExecuteScalarAsync(CancellationToken.None);
        Assert.Equal(42L, result);
    }
    
    // Helper class for testing
    private class FakeDbConnection : DbConnection
    {
        [AllowNull]
        public override string ConnectionString { get; set; } = "";
        public override string Database => "FakeDb";
        public override string DataSource => "FakeSource";
        public override string ServerVersion => "1.0.0";
        public override ConnectionState State => ConnectionState.Closed;

        public override void ChangeDatabase(string databaseName) { }
        public override void Close() { }
        public override void Open() { }
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => null!;
        protected override DbCommand CreateDbCommand() => null!;
    }
}