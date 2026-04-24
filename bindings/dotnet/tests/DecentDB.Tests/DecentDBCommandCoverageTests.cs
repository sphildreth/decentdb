using System.Data;
using System.Data.Common;
using System.Reflection;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

public sealed class DecentDBCommandCoverageTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_cmd_cov_{Guid.NewGuid():N}.ddb");

    [Fact]
    public void ExecuteNonQuery_OnSelectStatement_DrainsRows()
    {
        using var connection = OpenConnection();
        Execute(connection, "CREATE TABLE cmd_cov_rows (id INTEGER PRIMARY KEY); INSERT INTO cmd_cov_rows (id) VALUES (1), (2), (3);");

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT id FROM cmd_cov_rows ORDER BY id";

        var rows = command.ExecuteNonQuery();
        Assert.Equal(3, rows);
    }

    [Fact]
    public void ActiveExecution_BlocksMutation_AndCancelResetsStatement()
    {
        using var connection = OpenConnection();
        Execute(connection, "CREATE TABLE cmd_cov_active (id INTEGER PRIMARY KEY); INSERT INTO cmd_cov_active (id) VALUES (1), (2);");

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT id FROM cmd_cov_active ORDER BY id";

        using var reader = command.ExecuteReader();
        Assert.Throws<InvalidOperationException>(() => command.CommandText = "SELECT 1");
        Assert.Throws<InvalidOperationException>(() => command.Connection = null);

        using var tx = connection.BeginTransaction();
        Assert.Throws<InvalidOperationException>(() => ((DbCommand)command).Transaction = tx);
        tx.Rollback();

        command.Cancel();
    }

    [Fact]
    public void Prepare_MultiStatementIsIgnored_AndOffsetParameterIsClamped()
    {
        using var connection = OpenConnection();
        Execute(connection, "CREATE TABLE cmd_cov_offset (id INTEGER PRIMARY KEY); INSERT INTO cmd_cov_offset (id) VALUES (1), (2);");

        using var multi = connection.CreateCommand();
        multi.CommandText = "SELECT 1; SELECT 2;";
        multi.Prepare();

        using var offset = connection.CreateCommand();
        offset.CommandText = "SELECT id FROM cmd_cov_offset ORDER BY id OFFSET @off";
        var p = offset.CreateParameter();
        p.ParameterName = "@off";
        p.Value = -5L;
        offset.Parameters.Add(p);

        offset.Prepare();
        Assert.Equal(0L, p.Value);
    }

    [Fact]
    public void PreparedNonQuery_RetriesOnceAfterSchemaChange()
    {
        using var connection = OpenConnection();
        Execute(connection, "CREATE TABLE cmd_cov_retry_write (id INTEGER PRIMARY KEY)");

        using var insert = connection.CreateCommand();
        insert.CommandText = "INSERT INTO cmd_cov_retry_write (id) VALUES (@id)";
        insert.Parameters.Add(new DecentDBParameter("@id", 1));
        insert.Prepare();

        Execute(connection, "CREATE TABLE cmd_cov_retry_write_schema_bump (id INTEGER PRIMARY KEY)");

        Assert.Equal(1, insert.ExecuteNonQuery());

        using var verify = connection.CreateCommand();
        verify.CommandText = "SELECT COUNT(*) FROM cmd_cov_retry_write";
        Assert.Equal(1L, verify.ExecuteScalar());
    }

    [Fact]
    public void PreparedReader_RetriesOnceAfterSchemaChange()
    {
        using var connection = OpenConnection();
        Execute(connection, "CREATE TABLE cmd_cov_retry_read (id INTEGER PRIMARY KEY); INSERT INTO cmd_cov_retry_read (id) VALUES (41)");

        using var select = connection.CreateCommand();
        select.CommandText = "SELECT id + 1 FROM cmd_cov_retry_read";
        select.Prepare();

        Execute(connection, "CREATE TABLE cmd_cov_retry_read_schema_bump (id INTEGER PRIMARY KEY)");

        Assert.Equal(42L, select.ExecuteScalar());
    }

    [Fact]
    public void TransactionalSchemaChange_AllowsFollowupInsertOnSameConnection()
    {
        using var connection = OpenConnection();
        using var transaction = connection.BeginTransaction();

        Execute(connection, "CREATE TABLE cmd_cov_tx_schema (id INTEGER PRIMARY KEY)");

        using var insert = connection.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText = "INSERT INTO cmd_cov_tx_schema (id) VALUES (1)";
        Assert.Equal(1, insert.ExecuteNonQuery());

        transaction.Commit();
    }

    [Fact]
    public void BindParameter_GuidDbTypeVariants_AreHandled()
    {
        using var connection = OpenConnection();
        Execute(connection, "CREATE TABLE cmd_cov_guid (id INTEGER PRIMARY KEY, g UUID)");

        var guid = Guid.NewGuid();

        using var insert = connection.CreateCommand();
        insert.CommandText = "INSERT INTO cmd_cov_guid (id, g) VALUES (@id, @g)";

        insert.Parameters.Add(new DecentDBParameter("@id", 1));
        insert.Parameters.Add(new DecentDBParameter("@g", guid) { DbType = DbType.Guid });
        Assert.Equal(1, insert.ExecuteNonQuery());

        insert.Parameters.Clear();
        insert.Parameters.Add(new DecentDBParameter("@id", 2));
        insert.Parameters.Add(new DecentDBParameter("@g", guid.ToString()) { DbType = DbType.Guid });
        Assert.Equal(1, insert.ExecuteNonQuery());

        insert.Parameters.Clear();
        insert.Parameters.Add(new DecentDBParameter("@id", 3));
        insert.Parameters.Add(new DecentDBParameter("@g", new byte[] { 1, 2, 3 }) { DbType = DbType.Guid });
        Assert.Throws<ArgumentException>(() => insert.ExecuteNonQuery());

        insert.Parameters.Clear();
        insert.Parameters.Add(new DecentDBParameter("@id", 4));
        insert.Parameters.Add(new DecentDBParameter("@g", 123) { DbType = DbType.Guid });
        Assert.Throws<ArgumentException>(() => insert.ExecuteNonQuery());
    }

    [Fact]
    public void BindParameter_CoversPrimitiveBranches_AndUnsupportedType()
    {
        using var connection = OpenConnection();
        Execute(connection, "CREATE TABLE cmd_cov_primitives (id INTEGER PRIMARY KEY, s INTEGER, b INTEGER, f REAL)");

        using var insert = connection.CreateCommand();
        insert.CommandText = "INSERT INTO cmd_cov_primitives (id, s, b, f) VALUES (@id, @s, @b, @f)";
        insert.Parameters.Add(new DecentDBParameter("@id", 1));
        insert.Parameters.Add(new DecentDBParameter("@s", (short)12));
        insert.Parameters.Add(new DecentDBParameter("@b", (byte)7));
        insert.Parameters.Add(new DecentDBParameter("@f", 3.25f));
        Assert.Equal(1, insert.ExecuteNonQuery());

        using var verify = connection.CreateCommand();
        verify.CommandText = "SELECT s, b, f FROM cmd_cov_primitives WHERE id = 1";
        using var reader = verify.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(12, reader.GetInt32(0));
        Assert.Equal(7, reader.GetInt32(1));
        Assert.Equal(3.25f, reader.GetFloat(2));

        insert.Parameters.Clear();
        insert.Parameters.Add(new DecentDBParameter("@id", 2));
        insert.Parameters.Add(new DecentDBParameter("@s", new Version(1, 2)));
        insert.Parameters.Add(new DecentDBParameter("@b", 1));
        insert.Parameters.Add(new DecentDBParameter("@f", 1.0));
        Assert.Throws<NotSupportedException>(() => insert.ExecuteNonQuery());
    }

    [Fact]
    public void MultiStatementReader_PathThrowsSingleStatementError()
    {
        using var connection = OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1; SELECT 2;";

        Assert.Throws<DecentDB.Native.DecentDBException>(() => command.ExecuteReader());
    }

    [Fact]
    public void Cancel_DisposesNonPreparedActiveStatement()
    {
        using var connection = OpenConnection();
        using var command = (DecentDBCommand)connection.CreateCommand();

        var getNativeDb = typeof(DecentDBConnection).GetMethod("GetNativeDb", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(getNativeDb);
        var nativeDb = (DecentDB.Native.DecentDB)getNativeDb!.Invoke(connection, null)!;
        var nativeStatement = nativeDb.Prepare("SELECT 1");

        var statementField = typeof(DecentDBCommand).GetField("_statement", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(statementField);
        statementField!.SetValue(command, nativeStatement);

        command.Cancel();
        Assert.Null(statementField.GetValue(command));
    }

    [Fact]
    public void CommandMetadataProperties_AndConnectionClearing_AreCovered()
    {
        using var connection = OpenConnection();
        using var command = new DecentDBCommand(connection)
        {
            DesignTimeVisible = true,
            UpdatedRowSource = UpdateRowSource.FirstReturnedRecord
        };

        Assert.True(command.DesignTimeVisible);
        Assert.Equal(UpdateRowSource.FirstReturnedRecord, command.UpdatedRowSource);

        ((DbCommand)command).Connection = null;
        Assert.Null(command.Connection);
    }

    [Fact]
    public void InternalStatementHelpers_CoverNullConnectionAndNonPreparedFinalizePath()
    {
        using var commandWithoutConnection = new DecentDBCommand();
        var ensurePrepared = typeof(DecentDBCommand)
            .GetMethod("EnsurePreparedStatement", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(ensurePrepared);

        var nullConnectionEx = Assert.Throws<TargetInvocationException>(() =>
            ensurePrepared!.Invoke(commandWithoutConnection, ["SELECT 1", true]));
        Assert.IsType<InvalidOperationException>(nullConnectionEx.InnerException);

        using var connection = OpenConnection();
        using var command = (DecentDBCommand)connection.CreateCommand();

        var getNativeDb = typeof(DecentDBConnection).GetMethod("GetNativeDb", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(getNativeDb);
        var nativeDb = (DecentDB.Native.DecentDB)getNativeDb!.Invoke(connection, null)!;
        var nativeStatement = nativeDb.Prepare("SELECT 1");

        var statementField = typeof(DecentDBCommand).GetField("_statement", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(statementField);
        statementField!.SetValue(command, nativeStatement);

        var finalizeStatement = typeof(DecentDBCommand).GetMethod("FinalizeStatement", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(finalizeStatement);
        finalizeStatement!.Invoke(command, null);
        Assert.Null(statementField.GetValue(command));
    }

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    private DecentDBConnection OpenConnection()
    {
        var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();
        return connection;
    }

    private static void Execute(DecentDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }
}
