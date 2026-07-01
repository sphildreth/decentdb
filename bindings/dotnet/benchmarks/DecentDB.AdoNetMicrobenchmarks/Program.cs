using System.Data;
using System.Data.Common;
using System.Globalization;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Order;
using BenchmarkDotNet.Running;
using DecentDB.AdoNet;
using Microsoft.Data.Sqlite;

var config = ManualConfig
    .Create(DefaultConfig.Instance)
    .WithArtifactsPath(Path.Combine(".tmp", "adonet-microbenchmarks", "artifacts"));
BenchmarkSwitcher.FromAssembly(typeof(AdoNetHotPathBenchmarks).Assembly).Run(args, config);

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.Declared)]
public class AdoNetHotPathBenchmarks
{
    private const int SeedRows = 4096;
    private const string Payload = "payload";

    private string? _databasePath;
    private DbConnection? _connection;
    private DbTransaction? _transaction;

    private DbCommand? _insertCommand;
    private DbParameter? _insertId;
    private DbParameter? _insertValue;
    private DbParameter? _insertPayload;

    private DbCommand? _pointReadCommand;
    private DbParameter? _pointReadId;

    private DbCommand? _updateCommand;
    private DbParameter? _updateId;
    private DbParameter? _updateValue;

    private DbCommand? _readerCommand;
    private DbParameter? _readerId;

    private DbCommand? _executeNonQueryCommand;
    private DbParameter? _executeNonQueryId;
    private DbParameter? _executeNonQueryValue;

    private DbCommand? _executeNonQueryAsyncCommand;
    private DbParameter? _executeNonQueryAsyncId;
    private DbParameter? _executeNonQueryAsyncValue;

    private long _nextInsertId = SeedRows;
    private int _nextPointReadId = 1;
    private int _nextUpdateId = 1;
    private int _nextReaderId = 1;
    private int _nextExecuteNonQueryId = 1;
    private int _nextExecuteNonQueryAsyncId = 1;
    private long _nextUpdateValue;

    [Params(BenchmarkProvider.DecentDB, BenchmarkProvider.SQLite)]
    public BenchmarkProvider Provider { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _databasePath = CreateDatabasePath(Provider);
        DeleteDatabaseFiles(_databasePath);

        _connection = CreateConnection(Provider, _databasePath);
        _connection.Open();
        ConfigureConnection(_connection, Provider);
        ExecuteNonQuery(_connection, null, "CREATE TABLE hot (id INTEGER PRIMARY KEY, value INTEGER NOT NULL, payload TEXT NOT NULL)");
        ExecuteNonQuery(_connection, null, "CREATE TABLE insert_sink (id INTEGER PRIMARY KEY, value INTEGER NOT NULL, payload TEXT NOT NULL)");
        SeedHotTable(_connection);

        _transaction = _connection.BeginTransaction();
        CreatePreparedCommands(_connection, _transaction);
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        DisposeCommands();

        try
        {
            _transaction?.Rollback();
        }
        catch (InvalidOperationException)
        {
        }
        catch (DbException)
        {
        }

        _transaction?.Dispose();
        _connection?.Dispose();

        if (_databasePath != null)
        {
            DeleteDatabaseFiles(_databasePath);
        }
    }

    [Benchmark]
    public int PreparedOneRowInsert()
    {
        var id = ++_nextInsertId;
        _insertId!.Value = id;
        _insertValue!.Value = id;
        _insertPayload!.Value = Payload;
        return _insertCommand!.ExecuteNonQuery();
    }

    [Benchmark]
    public long PreparedPointReadScalar()
    {
        _pointReadId!.Value = NextId(ref _nextPointReadId);
        var value = _pointReadCommand!.ExecuteScalar();
        return Convert.ToInt64(value, CultureInfo.InvariantCulture);
    }

    [Benchmark]
    public int PreparedOneRowUpdate()
    {
        _updateId!.Value = NextId(ref _nextUpdateId);
        _updateValue!.Value = ++_nextUpdateValue;
        return _updateCommand!.ExecuteNonQuery();
    }

    [Benchmark]
    public long ReaderCreationDisposal()
    {
        _readerId!.Value = NextId(ref _nextReaderId);
        using var reader = _readerCommand!.ExecuteReader(CommandBehavior.SingleRow);
        if (!reader.Read())
        {
            throw new InvalidOperationException("Expected the point-read reader to return one row.");
        }

        return reader.GetInt64(0);
    }

    [Benchmark(Baseline = true)]
    public int ExecuteNonQuerySync()
    {
        _executeNonQueryId!.Value = NextId(ref _nextExecuteNonQueryId);
        _executeNonQueryValue!.Value = ++_nextUpdateValue;
        return _executeNonQueryCommand!.ExecuteNonQuery();
    }

    [Benchmark]
    public async Task<int> ExecuteNonQueryAsync()
    {
        _executeNonQueryAsyncId!.Value = NextId(ref _nextExecuteNonQueryAsyncId);
        _executeNonQueryAsyncValue!.Value = ++_nextUpdateValue;
        return await _executeNonQueryAsyncCommand!.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    private static DbConnection CreateConnection(BenchmarkProvider provider, string path)
    {
        return provider switch
        {
            BenchmarkProvider.DecentDB => new DecentDBConnection(
                $"Data Source={path};Cache Size=128MB;Retain Paged Row Sources After Commit=True;Paged Row Storage=False;WAL Auto Checkpoint=0"),
            BenchmarkProvider.SQLite => new SqliteConnection($"Data Source={path}"),
            _ => throw new ArgumentOutOfRangeException(nameof(provider), provider, null),
        };
    }

    private static void ConfigureConnection(DbConnection connection, BenchmarkProvider provider)
    {
        if (provider != BenchmarkProvider.SQLite)
        {
            return;
        }

        ExecuteNonQuery(connection, null, "PRAGMA journal_mode=WAL");
        ExecuteNonQuery(connection, null, "PRAGMA synchronous=NORMAL");
        ExecuteNonQuery(connection, null, "PRAGMA foreign_keys=ON");
        ExecuteNonQuery(connection, null, "PRAGMA temp_store=MEMORY");
        ExecuteNonQuery(connection, null, "PRAGMA cache_size=-65536");
    }

    private static void SeedHotTable(DbConnection connection)
    {
        using var transaction = connection.BeginTransaction();
        using var command = CreateCommand(connection, transaction, "INSERT INTO hot (id, value, payload) VALUES (@id, @value, @payload)");
        var id = AddParameter(command, "@id", DbType.Int64);
        var value = AddParameter(command, "@value", DbType.Int64);
        var payload = AddParameter(command, "@payload", DbType.String);
        command.Prepare();

        for (var i = 1; i <= SeedRows; i++)
        {
            id.Value = i;
            value.Value = i;
            payload.Value = Payload;
            command.ExecuteNonQuery();
        }

        transaction.Commit();
    }

    private void CreatePreparedCommands(DbConnection connection, DbTransaction transaction)
    {
        _insertCommand = CreateCommand(connection, transaction, "INSERT INTO insert_sink (id, value, payload) VALUES (@id, @value, @payload)");
        _insertId = AddParameter(_insertCommand, "@id", DbType.Int64);
        _insertValue = AddParameter(_insertCommand, "@value", DbType.Int64);
        _insertPayload = AddParameter(_insertCommand, "@payload", DbType.String);
        Prepare(_insertCommand);

        _pointReadCommand = CreateCommand(connection, transaction, "SELECT value FROM hot WHERE id = @id");
        _pointReadId = AddParameter(_pointReadCommand, "@id", DbType.Int64);
        Prepare(_pointReadCommand);

        _updateCommand = CreateCommand(connection, transaction, "UPDATE hot SET value = @value WHERE id = @id");
        _updateValue = AddParameter(_updateCommand, "@value", DbType.Int64);
        _updateId = AddParameter(_updateCommand, "@id", DbType.Int64);
        Prepare(_updateCommand);

        _readerCommand = CreateCommand(connection, transaction, "SELECT value, payload FROM hot WHERE id = @id");
        _readerId = AddParameter(_readerCommand, "@id", DbType.Int64);
        Prepare(_readerCommand);

        _executeNonQueryCommand = CreateCommand(connection, transaction, "UPDATE hot SET value = @value WHERE id = @id");
        _executeNonQueryValue = AddParameter(_executeNonQueryCommand, "@value", DbType.Int64);
        _executeNonQueryId = AddParameter(_executeNonQueryCommand, "@id", DbType.Int64);
        Prepare(_executeNonQueryCommand);

        _executeNonQueryAsyncCommand = CreateCommand(connection, transaction, "UPDATE hot SET value = @value WHERE id = @id");
        _executeNonQueryAsyncValue = AddParameter(_executeNonQueryAsyncCommand, "@value", DbType.Int64);
        _executeNonQueryAsyncId = AddParameter(_executeNonQueryAsyncCommand, "@id", DbType.Int64);
        Prepare(_executeNonQueryAsyncCommand);
    }

    private void DisposeCommands()
    {
        _insertCommand?.Dispose();
        _pointReadCommand?.Dispose();
        _updateCommand?.Dispose();
        _readerCommand?.Dispose();
        _executeNonQueryCommand?.Dispose();
        _executeNonQueryAsyncCommand?.Dispose();
    }

    private static DbCommand CreateCommand(DbConnection connection, DbTransaction? transaction, string sql)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        return command;
    }

    private static DbParameter AddParameter(DbCommand command, string name, DbType type)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.DbType = type;
        command.Parameters.Add(parameter);
        return parameter;
    }

    private static void Prepare(DbCommand command)
    {
        command.Prepare();
    }

    private static void ExecuteNonQuery(DbConnection connection, DbTransaction? transaction, string sql)
    {
        using var command = CreateCommand(connection, transaction, sql);
        command.ExecuteNonQuery();
    }

    private static long NextId(ref int nextId)
    {
        var id = nextId++;
        if (nextId > SeedRows)
        {
            nextId = 1;
        }

        return id;
    }

    private static string CreateDatabasePath(BenchmarkProvider provider)
    {
        var root = Path.Combine(".tmp", "adonet-microbenchmarks", "databases");
        Directory.CreateDirectory(root);
        var extension = provider == BenchmarkProvider.SQLite ? ".db" : ".ddb";
        return Path.Combine(root, $"{provider}-{Guid.NewGuid():N}{extension}");
    }

    private static void DeleteDatabaseFiles(string path)
    {
        var directory = Path.GetDirectoryName(path);
        var prefix = Path.GetFileName(path);
        if (directory == null || prefix.Length == 0 || !Directory.Exists(directory))
        {
            return;
        }

        foreach (var file in Directory.EnumerateFiles(directory, prefix + "*"))
        {
            File.Delete(file);
        }
    }
}

public enum BenchmarkProvider
{
    DecentDB,
    SQLite,
}
