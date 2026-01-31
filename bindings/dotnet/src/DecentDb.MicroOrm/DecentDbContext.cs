using System;
using System.Collections.Concurrent;
using System.Data.Common;
using DecentDb.AdoNet;

namespace DecentDb.MicroOrm;

public class DecentDbContext : IDisposable
{
    private readonly string _connectionString;
    private readonly bool _pooling;

    private DecentDbConnection? _connection;
    private DbTransaction? _transaction;

    private readonly ConcurrentDictionary<Type, object> _sets = new();

    public DecentDbContext(string dataSourcePath, bool pooling = true)
    {
        if (string.IsNullOrWhiteSpace(dataSourcePath))
        {
            throw new ArgumentException("Data source path must be provided.", nameof(dataSourcePath));
        }

        _connectionString = $"Data Source={dataSourcePath}";
        _pooling = pooling;

        InitializeDbSets();
    }

    public DbTransaction BeginTransaction()
    {
        EnsureOpenConnection();
        _transaction = _connection!.BeginTransaction();
        return _transaction;
    }

    public DbTransaction BeginTransaction(System.Data.IsolationLevel isolationLevel)
    {
        EnsureOpenConnection();
        _transaction = _connection!.BeginTransaction(isolationLevel);
        return _transaction;
    }

    public DbSet<T> Set<T>() where T : class, new()
    {
        return (DbSet<T>)_sets.GetOrAdd(typeof(T), _ => new DbSet<T>(this));
    }

    internal ConnectionScope AcquireConnectionScope()
    {
        // If a transaction is active, always stick to the transaction's connection.
        if (_transaction != null)
        {
            EnsureOpenConnection();
            return new ConnectionScope(_connection!, disposeConnection: false);
        }

        if (_pooling)
        {
            EnsureOpenConnection();
            return new ConnectionScope(_connection!, disposeConnection: false);
        }

        // Non-pooled mode: open/close per operation.
        var conn = new DecentDbConnection(_connectionString);
        conn.Open();
        return new ConnectionScope(conn, disposeConnection: true);
    }

    internal DbTransaction? CurrentTransaction => _transaction;

    private void EnsureOpenConnection()
    {
        if (_connection != null && _connection.State == System.Data.ConnectionState.Open)
        {
            return;
        }

        _connection = new DecentDbConnection(_connectionString);
        _connection.Open();
    }

    internal readonly struct ConnectionScope : IDisposable
    {
        private readonly bool _disposeConnection;

        public ConnectionScope(DecentDbConnection connection, bool disposeConnection)
        {
            Connection = connection;
            _disposeConnection = disposeConnection;
        }

        public DecentDbConnection Connection { get; }

        public void Dispose()
        {
            if (_disposeConnection)
            {
                Connection.Dispose();
            }
        }
    }

    private void InitializeDbSets()
    {
        // If a derived context defines DbSet<T> properties with setters, populate them.
        var props = GetType().GetProperties(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic);
        foreach (var prop in props)
        {
            if (!prop.CanWrite) continue;
            var pt = prop.PropertyType;
            if (!pt.IsGenericType) continue;
            if (pt.GetGenericTypeDefinition() != typeof(DbSet<>)) continue;

            var entityType = pt.GetGenericArguments()[0];
            var set = _sets.GetOrAdd(entityType, t =>
            {
                var ctor = pt.GetConstructor(new[] { typeof(DecentDbContext) });
                return ctor!.Invoke(new object[] { this });
            });

            prop.SetValue(this, set);
        }
    }

    public void Dispose()
    {
        _transaction?.Dispose();
        _transaction = null;

        _connection?.Dispose();
        _connection = null;
    }
}
