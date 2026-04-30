using System;
using System.Data.Common;

namespace DecentDB.AdoNet;

/// <summary>
/// Strongly-typed builder for DecentDB connection strings.
/// </summary>
public sealed class DecentDBConnectionStringBuilder : DbConnectionStringBuilder
{
    private const string DataSourceKey = "Data Source";
    private const string CacheSizeKey = "Cache Size";
    private const string LoggingKey = "Logging";
    private const string LogLevelKey = "LogLevel";
    private const string CommandTimeoutKey = "Command Timeout";
    private const string PoolingKey = "Pooling";

    public DecentDBConnectionStringBuilder()
    {
    }

    public DecentDBConnectionStringBuilder(string connectionString)
    {
        ConnectionString = connectionString;
    }

    /// <summary>
    /// Path to the database file. Required.
    /// </summary>
    public string DataSource
    {
        get => TryGetValue(DataSourceKey, out var v) ? (string)v : string.Empty;
        set => this[DataSourceKey] = value;
    }

    /// <summary>
    /// Cache size in pages (integer) or with a unit suffix (e.g., <c>64MB</c>).
    /// Passed directly to the engine. Optional.
    /// </summary>
    public string? CacheSize
    {
        get => TryGetValue(CacheSizeKey, out var v) ? (string)v : null;
        set
        {
            if (value == null) Remove(CacheSizeKey);
            else this[CacheSizeKey] = value;
        }
    }

    /// <summary>
    /// When true, detailed SQL logging is enabled via <see cref="DecentDBConnection.SqlExecuting"/> and <see cref="DecentDBConnection.SqlExecuted"/> events.
    /// Optional; default is <c>false</c>.
    /// </summary>
    public bool Logging
    {
        get => TryGetValue(LoggingKey, out var v) && v is string s && bool.TryParse(s, out var b) && b;
        set => this[LoggingKey] = value.ToString();
    }

    /// <summary>
    /// Logging verbosity: <c>Debug</c>, <c>Info</c>, <c>Warn</c>, <c>Error</c>. Optional; default is <c>Debug</c>.
    /// </summary>
    public string? LogLevel
    {
        get => TryGetValue(LogLevelKey, out var v) ? (string)v : null;
        set
        {
            if (value == null) Remove(LogLevelKey);
            else this[LogLevelKey] = value;
        }
    }

    /// <summary>
    /// Command timeout in seconds. Optional; default is <c>30</c>.
    /// </summary>
    public int CommandTimeout
    {
        get => TryGetValue(CommandTimeoutKey, out var v) && v is string s && int.TryParse(s, out var i) ? i : 30;
        set
        {
            if (value < 0) throw new ArgumentOutOfRangeException(nameof(value));
            this[CommandTimeoutKey] = value.ToString();
        }
    }

    /// <summary>
    /// When true, <see cref="DecentDB.MicroOrm.DecentDBContext"/> reuses a single open connection across operations.
    /// ADO.NET-only callers currently ignore this key. Optional; default is <c>true</c>.
    /// </summary>
    public bool Pooling
    {
        get
        {
            if (!TryGetValue(PoolingKey, out var v) || v is not string s)
                return true;
            if (bool.TryParse(s, out var b)) return b;
            if (s == "1") return true;
            if (s == "0") return false;
            return true;
        }
        set => this[PoolingKey] = value ? "True" : "False";
    }
}
