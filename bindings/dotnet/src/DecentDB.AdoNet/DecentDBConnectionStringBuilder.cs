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
    private const string RetainPagedRowSourcesAfterCommitKey = "Retain Paged Row Sources After Commit";
    private const string PagedRowStorageKey = "Paged Row Storage";
    private const string PersistentPkIndexKey = "Persistent PK Index";
    private const string WalAutoCheckpointKey = "WAL Auto Checkpoint";
    private const string WriteQueueEnabledKey = "Write Queue Enabled";
    private const string WriteQueueCapacityKey = "Write Queue Capacity";
    private const string WriteQueueDefaultTimeoutMsKey = "Write Queue Default Timeout Ms";
    private const string WriteQueueStrictGroupCommitKey = "Write Queue Strict Group Commit";
    private const string WriteQueueMaxBatchKey = "Write Queue Max Batch";
    private const string WriteQueueMaxGroupDelayUsKey = "Write Queue Max Group Delay Us";

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
    /// Keeps paged row sources resident after commits on this handle. Optional; engine default is <c>false</c>.
    /// </summary>
    public bool? RetainPagedRowSourcesAfterCommit
    {
        get => GetNullableBool(RetainPagedRowSourcesAfterCommitKey);
        set => SetNullableBool(RetainPagedRowSourcesAfterCommitKey, value);
    }

    /// <summary>
    /// Enables paged row storage. Optional; engine default is <c>true</c>.
    /// </summary>
    public bool? PagedRowStorage
    {
        get => GetNullableBool(PagedRowStorageKey);
        set => SetNullableBool(PagedRowStorageKey, value);
    }

    /// <summary>
    /// Enables the persistent primary-key locator index. Optional; engine default is <c>false</c>.
    /// </summary>
    public bool? PersistentPkIndex
    {
        get => GetNullableBool(PersistentPkIndexKey);
        set => SetNullableBool(PersistentPkIndexKey, value);
    }

    /// <summary>
    /// WAL auto-checkpoint page threshold. Use <c>0</c> to disable. Optional.
    /// </summary>
    public string? WalAutoCheckpoint
    {
        get => TryGetValue(WalAutoCheckpointKey, out var v) ? (string)v : null;
        set
        {
            if (value == null) Remove(WalAutoCheckpointKey);
            else this[WalAutoCheckpointKey] = value;
        }
    }

    /// <summary>
    /// Enables engine-owned queued writes for connection-level write execution. Optional.
    /// </summary>
    public bool? WriteQueueEnabled
    {
        get => GetNullableBool(WriteQueueEnabledKey);
        set => SetNullableBool(WriteQueueEnabledKey, value);
    }

    /// <summary>
    /// Maximum admitted queued writes waiting for execution. Optional.
    /// </summary>
    public int? WriteQueueCapacity
    {
        get => GetNullableInt(WriteQueueCapacityKey);
        set => SetNullableInt(WriteQueueCapacityKey, value);
    }

    /// <summary>
    /// Default queued-write timeout in milliseconds. Optional; 0 means no default timeout.
    /// </summary>
    public int? WriteQueueDefaultTimeoutMs
    {
        get => GetNullableInt(WriteQueueDefaultTimeoutMsKey);
        set => SetNullableInt(WriteQueueDefaultTimeoutMsKey, value);
    }

    /// <summary>
    /// Enables strict durable group commit for queued writes. Optional.
    /// </summary>
    public bool? WriteQueueStrictGroupCommit
    {
        get => GetNullableBool(WriteQueueStrictGroupCommitKey);
        set => SetNullableBool(WriteQueueStrictGroupCommitKey, value);
    }

    /// <summary>
    /// Maximum ready queued requests drained in one executor pass. Optional.
    /// </summary>
    public int? WriteQueueMaxBatch
    {
        get => GetNullableInt(WriteQueueMaxBatchKey);
        set => SetNullableInt(WriteQueueMaxBatchKey, value);
    }

    /// <summary>
    /// Optional group-commit collection delay in microseconds. Optional.
    /// </summary>
    public int? WriteQueueMaxGroupDelayUs
    {
        get => GetNullableInt(WriteQueueMaxGroupDelayUsKey);
        set => SetNullableInt(WriteQueueMaxGroupDelayUsKey, value);
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

    private bool? GetNullableBool(string key)
    {
        if (!TryGetValue(key, out var v) || v is not string s)
        {
            return null;
        }

        if (bool.TryParse(s, out var b)) return b;
        if (s == "1") return true;
        if (s == "0") return false;
        return null;
    }

    private void SetNullableBool(string key, bool? value)
    {
        if (value == null)
        {
            Remove(key);
            return;
        }

        this[key] = value.Value ? "True" : "False";
    }

    private int? GetNullableInt(string key)
    {
        if (!TryGetValue(key, out var v) || v is not string s)
        {
            return null;
        }

        return int.TryParse(s, out var i) ? i : null;
    }

    private void SetNullableInt(string key, int? value)
    {
        if (value == null)
        {
            Remove(key);
            return;
        }

        if (value.Value < 0) throw new ArgumentOutOfRangeException(nameof(value));
        this[key] = value.Value.ToString();
    }
}
