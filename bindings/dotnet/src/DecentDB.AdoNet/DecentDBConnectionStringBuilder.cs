using System;
using System.Data.Common;

namespace DecentDB.AdoNet;

public sealed class DecentDBConnectionStringBuilder : DbConnectionStringBuilder
{
    private const string DataSourceKey = "Data Source";
    private const string CacheSizeKey = "Cache Size";
    private const string LoggingKey = "Logging";
    private const string LogLevelKey = "LogLevel";
    private const string CommandTimeoutKey = "Command Timeout";

    public DecentDBConnectionStringBuilder()
    {
    }

    public DecentDBConnectionStringBuilder(string connectionString)
    {
        ConnectionString = connectionString;
    }

    public string DataSource
    {
        get => TryGetValue(DataSourceKey, out var v) ? (string)v : string.Empty;
        set => this[DataSourceKey] = value;
    }

    public string? CacheSize
    {
        get => TryGetValue(CacheSizeKey, out var v) ? (string)v : null;
        set
        {
            if (value == null) Remove(CacheSizeKey);
            else this[CacheSizeKey] = value;
        }
    }

    public bool Logging
    {
        get => TryGetValue(LoggingKey, out var v) && v is string s && bool.TryParse(s, out var b) && b;
        set => this[LoggingKey] = value.ToString();
    }

    public string? LogLevel
    {
        get => TryGetValue(LogLevelKey, out var v) ? (string)v : null;
        set
        {
            if (value == null) Remove(LogLevelKey);
            else this[LogLevelKey] = value;
        }
    }

    public int CommandTimeout
    {
        get => TryGetValue(CommandTimeoutKey, out var v) && v is string s && int.TryParse(s, out var i) ? i : 30;
        set
        {
            if (value < 0) throw new ArgumentOutOfRangeException(nameof(value));
            this[CommandTimeoutKey] = value.ToString();
        }
    }
}
