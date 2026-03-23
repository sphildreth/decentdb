using System;
using System.Collections.Generic;

namespace DecentDB.AdoNet;

public enum SqlLogLevel
{
    Verbose = 0,
    Debug = 1,
    Info = 2,
    Warning = 3,
    Error = 4,
}

public sealed class SqlParameterValue
{
    public SqlParameterValue(int ordinal1Based, string name, object? value)
    {
        Ordinal1Based = ordinal1Based;
        Name = name;
        Value = value;
    }

    public int Ordinal1Based { get; }
    public string Name { get; }
    public object? Value { get; }
}

public class SqlExecutingEventArgs : EventArgs
{
    public SqlExecutingEventArgs(string sql, IReadOnlyList<SqlParameterValue> parameters, DateTimeOffset timestamp)
    {
        Sql = sql;
        Parameters = parameters;
        Timestamp = timestamp;
    }

    public string Sql { get; }
    public IReadOnlyList<SqlParameterValue> Parameters { get; }
    public DateTimeOffset Timestamp { get; }
}

public sealed class SqlExecutedEventArgs : SqlExecutingEventArgs
{
    public SqlExecutedEventArgs(
        string sql,
        IReadOnlyList<SqlParameterValue> parameters,
        DateTimeOffset timestamp,
        TimeSpan duration,
        long rowsAffected,
        Exception? exception)
        : base(sql, parameters, timestamp)
    {
        Duration = duration;
        RowsAffected = rowsAffected;
        Exception = exception;
    }

    public TimeSpan Duration { get; }
    public long RowsAffected { get; }
    public Exception? Exception { get; }
}

internal sealed class SqlObservation
{
    public SqlObservation(long startTimestamp, string sql, IReadOnlyList<SqlParameterValue> parameters)
    {
        StartTimestamp = startTimestamp;
        Sql = sql;
        Parameters = parameters;
        Timestamp = DateTimeOffset.UtcNow;
    }

    public long StartTimestamp { get; }
    public DateTimeOffset Timestamp { get; }
    public string Sql { get; }
    public IReadOnlyList<SqlParameterValue> Parameters { get; }
}
