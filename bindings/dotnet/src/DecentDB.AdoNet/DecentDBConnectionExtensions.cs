using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;

namespace DecentDB.AdoNet;

public static class DecentDBConnectionExtensions
{
    public static void Checkpoint(this DecentDBConnection connection)
    {
        if (connection == null) throw new ArgumentNullException(nameof(connection));
        connection.Checkpoint();
    }

    /// <summary>
    /// Executes EXPLAIN or EXPLAIN ANALYZE for a SQL statement and returns the plan text.
    /// </summary>
    /// <param name="connection">An open DecentDB connection.</param>
    /// <param name="sql">The SQL statement to inspect.</param>
    /// <param name="analyze">When true, executes EXPLAIN ANALYZE and includes actual metrics.</param>
    public static DecentDBQueryPlan ExplainQuery(
        this DecentDBConnection connection,
        string sql,
        bool analyze = false)
    {
        if (connection == null) throw new ArgumentNullException(nameof(connection));
        if (string.IsNullOrWhiteSpace(sql))
            throw new ArgumentException("SQL cannot be null or empty.", nameof(sql));
        if (connection.State != ConnectionState.Open)
            throw new InvalidOperationException("Connection is not open.");

        var explainSql = analyze ? $"EXPLAIN ANALYZE {sql}" : $"EXPLAIN {sql}";
        var stopwatch = Stopwatch.StartNew();
        var lines = new List<string>();

        using (var command = connection.CreateCommand())
        {
            command.CommandText = explainSql;
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                lines.Add(Convert.ToString(reader.GetValue(0)) ?? string.Empty);
            }
        }

        stopwatch.Stop();
        return new DecentDBQueryPlan(
            sql,
            explainSql,
            analyze,
            lines,
            stopwatch.Elapsed);
    }
}
