using System;
using System.Collections.Generic;

namespace DecentDB.AdoNet;

/// <summary>
/// Query-plan output captured from DecentDB's EXPLAIN or EXPLAIN ANALYZE statement.
/// </summary>
public sealed class DecentDBQueryPlan
{
    public DecentDBQueryPlan(
        string sql,
        string explainSql,
        bool analyze,
        IReadOnlyList<string> lines,
        TimeSpan duration)
    {
        Sql = sql;
        ExplainSql = explainSql;
        Analyze = analyze;
        Lines = lines;
        Duration = duration;
        Text = string.Join(Environment.NewLine, lines);
    }

    public string Sql { get; }
    public string ExplainSql { get; }
    public bool Analyze { get; }
    public IReadOnlyList<string> Lines { get; }
    public string Text { get; }
    public TimeSpan Duration { get; }
}
