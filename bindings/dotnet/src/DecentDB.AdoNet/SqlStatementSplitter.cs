using System;
using System.Collections.Generic;

namespace DecentDB.AdoNet
{
    /// <summary>
    /// Splits a multi-statement SQL string into individual statements.
    /// Handles single-quoted strings, double-quoted identifiers, and hex literals (X'...').
    /// </summary>
    public static class SqlStatementSplitter
    {
        public static List<string> Split(string sql)
        {
            if (string.IsNullOrWhiteSpace(sql))
                return new List<string>();

            var statements = new List<string>();
            var i = 0;
            var stmtStart = 0;

            while (i < sql.Length)
            {
                var ch = sql[i];

                if (ch == '\'')
                {
                    i = SkipQuotedString(sql, i, '\'');
                }
                else if (ch == '"')
                {
                    i = SkipQuotedString(sql, i, '"');
                }
                else if (ch == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
                {
                    // Line comment: skip to end of line
                    i = sql.IndexOf('\n', i);
                    if (i < 0) i = sql.Length;
                    else i++;
                }
                else if (ch == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
                {
                    // Block comment: skip to */
                    var end = sql.IndexOf("*/", i + 2, StringComparison.Ordinal);
                    i = end < 0 ? sql.Length : end + 2;
                }
                else if (ch == ';')
                {
                    var stmt = sql.Substring(stmtStart, i - stmtStart).Trim();
                    if (stmt.Length > 0)
                        statements.Add(stmt);
                    stmtStart = i + 1;
                    i++;
                }
                else
                {
                    i++;
                }
            }

            // Remaining text after last semicolon
            if (stmtStart < sql.Length)
            {
                var stmt = sql.Substring(stmtStart).Trim();
                if (stmt.Length > 0)
                    statements.Add(stmt);
            }

            return statements;
        }

        private static int SkipQuotedString(string sql, int start, char quoteChar)
        {
            var i = start + 1;
            while (i < sql.Length)
            {
                if (sql[i] == quoteChar)
                {
                    // Check for escaped quote (doubled quote char)
                    if (i + 1 < sql.Length && sql[i + 1] == quoteChar)
                    {
                        i += 2;
                        continue;
                    }
                    return i + 1;
                }
                i++;
            }
            return sql.Length;
        }
    }
}
