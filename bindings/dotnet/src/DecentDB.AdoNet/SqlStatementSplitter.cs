using System;
using System.Collections.Generic;

namespace DecentDB.AdoNet
{
    /// <summary>
    /// Splits a multi-statement SQL string into individual statements.
    /// Handles single-quoted strings, double-quoted identifiers, hex literals (X'...'),
    /// line and block comments, and compound bodies (BEGIN ... END) for CREATE TRIGGER / PROCEDURE.
    /// </summary>
    public static class SqlStatementSplitter
    {
        public static List<string> Split(string sql)
        {
            if (string.IsNullOrWhiteSpace(sql))
                return new List<string>();

            var statements = new List<string>();
            int pos = 0;
            int stmtStart = 0;
            int len = sql.Length;

            // Compound-body state
            bool inCompound = false;
            int compoundDepth = 0;
            bool afterCreate = false;   // saw CREATE at statement start
            bool expectCompound = false; // after CREATE TRIGGER|PROCEDURE, expect BEGIN

            while (pos < len)
            {
                char c = sql[pos];

                // --- Strings ---
                if (c == '\'' || c == '"')
                {
                    pos = SkipQuotedString(sql, pos);
                    afterCreate = false;
                    continue;
                }

                // --- Line comment ---
                if (c == '-' && pos + 1 < len && sql[pos + 1] == '-')
                {
                    int nl = sql.IndexOf('\n', pos);
                    if (nl < 0) pos = len;
                    else pos = nl + 1;
                    afterCreate = false;
                    continue;
                }

                // --- Block comment ---
                if (c == '/' && pos + 1 < len && sql[pos + 1] == '*')
                {
                    int end = sql.IndexOf("*/", pos + 2, StringComparison.Ordinal);
                    if (end < 0) pos = len;
                    else pos = end + 2;
                    afterCreate = false;
                    continue;
                }

                // --- Statement terminator ---
                if (c == ';' && !inCompound)
                {
                    var stmt = sql.Substring(stmtStart, pos - stmtStart).Trim();
                    if (stmt.Length > 0)
                        statements.Add(stmt);
                    stmtStart = pos + 1;
                    // Reset state for next statement
                    afterCreate = false;
                    expectCompound = false;
                    inCompound = false;
                    compoundDepth = 0;
                    pos++;
                    continue;
                }

                // --- Keyword detection & state machine ---
                // If we're at a position where a new token could start and we're on a letter, read the word.
                if (char.IsLetter(c))
                {
                    int wordStart = pos;
                    pos++;
                    while (pos < len && char.IsLetterOrDigit(sql[pos]) && sql[pos] != '(' && sql[pos] != ')' && sql[pos] != ',' && sql[pos] != ';')
                        pos++;

                    string word = sql[wordStart..pos].ToUpperInvariant();

                    if (!inCompound)
                    {
                        // At statement start, we care about CREATE
                        if (!afterCreate && stmtStart + (wordStart - stmtStart) <= 1) // roughly near start
                        {
                            if (word == "CREATE")
                                afterCreate = true;
                        }
                        else if (afterCreate)
                        {
                            if (word == "TRIGGER" || word == "PROCEDURE")
                            {
                                expectCompound = true;
                                afterCreate = false;
                            }
                            else
                            {
                                // Some other CREATE object; no compound expected
                                afterCreate = false;
                            }
                        }

                        if (expectCompound && word == "BEGIN")
                        {
                            inCompound = true;
                            compoundDepth = 1;
                            expectCompound = false;
                        }
                    }
                    else // inCompound
                    {
                        if (word == "BEGIN")
                        {
                            compoundDepth++;
                        }
                        else if (word == "END")
                        {
                            compoundDepth--;
                            if (compoundDepth == 0)
                            {
                                inCompound = false;
                            }
                        }
                    }

                    continue;
                }

                // Any other character
                pos++;
            }

            // Check for unbalanced BEGIN at EOF
            if (inCompound)
            {
                throw new FormatException("CREATE TRIGGER body is missing END;");
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

        private static int SkipQuotedString(string sql, int start)
        {
            char quoteChar = sql[start];
            var i = start + 1;
            while (i < sql.Length)
            {
                if (sql[i] == quoteChar)
                {
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
