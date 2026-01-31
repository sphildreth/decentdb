using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace DecentDb.AdoNet
{
    public static class SqlParameterRewriter
    {
        public static (string Sql, Dictionary<int, DbParameter> Parameters) Rewrite(
            string sql,
            IReadOnlyList<DbParameter> parameters)
        {
            if (sql == null) throw new ArgumentNullException(nameof(sql));

            if (parameters.Count == 0)
            {
                return (sql, new Dictionary<int, DbParameter>());
            }

            var parameterByName = new Dictionary<string, DbParameter>(StringComparer.Ordinal);
            var unnamedParameters = new List<DbParameter>();

            for (var i = 0; i < parameters.Count; i++)
            {
                var p = parameters[i];
                var name = NormalizeParameterName(p.ParameterName);
                if (name == null)
                {
                    unnamedParameters.Add(p);
                    continue;
                }

                parameterByName[name] = p;

                var stripped = StripParameterPrefix(name);
                if (stripped != null)
                {
                    parameterByName[stripped] = p;
                }
            }

            var usedIndices = new HashSet<int>();
            var indexToName = new Dictionary<int, string>();
            var nameToIndex = new Dictionary<string, int>(StringComparer.Ordinal);
            var nextAutoIndex = 1;

            int AllocateIndex()
            {
                while (usedIndices.Contains(nextAutoIndex))
                {
                    nextAutoIndex++;
                }

                usedIndices.Add(nextAutoIndex);
                return nextAutoIndex++;
            }

            var rewritten = new StringBuilder(sql.Length + 16);
            var inSingleQuote = false;
            var inDoubleQuote = false;
            var inLineComment = false;
            var inBlockComment = false;

            var iPos = 0;
            while (iPos < sql.Length)
            {
                var ch = sql[iPos];

                if (inLineComment)
                {
                    rewritten.Append(ch);
                    if (ch == '\n')
                    {
                        inLineComment = false;
                    }
                    iPos++;
                    continue;
                }

                if (inBlockComment)
                {
                    rewritten.Append(ch);
                    if (ch == '*' && iPos + 1 < sql.Length && sql[iPos + 1] == '/')
                    {
                        rewritten.Append('/');
                        iPos += 2;
                        inBlockComment = false;
                        continue;
                    }
                    iPos++;
                    continue;
                }

                if (!inSingleQuote && !inDoubleQuote)
                {
                    if (ch == '-' && iPos + 1 < sql.Length && sql[iPos + 1] == '-')
                    {
                        rewritten.Append("--");
                        iPos += 2;
                        inLineComment = true;
                        continue;
                    }

                    if (ch == '/' && iPos + 1 < sql.Length && sql[iPos + 1] == '*')
                    {
                        rewritten.Append("/*");
                        iPos += 2;
                        inBlockComment = true;
                        continue;
                    }
                }

                if (ch == '\'' && !inDoubleQuote)
                {
                    if (inSingleQuote && iPos + 1 < sql.Length && sql[iPos + 1] == '\'')
                    {
                        rewritten.Append("''");
                        iPos += 2;
                        continue;
                    }
                    inSingleQuote = !inSingleQuote;
                    rewritten.Append(ch);
                    iPos++;
                    continue;
                }

                if (ch == '"' && !inSingleQuote)
                {
                    if (inDoubleQuote && iPos + 1 < sql.Length && sql[iPos + 1] == '"')
                    {
                        rewritten.Append("\"\"");
                        iPos += 2;
                        continue;
                    }
                    inDoubleQuote = !inDoubleQuote;
                    rewritten.Append(ch);
                    iPos++;
                    continue;
                }

                if (inSingleQuote || inDoubleQuote)
                {
                    rewritten.Append(ch);
                    iPos++;
                    continue;
                }

                if (ch == '$' && iPos + 1 < sql.Length && char.IsDigit(sql[iPos + 1]))
                {
                    var start = iPos + 1;
                    var end = start;
                    while (end < sql.Length && char.IsDigit(sql[end]))
                    {
                        end++;
                    }

                    if (int.TryParse(sql.Substring(start, end - start), out var explicitIndex) && explicitIndex > 0)
                    {
                        usedIndices.Add(explicitIndex);
                        if (explicitIndex >= nextAutoIndex)
                        {
                            nextAutoIndex = explicitIndex + 1;
                        }

                        rewritten.Append('$').Append(explicitIndex);
                        iPos = end;
                        continue;
                    }
                }

                if (ch == '@' && iPos + 1 < sql.Length)
                {
                    var start = iPos;
                    var j = iPos + 1;

                    if (sql[j] == 'p' && j + 1 < sql.Length && char.IsDigit(sql[j + 1]))
                    {
                        var k = j + 1;
                        while (k < sql.Length && char.IsDigit(sql[k]))
                        {
                            k++;
                        }

                        if (int.TryParse(sql.Substring(j + 1, k - (j + 1)), out var pNum) && pNum >= 0)
                        {
                            var idx = pNum + 1;
                            usedIndices.Add(idx);
                            if (idx >= nextAutoIndex)
                            {
                                nextAutoIndex = idx + 1;
                            }

                            rewritten.Append('$').Append(idx);
                            iPos = k;
                            continue;
                        }
                    }

                    if (char.IsLetter(sql[j]) || sql[j] == '_')
                    {
                        while (j < sql.Length && (char.IsLetterOrDigit(sql[j]) || sql[j] == '_'))
                        {
                            j++;
                        }

                        var name = sql.Substring(start + 1, j - start - 1);
                        if (!nameToIndex.TryGetValue(name, out var idx))
                        {
                            idx = AllocateIndex();
                            nameToIndex[name] = idx;
                            indexToName[idx] = name;
                        }

                        rewritten.Append('$').Append(idx);
                        iPos = j;
                        continue;
                    }
                }

                rewritten.Append(ch);
                iPos++;
            }

            var paramMap = new Dictionary<int, DbParameter>();
            var unnamedCursor = 0;

            var indices = new List<int>(usedIndices);
            indices.Sort();

            foreach (var index in indices)
            {
                if (indexToName.TryGetValue(index, out var name))
                {
                    if (!TryResolveNamed(parameterByName, name, out var parameter))
                    {
                        throw new InvalidOperationException($"Missing value for parameter '@{name}'.");
                    }
                    paramMap[index] = parameter;
                    continue;
                }

                if (TryResolveIndexed(parameterByName, index, out var indexedParameter))
                {
                    paramMap[index] = indexedParameter;
                    continue;
                }

                if (unnamedCursor < unnamedParameters.Count)
                {
                    paramMap[index] = unnamedParameters[unnamedCursor++];
                    continue;
                }

                throw new InvalidOperationException($"Missing value for parameter '${index}'.");
            }

            return (rewritten.ToString(), paramMap);
        }

        private static bool TryResolveNamed(Dictionary<string, DbParameter> parametersByName, string name, out DbParameter parameter)
        {
            if (parametersByName.TryGetValue(name, out parameter)) return true;
            if (parametersByName.TryGetValue("@" + name, out parameter)) return true;
            return false;
        }

        private static bool TryResolveIndexed(Dictionary<string, DbParameter> parametersByName, int index1Based, out DbParameter parameter)
        {
            if (parametersByName.TryGetValue("$" + index1Based, out parameter)) return true;
            if (parametersByName.TryGetValue(index1Based.ToString(), out parameter)) return true;

            var pNum = index1Based - 1;
            if (pNum >= 0)
            {
                if (parametersByName.TryGetValue("@p" + pNum, out parameter)) return true;
                if (parametersByName.TryGetValue("p" + pNum, out parameter)) return true;
            }

            parameter = null!;
            return false;
        }

        private static string? NormalizeParameterName(string? parameterName)
        {
            if (string.IsNullOrWhiteSpace(parameterName)) return null;
            return parameterName.Trim();
        }

        private static string? StripParameterPrefix(string parameterName)
        {
            if (parameterName.Length <= 1) return null;

            if (parameterName[0] == '@' || parameterName[0] == '$')
            {
                return parameterName.Substring(1);
            }

            return null;
        }
    }
}
