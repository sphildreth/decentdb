using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics.CodeAnalysis;
using DecentDB.Native;

namespace DecentDB.AdoNet
{
    public sealed class DecentDBCommand : DbCommand
    {
        private const int StackallocTextByteLimit = 256;

        private DecentDBConnection? _connection;
        private string _commandText = string.Empty;
        private int _commandTimeout = 30;
        private readonly List<DecentDBParameter> _parameters = new();
        private readonly DecentDBParameterCollection _parameterCollection;
        private DecentDBTransaction? _transaction;
        private PreparedStatement? _statement;
        private PreparedStatement? _preparedStatement;
        private string? _preparedSql;
        private Native.DecentDB? _preparedDb;
        private bool _preparedStatementFromConnectionCache;
        private bool _statementCanSkipFinalizeReset;
        private string? _cachedSplitSql;
        private List<string>? _cachedSplitStatements;
        private string? _cachedRewriteSourceSql;
        private string? _cachedRewriteSql;
        private Dictionary<int, DbParameter>? _cachedRewriteParamMap;
        private DbParameter[]? _cachedRewriteParameterRefs;
        private string?[]? _cachedRewriteParameterNames;
        private bool _cachedRewriteNeedsOffsetClamp;
        private SingleRowReadPlan? _cachedSingleRowReadPlan;
        private SingleInt64NonQueryPlan? _cachedSingleInt64NonQueryPlan;
        private Int64TextFloat64NonQueryPlan? _cachedInt64TextFloat64NonQueryPlan;
        private bool _disposed;

        public DecentDBCommand()
        {
            _connection = null;
            _parameterCollection = new DecentDBParameterCollection(_parameters);
        }

        public DecentDBCommand(DecentDBConnection connection)
        {
            _connection = connection;
            _parameterCollection = new DecentDBParameterCollection(_parameters);
            _commandTimeout = connection.DefaultCommandTimeoutSeconds;
        }

        public DecentDBCommand(DecentDBConnection connection, string commandText)
        {
            _connection = connection;
            _commandText = commandText;
            _parameterCollection = new DecentDBParameterCollection(_parameters);
            _commandTimeout = connection.DefaultCommandTimeoutSeconds;
        }

        internal DecentDBConnection OwnerConnection => _connection ?? throw new InvalidOperationException("Command has no connection");

        [AllowNull]
        public override string CommandText
        {
            get => _commandText;
            set
            {
                if (_statement != null)
                {
                    throw new InvalidOperationException("Cannot change CommandText while command is executing");
                }
                InvalidatePreparedStatement();
                InvalidateSplitCache();
                InvalidateRewriteCache();
                _commandText = value ?? string.Empty;
            }
        }

        public override int CommandTimeout
        {
            get => _commandTimeout;
            set
            {
                if (value < 0) throw new ArgumentException("CommandTimeout must be non-negative");
                _commandTimeout = value;
            }
        }

        public override CommandType CommandType
        {
            get => CommandType.Text;
            set
            {
                if (value != CommandType.Text)
                {
                    throw new NotSupportedException("Only CommandType.Text is supported");
                }
            }
        }

        public override bool DesignTimeVisible { get; set; }

        public override UpdateRowSource UpdatedRowSource { get; set; }

        protected override DbConnection? DbConnection
        {
            get => _connection;
            set
            {
                if (value == null)
                {
                    if (_statement != null)
                    {
                        throw new InvalidOperationException("Cannot change connection while command is executing");
                    }
                    InvalidatePreparedStatement();
                    _connection = null;
                    return;
                }

                if (value is not DecentDBConnection conn)
                {
                    throw new ArgumentException("Must be a DecentDBConnection");
                }
                if (_statement != null)
                {
                    throw new InvalidOperationException("Cannot change connection while command is executing");
                }
                if (!ReferenceEquals(_connection, conn))
                {
                    InvalidatePreparedStatement();
                }
                _connection = conn;
            }
        }


        protected override DbParameterCollection DbParameterCollection => _parameterCollection;

        protected override DbTransaction? DbTransaction
        {
            get => _transaction;
            set
            {
                if (_statement != null)
                {
                    throw new InvalidOperationException("Cannot change transaction while command is executing");
                }
                _transaction = value as DecentDBTransaction;
            }
        }

        public override void Cancel()
        {
            if (_statement != null)
            {
                _statementCanSkipFinalizeReset = false;
                if (ReferenceEquals(_statement, _preparedStatement))
                {
                    _statement.Reset().ClearBindings();
                }
                else
                {
                    _statement.Dispose();
                }

                _statement = null;
            }
        }

        public override int ExecuteNonQuery()
        {
            var statements = GetSplitStatements();
            if (statements.Count <= 1)
            {
                return ExecuteSingleNonQuery();
            }

            // Multi-statement: execute each individually, sum affected rows
            var totalRows = 0;
            var savedText = _commandText;
            try
            {
                foreach (var stmt in statements)
                {
                    _commandText = stmt;
                    InvalidateSplitCache();
                    InvalidateRewriteCache();
                    using var reader = ExecuteDbDataReader(CommandBehavior.Default);
                    while (reader.Read()) { }
                    if (reader.RecordsAffected > 0)
                        totalRows += reader.RecordsAffected;
                }
            }
            finally
            {
                _commandText = savedText;
                InvalidateSplitCache();
                InvalidateRewriteCache();
            }
            return totalRows;
        }

        public override object? ExecuteScalar()
        {
            if (TryExecutePragmaScalar(out var pragmaValue))
            {
                return pragmaValue;
            }

            if (TryExecuteSingleInt64Scalar(out var fastValue))
            {
                return fastValue;
            }

            using var reader = ExecuteDbDataReader(CommandBehavior.Default);
            if (reader.Read())
            {
                return reader[0];
            }
            return null;
        }

        private bool TryExecuteSingleInt64Scalar(out object? value)
        {
            value = null;
            if (_connection == null)
            {
                throw new InvalidOperationException("Command has no connection");
            }

            if (GetSplitStatements().Count != 1)
            {
                return false;
            }

            var db = _connection.GetNativeDb();
            var (sql, paramMap, needsOffsetClamp) = GetRewrittenSqlAndParameters();
            if (needsOffsetClamp)
            {
                SqlParameterRewriter.ClampOffsetParameters(sql, paramMap);
            }

            if (!TryGetSingleInt64Parameter(paramMap, out var fastIndex, out var fastParamValue))
            {
                return false;
            }

            var observation = StartSqlObservationIfEnabled(_connection, sql, paramMap);
            try
            {
                for (var attempt = 0; ; attempt++)
                {
                    var stmt = EnsurePreparedStatement(sql, resetForExecution: false);
                    var stepResult = stmt.BindInt64StepAndCaptureRowView(fastIndex, fastParamValue);
                    if (stepResult < 0)
                    {
                        InvalidatePreparedStatement(discardFromConnectionCache: true);

                        var ex = new DecentDBException(stepResult, db.LastErrorMessage, sql);
                        if (attempt == 0 && IsSchemaChangedPreparedStatementError(ex))
                        {
                            _connection.ClearPreparedStatementCacheForSchemaChange();
                            continue;
                        }

                        throw ex;
                    }

                    value = stepResult == 1 ? stmt.GetValueObject(0) : null;
                    if (observation != null)
                    {
                        _connection.CompleteSqlObservation(observation, stepResult == 1 ? 1 : 0, exception: null);
                    }

                    return true;
                }
            }
            catch (Exception ex)
            {
                if (observation != null)
                {
                    _connection.CompleteSqlObservation(observation, rowsAffected: 0, ex);
                }

                throw;
            }
        }

        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(ExecuteNonQuery());
        }

        public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(ExecuteScalar());
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            if (_connection == null)
            {
                throw new InvalidOperationException("Command has no connection");
            }

            var db = _connection.GetNativeDb();
            var (sql, paramMap, needsOffsetClamp) = GetRewrittenSqlAndParameters();
            if (needsOffsetClamp)
            {
                SqlParameterRewriter.ClampOffsetParameters(sql, paramMap);
            }
            var observation = StartSqlObservationIfEnabled(_connection, sql, paramMap);
            var canUseSingleInt64Step = TryGetSingleInt64Parameter(paramMap, out var fastIndex, out var fastValue);

            PreparedStatement? stmt = null;
            var usingPreparedStatementCache = false;
            try
            {
                for (var attempt = 0; ; attempt++)
                {
                    stmt = null;
                    usingPreparedStatementCache = false;
                    if (GetSplitStatements().Count <= 1)
                    {
                        stmt = EnsurePreparedStatement(sql, resetForExecution: !canUseSingleInt64Step);
                        usingPreparedStatementCache = true;
                    }
                    else
                    {
                        stmt = db.Prepare(sql);
                    }

                    if (canUseSingleInt64Step)
                    {
                        var knownMaxOneRow = TryGetCachedSingleRowReadPlan(
                            sql,
                            fastIndex,
                            out var singleRowPlan) && singleRowPlan.KnownMaxOneRow;

                        _statement = stmt;
                        _statementCanSkipFinalizeReset = true;

                        var fastStepResult = stmt.BindInt64StepAndCaptureRowView(fastIndex, fastValue);
                        if (fastStepResult < 0)
                        {
                            _statement = null;
                            _statementCanSkipFinalizeReset = false;

                            if (usingPreparedStatementCache)
                            {
                                InvalidatePreparedStatement(discardFromConnectionCache: true);
                            }
                            else
                            {
                                stmt.Dispose();
                            }

                            var ex = new DecentDBException(fastStepResult, db.LastErrorMessage, sql);
                            if (attempt == 0 && IsSchemaChangedPreparedStatementError(ex))
                            {
                                _cachedSingleRowReadPlan = null;
                                _connection.ClearPreparedStatementCacheForSchemaChange();
                                continue;
                            }

                            throw ex;
                        }

                        return new DecentDBDataReader(this, stmt, fastStepResult, observation, knownMaxOneRow);
                    }

                    _statementCanSkipFinalizeReset = false;
                    foreach (var kvp in paramMap)
                    {
                        BindParameter(stmt, kvp.Key, kvp.Value);
                    }

                    _statement = stmt;

                    var stepResult = stmt.Step();
                    if (stepResult < 0)
                    {
                        _statement = null;
                        _statementCanSkipFinalizeReset = false;

                        if (usingPreparedStatementCache)
                        {
                            InvalidatePreparedStatement(discardFromConnectionCache: true);
                        }
                        else
                        {
                            stmt.Dispose();
                        }

                        var ex = new DecentDBException(stepResult, db.LastErrorMessage, sql);
                        if (attempt == 0 && IsSchemaChangedPreparedStatementError(ex))
                        {
                            _cachedSingleRowReadPlan = null;
                            _connection.ClearPreparedStatementCacheForSchemaChange();
                            continue;
                        }

                        throw ex;
                    }

                    return new DecentDBDataReader(this, stmt, stepResult, observation);
                }
            }
            catch (Exception ex)
            {
                if (_statement == null && stmt != null)
                {
                    if (!usingPreparedStatementCache || !ReferenceEquals(stmt, _preparedStatement))
                    {
                        stmt.Dispose();
                    }
                }
                _statement = null;
                _statementCanSkipFinalizeReset = false;

                if (observation != null)
                {
                    _connection.CompleteSqlObservation(observation, rowsAffected: 0, ex);
                }

                throw;
            }
        }

        private static IReadOnlyList<SqlParameterValue> SnapshotParameters(Dictionary<int, DbParameter> paramMap)
        {
            if (paramMap.Count == 0)
            {
                return Array.Empty<SqlParameterValue>();
            }

            var keys = paramMap.Keys.ToArray();
            Array.Sort(keys);

            var values = new SqlParameterValue[keys.Length];
            for (var i = 0; i < keys.Length; i++)
            {
                var ordinal = keys[i];
                var p = paramMap[ordinal];
                var name = string.IsNullOrEmpty(p.ParameterName) ? $"${ordinal}" : p.ParameterName;
                var v = p.Value == DBNull.Value ? null : p.Value;
                values[i] = new SqlParameterValue(ordinal, name, v);
            }

            return values;
        }

        private static SqlObservation? StartSqlObservationIfEnabled(
            DecentDBConnection connection,
            string sql,
            Dictionary<int, DbParameter> paramMap)
        {
            if (!connection.IsSqlObservationEnabled)
            {
                return null;
            }

            return connection.TryStartSqlObservation(sql, SnapshotParameters(paramMap));
        }

        private IReadOnlyList<string> GetSplitStatements()
        {
            if (_cachedSplitStatements != null &&
                string.Equals(_cachedSplitSql, _commandText, StringComparison.Ordinal))
            {
                return _cachedSplitStatements;
            }

            _cachedSplitStatements = SqlStatementSplitter.Split(_commandText);
            _cachedSplitSql = _commandText;
            return _cachedSplitStatements;
        }

        private (string Sql, Dictionary<int, DbParameter> Parameters, bool NeedsOffsetClamp) GetRewrittenSqlAndParameters()
        {
            if (CanReuseRewriteCache())
            {
                return (_cachedRewriteSql!, _cachedRewriteParamMap!, _cachedRewriteNeedsOffsetClamp);
            }

            var (sql, parameters) = SqlParameterRewriter.Rewrite(_commandText, _parameters);
            sql = SqlParameterRewriter.StripUpdateDeleteAlias(sql);
            CaptureRewriteCache(sql, parameters);
            return (sql, parameters, _cachedRewriteNeedsOffsetClamp);
        }

        private bool CanReuseRewriteCache()
        {
            if (_cachedRewriteSql == null ||
                _cachedRewriteParamMap == null ||
                _cachedRewriteSourceSql == null ||
                _cachedRewriteParameterRefs == null ||
                _cachedRewriteParameterNames == null)
            {
                return false;
            }

            if (!string.Equals(_cachedRewriteSourceSql, _commandText, StringComparison.Ordinal))
            {
                return false;
            }

            if (_cachedRewriteParameterRefs.Length != _parameters.Count ||
                _cachedRewriteParameterNames.Length != _parameters.Count)
            {
                return false;
            }

            for (var i = 0; i < _parameters.Count; i++)
            {
                if (!ReferenceEquals(_cachedRewriteParameterRefs[i], _parameters[i]))
                {
                    return false;
                }

                if (!string.Equals(
                        _cachedRewriteParameterNames[i],
                        _parameters[i].ParameterName,
                        StringComparison.Ordinal))
                {
                    return false;
                }
            }

            return true;
        }

        private void CaptureRewriteCache(string rewrittenSql, Dictionary<int, DbParameter> parameters)
        {
            var refs = new DbParameter[_parameters.Count];
            var names = new string?[_parameters.Count];
            for (var i = 0; i < _parameters.Count; i++)
            {
                refs[i] = _parameters[i];
                names[i] = _parameters[i].ParameterName;
            }

            _cachedRewriteSourceSql = _commandText;
            _cachedRewriteSql = rewrittenSql;
            _cachedRewriteParamMap = parameters;
            _cachedRewriteParameterRefs = refs;
            _cachedRewriteParameterNames = names;
            _cachedRewriteNeedsOffsetClamp = rewrittenSql.IndexOf("OFFSET", StringComparison.OrdinalIgnoreCase) >= 0;
        }

        private void InvalidateSplitCache()
        {
            _cachedSplitSql = null;
            _cachedSplitStatements = null;
        }

        private void InvalidateRewriteCache()
        {
            _cachedRewriteSourceSql = null;
            _cachedRewriteSql = null;
            _cachedRewriteParamMap = null;
            _cachedRewriteParameterRefs = null;
            _cachedRewriteParameterNames = null;
            _cachedRewriteNeedsOffsetClamp = false;
            _cachedSingleRowReadPlan = null;
            _cachedSingleInt64NonQueryPlan = null;
            _cachedInt64TextFloat64NonQueryPlan = null;
        }

        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(ExecuteDbDataReader(behavior));
        }

        protected override DbParameter CreateDbParameter()
        {
            return CreateParameter();
        }

        public new DecentDBParameter CreateParameter()
        {
            var param = new DecentDBParameter();
            return param;
        }

        public override void Prepare()
        {
            if (_connection == null || _connection.State != ConnectionState.Open)
            {
                throw new InvalidOperationException("Connection must be open to prepare command");
            }

            var statements = GetSplitStatements();
            if (statements.Count != 1)
            {
                return;
            }

            var (sql, paramMap, needsOffsetClamp) = GetRewrittenSqlAndParameters();
            if (needsOffsetClamp)
            {
                SqlParameterRewriter.ClampOffsetParameters(sql, paramMap);
            }

            EnsurePreparedStatement(sql, resetForExecution: false);
        }

        internal static void BindParameter(PreparedStatement stmt, int index1Based, DbParameter parameter)
        {
            var value = parameter.Value;
            if (value == null || value == DBNull.Value)
            {
                stmt.BindNull(index1Based);
                return;
            }

            if (parameter.DbType == DbType.Guid)
            {
                if (value is Guid parameterGuid)
                {
                    stmt.BindGuid(index1Based, parameterGuid);
                    return;
                }

                if (value is byte[] guidBytes)
                {
                    if (guidBytes.Length != 16)
                    {
                        throw new ArgumentException(
                            "GUID parameters must use 16-byte values.",
                            nameof(parameter));
                    }

                    stmt.BindGuid(index1Based, new Guid(guidBytes));
                    return;
                }

                if (value is string guidText && Guid.TryParse(guidText, out var parsedGuid))
                {
                    stmt.BindGuid(index1Based, parsedGuid);
                    return;
                }

                throw new ArgumentException(
                    $"Unsupported GUID parameter value type: {value.GetType().FullName}",
                    nameof(parameter));
            }

            if (value is long l)
            {
                stmt.BindInt64(index1Based, l);
                return;
            }

            if (value is int i32)
            {
                stmt.BindInt64(index1Based, i32);
                return;
            }

            if (value is short i16)
            {
                stmt.BindInt64(index1Based, i16);
                return;
            }

            if (value is sbyte si8)
            {
                stmt.BindInt64(index1Based, si8);
                return;
            }

            if (value is byte i8)
            {
                stmt.BindInt64(index1Based, i8);
                return;
            }

            if (value is ulong u64)
            {
                if (u64 > long.MaxValue)
                {
                    throw new OverflowException($"UInt64 value {u64} exceeds DecentDB INT64 range.");
                }

                stmt.BindInt64(index1Based, (long)u64);
                return;
            }

            if (value is uint u32)
            {
                stmt.BindInt64(index1Based, u32);
                return;
            }

            if (value is ushort u16)
            {
                stmt.BindInt64(index1Based, u16);
                return;
            }

            if (value is double f64)
            {
                stmt.BindFloat64(index1Based, f64);
                return;
            }

            if (value is float f32)
            {
                stmt.BindFloat64(index1Based, f32);
                return;
            }

            if (value is decimal dec)
            {
                stmt.BindDecimal(index1Based, NormalizeDecimalScale(parameter, dec));
                return;
            }

            if (value is bool b)
            {
                stmt.BindBool(index1Based, b);
                return;
            }

            if (value is string s)
            {
                if (parameter.Size > 0)
                {
                    var byteCount = Encoding.UTF8.GetByteCount(s);
                    if (byteCount > parameter.Size)
                    {
                        throw new ArgumentException($"Value exceeds Size({parameter.Size}) bytes (UTF-8). Actual: {byteCount} bytes.");
                    }
                }

                stmt.BindText(index1Based, s);
                return;
            }

            if (value is char c)
            {
                if (char.IsSurrogate(c))
                {
                    throw new ArgumentException(
                        "Surrogate char values are not supported; use string for code points above U+FFFF.");
                }

                stmt.BindText(index1Based, c.ToString());
                return;
            }

            if (value is DateTime dt)
            {
                var utc = dt.Kind == DateTimeKind.Utc ? dt : dt.ToUniversalTime();
                var micros = (utc.Ticks - DateTime.UnixEpoch.Ticks) / 10L;
                stmt.BindDatetime(index1Based, micros);
                return;
            }

            if (value is DateTimeOffset dto)
            {
                var utc = dto.ToUniversalTime();
                var micros = (utc.UtcTicks - DateTime.UnixEpoch.Ticks) / 10L;
                stmt.BindDatetime(index1Based, micros);
                return;
            }

            if (value is TimeSpan ts)
            {
                stmt.BindInt64(index1Based, ts.Ticks);
                return;
            }

            if (value is DateOnly date)
            {
                var epoch = DateOnly.FromDateTime(DateTime.UnixEpoch);
                stmt.BindInt64(index1Based, date.DayNumber - epoch.DayNumber);
                return;
            }

            if (value is TimeOnly time)
            {
                stmt.BindInt64(index1Based, time.Ticks);
                return;
            }

            if (value is byte[] blob)
            {
                stmt.BindBlob(index1Based, blob);
                return;
            }

            if (value is Guid guid)
            {
                stmt.BindGuid(index1Based, guid);
                return;
            }

            if (value.GetType().IsEnum)
            {
                stmt.BindInt64(index1Based, Convert.ToInt64(value));
                return;
            }

            throw new NotSupportedException($"Unsupported parameter type: {value.GetType().FullName}");
        }

        private static bool TryGetSingleInt64Parameter(
            Dictionary<int, DbParameter> paramMap,
            out int index1Based,
            out long value)
        {
            index1Based = 0;
            value = 0;
            if (paramMap.Count != 1)
            {
                return false;
            }

            foreach (var kvp in paramMap)
            {
                if (!TryGetInt64ParameterValue(kvp.Value.Value, out value))
                {
                    return false;
                }

                index1Based = kvp.Key;
                return true;
            }

            return false;
        }

        private static bool TryGetInt64ParameterValue(object? rawValue, out long value)
        {
            switch (rawValue)
            {
                case long l:
                    value = l;
                    return true;
                case int i32:
                    value = i32;
                    return true;
                case short i16:
                    value = i16;
                    return true;
                case sbyte si8:
                    value = si8;
                    return true;
                case byte u8:
                    value = u8;
                    return true;
                case uint u32:
                    value = u32;
                    return true;
                case ushort u16:
                    value = u16;
                    return true;
                case ulong u64 when u64 <= long.MaxValue:
                    value = (long)u64;
                    return true;
                default:
                    value = 0;
                    return false;
            }
        }

        private bool TryGetCachedSingleRowReadPlan(
            string sql,
            int parameterIndex1Based,
            [NotNullWhen(true)] out SingleRowReadPlan? plan)
        {
            if (_cachedSingleRowReadPlan != null &&
                _cachedSingleRowReadPlan.Matches(_commandText, sql, parameterIndex1Based, _parameters))
            {
                plan = _cachedSingleRowReadPlan;
                return true;
            }

            plan = null;
            if (_connection == null)
            {
                return false;
            }

            var knownMaxOneRow = TryDescribeKnownSingleRowRead(sql, parameterIndex1Based);
            plan = new SingleRowReadPlan(_commandText, sql, parameterIndex1Based, _parameters, knownMaxOneRow);
            _cachedSingleRowReadPlan = plan;
            return true;
        }

        private bool TryDescribeKnownSingleRowRead(string sql, int parameterIndex1Based)
        {
            if (_connection == null)
            {
                return false;
            }

            try
            {
                if (!TryParseSingleTablePrimaryKeyEquality(
                        sql,
                        parameterIndex1Based,
                        out var sourceTable,
                        out var sourceColumn))
                {
                    return false;
                }

                return TableHasSingleColumnPrimaryKey(sourceTable, sourceColumn);
            }
            catch (DecentDBException)
            {
                return false;
            }
            catch (JsonException)
            {
                return false;
            }
            catch (ArgumentException)
            {
                return false;
            }
            catch (InvalidOperationException)
            {
                return false;
            }
        }

        private bool TableHasSingleColumnPrimaryKey(string tableName, string columnName)
        {
            if (_connection == null)
            {
                return false;
            }

            using var columnsDocument = JsonDocument.Parse(_connection.GetTableColumnsJson(tableName));
            if (columnsDocument.RootElement.ValueKind != JsonValueKind.Array)
            {
                return false;
            }

            var primaryKeyCount = 0;
            var matchedPrimaryKey = false;
            foreach (var column in columnsDocument.RootElement.EnumerateArray())
            {
                if (!JsonBoolEquals(column, "primary_key", expected: true))
                {
                    continue;
                }

                primaryKeyCount++;
                if (TryGetNonEmptyJsonString(column, "name", out var name) &&
                    IdentifiersEqual(name, columnName))
                {
                    matchedPrimaryKey = true;
                }
            }

            return primaryKeyCount == 1 && matchedPrimaryKey;
        }

        private static bool JsonBoolEquals(JsonElement element, string propertyName, bool expected)
        {
            return element.TryGetProperty(propertyName, out var property) &&
                property.ValueKind is JsonValueKind.True or JsonValueKind.False &&
                property.GetBoolean() == expected;
        }

        private static bool TryGetNonEmptyJsonString(
            JsonElement element,
            string propertyName,
            [NotNullWhen(true)] out string? value)
        {
            value = null;
            if (!element.TryGetProperty(propertyName, out var property) ||
                property.ValueKind != JsonValueKind.String)
            {
                return false;
            }

            value = property.GetString();
            return !string.IsNullOrWhiteSpace(value);
        }

        private static bool TryParseSingleTablePrimaryKeyEquality(
            string sql,
            int parameterIndex1Based,
            [NotNullWhen(true)] out string? sourceTable,
            [NotNullWhen(true)] out string? sourceColumn)
        {
            sourceTable = null;
            sourceColumn = null;
            var tokens = TokenizeSqlShape(sql);
            var whereIndex = IndexOfTopLevelKeyword(tokens, "where", start: 0, end: tokens.Count);
            if (whereIndex < 0 ||
                !HasSingleTopLevelTableSource(tokens, whereIndex) ||
                !TryGetSingleTopLevelTableName(tokens, whereIndex, out sourceTable))
            {
                return false;
            }

            var start = whereIndex + 1;
            var end = FindWhereClauseEnd(tokens, start);
            TrimTrivia(tokens, ref start, ref end);
            TrimWrappingParentheses(tokens, ref start, ref end);

            return TryMatchColumnParameterEquality(
                tokens,
                start,
                end,
                parameterIndex1Based,
                out sourceColumn);
        }

        private static int IndexOfTopLevelKeyword(
            List<SqlShapeToken> tokens,
            string keyword,
            int start,
            int end)
        {
            var depth = 0;
            for (var i = start; i < end; i++)
            {
                var token = tokens[i];
                if (token.Kind == SqlShapeTokenKind.OpenParen)
                {
                    depth++;
                    continue;
                }

                if (token.Kind == SqlShapeTokenKind.CloseParen)
                {
                    depth = Math.Max(0, depth - 1);
                    continue;
                }

                if (depth == 0 && IsKeyword(token, keyword))
                {
                    return i;
                }
            }

            return -1;
        }

        private static bool HasSingleTopLevelTableSource(List<SqlShapeToken> tokens, int whereIndex)
        {
            var fromIndex = IndexOfTopLevelKeyword(tokens, "from", start: 0, end: whereIndex);
            if (fromIndex < 0)
            {
                return false;
            }

            var depth = 0;
            for (var i = fromIndex + 1; i < whereIndex; i++)
            {
                var token = tokens[i];
                if (token.Kind == SqlShapeTokenKind.OpenParen)
                {
                    if (depth == 0)
                    {
                        return false;
                    }

                    depth++;
                    continue;
                }

                if (token.Kind == SqlShapeTokenKind.CloseParen)
                {
                    depth = Math.Max(0, depth - 1);
                    continue;
                }

                if (depth != 0)
                {
                    continue;
                }

                if (token.Kind == SqlShapeTokenKind.Comma ||
                    IsKeyword(token, "join"))
                {
                    return false;
                }
            }

            return true;
        }

        private static bool TryGetSingleTopLevelTableName(
            List<SqlShapeToken> tokens,
            int whereIndex,
            [NotNullWhen(true)] out string? tableName)
        {
            tableName = null;
            var fromIndex = IndexOfTopLevelKeyword(tokens, "from", start: 0, end: whereIndex);
            if (fromIndex < 0 || fromIndex + 1 >= whereIndex)
            {
                return false;
            }

            var tableToken = tokens[fromIndex + 1];
            if (tableToken.Kind != SqlShapeTokenKind.Identifier)
            {
                return false;
            }

            if (fromIndex + 3 < whereIndex &&
                tokens[fromIndex + 2].Kind == SqlShapeTokenKind.Dot &&
                tokens[fromIndex + 3].Kind == SqlShapeTokenKind.Identifier)
            {
                // GetTableColumnsJson is table-name scoped today; keep this
                // conservative rather than guessing how to resolve schemas.
                return false;
            }

            tableName = tableToken.Text;
            return !string.IsNullOrWhiteSpace(tableName);
        }

        private static int FindWhereClauseEnd(List<SqlShapeToken> tokens, int start)
        {
            var depth = 0;
            for (var i = start; i < tokens.Count; i++)
            {
                var token = tokens[i];
                if (token.Kind == SqlShapeTokenKind.OpenParen)
                {
                    depth++;
                    continue;
                }

                if (token.Kind == SqlShapeTokenKind.CloseParen)
                {
                    depth = Math.Max(0, depth - 1);
                    continue;
                }

                if (depth != 0)
                {
                    continue;
                }

                if (token.Kind == SqlShapeTokenKind.Semicolon ||
                    IsKeyword(token, "group") ||
                    IsKeyword(token, "order") ||
                    IsKeyword(token, "limit") ||
                    IsKeyword(token, "offset") ||
                    IsKeyword(token, "union") ||
                    IsKeyword(token, "except") ||
                    IsKeyword(token, "intersect"))
                {
                    return i;
                }
            }

            return tokens.Count;
        }

        private static void TrimTrivia(List<SqlShapeToken> tokens, ref int start, ref int end)
        {
            while (start < end && tokens[start].Kind == SqlShapeTokenKind.Semicolon)
            {
                start++;
            }

            while (end > start && tokens[end - 1].Kind == SqlShapeTokenKind.Semicolon)
            {
                end--;
            }
        }

        private static void TrimWrappingParentheses(List<SqlShapeToken> tokens, ref int start, ref int end)
        {
            while (end - start >= 2 &&
                tokens[start].Kind == SqlShapeTokenKind.OpenParen &&
                tokens[end - 1].Kind == SqlShapeTokenKind.CloseParen &&
                MatchingCloseParenthesis(tokens, start, end) == end - 1)
            {
                start++;
                end--;
                TrimTrivia(tokens, ref start, ref end);
            }
        }

        private static int MatchingCloseParenthesis(List<SqlShapeToken> tokens, int openIndex, int end)
        {
            var depth = 0;
            for (var i = openIndex; i < end; i++)
            {
                if (tokens[i].Kind == SqlShapeTokenKind.OpenParen)
                {
                    depth++;
                }
                else if (tokens[i].Kind == SqlShapeTokenKind.CloseParen)
                {
                    depth--;
                    if (depth == 0)
                    {
                        return i;
                    }
                }
            }

            return -1;
        }

        private static bool TryMatchColumnParameterEquality(
            List<SqlShapeToken> tokens,
            int start,
            int end,
            int parameterIndex1Based,
            [NotNullWhen(true)] out string? sourceColumn)
        {
            sourceColumn = null;
            var equalsIndex = -1;
            for (var i = start; i < end; i++)
            {
                if (tokens[i].Kind != SqlShapeTokenKind.Equals)
                {
                    continue;
                }

                if (equalsIndex >= 0)
                {
                    return false;
                }

                equalsIndex = i;
            }

            if (equalsIndex < 0)
            {
                return false;
            }

            if (TryGetColumnReferenceName(tokens, start, equalsIndex, out var leftColumn) &&
                IsParameterReference(tokens, equalsIndex + 1, end, parameterIndex1Based))
            {
                sourceColumn = leftColumn;
                return true;
            }

            if (IsParameterReference(tokens, start, equalsIndex, parameterIndex1Based) &&
                TryGetColumnReferenceName(tokens, equalsIndex + 1, end, out var rightColumn))
            {
                sourceColumn = rightColumn;
                return true;
            }

            return false;
        }

        private static bool TryGetColumnReferenceName(
            List<SqlShapeToken> tokens,
            int start,
            int end,
            [NotNullWhen(true)] out string? sourceColumn)
        {
            sourceColumn = null;
            var length = end - start;
            if (length == 1)
            {
                if (tokens[start].Kind != SqlShapeTokenKind.Identifier)
                {
                    return false;
                }

                sourceColumn = tokens[start].Text;
                return !string.IsNullOrWhiteSpace(sourceColumn);
            }

            if (length == 3 &&
                tokens[start].Kind == SqlShapeTokenKind.Identifier &&
                tokens[start + 1].Kind == SqlShapeTokenKind.Dot &&
                tokens[start + 2].Kind == SqlShapeTokenKind.Identifier)
            {
                sourceColumn = tokens[start + 2].Text;
                return !string.IsNullOrWhiteSpace(sourceColumn);
            }

            return false;
        }

        private static bool IsParameterReference(
            List<SqlShapeToken> tokens,
            int start,
            int end,
            int parameterIndex1Based)
        {
            return end - start == 1 &&
                tokens[start].Kind == SqlShapeTokenKind.Parameter &&
                tokens[start].ParameterIndex == parameterIndex1Based;
        }

        private static bool IsKeyword(SqlShapeToken token, string keyword)
        {
            return token.Kind == SqlShapeTokenKind.Identifier &&
                string.Equals(token.Text, keyword, StringComparison.OrdinalIgnoreCase);
        }

        private static bool IdentifiersEqual(string left, string right)
        {
            return string.Equals(left, right, StringComparison.OrdinalIgnoreCase);
        }

        private static List<SqlShapeToken> TokenizeSqlShape(string sql)
        {
            var tokens = new List<SqlShapeToken>();
            for (var i = 0; i < sql.Length;)
            {
                var ch = sql[i];
                if (char.IsWhiteSpace(ch))
                {
                    i++;
                    continue;
                }

                if (ch == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
                {
                    i += 2;
                    while (i < sql.Length && sql[i] != '\n')
                    {
                        i++;
                    }
                    continue;
                }

                if (ch == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
                {
                    i += 2;
                    while (i + 1 < sql.Length && (sql[i] != '*' || sql[i + 1] != '/'))
                    {
                        i++;
                    }
                    i = Math.Min(sql.Length, i + 2);
                    continue;
                }

                if (ch == '\'')
                {
                    i = SkipQuotedString(sql, i, '\'');
                    continue;
                }

                if (ch == '"')
                {
                    var (identifier, next) = ReadDelimitedIdentifier(sql, i, '"', '"');
                    tokens.Add(new SqlShapeToken(SqlShapeTokenKind.Identifier, identifier));
                    i = next;
                    continue;
                }

                if (ch == '[')
                {
                    var (identifier, next) = ReadDelimitedIdentifier(sql, i, '[', ']');
                    tokens.Add(new SqlShapeToken(SqlShapeTokenKind.Identifier, identifier));
                    i = next;
                    continue;
                }

                if (IsIdentifierStart(ch))
                {
                    var start = i;
                    i++;
                    while (i < sql.Length && IsIdentifierPart(sql[i]))
                    {
                        i++;
                    }

                    tokens.Add(new SqlShapeToken(SqlShapeTokenKind.Identifier, sql[start..i]));
                    continue;
                }

                if (ch == '$' && i + 1 < sql.Length && char.IsAsciiDigit(sql[i + 1]))
                {
                    var start = i + 1;
                    i += 2;
                    while (i < sql.Length && char.IsAsciiDigit(sql[i]))
                    {
                        i++;
                    }

                    if (int.TryParse(sql[start..i], NumberStyles.None, CultureInfo.InvariantCulture, out var index))
                    {
                        tokens.Add(new SqlShapeToken(SqlShapeTokenKind.Parameter, parameterIndex: index));
                    }
                    continue;
                }

                tokens.Add(ch switch
                {
                    '=' => new SqlShapeToken(SqlShapeTokenKind.Equals),
                    '.' => new SqlShapeToken(SqlShapeTokenKind.Dot),
                    ',' => new SqlShapeToken(SqlShapeTokenKind.Comma),
                    '(' => new SqlShapeToken(SqlShapeTokenKind.OpenParen),
                    ')' => new SqlShapeToken(SqlShapeTokenKind.CloseParen),
                    ';' => new SqlShapeToken(SqlShapeTokenKind.Semicolon),
                    _ => new SqlShapeToken(SqlShapeTokenKind.Other)
                });
                i++;
            }

            return tokens;
        }

        private static int SkipQuotedString(string sql, int start, char quote)
        {
            var i = start + 1;
            while (i < sql.Length)
            {
                if (sql[i] != quote)
                {
                    i++;
                    continue;
                }

                if (i + 1 < sql.Length && sql[i + 1] == quote)
                {
                    i += 2;
                    continue;
                }

                return i + 1;
            }

            return sql.Length;
        }

        private static (string Identifier, int Next) ReadDelimitedIdentifier(
            string sql,
            int start,
            char open,
            char close)
        {
            var value = new StringBuilder();
            var i = start + 1;
            while (i < sql.Length)
            {
                if (sql[i] != close)
                {
                    value.Append(sql[i]);
                    i++;
                    continue;
                }

                if (open == close && i + 1 < sql.Length && sql[i + 1] == close)
                {
                    value.Append(close);
                    i += 2;
                    continue;
                }

                return (value.ToString(), i + 1);
            }

            return (value.ToString(), sql.Length);
        }

        private static bool IsIdentifierStart(char ch)
        {
            return ch == '_' ||
                (ch >= 'A' && ch <= 'Z') ||
                (ch >= 'a' && ch <= 'z');
        }

        private static bool IsIdentifierPart(char ch)
        {
            return IsIdentifierStart(ch) ||
                (ch >= '0' && ch <= '9');
        }

        private static decimal NormalizeDecimalScale(DbParameter parameter, decimal value)
        {
            if (parameter is not DecentDBParameter decentParameter || !decentParameter.HasScale)
            {
                return value;
            }

            return DecimalScaleNormalizer.Normalize(value, decentParameter.Scale);
        }

        internal void FinalizeStatement()
        {
            if (_statement == null)
            {
                return;
            }

            if (ReferenceEquals(_statement, _preparedStatement))
            {
                if (_statementCanSkipFinalizeReset)
                {
                    _statementCanSkipFinalizeReset = false;
                    _statement = null;
                    return;
                }

                _statement.Reset().ClearBindings();
                _statement = null;
                return;
            }

            _statement.Dispose();
            _statement = null;
            _statementCanSkipFinalizeReset = false;
        }

        private int ExecuteSingleNonQuery()
        {
            if (_connection == null)
            {
                throw new InvalidOperationException("Command has no connection");
            }

            if (_parameters.Count == 0 && TryExecutePragmaNonQuery(out var pragmaRowsAffected))
            {
                return pragmaRowsAffected;
            }

            if (TryExecuteCachedSingleInt64NonQuery(out var singleInt64RowsAffected))
            {
                return singleInt64RowsAffected;
            }

            if (TryExecuteCachedInt64TextFloat64NonQuery(out var fastRowsAffected))
            {
                return fastRowsAffected;
            }

            var (sql, paramMap, needsOffsetClamp) = GetRewrittenSqlAndParameters();
            if (needsOffsetClamp)
            {
                SqlParameterRewriter.ClampOffsetParameters(sql, paramMap);
            }

            var observation = StartSqlObservationIfEnabled(_connection, sql, paramMap);

            try
            {
                for (var attempt = 0; ; attempt++)
                {
                    var stmt = EnsurePreparedStatement(sql, resetForExecution: false);

                    int rowsAffected;
                    try
                    {
                        if (!TryExecuteInt64TextFloat64SingleNonQuery(stmt, paramMap, out rowsAffected) &&
                            !TryExecuteTypedSingleNonQuery(stmt, paramMap, out rowsAffected))
                        {
                            stmt.Reset().ClearBindings();
                            foreach (var kvp in paramMap)
                            {
                                BindParameter(stmt, kvp.Key, kvp.Value);
                            }

                            var stepResult = stmt.Step();
                            while (stepResult == 1)
                            {
                                stepResult = stmt.Step();
                            }

                            if (stepResult < 0)
                            {
                                throw new DecentDBException(
                                    stepResult,
                                    _connection.GetNativeDb().LastErrorMessage,
                                    sql);
                            }

                            rowsAffected = (int)stmt.RowsAffected;
                            stmt.Reset().ClearBindings();
                        }
                    }
                    catch (DecentDBException ex)
                    {
                        InvalidatePreparedStatement(discardFromConnectionCache: true);
                        if (attempt == 0 && IsSchemaChangedPreparedStatementError(ex))
                        {
                            _connection.ClearPreparedStatementCacheForSchemaChange();
                            continue;
                        }

                        throw;
                    }

                    if (observation != null)
                    {
                        _connection.CompleteSqlObservation(observation, rowsAffected, exception: null);
                    }

                    return rowsAffected;
                }
            }
            catch (Exception ex)
            {
                InvalidatePreparedStatement(discardFromConnectionCache: true);

                if (observation != null)
                {
                    _connection.CompleteSqlObservation(observation, rowsAffected: 0, ex);
                }

                throw;
            }
        }

        private bool TryExecuteInt64TextFloat64SingleNonQuery(
            PreparedStatement stmt,
            Dictionary<int, DbParameter> paramMap,
            out int rowsAffected)
        {
            rowsAffected = 0;
            if (paramMap.Count != 3 ||
                !paramMap.TryGetValue(1, out var intParameter) ||
                !paramMap.TryGetValue(2, out var textParameter) ||
                !paramMap.TryGetValue(3, out var floatParameter))
            {
                return false;
            }

            return TryExecuteInt64TextFloat64SingleNonQuery(
                stmt,
                intParameter,
                textParameter,
                floatParameter,
                out rowsAffected);
        }

        private bool TryExecuteCachedSingleInt64NonQuery(out int rowsAffected)
        {
            rowsAffected = 0;
            if (_connection == null ||
                _connection.IsSqlObservationEnabled ||
                !TryGetCachedSingleInt64NonQueryPlan(out var plan))
            {
                return false;
            }

            for (var attempt = 0; ; attempt++)
            {
                var stmt = EnsurePreparedStatement(plan.Sql, resetForExecution: false);
                var rawValue = plan.Parameter.Value;
                if (rawValue == null ||
                    rawValue == DBNull.Value ||
                    !TryGetOptimizedInt64(plan.Parameter, rawValue, out var intValue))
                {
                    return false;
                }

                try
                {
                    rowsAffected = checked((int)stmt.RebindInt64Execute(intValue));
                    return true;
                }
                catch (DecentDBException ex)
                {
                    InvalidatePreparedStatement(discardFromConnectionCache: true);
                    if (attempt == 0 && IsSchemaChangedPreparedStatementError(ex))
                    {
                        _connection.ClearPreparedStatementCacheForSchemaChange();
                        continue;
                    }

                    throw;
                }
                catch
                {
                    InvalidatePreparedStatement(discardFromConnectionCache: true);
                    throw;
                }
            }
        }

        private bool TryGetCachedSingleInt64NonQueryPlan(
            [NotNullWhen(true)] out SingleInt64NonQueryPlan? plan)
        {
            if (_cachedSingleInt64NonQueryPlan != null &&
                _cachedSingleInt64NonQueryPlan.Matches(_commandText, _parameters))
            {
                plan = _cachedSingleInt64NonQueryPlan;
                return true;
            }

            plan = null;
            if (_parameters.Count != 1 || GetSplitStatements().Count != 1)
            {
                return false;
            }

            var (sql, paramMap, needsOffsetClamp) = GetRewrittenSqlAndParameters();
            if (needsOffsetClamp ||
                paramMap.Count != 1 ||
                !paramMap.TryGetValue(1, out var parameter))
            {
                return false;
            }

            var value = parameter.Value;
            if (value == null ||
                value == DBNull.Value ||
                !TryGetOptimizedInt64(parameter, value, out _))
            {
                return false;
            }

            plan = new SingleInt64NonQueryPlan(_commandText, sql, _parameters[0], parameter);
            _cachedSingleInt64NonQueryPlan = plan;
            return true;
        }

        private bool TryExecuteCachedInt64TextFloat64NonQuery(out int rowsAffected)
        {
            rowsAffected = 0;
            if (_connection == null ||
                _connection.IsSqlObservationEnabled ||
                !TryGetCachedInt64TextFloat64NonQueryPlan(out var plan))
            {
                return false;
            }

            for (var attempt = 0; ; attempt++)
            {
                var stmt = EnsurePreparedStatement(plan.Sql, resetForExecution: false);
                try
                {
                    if (!TryExecuteInt64TextFloat64SingleNonQuery(
                            stmt,
                            plan.IntParameter,
                            plan.TextParameter,
                            plan.FloatParameter,
                            out rowsAffected))
                    {
                        return false;
                    }

                    return true;
                }
                catch (DecentDBException ex)
                {
                    InvalidatePreparedStatement(discardFromConnectionCache: true);
                    if (attempt == 0 && IsSchemaChangedPreparedStatementError(ex))
                    {
                        _connection.ClearPreparedStatementCacheForSchemaChange();
                        continue;
                    }

                    throw;
                }
                catch
                {
                    InvalidatePreparedStatement(discardFromConnectionCache: true);
                    throw;
                }
            }
        }

        private bool TryGetCachedInt64TextFloat64NonQueryPlan(
            [NotNullWhen(true)] out Int64TextFloat64NonQueryPlan? plan)
        {
            if (_cachedInt64TextFloat64NonQueryPlan != null &&
                _cachedInt64TextFloat64NonQueryPlan.Matches(_commandText, _parameters))
            {
                plan = _cachedInt64TextFloat64NonQueryPlan;
                return true;
            }

            plan = null;
            if (_parameters.Count != 3 || GetSplitStatements().Count != 1)
            {
                return false;
            }

            var (sql, paramMap, needsOffsetClamp) = GetRewrittenSqlAndParameters();
            if (needsOffsetClamp ||
                paramMap.Count != 3 ||
                !paramMap.TryGetValue(1, out var intParameter) ||
                !paramMap.TryGetValue(2, out var textParameter) ||
                !paramMap.TryGetValue(3, out var floatParameter))
            {
                return false;
            }

            plan = new Int64TextFloat64NonQueryPlan(
                _commandText,
                sql,
                _parameters,
                intParameter,
                textParameter,
                floatParameter);
            _cachedInt64TextFloat64NonQueryPlan = plan;
            return true;
        }

        private bool TryExecuteInt64TextFloat64SingleNonQuery(
            PreparedStatement stmt,
            DbParameter intParameter,
            DbParameter textParameter,
            DbParameter floatParameter,
            out int rowsAffected)
        {
            rowsAffected = 0;
            var intRaw = intParameter.Value;
            var textRaw = textParameter.Value;
            var floatRaw = floatParameter.Value;
            if (intRaw == null ||
                intRaw == DBNull.Value ||
                textRaw == null ||
                textRaw == DBNull.Value ||
                floatRaw == null ||
                floatRaw == DBNull.Value)
            {
                return false;
            }

            if (!TryGetOptimizedInt64(intParameter, intRaw, out var intValue) ||
                !TryGetOptimizedFloat64(floatRaw, out var floatValue) ||
                textParameter.DbType == DbType.Guid ||
                textRaw is not string text)
            {
                return false;
            }

            if (textParameter.Size > 0)
            {
                var byteCount = Encoding.UTF8.GetByteCount(text);
                if (byteCount > textParameter.Size)
                {
                    throw new ArgumentException(
                        $"Value exceeds Size({textParameter.Size}) bytes (UTF-8). Actual: {byteCount} bytes.");
                }
            }

            var maxByteCount = Encoding.UTF8.GetMaxByteCount(text.Length);
            if (maxByteCount <= StackallocTextByteLimit)
            {
                Span<byte> textBuffer = stackalloc byte[maxByteCount];
                var actualByteCount = EncodeUtf8Parameter(text, textBuffer);
                rowsAffected = checked((int)stmt.ExecuteBatchInt64TextFloat64OneRow(
                    intValue,
                    textBuffer[..actualByteCount],
                    floatValue));
                return true;
            }

            rowsAffected = checked((int)stmt.ExecuteBatchInt64TextFloat64OneRow(
                intValue,
                Encoding.UTF8.GetBytes(text),
                floatValue));
            return true;
        }

        private static int EncodeUtf8Parameter(string text, Span<byte> destination)
        {
            for (var i = 0; i < text.Length; i++)
            {
                var ch = text[i];
                if (ch > 0x7F)
                {
                    return Encoding.UTF8.GetBytes(text.AsSpan(), destination);
                }

                destination[i] = (byte)ch;
            }

            return text.Length;
        }

        private bool TryExecuteTypedSingleNonQuery(
            PreparedStatement stmt,
            Dictionary<int, DbParameter> paramMap,
            out int rowsAffected)
        {
            rowsAffected = 0;
            if (paramMap.Count == 0 || paramMap.Count > 8)
            {
                return false;
            }

            Span<byte> signatureUtf8 = stackalloc byte[paramMap.Count + 1];
            Span<long> i64Values = stackalloc long[paramMap.Count];
            Span<double> f64Values = stackalloc double[paramMap.Count];
            byte[]? text0 = null;
            byte[]? text1 = null;
            byte[]? text2 = null;
            var i64Count = 0;
            var f64Count = 0;
            var textCount = 0;

            for (var ordinal = 1; ordinal <= paramMap.Count; ordinal++)
            {
                if (!paramMap.TryGetValue(ordinal, out var parameter))
                {
                    return false;
                }

                var value = parameter.Value;
                if (value == null || value == DBNull.Value)
                {
                    return false;
                }

                if (TryGetOptimizedInt64(parameter, value, out var intValue))
                {
                    signatureUtf8[ordinal - 1] = (byte)'i';
                    i64Values[i64Count++] = intValue;
                    continue;
                }

                if (TryGetOptimizedBoolean(parameter, value, out var boolValue))
                {
                    signatureUtf8[ordinal - 1] = (byte)'b';
                    i64Values[i64Count++] = boolValue ? 1 : 0;
                    continue;
                }

                if (TryGetOptimizedFloat64(value, out var floatValue))
                {
                    signatureUtf8[ordinal - 1] = (byte)'f';
                    f64Values[f64Count++] = floatValue;
                    continue;
                }

                if (TryGetOptimizedTextBytes(parameter, value, out var textBytes))
                {
                    signatureUtf8[ordinal - 1] = (byte)'t';
                    if (textCount == 0)
                    {
                        text0 = textBytes;
                    }
                    else if (textCount == 1)
                    {
                        text1 = textBytes;
                    }
                    else if (textCount == 2)
                    {
                        text2 = textBytes;
                    }
                    else
                    {
                        return false;
                    }

                    textCount++;
                    continue;
                }

                return false;
            }

            signatureUtf8[paramMap.Count] = 0;
            rowsAffected = checked((int)stmt.ExecuteBatchTypedOneRow(
                signatureUtf8,
                i64Values[..i64Count],
                f64Values[..f64Count],
                text0,
                text1,
                text2,
                textCount));
            return true;
        }

        private static bool TryGetOptimizedInt64(
            DbParameter parameter,
            object value,
            out long result)
        {
            result = default;
            if (parameter.DbType == DbType.Guid)
            {
                return false;
            }

            switch (value)
            {
                case long int64:
                    result = int64;
                    return true;
                case int int32:
                    result = int32;
                    return true;
                case short int16:
                    result = int16;
                    return true;
                case sbyte int8:
                    result = int8;
                    return true;
                case byte uint8:
                    result = uint8;
                    return true;
                case uint uint32:
                    result = uint32;
                    return true;
                case ushort uint16:
                    result = uint16;
                    return true;
                case ulong uint64 when uint64 <= long.MaxValue:
                    result = (long)uint64;
                    return true;
                default:
                    return false;
            }
        }

        private static bool TryGetOptimizedBoolean(
            DbParameter parameter,
            object value,
            out bool result)
        {
            result = default;
            if (parameter.DbType == DbType.Guid)
            {
                return false;
            }

            if (value is bool boolValue)
            {
                result = boolValue;
                return true;
            }

            return false;
        }

        private static bool TryGetOptimizedFloat64(object value, out double result)
        {
            switch (value)
            {
                case double float64:
                    result = float64;
                    return true;
                case float float32:
                    result = float32;
                    return true;
                default:
                    result = default;
                    return false;
            }
        }

        private static bool TryGetOptimizedTextBytes(
            DbParameter parameter,
            object value,
            out byte[]? utf8Bytes)
        {
            utf8Bytes = null;
            if (parameter.DbType == DbType.Guid)
            {
                return false;
            }

            if (value is not string text)
            {
                return false;
            }

            if (parameter.Size > 0)
            {
                var byteCount = Encoding.UTF8.GetByteCount(text);
                if (byteCount > parameter.Size)
                {
                    throw new ArgumentException(
                        $"Value exceeds Size({parameter.Size}) bytes (UTF-8). Actual: {byteCount} bytes.");
                }
            }

            utf8Bytes = Encoding.UTF8.GetBytes(text);
            return true;
        }

        private bool TryExecutePragmaNonQuery(out int rowsAffected)
        {
            rowsAffected = 0;
            if (_connection == null)
            {
                return false;
            }

            if (!TryParsePragma(_commandText, out var pragmaName, out var pragmaArgument))
            {
                return false;
            }

            if (!pragmaName.Equals("wal_checkpoint", StringComparison.OrdinalIgnoreCase) &&
                !pragmaName.Equals("journal_mode", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            var observation = StartSqlObservationIfEnabled(_connection, _commandText, new Dictionary<int, DbParameter>());
            try
            {
                if (pragmaName.Equals("wal_checkpoint", StringComparison.OrdinalIgnoreCase))
                {
                    _connection.Checkpoint();
                }
                else if (!string.IsNullOrWhiteSpace(pragmaArgument))
                {
                    _ = NormalizePragmaJournalMode(pragmaArgument!);
                }

                if (observation != null)
                {
                    _connection.CompleteSqlObservation(observation, rowsAffected, exception: null);
                }

                return true;
            }
            catch (Exception ex)
            {
                if (observation != null)
                {
                    _connection.CompleteSqlObservation(observation, rowsAffected: 0, ex);
                }

                throw;
            }
        }

        private bool TryExecutePragmaScalar(out object? value)
        {
            value = null;
            if (_connection == null)
            {
                return false;
            }

            if (!TryParsePragma(_commandText, out var pragmaName, out var pragmaArgument) ||
                !pragmaName.Equals("journal_mode", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            var observation = StartSqlObservationIfEnabled(_connection, _commandText, new Dictionary<int, DbParameter>());
            try
            {
                value = string.IsNullOrWhiteSpace(pragmaArgument)
                    ? "WAL"
                    : NormalizePragmaJournalMode(pragmaArgument!);

                if (observation != null)
                {
                    _connection.CompleteSqlObservation(observation, rowsAffected: 0, exception: null);
                }

                return true;
            }
            catch (Exception ex)
            {
                if (observation != null)
                {
                    _connection.CompleteSqlObservation(observation, rowsAffected: 0, ex);
                }

                throw;
            }
        }

        private static string NormalizePragmaJournalMode(string journalModeToken)
        {
            var normalized = journalModeToken.Trim().Trim('"', '\'').ToUpperInvariant();
            return normalized switch
            {
                "WAL" => "WAL",
                "DELETE" => "DELETE",
                _ => throw new NotSupportedException($"Unsupported PRAGMA journal_mode value '{journalModeToken}'.")
            };
        }

        private static bool TryParsePragma(string sql, out string pragmaName, out string? pragmaArgument)
        {
            pragmaName = string.Empty;
            pragmaArgument = null;

            if (string.IsNullOrWhiteSpace(sql))
            {
                return false;
            }

            var trimmed = sql.Trim();
            if (trimmed.EndsWith(';'))
            {
                trimmed = trimmed[..^1].TrimEnd();
            }

            if (!trimmed.StartsWith("PRAGMA", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            var pragmaBody = trimmed["PRAGMA".Length..].Trim();
            if (pragmaBody.Length == 0)
            {
                return false;
            }

            string pragmaNamePart = pragmaBody;
            string? pragmaArgumentPart = null;

            var equalsIndex = pragmaBody.IndexOf('=');
            var openParenIndex = pragmaBody.IndexOf('(');
            if (openParenIndex >= 0)
            {
                pragmaNamePart = pragmaBody[..openParenIndex].Trim();
                var closeParenIndex = pragmaBody.LastIndexOf(')');
                pragmaArgumentPart = closeParenIndex > openParenIndex
                    ? pragmaBody[(openParenIndex + 1)..closeParenIndex].Trim()
                    : pragmaBody[(openParenIndex + 1)..].Trim();
            }
            else if (equalsIndex >= 0)
            {
                pragmaNamePart = pragmaBody[..equalsIndex].Trim();
                pragmaArgumentPart = pragmaBody[(equalsIndex + 1)..].Trim();
            }

            var dotIndex = pragmaNamePart.LastIndexOf('.');
            if (dotIndex >= 0 && dotIndex < pragmaNamePart.Length - 1)
            {
                pragmaNamePart = pragmaNamePart[(dotIndex + 1)..];
            }

            if (pragmaNamePart.Length == 0)
            {
                return false;
            }

            pragmaName = pragmaNamePart;
            pragmaArgument = string.IsNullOrWhiteSpace(pragmaArgumentPart) ? null : pragmaArgumentPart;
            return true;
        }

        private readonly struct SqlShapeToken
        {
            public SqlShapeToken(
                SqlShapeTokenKind kind,
                string text = "",
                int parameterIndex = 0)
            {
                Kind = kind;
                Text = text;
                ParameterIndex = parameterIndex;
            }

            public SqlShapeTokenKind Kind { get; }

            public string Text { get; }

            public int ParameterIndex { get; }
        }

        private enum SqlShapeTokenKind
        {
            Identifier,
            Parameter,
            Equals,
            Dot,
            Comma,
            OpenParen,
            CloseParen,
            Semicolon,
            Other
        }

        private sealed class SingleRowReadPlan
        {
            private readonly DbParameter[] _parameterRefs;
            private readonly string?[] _parameterNames;

            public SingleRowReadPlan(
                string sourceSql,
                string sql,
                int parameterIndex1Based,
                IReadOnlyList<DecentDBParameter> parameters,
                bool knownMaxOneRow)
            {
                SourceSql = sourceSql;
                Sql = sql;
                ParameterIndex1Based = parameterIndex1Based;
                KnownMaxOneRow = knownMaxOneRow;
                _parameterRefs = new DbParameter[parameters.Count];
                _parameterNames = new string?[parameters.Count];
                for (var i = 0; i < parameters.Count; i++)
                {
                    _parameterRefs[i] = parameters[i];
                    _parameterNames[i] = parameters[i].ParameterName;
                }
            }

            public string SourceSql { get; }

            public string Sql { get; }

            public int ParameterIndex1Based { get; }

            public bool KnownMaxOneRow { get; }

            public bool Matches(
                string sourceSql,
                string sql,
                int parameterIndex1Based,
                IReadOnlyList<DecentDBParameter> parameters)
            {
                if (!string.Equals(SourceSql, sourceSql, StringComparison.Ordinal) ||
                    !string.Equals(Sql, sql, StringComparison.Ordinal) ||
                    ParameterIndex1Based != parameterIndex1Based ||
                    _parameterRefs.Length != parameters.Count ||
                    _parameterNames.Length != parameters.Count)
                {
                    return false;
                }

                for (var i = 0; i < parameters.Count; i++)
                {
                    if (!ReferenceEquals(_parameterRefs[i], parameters[i]) ||
                        !string.Equals(_parameterNames[i], parameters[i].ParameterName, StringComparison.Ordinal))
                    {
                        return false;
                    }
                }

                return true;
            }
        }

        private sealed class SingleInt64NonQueryPlan
        {
            public SingleInt64NonQueryPlan(
                string sourceSql,
                string sql,
                DecentDBParameter collectionParameter,
                DbParameter parameter)
            {
                SourceSql = sourceSql;
                Sql = sql;
                CollectionParameter = collectionParameter;
                CollectionParameterName = collectionParameter.ParameterName;
                Parameter = parameter;
                ParameterName = parameter.ParameterName;
            }

            public string SourceSql { get; }

            public string Sql { get; }

            public DbParameter Parameter { get; }

            private DecentDBParameter CollectionParameter { get; }

            private string CollectionParameterName { get; }

            private string ParameterName { get; }

            public bool Matches(string commandText, IReadOnlyList<DecentDBParameter> parameters)
            {
                return parameters.Count == 1 &&
                       string.Equals(SourceSql, commandText, StringComparison.Ordinal) &&
                       ReferenceEquals(CollectionParameter, parameters[0]) &&
                       string.Equals(CollectionParameterName, parameters[0].ParameterName, StringComparison.Ordinal) &&
                       string.Equals(ParameterName, Parameter.ParameterName, StringComparison.Ordinal);
            }
        }

        private sealed class Int64TextFloat64NonQueryPlan
        {
            public Int64TextFloat64NonQueryPlan(
                string sourceSql,
                string sql,
                IReadOnlyList<DecentDBParameter> parameterCollection,
                DbParameter intParameter,
                DbParameter textParameter,
                DbParameter floatParameter)
            {
                SourceSql = sourceSql;
                Sql = sql;
                Parameter0 = parameterCollection[0];
                Parameter1 = parameterCollection[1];
                Parameter2 = parameterCollection[2];
                IntParameter = intParameter;
                TextParameter = textParameter;
                FloatParameter = floatParameter;
                Parameter0Name = parameterCollection[0].ParameterName;
                Parameter1Name = parameterCollection[1].ParameterName;
                Parameter2Name = parameterCollection[2].ParameterName;
                IntParameterName = intParameter.ParameterName;
                TextParameterName = textParameter.ParameterName;
                FloatParameterName = floatParameter.ParameterName;
            }

            public string SourceSql { get; }

            public string Sql { get; }

            public DbParameter IntParameter { get; }

            public DbParameter TextParameter { get; }

            public DbParameter FloatParameter { get; }

            private DbParameter Parameter0 { get; }

            private DbParameter Parameter1 { get; }

            private DbParameter Parameter2 { get; }

            private string Parameter0Name { get; }

            private string Parameter1Name { get; }

            private string Parameter2Name { get; }

            private string IntParameterName { get; }

            private string TextParameterName { get; }

            private string FloatParameterName { get; }

            public bool Matches(string commandText, IReadOnlyList<DecentDBParameter> parameters)
            {
                return parameters.Count == 3 &&
                       string.Equals(SourceSql, commandText, StringComparison.Ordinal) &&
                       ReferenceEquals(Parameter0, parameters[0]) &&
                       ReferenceEquals(Parameter1, parameters[1]) &&
                       ReferenceEquals(Parameter2, parameters[2]) &&
                       string.Equals(Parameter0Name, parameters[0].ParameterName, StringComparison.Ordinal) &&
                       string.Equals(Parameter1Name, parameters[1].ParameterName, StringComparison.Ordinal) &&
                       string.Equals(Parameter2Name, parameters[2].ParameterName, StringComparison.Ordinal) &&
                       string.Equals(IntParameterName, IntParameter.ParameterName, StringComparison.Ordinal) &&
                       string.Equals(TextParameterName, TextParameter.ParameterName, StringComparison.Ordinal) &&
                       string.Equals(FloatParameterName, FloatParameter.ParameterName, StringComparison.Ordinal);
            }
        }

        private PreparedStatement EnsurePreparedStatement(string sql, bool resetForExecution)
        {
            if (_connection == null)
            {
                throw new InvalidOperationException("Command has no connection");
            }

            var nativeDb = _connection.GetNativeDb();
            if (_preparedStatement != null &&
                ReferenceEquals(_preparedDb, nativeDb) &&
                string.Equals(_preparedSql, sql, StringComparison.Ordinal))
            {
                if (resetForExecution)
                {
                    _preparedStatement.Reset().ClearBindings();
                }

                return _preparedStatement;
            }

            InvalidatePreparedStatement();

            _preparedStatement = _connection.GetOrAddPreparedStatement(sql);
            _preparedSql = sql;
            _preparedDb = nativeDb;
            _preparedStatementFromConnectionCache = true;
            if (resetForExecution)
            {
                _preparedStatement.Reset().ClearBindings();
            }
            return _preparedStatement;
        }

        private void InvalidatePreparedStatement(bool discardFromConnectionCache = false)
        {
            if (ReferenceEquals(_statement, _preparedStatement))
            {
                _statement = null;
            }
            _statementCanSkipFinalizeReset = false;

            if (_preparedStatement != null)
            {
                if (_preparedStatementFromConnectionCache &&
                    discardFromConnectionCache &&
                    _connection != null &&
                    _preparedSql != null)
                {
                    _connection.InvalidateCachedPreparedStatement(_preparedSql, _preparedStatement);
                }
                else if (!_preparedStatementFromConnectionCache)
                {
                    _preparedStatement.Dispose();
                }
            }

            _preparedStatement = null;
            _preparedSql = null;
            _preparedDb = null;
            _preparedStatementFromConnectionCache = false;
        }

        private static bool IsSchemaChangedPreparedStatementError(DecentDBException ex)
        {
            return (ex.ErrorCode == -5 || ex.ErrorCode == 5) &&
                ex.Message.Contains(
                    "prepared statement is no longer valid because the schema changed",
                    StringComparison.Ordinal);
        }

        protected override void Dispose(bool disposing)
        {
            if (_disposed) return;

            if (disposing)
            {
                FinalizeStatement();
                InvalidatePreparedStatement();
                InvalidateSplitCache();
                InvalidateRewriteCache();
            }

            _disposed = true;
            base.Dispose(disposing);
        }
    }
}
