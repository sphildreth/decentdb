using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics.CodeAnalysis;
using DecentDB.Native;

namespace DecentDB.AdoNet
{
    public sealed class DecentDBCommand : DbCommand
    {
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
        private string? _cachedSplitSql;
        private List<string>? _cachedSplitStatements;
        private string? _cachedRewriteSourceSql;
        private string? _cachedRewriteSql;
        private Dictionary<int, DbParameter>? _cachedRewriteParamMap;
        private DbParameter[]? _cachedRewriteParameterRefs;
        private string?[]? _cachedRewriteParameterNames;
        private bool _cachedRewriteNeedsOffsetClamp;
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
            using var reader = ExecuteDbDataReader(CommandBehavior.Default);
            if (reader.Read())
            {
                return reader[0];
            }
            return null;
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

            PreparedStatement? stmt = null;
            var usingPreparedStatementCache = false;
            try
            {
                if (GetSplitStatements().Count <= 1)
                {
                    stmt = EnsurePreparedStatement(sql, resetForExecution: true);
                    usingPreparedStatementCache = true;
                }
                else
                {
                    stmt = db.Prepare(sql);
                }

                foreach (var kvp in paramMap)
                {
                    BindParameter(stmt, kvp.Key, kvp.Value);
                }

                _statement = stmt;

                var stepResult = stmt.Step();
                if (stepResult < 0)
                {
                    _statement = null;

                    if (usingPreparedStatementCache)
                    {
                        InvalidatePreparedStatement(discardFromConnectionCache: true);
                    }
                    else
                    {
                        stmt.Dispose();
                    }

                    throw new DecentDBException(stepResult, db.LastErrorMessage, sql);
                }

                return new DecentDBDataReader(this, stmt, stepResult, observation);
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

            if (value is byte i8)
            {
                stmt.BindInt64(index1Based, i8);
                return;
            }

            if (value is ulong u64)
            {
                stmt.BindInt64(index1Based, unchecked((long)u64));
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
                stmt.BindBlob(index1Based, guid.ToByteArray());
                return;
            }

            if (value.GetType().IsEnum)
            {
                stmt.BindInt64(index1Based, Convert.ToInt64(value));
                return;
            }

            throw new NotSupportedException($"Unsupported parameter type: {value.GetType().FullName}");
        }

        private static decimal NormalizeDecimalScale(DbParameter parameter, decimal value)
        {
            if (parameter is not DecentDBParameter decentParameter || !decentParameter.HasScale)
            {
                return value;
            }

            return NormalizeDecimalScale(value, decentParameter.Scale);
        }

        private static decimal NormalizeDecimalScale(decimal value, int scale)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(scale);

            if (scale > 28)
            {
                throw new ArgumentOutOfRangeException(nameof(scale), "Decimal scale must be between 0 and 28.");
            }

            var rounded = decimal.Round(value, scale, MidpointRounding.ToEven);
            var normalized = rounded.ToString($"F{scale}", CultureInfo.InvariantCulture);
            return decimal.Parse(normalized, NumberStyles.Number, CultureInfo.InvariantCulture);
        }

        internal void FinalizeStatement()
        {
            if (_statement == null)
            {
                return;
            }

            if (ReferenceEquals(_statement, _preparedStatement))
            {
                _statement.Reset().ClearBindings();
                _statement = null;
                return;
            }

            _statement.Dispose();
            _statement = null;
        }

        private int ExecuteSingleNonQuery()
        {
            if (_connection == null)
            {
                throw new InvalidOperationException("Command has no connection");
            }

            var (sql, paramMap, needsOffsetClamp) = GetRewrittenSqlAndParameters();
            if (needsOffsetClamp)
            {
                SqlParameterRewriter.ClampOffsetParameters(sql, paramMap);
            }

            var observation = StartSqlObservationIfEnabled(_connection, sql, paramMap);

            try
            {
                var stmt = EnsurePreparedStatement(sql, resetForExecution: true);

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
                    var ex = new DecentDBException(stepResult,
                        _connection.GetNativeDb().LastErrorMessage, sql);
                    InvalidatePreparedStatement(discardFromConnectionCache: true);
                    throw ex;
                }

                if (observation != null)
                {
                    _connection.CompleteSqlObservation(observation, stmt.RowsAffected, exception: null);
                }

                return (int)stmt.RowsAffected;
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
