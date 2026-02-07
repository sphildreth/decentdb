using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics.CodeAnalysis;
using DecentDb.Native;

namespace DecentDb.AdoNet
{
    public sealed class DecentDbCommand : DbCommand
    {
        private DecentDbConnection? _connection;
        private string _commandText = string.Empty;
        private int _commandTimeout = 30;
        private readonly List<DecentDbParameter> _parameters = new();
        private readonly DecentDbParameterCollection _parameterCollection;
        private DecentDbTransaction? _transaction;
        private PreparedStatement? _statement;
        private bool _disposed;

        public DecentDbCommand()
        {
            _connection = null;
            _parameterCollection = new DecentDbParameterCollection(_parameters);
        }

        public DecentDbCommand(DecentDbConnection connection)
        {
            _connection = connection;
            _parameterCollection = new DecentDbParameterCollection(_parameters);
            _commandTimeout = connection.DefaultCommandTimeoutSeconds;
        }

        public DecentDbCommand(DecentDbConnection connection, string commandText)
        {
            _connection = connection;
            _commandText = commandText;
            _parameterCollection = new DecentDbParameterCollection(_parameters);
            _commandTimeout = connection.DefaultCommandTimeoutSeconds;
        }

        internal DecentDbConnection OwnerConnection => _connection ?? throw new InvalidOperationException("Command has no connection");

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
                    _connection = null;
                    return;
                }

                if (value is not DecentDbConnection conn)
                {
                    throw new ArgumentException("Must be a DecentDbConnection");
                }
                if (_statement != null)
                {
                    throw new InvalidOperationException("Cannot change connection while command is executing");
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
                _transaction = value as DecentDbTransaction;
            }
        }

        public override void Cancel()
        {
            if (_statement != null)
            {
                _statement.Dispose();
                _statement = null;
            }
        }

        public override int ExecuteNonQuery()
        {
            using var reader = ExecuteDbDataReader(CommandBehavior.Default);
            while (reader.Read()) { }
            return reader.RecordsAffected;
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
            return Task.FromResult(ExecuteNonQuery());
        }

        public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(ExecuteScalar());
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            if (_connection == null)
            {
                throw new InvalidOperationException("Command has no connection");
            }

            var db = _connection.GetNativeDb();
            var (sql, paramMap) = SqlParameterRewriter.Rewrite(_commandText, _parameters);

            var observation = _connection.TryStartSqlObservation(sql, SnapshotParameters(paramMap));

            PreparedStatement? stmt = null;
            try
            {
                stmt = db.Prepare(sql);

                foreach (var kvp in paramMap)
                {
                    BindParameter(stmt, kvp.Key, kvp.Value);
                }

                _statement = stmt;

                var stepResult = stmt.Step();
                if (stepResult < 0)
                {
                    var ex = new DecentDbException(stmt.RowsAffected > 0 ? (int)stmt.RowsAffected : stepResult,
                        db.LastErrorMessage, sql);
                    if (observation != null)
                    {
                        _connection.CompleteSqlObservation(observation, stmt.RowsAffected, ex);
                    }
                    throw ex;
                }

                return new DecentDbDataReader(this, stmt, stepResult, observation);
            }
            catch (Exception ex)
            {
                if (_statement == null && stmt != null)
                {
                    stmt.Dispose();
                }

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

        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            return Task.FromResult(ExecuteDbDataReader(behavior));
        }

        protected override DbParameter CreateDbParameter()
        {
            return CreateParameter();
        }

        public new DecentDbParameter CreateParameter()
        {
            var param = new DecentDbParameter();
            return param;
        }

        public override void Prepare()
        {
            if (_connection == null || _connection.State != ConnectionState.Open)
            {
                throw new InvalidOperationException("Connection must be open to prepare command");
            }
        }

        private void BindParameter(PreparedStatement stmt, int index1Based, DbParameter parameter)
        {
            var value = parameter.Value;
            if (value == null || value == DBNull.Value)
            {
                stmt.BindNull(index1Based);
                return;
            }

            var type = value.GetType();
            if (type == typeof(long) || type == typeof(int) || type == typeof(short) || type == typeof(byte))
            {
                stmt.BindInt64(index1Based, Convert.ToInt64(value));
            }
            else if (type == typeof(ulong) || type == typeof(uint) || type == typeof(ushort))
            {
                stmt.BindInt64(index1Based, (long)Convert.ToUInt64(value));
            }
            else if (type == typeof(double) || type == typeof(float))
            {
                stmt.BindFloat64(index1Based, Convert.ToDouble(value));
            }
            else if (type == typeof(decimal))
            {
                stmt.BindText(index1Based, value.ToString()!);
            }
            else if (type == typeof(bool))
            {
                stmt.BindInt64(index1Based, (bool)value ? 1 : 0);
            }
            else if (type == typeof(string))
            {
                var s = (string)value;
                if (parameter.Size > 0)
                {
                    var byteCount = Encoding.UTF8.GetByteCount(s);
                    if (byteCount > parameter.Size)
                    {
                        throw new ArgumentException($"Value exceeds Size({parameter.Size}) bytes (UTF-8). Actual: {byteCount} bytes.");
                    }
                }

                stmt.BindText(index1Based, s);
            }
            else if (type == typeof(DateTime))
            {
                var dt = (DateTime)value;
                var utc = dt.Kind == DateTimeKind.Utc ? dt : dt.ToUniversalTime();
                var ms = new DateTimeOffset(utc, TimeSpan.Zero).ToUnixTimeMilliseconds();
                stmt.BindInt64(index1Based, ms);
            }
            else if (type == typeof(DateTimeOffset))
            {
                var dto = ((DateTimeOffset)value).ToUniversalTime();
                var ms = dto.ToUnixTimeMilliseconds();
                stmt.BindInt64(index1Based, ms);
            }
            else if (type == typeof(TimeSpan))
            {
                stmt.BindInt64(index1Based, ((TimeSpan)value).Ticks);
            }
            else if (type == typeof(DateOnly))
            {
                var date = (DateOnly)value;
                var epoch = DateOnly.FromDateTime(DateTime.UnixEpoch);
                stmt.BindInt64(index1Based, date.DayNumber - epoch.DayNumber);
            }
            else if (type == typeof(TimeOnly))
            {
                stmt.BindInt64(index1Based, ((TimeOnly)value).Ticks);
            }
            else if (type == typeof(byte[]))
            {
                stmt.BindBlob(index1Based, (byte[])value);
            }
            else if (type == typeof(Guid))
            {
                stmt.BindBlob(index1Based, ((Guid)value).ToByteArray());
            }
            else
            {
                throw new NotSupportedException($"Unsupported parameter type: {type.FullName}");
            }
        }

        internal void FinalizeStatement()
        {
            _statement?.Dispose();
            _statement = null;
        }

        protected override void Dispose(bool disposing)
        {
            if (_disposed) return;

            if (disposing)
            {
                FinalizeStatement();
            }

            _disposed = true;
            base.Dispose(disposing);
        }
    }
}
