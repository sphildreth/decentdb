using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using DecentDB.Native;

namespace DecentDB.AdoNet
{
    public sealed class DecentDBDataReader : DbDataReader
    {
        private readonly DecentDBCommand _command;
        private PreparedStatement _statement;
        private SqlObservation? _sqlObservation;
        private bool _sqlObservationCompleted;
        private bool _hasRows;
        private bool _isClosed;
        private int _recordsAffected;

        private readonly int _initialStepResult;
        private bool _initialStepConsumed;

        internal DecentDBDataReader(DecentDBCommand command, PreparedStatement statement, int initialStepResult, SqlObservation? observation)
        {
            _command = command;
            _statement = statement;
            _initialStepResult = initialStepResult;
            _sqlObservation = observation;
            _hasRows = initialStepResult == 1;
            _recordsAffected = -1;
        }

        public override int Depth => 0;

        public override int FieldCount => _statement.ColumnCount;

        public override bool HasRows => _hasRows;

        public override bool IsClosed => _isClosed;

        public override int RecordsAffected
        {
            get
            {
                if (_recordsAffected < 0)
                {
                    _recordsAffected = (int)_statement.RowsAffected;
                }
                return _recordsAffected;
            }
        }

        public override object this[int ordinal] => GetValue(ordinal);

        public override object this[string name] => GetValue(GetOrdinal(name));

        public override string GetName(int ordinal)
        {
            return _statement.ColumnName(ordinal);
        }

        public override string GetDataTypeName(int ordinal)
        {
            var type = _statement.ColumnType(ordinal);
            return type switch
            {
                0 => "NULL",
                1 => "BIGINT",
                2 => "BOOLEAN",
                3 => "DOUBLE",
                4 => "TEXT",
                5 => "BLOB",
                _ => "UNKNOWN"
            };
        }

        public override Type GetFieldType(int ordinal)
        {
            var type = _statement.ColumnType(ordinal);
            return type switch
            {
                0 => typeof(object),
                1 => typeof(long),
                2 => typeof(bool),
                3 => typeof(double),
                4 => typeof(string),
                5 => typeof(byte[]),
                _ => typeof(object)
            };
        }

        public override object GetValue(int ordinal)
        {
            if (_statement.IsNull(ordinal))
            {
                return DBNull.Value;
            }

            var type = _statement.ColumnType(ordinal);
            return type switch
            {
                1 => _statement.GetInt64(ordinal),
                2 => _statement.GetInt64(ordinal) != 0,
                3 => _statement.GetFloat64(ordinal),
                4 => _statement.GetText(ordinal),
                5 => _statement.GetBlob(ordinal),
                _ => DBNull.Value
            };
        }

        public override T GetFieldValue<T>(int ordinal)
        {
            var requestedType = typeof(T);

            if (_statement.IsNull(ordinal))
            {
                return default!;
            }

            var nonNullableType = Nullable.GetUnderlyingType(requestedType) ?? requestedType;

            object boxed;
            if (nonNullableType == typeof(string))
            {
                boxed = _statement.GetText(ordinal);
            }
            else if (nonNullableType == typeof(short))
            {
                boxed = (short)_statement.GetInt64(ordinal);
            }
            else if (nonNullableType == typeof(int))
            {
                boxed = (int)_statement.GetInt64(ordinal);
            }
            else if (nonNullableType == typeof(long))
            {
                boxed = _statement.GetInt64(ordinal);
            }
            else if (nonNullableType == typeof(bool))
            {
                boxed = _statement.GetInt64(ordinal) != 0;
            }
            else if (nonNullableType == typeof(float))
            {
                boxed = (float)_statement.GetFloat64(ordinal);
            }
            else if (nonNullableType == typeof(double))
            {
                boxed = _statement.GetFloat64(ordinal);
            }
            else if (nonNullableType == typeof(byte[]))
            {
                boxed = _statement.GetBlob(ordinal);
            }
            else if (nonNullableType == typeof(DateTime))
            {
                var ms = _statement.GetInt64(ordinal);
                boxed = DateTimeOffset.FromUnixTimeMilliseconds(ms).UtcDateTime;
            }
            else if (nonNullableType == typeof(DateTimeOffset))
            {
                var ms = _statement.GetInt64(ordinal);
                boxed = DateTimeOffset.FromUnixTimeMilliseconds(ms);
            }
            else if (nonNullableType == typeof(DateOnly))
            {
                var days = _statement.GetInt64(ordinal);
                var epoch = DateOnly.FromDateTime(DateTime.UnixEpoch);
                boxed = epoch.AddDays(checked((int)days));
            }
            else if (nonNullableType == typeof(TimeOnly))
            {
                var ticks = _statement.GetInt64(ordinal);
                boxed = new TimeOnly(ticks);
            }
            else if (nonNullableType == typeof(TimeSpan))
            {
                var ticks = _statement.GetInt64(ordinal);
                boxed = TimeSpan.FromTicks(ticks);
            }
            else if (nonNullableType == typeof(decimal))
            {
                var str = _statement.GetText(ordinal);
                boxed = decimal.Parse(str);
            }
            else if (nonNullableType == typeof(Guid))
            {
                var bytes = _statement.GetBlob(ordinal);
                boxed = new Guid(bytes);
            }
            else if (nonNullableType.IsEnum)
            {
                var raw = _statement.GetInt64(ordinal);
                boxed = Enum.ToObject(nonNullableType, raw);
            }
            else
            {
                boxed = GetValue(ordinal)!;
            }

            return (T)boxed;
        }

        public override int GetInt32(int ordinal)
        {
            return (int)_statement.GetInt64(ordinal);
        }

        public override long GetInt64(int ordinal)
        {
            return _statement.GetInt64(ordinal);
        }

        public override double GetDouble(int ordinal)
        {
            return _statement.GetFloat64(ordinal);
        }

        public override string GetString(int ordinal)
        {
            return _statement.GetText(ordinal);
        }

        public override bool GetBoolean(int ordinal)
        {
            return _statement.GetInt64(ordinal) != 0;
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        {
            var bytes = _statement.GetBlob(ordinal);
            if (buffer == null)
            {
                return bytes.Length;
            }

            var available = bytes.Length - (int)dataOffset;
            var toCopy = Math.Min(length, available);
            Array.Copy(bytes, dataOffset, buffer, bufferOffset, toCopy);
            return toCopy;
        }

        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        {
            var str = _statement.GetText(ordinal);
            if (buffer == null)
            {
                return str.Length;
            }

            var available = str.Length - (int)dataOffset;
            var toCopy = Math.Min(length, available);
            str.CopyTo((int)dataOffset, buffer, bufferOffset, toCopy);
            return toCopy;
        }

        public override char GetChar(int ordinal)
        {
            var str = _statement.GetText(ordinal);
            return str.Length > 0 ? str[0] : '\0';
        }

        public override short GetInt16(int ordinal)
        {
            return (short)_statement.GetInt64(ordinal);
        }

        public override float GetFloat(int ordinal)
        {
            return (float)_statement.GetFloat64(ordinal);
        }

        public override byte GetByte(int ordinal)
        {
            return (byte)_statement.GetInt64(ordinal);
        }

        public override Guid GetGuid(int ordinal)
        {
            var bytes = _statement.GetBlob(ordinal);
            return new Guid(bytes);
        }

        public override DateTime GetDateTime(int ordinal)
        {
            var ms = _statement.GetInt64(ordinal);
            return DateTimeOffset.FromUnixTimeMilliseconds(ms).UtcDateTime;
        }

        public override decimal GetDecimal(int ordinal)
        {
            var str = _statement.GetText(ordinal);
            return decimal.Parse(str);
        }

        public override bool IsDBNull(int ordinal)
        {
            return _statement.IsNull(ordinal);
        }

        public override int GetOrdinal(string name)
        {
            var count = _statement.ColumnCount;
            for (int i = 0; i < count; i++)
            {
                if (_statement.ColumnName(i).Equals(name, StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
            }
            throw new IndexOutOfRangeException($"Column '{name}' not found");
        }

        public override int GetValues(object[] values)
        {
            var count = Math.Min(FieldCount, values.Length);
            for (int i = 0; i < count; i++)
            {
                values[i] = GetValue(i) ?? DBNull.Value;
            }
            return count;
        }

        public override bool Read()
        {
            if (_isClosed)
            {
                throw new InvalidOperationException("Reader is closed");
            }

            if (!_initialStepConsumed)
            {
                _initialStepConsumed = true;

                if (_initialStepResult < 0)
                {
                    throw new DecentDBException(_initialStepResult, "Step failed", _command.CommandText);
                }

                return _initialStepResult == 1;
            }

            var result = _statement.Step();
            if (result < 0)
            {
                var ex = new DecentDBException(result, "Step failed", _command.CommandText);
                CompleteSqlObservationOnce(exception: ex);
                throw ex;
            }

            return result == 1;
        }

        public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(Read());
        }

        public override bool NextResult()
        {
            return false;
        }

        public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(false);
        }

        public override IEnumerator GetEnumerator()
        {
            return new DbEnumerator(this);
        }

        public override void Close()
        {
            if (_isClosed) return;
            _isClosed = true;

            CompleteSqlObservationOnce(exception: null);
            _command.FinalizeStatement();
        }

        private void CompleteSqlObservationOnce(Exception? exception)
        {
            if (_sqlObservationCompleted) return;
            if (_sqlObservation == null) return;

            _sqlObservationCompleted = true;
            _command.OwnerConnection.CompleteSqlObservation(_sqlObservation, _statement.RowsAffected, exception);
            _sqlObservation = null;
        }

        protected override void Dispose(bool disposing)
        {
            if (!_isClosed)
            {
                Close();
            }
            base.Dispose(disposing);
        }
    }
}
