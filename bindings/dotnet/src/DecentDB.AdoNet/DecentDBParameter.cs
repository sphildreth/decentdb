using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace DecentDb.AdoNet
{
    public sealed class DecentDbParameter : DbParameter
    {
        private string _parameterName = string.Empty;
        private object? _value = DBNull.Value;
        private DbType _dbType = DbType.String;
        private int _size;
        private byte _precision;
        private byte _scale;
        private ParameterDirection _direction = ParameterDirection.Input;
        private bool _isNullable;

        [AllowNull]
        public override string ParameterName
        {
            get => _parameterName;
            set => _parameterName = value ?? string.Empty;
        }

        public override object? Value
        {
            get => _value;
            set => _value = value ?? DBNull.Value;
        }

        public override DbType DbType
        {
            get => _dbType;
            set
            {
                if (!IsValidDbType(value))
                {
                    throw new ArgumentException($"Invalid DbType: {value}");
                }
                _dbType = value;
            }
        }

        public override int Size
        {
            get => _size;
            set
            {
                if (value < 0) throw new ArgumentException("Size must be non-negative");
                _size = value;
            }
        }

        public override byte Precision
        {
            get => _precision;
            set => _precision = value;
        }

        public override byte Scale
        {
            get => _scale;
            set => _scale = value;
        }

        public override ParameterDirection Direction
        {
            get => _direction;
            set
            {
                if (value != ParameterDirection.Input && value != ParameterDirection.InputOutput)
                {
                    throw new NotSupportedException("Only Input and InputOutput directions are supported");
                }
                _direction = value;
            }
        }

        public override bool IsNullable
        {
            get => _isNullable;
            set => _isNullable = value;
        }

        public override bool SourceColumnNullMapping { get; set; }

        public override DataRowVersion SourceVersion { get; set; } = DataRowVersion.Current;

        [AllowNull]
        public override string SourceColumn { get; set; } = string.Empty;

        public DecentDbParameter()
        {
        }

        public DecentDbParameter(string name, object? value)
        {
            _parameterName = name;
            _value = value ?? DBNull.Value;
        }

        public DecentDbParameter(string name, DbType dbType)
        {
            _parameterName = name;
            _dbType = dbType;
        }

        public DecentDbParameter(string name, DbType dbType, int size)
        {
            _parameterName = name;
            _dbType = dbType;
            _size = size;
        }

        public override void ResetDbType()
        {
            _dbType = DbType.String;
        }

        private static bool IsValidDbType(DbType dbType)
        {
            return dbType switch
            {
                DbType.AnsiString or DbType.AnsiStringFixedLength or DbType.String or DbType.StringFixedLength
                    or DbType.Xml or DbType.Binary or DbType.Boolean or DbType.Byte
                    or DbType.Currency or DbType.Date or DbType.DateTime or DbType.DateTime2
                    or DbType.DateTimeOffset or DbType.Decimal or DbType.Double or DbType.Guid
                    or DbType.Int16 or DbType.Int32 or DbType.Int64 or DbType.Object
                    or DbType.SByte or DbType.Single or DbType.Time or DbType.UInt16
                    or DbType.UInt32 or DbType.UInt64 or DbType.VarNumeric => true,
                _ => false
            };
        }
    }
}
