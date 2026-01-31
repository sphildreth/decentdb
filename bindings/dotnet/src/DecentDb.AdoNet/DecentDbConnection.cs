using System;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Text;
using DecentDb.Native;

namespace DecentDb.AdoNet
{
    public sealed class DecentDbConnection : DbConnection
    {
        private Native.DecentDb? _db;
        private string _connectionString = string.Empty;
        private ConnectionState _state;
        private string _dataSource = string.Empty;

        public DecentDbConnection()
        {
        }

        public DecentDbConnection(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public override string ConnectionString
        {
            get => _connectionString;
            set
            {
                if (_state == ConnectionState.Open)
                {
                    throw new InvalidOperationException("Cannot change connection string while connection is open");
                }
                _connectionString = value ?? string.Empty;
                _dataSource = ParseDataSource(value);
            }
        }

        public override string Database => Path.GetFileNameWithoutExtension(_dataSource);

        public override string DataSource => _dataSource;

        public override string ServerVersion => "1.0.0";

        public override ConnectionState State => _state;

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            if (_db == null)
            {
                throw new InvalidOperationException("Connection is not open");
            }

            if (isolationLevel != IsolationLevel.Snapshot &&
                isolationLevel != IsolationLevel.ReadCommitted &&
                isolationLevel != IsolationLevel.ReadUncommitted)
            {
                isolationLevel = IsolationLevel.Snapshot;
            }

            using var cmd = CreateCommand();
            cmd.CommandText = "BEGIN";
            cmd.ExecuteNonQuery();

            return new DecentDbTransaction(this, isolationLevel);
        }

        public override void Close()
        {
            if (_state == ConnectionState.Closed) return;

            try
            {
                _db?.Dispose();
                _db = null;
            }
            finally
            {
                _state = ConnectionState.Closed;
            }
        }

        public override void Open()
        {
            if (_state == ConnectionState.Open) return;

            var path = _dataSource;
            if (string.IsNullOrEmpty(path))
            {
                throw new InvalidOperationException("Data Source is required");
            }

            if (!Path.IsPathRooted(path))
            {
                path = Path.Combine(Environment.CurrentDirectory, path);
            }

            var directory = Path.GetDirectoryName(path);
            if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var options = ParseOptions(_connectionString);

            try
            {
                _db = new Native.DecentDb(path, options);
                _state = ConnectionState.Open;
            }
            catch (Exception ex)
            {
                _state = ConnectionState.Closed;
                throw new InvalidOperationException($"Failed to open database: {ex.Message}", ex);
            }
        }

        protected override DbCommand CreateDbCommand()
        {
            return new DecentDbCommand(this);
        }

        public override void ChangeDatabase(string databaseName)
        {
            throw new NotSupportedException("DecentDB does not support changing databases");
        }

        public void Checkpoint()
        {
            if (_db == null)
            {
                throw new InvalidOperationException("Connection is not open");
            }
            _db.Checkpoint();
        }

        internal Native.DecentDb GetNativeDb()
        {
            return _db ?? throw new InvalidOperationException("Connection is not open");
        }

        private static string ParseDataSource(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString)) return string.Empty;

            var parts = connectionString.Split(';');
            foreach (var part in parts)
            {
                var kv = part.Split('=', 2);
                if (kv.Length == 2)
                {
                    var key = kv[0].Trim();
                    if (key.Equals("Data Source", StringComparison.OrdinalIgnoreCase) ||
                        key.Equals("Filename", StringComparison.OrdinalIgnoreCase) ||
                        key.Equals("Database", StringComparison.OrdinalIgnoreCase))
                    {
                        return kv[1].Trim();
                    }
                }
            }
            return string.Empty;
        }

        private static string ParseOptions(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString)) return string.Empty;

            var options = new StringBuilder();
            var parts = connectionString.Split(';');

            foreach (var part in parts)
            {
                var kv = part.Split('=', 2);
                if (kv.Length == 2)
                {
                    var key = kv[0].Trim();
                    var value = kv[1].Trim();

                    if (key.Equals("Cache Size", StringComparison.OrdinalIgnoreCase))
                    {
                        if (options.Length > 0) options.Append('&');
                        options.Append("cache_pages=").Append(value);
                    }
                }
            }

            return options.ToString();
        }

        protected override void Dispose(bool disposing)
        {
            Close();
            base.Dispose(disposing);
        }
    }
}
