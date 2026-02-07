using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Collections.Generic;
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

        private string _nativeOptions = string.Empty;
        private bool _loggingEnabled;
        private SqlLogLevel _logLevel = SqlLogLevel.Debug;
        private int _defaultCommandTimeoutSeconds = 30;

        private EventHandler<SqlExecutingEventArgs>? _sqlExecuting;
        private EventHandler<SqlExecutedEventArgs>? _sqlExecuted;

        public event EventHandler<SqlExecutingEventArgs>? SqlExecuting
        {
            add => _sqlExecuting += value;
            remove => _sqlExecuting -= value;
        }

        public event EventHandler<SqlExecutedEventArgs>? SqlExecuted
        {
            add => _sqlExecuted += value;
            remove => _sqlExecuted -= value;
        }

        public DecentDbConnection()
        {
        }

        public DecentDbConnection(string connectionString)
        {
            ConnectionString = connectionString;
        }

        [AllowNull]
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

                var parsed = ParseConnectionString(_connectionString);
                _dataSource = parsed.DataSource;
                _nativeOptions = parsed.NativeOptions;
                _loggingEnabled = parsed.LoggingEnabled;
                _logLevel = parsed.LogLevel;
                _defaultCommandTimeoutSeconds = parsed.DefaultCommandTimeoutSeconds;
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

            try
            {
                _db = new Native.DecentDb(path, _nativeOptions);
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

        internal int DefaultCommandTimeoutSeconds => _defaultCommandTimeoutSeconds;

        internal SqlObservation? TryStartSqlObservation(string sql, IReadOnlyList<SqlParameterValue> parameters)
        {
            // Zero-cost when disabled: one predictable branch, no allocations.
            if (!_loggingEnabled && _sqlExecuting == null && _sqlExecuted == null)
            {
                return null;
            }

            var obs = new SqlObservation(Stopwatch.GetTimestamp(), sql, parameters);

            if (_loggingEnabled && _logLevel <= SqlLogLevel.Debug)
            {
                Trace.WriteLine($"DecentDB SQL executing: {sql}");
            }

            _sqlExecuting?.Invoke(this, new SqlExecutingEventArgs(sql, parameters, obs.Timestamp));
            return obs;
        }

        internal void CompleteSqlObservation(SqlObservation obs, long rowsAffected, Exception? exception)
        {
            var duration = Stopwatch.GetElapsedTime(obs.StartTimestamp, Stopwatch.GetTimestamp());

            if (_loggingEnabled && _logLevel <= SqlLogLevel.Debug)
            {
                var status = exception == null ? "ok" : "error";
                Trace.WriteLine($"DecentDB SQL executed ({status}) in {duration.TotalMilliseconds:F3}ms: {obs.Sql}");
            }

            _sqlExecuted?.Invoke(this, new SqlExecutedEventArgs(obs.Sql, obs.Parameters, obs.Timestamp, duration, rowsAffected, exception));
        }

        private readonly struct ParsedConnectionString
        {
            public ParsedConnectionString(
                string dataSource,
                string nativeOptions,
                bool loggingEnabled,
                SqlLogLevel logLevel,
                int defaultCommandTimeoutSeconds)
            {
                DataSource = dataSource;
                NativeOptions = nativeOptions;
                LoggingEnabled = loggingEnabled;
                LogLevel = logLevel;
                DefaultCommandTimeoutSeconds = defaultCommandTimeoutSeconds;
            }

            public string DataSource { get; }
            public string NativeOptions { get; }
            public bool LoggingEnabled { get; }
            public SqlLogLevel LogLevel { get; }
            public int DefaultCommandTimeoutSeconds { get; }
        }

        private static ParsedConnectionString ParseConnectionString(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                return new ParsedConnectionString(string.Empty, string.Empty, loggingEnabled: false, SqlLogLevel.Debug, 30);
            }

            var kvps = ParseKeyValuePairs(connectionString);

            var dataSource = GetFirstValue(kvps, "Data Source", "Filename", "Database") ?? string.Empty;

            var nativeOptions = BuildNativeOptions(kvps);

            var loggingEnabled = TryParseBool(GetFirstValue(kvps, "Logging"), out var logEnabled) && logEnabled;
            var logLevel = TryParseLogLevel(GetFirstValue(kvps, "LogLevel"), out var ll) ? ll : SqlLogLevel.Debug;

            var defaultCommandTimeoutSeconds = 30;
            if (TryParseNonNegativeInt(GetFirstValue(kvps, "Command Timeout"), out var cmdTimeout))
            {
                defaultCommandTimeoutSeconds = cmdTimeout;
            }

            return new ParsedConnectionString(dataSource, nativeOptions, loggingEnabled, logLevel, defaultCommandTimeoutSeconds);
        }

        private static Dictionary<string, string> ParseKeyValuePairs(string connectionString)
        {
            var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var part in connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries))
            {
                var kv = part.Split('=', 2);
                if (kv.Length != 2) continue;
                var key = kv[0].Trim();
                var value = kv[1].Trim();
                if (key.Length == 0) continue;
                dict[key] = value;
            }
            return dict;
        }

        private static string? GetFirstValue(Dictionary<string, string> dict, params string[] keys)
        {
            foreach (var key in keys)
            {
                if (dict.TryGetValue(key, out var value))
                {
                    return value;
                }
            }
            return null;
        }

        private static string BuildNativeOptions(Dictionary<string, string> kvps)
        {
            var options = new StringBuilder();

            if (kvps.TryGetValue("Cache Size", out var cacheSize) && !string.IsNullOrWhiteSpace(cacheSize))
            {
                // Delegate parsing to the native layer. Supports pages (int) or e.g. "64MB".
                options.Append("cache_size=").Append(cacheSize.Trim());
            }

            return options.ToString();
        }

        private static bool TryParseBool(string? value, out bool result)
        {
            if (value == null)
            {
                result = false;
                return false;
            }

            if (bool.TryParse(value, out result))
            {
                return true;
            }

            if (value == "0") { result = false; return true; }
            if (value == "1") { result = true; return true; }

            result = false;
            return false;
        }

        private static bool TryParseLogLevel(string? value, out SqlLogLevel logLevel)
        {
            if (value == null)
            {
                logLevel = SqlLogLevel.Debug;
                return false;
            }

            if (Enum.TryParse<SqlLogLevel>(value, ignoreCase: true, out logLevel))
            {
                return true;
            }

            logLevel = SqlLogLevel.Debug;
            return false;
        }

        private static bool TryParseNonNegativeInt(string? value, out int parsed)
        {
            if (value != null && int.TryParse(value, out parsed) && parsed >= 0)
            {
                return true;
            }

            parsed = 0;
            return false;
        }

        protected override void Dispose(bool disposing)
        {
            Close();
            base.Dispose(disposing);
        }
    }
}
