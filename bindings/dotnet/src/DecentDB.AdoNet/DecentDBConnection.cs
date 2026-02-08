using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Collections.Generic;
using System.Text;
using DecentDB.Native;

namespace DecentDB.AdoNet
{
    public sealed class DecentDBConnection : DbConnection
    {
        private Native.DecentDB? _db;
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

        public DecentDBConnection()
        {
        }

        public DecentDBConnection(string connectionString)
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

            return new DecentDBTransaction(this, isolationLevel);
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
                _db = new Native.DecentDB(path, _nativeOptions);
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
            return new DecentDBCommand(this);
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

        internal Native.DecentDB GetNativeDb()
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

        // ───── Schema introspection (JSON) ─────

        /// <summary>
        /// Returns a JSON array of table names, e.g. ["users","items"].
        /// </summary>
        public string ListTablesJson()
        {
            return GetNativeDb().ListTablesJson();
        }

        /// <summary>
        /// Returns a JSON array of column metadata for a given table.
        /// Each element: {"name":"col","type":"INTEGER","not_null":false,"unique":false,"primary_key":false,...}
        /// </summary>
        public string GetTableColumnsJson(string tableName)
        {
            return GetNativeDb().GetTableColumnsJson(tableName);
        }

        // ───── ADO.NET GetSchema ─────

        /// <summary>
        /// Returns the list of supported schema collections.
        /// </summary>
        public override DataTable GetSchema()
        {
            return GetSchema("MetaDataCollections");
        }

        /// <summary>
        /// Returns schema information for the specified collection.
        /// Supported collections: MetaDataCollections, Tables, Columns.
        /// </summary>
        public override DataTable GetSchema(string collectionName)
        {
            return GetSchema(collectionName, null);
        }

        /// <summary>
        /// Returns schema information for the specified collection, optionally
        /// filtered by <paramref name="restrictionValues"/>.
        /// For "Columns", the first restriction is the table name.
        /// </summary>
        public override DataTable GetSchema(string collectionName, string?[]? restrictionValues)
        {
            if (_db == null)
                throw new InvalidOperationException("Connection is not open");

            switch (collectionName.ToUpperInvariant())
            {
                case "METADATACOLLECTIONS":
                    return BuildMetaDataCollectionsTable();
                case "TABLES":
                    return BuildTablesTable();
                case "COLUMNS":
                    string? tableFilter = restrictionValues is { Length: > 0 } ? restrictionValues[0] : null;
                    return BuildColumnsTable(tableFilter);
                default:
                    throw new ArgumentException($"Unsupported schema collection: {collectionName}", nameof(collectionName));
            }
        }

        private static DataTable BuildMetaDataCollectionsTable()
        {
            var dt = new DataTable("MetaDataCollections");
            dt.Columns.Add("CollectionName", typeof(string));
            dt.Columns.Add("NumberOfRestrictions", typeof(int));

            dt.Rows.Add("MetaDataCollections", 0);
            dt.Rows.Add("Tables", 0);
            dt.Rows.Add("Columns", 1);
            return dt;
        }

        private DataTable BuildTablesTable()
        {
            var dt = new DataTable("Tables");
            dt.Columns.Add("TABLE_NAME", typeof(string));
            dt.Columns.Add("TABLE_TYPE", typeof(string));

            var json = System.Text.Json.JsonDocument.Parse(ListTablesJson());
            foreach (var element in json.RootElement.EnumerateArray())
            {
                dt.Rows.Add(element.GetString(), "TABLE");
            }
            return dt;
        }

        private DataTable BuildColumnsTable(string? tableFilter)
        {
            var dt = new DataTable("Columns");
            dt.Columns.Add("TABLE_NAME", typeof(string));
            dt.Columns.Add("COLUMN_NAME", typeof(string));
            dt.Columns.Add("DATA_TYPE", typeof(string));
            dt.Columns.Add("IS_NULLABLE", typeof(bool));
            dt.Columns.Add("IS_UNIQUE", typeof(bool));
            dt.Columns.Add("IS_PRIMARY_KEY", typeof(bool));

            var tables = System.Text.Json.JsonDocument.Parse(ListTablesJson());
            foreach (var tableElement in tables.RootElement.EnumerateArray())
            {
                var tableName = tableElement.GetString()!;
                if (tableFilter != null && !string.Equals(tableName, tableFilter, StringComparison.OrdinalIgnoreCase))
                    continue;

                var cols = System.Text.Json.JsonDocument.Parse(GetTableColumnsJson(tableName));
                foreach (var col in cols.RootElement.EnumerateArray())
                {
                    dt.Rows.Add(
                        tableName,
                        col.GetProperty("name").GetString(),
                        col.GetProperty("type").GetString(),
                        !col.GetProperty("not_null").GetBoolean(),
                        col.GetProperty("unique").GetBoolean(),
                        col.GetProperty("primary_key").GetBoolean());
                }
            }
            return dt;
        }
    }
}
