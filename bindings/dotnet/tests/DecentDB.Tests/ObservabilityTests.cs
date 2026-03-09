using System.Diagnostics;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests
{
    public class ObservabilityTests : IDisposable
    {
        private static readonly object TraceSync = new();
        private readonly string _dbPath;
        private readonly DecentDBConnection _connection;

        public ObservabilityTests()
        {
            _dbPath = Path.Combine(Path.GetTempPath(), $"decentdb_obs_{Guid.NewGuid()}.ddb");
            var connStr = $"Data Source={_dbPath}";
            _connection = new DecentDBConnection(connStr);
            _connection.Open();
        }

        public void Dispose()
        {
            _connection.Dispose();
            if (File.Exists(_dbPath))
            {
                File.Delete(_dbPath);
            }
        }

        [Fact]
        public void TestSqlExecutingEvent()
        {
            bool eventFired = false;
            string? capturedSql = null;

            _connection.SqlExecuting += (sender, args) =>
            {
                eventFired = true;
                capturedSql = args.Sql;
            };

            var cmd = _connection.CreateCommand();
            cmd.CommandText = "CREATE TABLE test (id INT)";
            cmd.ExecuteNonQuery();

            Assert.True(eventFired);
            Assert.Contains("CREATE TABLE test", capturedSql);
        }

        [Fact]
        public void TestDebugTraceRedactsSqlText()
        {
            var dbPath = Path.Combine(Path.GetTempPath(), $"decentdb_obs_trace_{Guid.NewGuid()}.ddb");
            try
            {
                using var conn = new DecentDBConnection($"Data Source={dbPath};Logging=1;LogLevel=Debug");
                conn.Open();

                using var writer = new StringWriter();
                using var listener = new TextWriterTraceListener(writer);

                lock (TraceSync)
                {
                    Trace.Listeners.Add(listener);
                    try
                    {
                        using var cmd = conn.CreateCommand();
                        cmd.CommandText = "SELECT 'salary=123456'";
                        var result = cmd.ExecuteScalar();

                        listener.Flush();
                        var traceOutput = writer.ToString();

                        Assert.Equal("salary=123456", result);
                        Assert.Contains("DecentDB SQL executing: SELECT statement", traceOutput);
                        Assert.Contains("DecentDB SQL executed (ok)", traceOutput);
                        Assert.DoesNotContain("salary=123456", traceOutput);
                    }
                    finally
                    {
                        Trace.Listeners.Remove(listener);
                    }
                }
            }
            finally
            {
                if (File.Exists(dbPath))
                {
                    File.Delete(dbPath);
                }
                if (File.Exists(dbPath + "-wal"))
                {
                    File.Delete(dbPath + "-wal");
                }
            }
        }
    }
}
