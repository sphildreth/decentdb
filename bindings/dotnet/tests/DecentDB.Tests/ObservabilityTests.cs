using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests
{
    public class ObservabilityTests : IDisposable
    {
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
    }
}
