using Dapper;
using DecentDb.AdoNet;
using Xunit;

namespace DecentDb.Tests
{
    public class DapperIntegrationTests : IDisposable
    {
        private readonly string _dbPath;
        private readonly DecentDbConnection _connection;

        public DapperIntegrationTests()
        {
            _dbPath = Path.Combine(Path.GetTempPath(), $"decentdb_dapper_{Guid.NewGuid()}.ddb");
            var connStr = $"Data Source={_dbPath}";
            _connection = new DecentDbConnection(connStr);
            _connection.Open();

            _connection.Execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT, active INT)");
            _connection.Execute("INSERT INTO users (id, name, email, active) VALUES (1, 'Alice', 'alice@example.com', 1)");
            _connection.Execute("INSERT INTO users (id, name, email, active) VALUES (2, 'Bob', 'bob@example.com', 0)");
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
        public void TestDapperQuery()
        {
            var users = _connection.Query<User>("SELECT * FROM users ORDER BY id").ToList();
            
            Assert.Equal(2, users.Count);
            Assert.Equal(1, users[0].Id);
            Assert.Equal("Alice", users[0].Name);
            Assert.True(users[0].Active); 
            Assert.Equal(2, users[1].Id);
            Assert.Equal("Bob", users[1].Name);
            Assert.False(users[1].Active);
        }

        [Fact]
        public void TestDapperParameters()
        {
            var user = _connection.QuerySingle<User>("SELECT * FROM users WHERE id = @id", new { id = 1 });
            Assert.Equal("Alice", user.Name);
        }

        [Fact]
        public void TestDapperExecute()
        {
            var affected = _connection.Execute("UPDATE users SET name = @name WHERE id = @id", new { name = "Alicia", id = 1 });
            Assert.Equal(1, affected);

            var user = _connection.QuerySingle<User>("SELECT * FROM users WHERE id = 1");
            Assert.Equal("Alicia", user.Name);
        }

        [Fact]
        public void TestDapperScalar()
        {
            var count = _connection.ExecuteScalar<long>("SELECT count(*) FROM users");
            Assert.Equal(2, count);
        }

        [Fact]
        public void TestDapperAsync()
        {
            // Verify async methods work (even if sync-over-async internally)
            var task = _connection.QueryAsync<User>("SELECT * FROM users");
            task.Wait();
            var users = task.Result;
            Assert.Equal(2, users.Count());
        }

        class User
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public string Email { get; set; }
            public bool Active { get; set; }
        }
    }
}
