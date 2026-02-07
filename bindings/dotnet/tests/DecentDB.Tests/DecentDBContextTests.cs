using System;
using System.IO;
using System.Threading.Tasks;
using DecentDB.AdoNet;
using DecentDB.MicroOrm;
using Xunit;

namespace DecentDB.Tests;

public sealed class DecentDBContextTests : IDisposable
{
    private readonly string _dbPath;

    public DecentDBContextTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_context_{Guid.NewGuid():N}.ddb");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);

        var walPath = _dbPath + "-wal";
        if (File.Exists(walPath))
            File.Delete(walPath);
    }

    [Fact]
    public void Constructor_Throws_ArgumentException_For_Empty_ConnectionString()
    {
        Assert.Throws<ArgumentException>(() => new DecentDBContext(""));
        Assert.Throws<ArgumentException>(() => new DecentDBContext("   "));
        Assert.Throws<ArgumentException>(() => new DecentDBContext(null!));
    }

    [Fact]
    public void Constructor_Accepts_Path_As_ConnectionString()
    {
        using var ctx = new DecentDBContext(_dbPath);
        Assert.NotNull(ctx);
    }

    [Fact]
    public void Constructor_Accepts_Full_ConnectionString()
    {
        using var ctx = new DecentDBContext($"Data Source={_dbPath};Pooling=true");
        Assert.NotNull(ctx);
    }

    [Fact]
    public void Constructor_Handles_Pooling_Option_From_ConnectionString()
    {
        using var ctx = new DecentDBContext($"Data Source={_dbPath};Pooling=false");
        Assert.NotNull(ctx);
    }

    [Fact]
    public async Task Connection_Is_Properly_Managed()
    {
        using var ctx = new DecentDBContext(_dbPath);
        
        // Create a table to ensure connection is opened
        using (var conn = new DecentDBConnection($"Data Source={_dbPath}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE test_persons (Id INTEGER PRIMARY KEY, Name TEXT)";
            cmd.ExecuteNonQuery();
        }
        
        var persons = ctx.Set<TestPerson>();
        await persons.InsertAsync(new TestPerson { Id = 1, Name = "Test" });
        
        // Verify we can query
        var person = await persons.GetAsync(1);
        Assert.NotNull(person);
        Assert.Equal("Test", person.Name);
    }

    [Fact]
    public void Transactions_Are_Properly_Managed()
    {
        using var ctx = new DecentDBContext(_dbPath);
        
        // Test BeginTransaction without isolation level
        using (var tx = ctx.BeginTransaction())
        {
            Assert.NotNull(tx);
        }
        
        // Test BeginTransaction with isolation level
        using (var tx = ctx.BeginTransaction(System.Data.IsolationLevel.ReadCommitted))
        {
            Assert.NotNull(tx);
        }
    }

    [Fact]
    public async Task Events_Are_Properly_Handled()
    {
        var sqlExecutingFired = false;
        var sqlExecutedFired = false;
        
        using var ctx = new DecentDBContext(_dbPath);
        
        ctx.SqlExecuting += (sender, args) =>
        {
            sqlExecutingFired = true;
        };
        
        ctx.SqlExecuted += (sender, args) =>
        {
            sqlExecutedFired = true;
        };
        
        // Create table first
        using (var conn = new DecentDBConnection($"Data Source={_dbPath}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE test_persons (Id INTEGER PRIMARY KEY, Name TEXT)";
            cmd.ExecuteNonQuery();
        }
        
        // Perform an operation that executes SQL
        var persons = ctx.Set<TestPerson>();
        await persons.InsertAsync(new TestPerson { Id = 1, Name = "Event Test" });
        
        Assert.True(sqlExecutingFired);
        Assert.True(sqlExecutedFired);
    }

    [Fact]
    public async Task Event_Add_Remove_Works()
    {
        using var ctx = new DecentDBContext(_dbPath);
        
        EventHandler<SqlExecutingEventArgs> handler = (sender, args) => { };
        
        // Add event handler
        ctx.SqlExecuting += handler;
        
        // Remove event handler
        ctx.SqlExecuting -= handler;
        
        // Create table first
        using (var conn = new DecentDBConnection($"Data Source={_dbPath}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE test_persons (Id INTEGER PRIMARY KEY, Name TEXT)";
            cmd.ExecuteNonQuery();
        }
        
        // Verify no exception occurs when performing operations
        var persons = ctx.Set<TestPerson>();
        await persons.InsertAsync(new TestPerson { Id = 1, Name = "Handler Test" });
        
        var person = await persons.GetAsync(1);
        Assert.NotNull(person);
    }

    [Fact]
    public async Task Connection_Scope_Management_With_Transactions()
    {
        using var ctx = new DecentDBContext(_dbPath);
        
        // Create table first
        using (var conn = new DecentDBConnection($"Data Source={_dbPath}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE test_persons (Id INTEGER PRIMARY KEY, Name TEXT)";
            cmd.ExecuteNonQuery();
        }
        
        // Start a transaction
        using var tx = ctx.BeginTransaction();
        
        // Insert within transaction
        var persons = ctx.Set<TestPerson>();
        await persons.InsertAsync(new TestPerson { Id = 1, Name = "Transaction Test" });
        
        // Query within transaction
        var person = await persons.GetAsync(1);
        Assert.NotNull(person);
        Assert.Equal("Transaction Test", person.Name);
        
        // Rollback the transaction
        tx.Rollback();
        
        // Verify data was rolled back
        var personAfterRollback = await persons.GetAsync(1);
        Assert.Null(personAfterRollback);
    }

    [Fact]
    public async Task Non_Pooled_Mode_Works()
    {
        using var ctx = new DecentDBContext(_dbPath, pooling: false);
        
        // Create table first
        using (var conn = new DecentDBConnection($"Data Source={_dbPath}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE test_persons (Id INTEGER PRIMARY KEY, Name TEXT)";
            cmd.ExecuteNonQuery();
        }
        
        var persons = ctx.Set<TestPerson>();
        await persons.InsertAsync(new TestPerson { Id = 1, Name = "Non-Pooled Test" });
        
        var person = await persons.GetAsync(1);
        Assert.NotNull(person);
        Assert.Equal("Non-Pooled Test", person.Name);
    }

    [Fact]
    public void Derived_Context_Initialization_Works()
    {
        using var ctx = new DerivedTestContext(_dbPath);
        Assert.NotNull(ctx.Persons);
    }

    private sealed class TestPerson
    {
        public long Id { get; set; }
        public string Name { get; set; } = "";
    }

    private sealed class DerivedTestContext : DecentDBContext
    {
        public DbSet<TestPerson> Persons { get; set; }

        public DerivedTestContext(string connectionString) : base(connectionString)
        {
        }
    }
}