using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DecentDB.AdoNet;
using DecentDB.MicroOrm;
using Xunit;

namespace DecentDB.Tests;

/// <summary>
/// Tests for new features: ConnectionStringBuilder, Factory, INSERT RETURNING (auto-increment),
/// raw SQL on Context, Upsert, InsertOrIgnore, GetSchema("Indexes"), and Projection.
/// </summary>
public sealed class NewFeatureTests : IDisposable
{
    private readonly string _dbPath;

    public NewFeatureTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_new_features_{Guid.NewGuid():N}.ddb");
    }

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    private static void TryDelete(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); } catch { }
    }

    private DecentDBConnection OpenConnection()
    {
        var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        return conn;
    }

    private void CreateTable(string ddl)
    {
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
    }

    // ───── Phase 2: ConnectionStringBuilder ─────

    [Fact]
    public void ConnectionStringBuilder_DataSource()
    {
        var builder = new DecentDBConnectionStringBuilder();
        builder.DataSource = "/tmp/test.ddb";
        Assert.Contains("Data Source=/tmp/test.ddb", builder.ConnectionString);
    }

    [Fact]
    public void ConnectionStringBuilder_RoundTrip()
    {
        var builder = new DecentDBConnectionStringBuilder("Data Source=/tmp/test.ddb;Cache Size=64MB;Logging=True;LogLevel=Info;Command Timeout=60");
        Assert.Equal("/tmp/test.ddb", builder.DataSource);
        Assert.Equal("64MB", builder.CacheSize);
        Assert.True(builder.Logging);
        Assert.Equal("Info", builder.LogLevel);
        Assert.Equal(60, builder.CommandTimeout);
    }

    [Fact]
    public void ConnectionStringBuilder_Defaults()
    {
        var builder = new DecentDBConnectionStringBuilder();
        Assert.Equal(string.Empty, builder.DataSource);
        Assert.Null(builder.CacheSize);
        Assert.False(builder.Logging);
        Assert.Null(builder.LogLevel);
        Assert.Equal(30, builder.CommandTimeout);
    }

    [Fact]
    public void ConnectionStringBuilder_SetProperties()
    {
        var builder = new DecentDBConnectionStringBuilder();
        builder.DataSource = "/tmp/my.ddb";
        builder.CacheSize = "32MB";
        builder.Logging = true;
        builder.LogLevel = "Debug";
        builder.CommandTimeout = 120;

        var cs = builder.ConnectionString;
        Assert.Contains("Data Source=/tmp/my.ddb", cs);
        Assert.Contains("Cache Size=32MB", cs);
        Assert.Contains("Logging=True", cs);
        Assert.Contains("LogLevel=Debug", cs);
        Assert.Contains("Command Timeout=120", cs);
    }

    [Fact]
    public void ConnectionStringBuilder_UsableWithConnection()
    {
        var builder = new DecentDBConnectionStringBuilder { DataSource = _dbPath };
        using var conn = new DecentDBConnection(builder.ConnectionString);
        conn.Open();
        Assert.Equal(System.Data.ConnectionState.Open, conn.State);
    }

    // ───── Phase 2: Factory ─────

    [Fact]
    public void Factory_Instance_NotNull()
    {
        Assert.NotNull(DecentDBFactory.Instance);
    }

    [Fact]
    public void Factory_CreateConnection()
    {
        var conn = DecentDBFactory.Instance.CreateConnection();
        Assert.NotNull(conn);
        Assert.IsType<DecentDBConnection>(conn);
    }

    [Fact]
    public void Factory_CreateCommand()
    {
        var cmd = DecentDBFactory.Instance.CreateCommand();
        Assert.NotNull(cmd);
        Assert.IsType<DecentDBCommand>(cmd);
    }

    [Fact]
    public void Factory_CreateParameter()
    {
        var param = DecentDBFactory.Instance.CreateParameter();
        Assert.NotNull(param);
        Assert.IsType<DecentDBParameter>(param);
    }

    [Fact]
    public void Factory_CreateConnectionStringBuilder()
    {
        var csb = DecentDBFactory.Instance.CreateConnectionStringBuilder();
        Assert.NotNull(csb);
        Assert.IsType<DecentDBConnectionStringBuilder>(csb);
    }

    [Fact]
    public void Factory_CanCreateDataSourceEnumerator_IsFalse()
    {
        Assert.False(DecentDBFactory.Instance.CanCreateDataSourceEnumerator);
    }

    // ───── Phase 3: INSERT RETURNING (auto-increment) ─────

    [Table("auto_items")]
    private sealed class AutoItem
    {
        public long Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Fact]
    public async Task InsertAsync_AutoIncrement_SetsId()
    {
        CreateTable("CREATE TABLE auto_items (id INTEGER PRIMARY KEY, name TEXT)");

        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<AutoItem>();

        var item = new AutoItem { Name = "First" };
        Assert.Equal(0, item.Id);

        await set.InsertAsync(item);
        Assert.True(item.Id > 0, "Id should be auto-assigned");

        var item2 = new AutoItem { Name = "Second" };
        await set.InsertAsync(item2);
        Assert.True(item2.Id > item.Id, "Second id should be greater");
    }

    [Fact]
    public async Task InsertAsync_ExplicitId_StillWorks()
    {
        CreateTable("CREATE TABLE auto_items (id INTEGER PRIMARY KEY, name TEXT)");

        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<AutoItem>();

        var item = new AutoItem { Id = 42, Name = "Explicit" };
        await set.InsertAsync(item);
        Assert.Equal(42, item.Id);

        var fetched = await set.GetAsync(42L);
        Assert.NotNull(fetched);
        Assert.Equal("Explicit", fetched!.Name);
    }

    [Fact]
    public async Task InsertAsync_AutoIncrement_ReadBack()
    {
        CreateTable("CREATE TABLE auto_items (id INTEGER PRIMARY KEY, name TEXT)");

        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<AutoItem>();

        var item = new AutoItem { Name = "ReadBack" };
        await set.InsertAsync(item);

        var fetched = await set.GetAsync(item.Id);
        Assert.NotNull(fetched);
        Assert.Equal("ReadBack", fetched!.Name);
        Assert.Equal(item.Id, fetched.Id);
    }

    // ───── Phase 4: Raw SQL on Context ─────

    [Fact]
    public async Task ExecuteNonQueryAsync_CreateAndInsert()
    {
        using var ctx = new DecentDBContext(_dbPath);
        await ctx.ExecuteNonQueryAsync("CREATE TABLE raw_test (id INTEGER PRIMARY KEY, val TEXT)");
        var rows = await ctx.ExecuteNonQueryAsync("INSERT INTO raw_test (id, val) VALUES (@p0, @p1)", 1L, "hello");
        Assert.True(rows >= 0);
    }

    [Fact]
    public async Task ExecuteScalarAsync_CountRows()
    {
        CreateTable("CREATE TABLE scalar_test (id INTEGER PRIMARY KEY, val TEXT)");

        using var ctx = new DecentDBContext(_dbPath);
        await ctx.ExecuteNonQueryAsync("INSERT INTO scalar_test (id, val) VALUES (@p0, @p1)", 1L, "a");
        await ctx.ExecuteNonQueryAsync("INSERT INTO scalar_test (id, val) VALUES (@p0, @p1)", 2L, "b");

        var count = await ctx.ExecuteScalarAsync<long>("SELECT COUNT(*) FROM scalar_test");
        Assert.Equal(2L, count);
    }

    [Table("query_items")]
    private sealed class QueryItem
    {
        public long Id { get; set; }
        public string Val { get; set; } = "";
    }

    [Fact]
    public async Task QueryAsync_ReturnsEntities()
    {
        CreateTable("CREATE TABLE query_items (id INTEGER PRIMARY KEY, val TEXT)");

        using var ctx = new DecentDBContext(_dbPath);
        await ctx.ExecuteNonQueryAsync("INSERT INTO query_items (id, val) VALUES (@p0, @p1)", 1L, "alpha");
        await ctx.ExecuteNonQueryAsync("INSERT INTO query_items (id, val) VALUES (@p0, @p1)", 2L, "beta");

        var items = await ctx.QueryAsync<QueryItem>("SELECT id, val FROM query_items ORDER BY id");
        Assert.Equal(2, items.Count);
        Assert.Equal("alpha", items[0].Val);
        Assert.Equal("beta", items[1].Val);
    }

    [Fact]
    public async Task QueryAsync_WithParameters()
    {
        CreateTable("CREATE TABLE query_items (id INTEGER PRIMARY KEY, val TEXT)");

        using var ctx = new DecentDBContext(_dbPath);
        await ctx.ExecuteNonQueryAsync("INSERT INTO query_items (id, val) VALUES (@p0, @p1)", 1L, "alpha");
        await ctx.ExecuteNonQueryAsync("INSERT INTO query_items (id, val) VALUES (@p0, @p1)", 2L, "beta");

        var items = await ctx.QueryAsync<QueryItem>("SELECT id, val FROM query_items WHERE val = @p0", "beta");
        Assert.Single(items);
        Assert.Equal("beta", items[0].Val);
    }

    // ───── Phase 5: Upsert & InsertOrIgnore ─────

    [Table("upsert_items")]
    private sealed class UpsertItem
    {
        public long Id { get; set; }
        public string Name { get; set; } = "";
        public int Score { get; set; }
    }

    [Fact]
    public async Task UpsertAsync_InsertsNewRow()
    {
        CreateTable("CREATE TABLE upsert_items (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)");

        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<UpsertItem>();

        var item = new UpsertItem { Id = 1, Name = "Alice", Score = 100 };
        await set.UpsertAsync(item);

        var fetched = await set.GetAsync(1L);
        Assert.NotNull(fetched);
        Assert.Equal("Alice", fetched!.Name);
        Assert.Equal(100, fetched.Score);
    }

    [Fact]
    public async Task UpsertAsync_UpdatesExistingRow()
    {
        CreateTable("CREATE TABLE upsert_items (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)");

        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<UpsertItem>();

        await set.InsertAsync(new UpsertItem { Id = 1, Name = "Alice", Score = 100 });

        // Upsert same id with updated values
        await set.UpsertAsync(new UpsertItem { Id = 1, Name = "Alice Updated", Score = 200 });

        var all = await set.ToListAsync();
        Assert.Single(all);
        Assert.Equal("Alice Updated", all[0].Name);
        Assert.Equal(200, all[0].Score);
    }

    [Fact]
    public async Task InsertOrIgnoreAsync_InsertsNewRow()
    {
        CreateTable("CREATE TABLE upsert_items (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)");

        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<UpsertItem>();

        await set.InsertOrIgnoreAsync(new UpsertItem { Id = 1, Name = "Alice", Score = 100 });

        var fetched = await set.GetAsync(1L);
        Assert.NotNull(fetched);
        Assert.Equal("Alice", fetched!.Name);
    }

    [Fact]
    public async Task InsertOrIgnoreAsync_IgnoresDuplicate()
    {
        CreateTable("CREATE TABLE upsert_items (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)");

        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<UpsertItem>();

        await set.InsertAsync(new UpsertItem { Id = 1, Name = "Original", Score = 100 });
        await set.InsertOrIgnoreAsync(new UpsertItem { Id = 1, Name = "Duplicate", Score = 999 });

        var fetched = await set.GetAsync(1L);
        Assert.NotNull(fetched);
        Assert.Equal("Original", fetched!.Name);
        Assert.Equal(100, fetched.Score);
    }

    // ───── Phase 6: GetSchema("Indexes") ─────

    [Fact]
    public void GetSchema_Indexes_ReturnsTable()
    {
        CreateTable("CREATE TABLE idx_test (id INTEGER PRIMARY KEY, name TEXT)");
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE INDEX idx_name ON idx_test (name)";
        cmd.ExecuteNonQuery();

        var dt = conn.GetSchema("Indexes");
        Assert.NotNull(dt);
        Assert.True(dt.Rows.Count >= 1);

        var nameRow = dt.Select("INDEX_NAME = 'idx_name'");
        Assert.Single(nameRow);
        Assert.Equal("idx_test", nameRow[0]["TABLE_NAME"]);
        Assert.Equal("name", nameRow[0]["COLUMNS"]);
        Assert.Equal(false, nameRow[0]["IS_UNIQUE"]);
        Assert.Equal("btree", nameRow[0]["INDEX_TYPE"]);
    }

    [Fact]
    public void GetSchema_Indexes_UniqueIndex()
    {
        CreateTable("CREATE TABLE idx_uniq (id INTEGER PRIMARY KEY, email TEXT)");
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE UNIQUE INDEX idx_email ON idx_uniq (email)";
        cmd.ExecuteNonQuery();

        var dt = conn.GetSchema("Indexes");
        var emailRow = dt.Select("INDEX_NAME = 'idx_email'");
        Assert.Single(emailRow);
        Assert.Equal(true, emailRow[0]["IS_UNIQUE"]);
    }

    [Fact]
    public void GetSchema_Indexes_FilterByTable()
    {
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE idx_a (id INTEGER PRIMARY KEY, v TEXT)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "CREATE TABLE idx_b (id INTEGER PRIMARY KEY, v TEXT)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "CREATE INDEX idx_a_v ON idx_a (v)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "CREATE INDEX idx_b_v ON idx_b (v)";
        cmd.ExecuteNonQuery();

        var dt = conn.GetSchema("Indexes", new[] { "idx_a" });
        foreach (System.Data.DataRow row in dt.Rows)
        {
            Assert.Equal("idx_a", row["TABLE_NAME"]);
        }
    }

    [Fact]
    public void GetSchema_MetaDataCollections_IncludesIndexes()
    {
        using var conn = OpenConnection();
        var dt = conn.GetSchema("MetaDataCollections");
        var indexRow = dt.Select("CollectionName = 'Indexes'");
        Assert.Single(indexRow);
    }

    // ───── Phase 7: Projection ─────

    [Table("proj_items")]
    private sealed class ProjItem
    {
        public long Id { get; set; }
        public string Name { get; set; } = "";
        public int Value { get; set; }
    }

    [Fact]
    public async Task SelectAsync_ProjectsSingleColumn()
    {
        CreateTable("CREATE TABLE proj_items (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)");

        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<ProjItem>();

        await set.InsertAsync(new ProjItem { Id = 1, Name = "Alpha", Value = 10 });
        await set.InsertAsync(new ProjItem { Id = 2, Name = "Beta", Value = 20 });

        var names = await set.SelectAsync(x => x.Name);
        Assert.Equal(2, names.Count);
        Assert.Contains("Alpha", names);
        Assert.Contains("Beta", names);
    }

    [Fact]
    public async Task SelectAsync_WithWhere()
    {
        CreateTable("CREATE TABLE proj_items (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)");

        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<ProjItem>();

        await set.InsertAsync(new ProjItem { Id = 1, Name = "Alpha", Value = 10 });
        await set.InsertAsync(new ProjItem { Id = 2, Name = "Beta", Value = 20 });

        var names = await set.Where(x => x.Value > 15).SelectAsync(x => x.Name);
        Assert.Single(names);
        Assert.Equal("Beta", names[0]);
    }
}
