using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore.Design.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class DatabaseModelFactoryAndDbFunctionsTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_factory_{Guid.NewGuid():N}.ddb");

    [Fact]
    public void DatabaseModelFactory_Create_FromConnectionString_LoadsTablesColumnsAndIndexes()
    {
        SeedSchema();

        var factory = new DecentDBDatabaseModelFactory();
        var model = factory.Create($"Data Source={_dbPath}", new DatabaseModelFactoryOptions([], []));

        var users = Assert.Single(model.Tables.Where(table => string.Equals(table.Name, "users", StringComparison.OrdinalIgnoreCase)));
        Assert.Equal(["id", "name", "active"], users.Columns.Select(c => c.Name).ToArray());
        Assert.NotNull(users.PrimaryKey);
        Assert.Equal("id", Assert.Single(users.PrimaryKey!.Columns).Name);
        Assert.Contains(users.Indexes, index => string.Equals(index.Name, "ix_users_name", StringComparison.OrdinalIgnoreCase) && index.IsUnique);
    }

    [Fact]
    public void DatabaseModelFactory_Create_WithTableFilter_OnlyReturnsIncludedTables()
    {
        SeedSchema();

        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        Assert.NotEqual(System.Data.ConnectionState.Open, connection.State);

        var factory = new DecentDBDatabaseModelFactory();
        var model = factory.Create(connection, new DatabaseModelFactoryOptions(["users"], []));

        var table = Assert.Single(model.Tables);
        Assert.Equal("users", table.Name, ignoreCase: true);
    }

    [Fact]
    public void DecentDBDbFunctionsExtensions_ThrowWhenCalledOutsideTranslation()
    {
        Assert.Throws<NotSupportedException>(() => EF.Functions.RowNumber(1));
        Assert.Throws<NotSupportedException>(() => EF.Functions.RowNumber("dep", 1));
        Assert.Throws<NotSupportedException>(() => EF.Functions.Rank(1));
        Assert.Throws<NotSupportedException>(() => EF.Functions.Rank("dep", 1));
        Assert.Throws<NotSupportedException>(() => EF.Functions.DenseRank(1));
        Assert.Throws<NotSupportedException>(() => EF.Functions.DenseRank("dep", 1));
        Assert.Throws<NotSupportedException>(() => EF.Functions.PercentRank(1));
        Assert.Throws<NotSupportedException>(() => EF.Functions.PercentRank("dep", 1));
        Assert.Throws<NotSupportedException>(() => EF.Functions.Lag("value", 1, defaultValue: "x"));
        Assert.Throws<NotSupportedException>(() => EF.Functions.Lag("dep", "value", 1, defaultValue: "x"));
        Assert.Throws<NotSupportedException>(() => EF.Functions.Lead("value", 1, defaultValue: "x"));
        Assert.Throws<NotSupportedException>(() => EF.Functions.Lead("dep", "value", 1, defaultValue: "x"));
        Assert.Throws<NotSupportedException>(() => EF.Functions.FirstValue("value", 1));
        Assert.Throws<NotSupportedException>(() => EF.Functions.FirstValue("dep", "value", 1));
        Assert.Throws<NotSupportedException>(() => EF.Functions.LastValue("value", 1));
        Assert.Throws<NotSupportedException>(() => EF.Functions.LastValue("dep", "value", 1));
        Assert.Throws<NotSupportedException>(() => EF.Functions.NthValue("value", 2, 1));
        Assert.Throws<NotSupportedException>(() => EF.Functions.NthValue("dep", "value", 2, 1));
    }

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    private void SeedSchema()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = "DROP TABLE IF EXISTS users;";
        command.ExecuteNonQuery();
        command.CommandText = "DROP TABLE IF EXISTS audit_log;";
        command.ExecuteNonQuery();
        command.CommandText = """
            CREATE TABLE users (
              id INTEGER PRIMARY KEY,
              name TEXT NOT NULL,
              active BOOLEAN NOT NULL
            );
            """;
        command.ExecuteNonQuery();
        command.CommandText = "CREATE UNIQUE INDEX ix_users_name ON users (name);";
        command.ExecuteNonQuery();
        command.CommandText = "CREATE TABLE audit_log (id INTEGER PRIMARY KEY, message TEXT NOT NULL);";
        command.ExecuteNonQuery();
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }
}
