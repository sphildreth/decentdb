using System.Data;
using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class ProviderErgonomicsTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_ergonomics_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    [Fact]
    public void UseDecentDB_WithConnectionStringBuilder_RegistersProviderExtension()
    {
        var optionsBuilder = new DbContextOptionsBuilder();
        var csb = new DecentDBConnectionStringBuilder
        {
            DataSource = _dbPath,
            CommandTimeout = 45
        };

        optionsBuilder.UseDecentDB(csb);

        var extension = optionsBuilder.Options.FindExtension<DecentDBOptionsExtension>();
        Assert.NotNull(extension);
    }

    [Fact]
    public void UseDecentDB_GenericBuilderOverload_CanOpenConnection()
    {
        var csb = new DecentDBConnectionStringBuilder
        {
            DataSource = _dbPath,
            CommandTimeout = 45
        };

        var optionsBuilder = new DbContextOptionsBuilder<ErgonomicsDbContext>();
        optionsBuilder.UseDecentDB(csb);

        using var context = new ErgonomicsDbContext(optionsBuilder.Options);
        using var connection = context.Database.GetDbConnection();

        connection.Open();
        Assert.Equal(ConnectionState.Open, connection.State);
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class ErgonomicsDbContext(DbContextOptions<ErgonomicsDbContext> options) : DbContext(options);
}
