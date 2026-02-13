using System.Data;
using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class ProviderSmokeTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_provider_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    [Fact]
    public void UseDecentDb_RegistersProviderExtension()
    {
        var optionsBuilder = new DbContextOptionsBuilder();
        optionsBuilder.UseDecentDB($"Data Source={_dbPath}");

        var extension = optionsBuilder.Options.FindExtension<DecentDBOptionsExtension>();
        Assert.NotNull(extension);
    }

    [Fact]
    public void UseDecentDb_CanOpenRelationalConnection()
    {
        var optionsBuilder = new DbContextOptionsBuilder<SmokeDbContext>();
        optionsBuilder.UseDecentDB($"Data Source={_dbPath}");

        using var context = new SmokeDbContext(optionsBuilder.Options);
        using var connection = context.Database.GetDbConnection();

        connection.Open();
        Assert.Equal(ConnectionState.Open, connection.State);
    }

    [Fact]
    public void UseDecentDb_WithExistingDbConnection_UsesSameConnection()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        var optionsBuilder = new DbContextOptionsBuilder<SmokeDbContext>();
        optionsBuilder.UseDecentDB(connection);
        using var context = new SmokeDbContext(optionsBuilder.Options);
        using var resolved = context.Database.GetDbConnection();

        Assert.Same(connection, resolved);
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch
        {
            // Best-effort test cleanup.
        }
    }

    private sealed class SmokeDbContext : DbContext
    {
        public SmokeDbContext(DbContextOptions<SmokeDbContext> options)
            : base(options)
        {
        }
    }
}
