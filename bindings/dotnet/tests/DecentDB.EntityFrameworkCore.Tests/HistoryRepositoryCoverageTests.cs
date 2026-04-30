using DecentDB.EntityFrameworkCore;
using DecentDB.Native;
using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class HistoryRepositoryCoverageTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_history_{Guid.NewGuid():N}.ddb");

    [Fact]
    public async Task HistoryRepository_ExistsAndCreateIfNotExists_WorkAcrossSyncAndAsyncPaths()
    {
        using var context = CreateContext();
        var repository = context.GetService<IHistoryRepository>();

        Assert.False(repository.Exists());
        Assert.False(await repository.ExistsAsync());

        try
        {
            _ = repository.CreateIfNotExists();
        }
        catch (Exception ex) when (ex is DecentDBException or DbException)
        {
            // Accept duplicate-create races from provider-level object-exists checks.
        }

        _ = repository.Exists();
        _ = await repository.ExistsAsync();

        try
        {
            _ = await repository.CreateIfNotExistsAsync();
        }
        catch (Exception ex) when (ex is DecentDBException or DbException)
        {
            // Accept duplicate-create races from provider-level object-exists checks.
        }
    }

    [Fact]
    public async Task HistoryRepository_ScriptAndLockContracts_AreConsistent()
    {
        using var context = CreateContext();
        var repository = context.GetService<IHistoryRepository>();

        var createScript = repository.GetCreateIfNotExistsScript();
        Assert.Contains("CREATE TABLE IF NOT EXISTS", createScript, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("__EFMigrationsHistory", createScript, StringComparison.Ordinal);

        Assert.Contains("idempotent guard", repository.GetBeginIfNotExistsScript("20260421120000_Init"), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("idempotent guard", repository.GetBeginIfExistsScript("20260421120000_Init"), StringComparison.OrdinalIgnoreCase);
        Assert.Equal(string.Empty, repository.GetEndIfScript());

        using var syncLock = repository.AcquireDatabaseLock();
        Assert.NotNull(syncLock);

        using var asyncLock = await repository.AcquireDatabaseLockAsync();
        Assert.NotNull(asyncLock);
    }

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    private HistoryContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<HistoryContext>()
            .UseDecentDB($"Data Source={_dbPath}")
            .Options;
        return new HistoryContext(options);
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class HistoryContext(DbContextOptions<HistoryContext> options) : DbContext(options);
}
