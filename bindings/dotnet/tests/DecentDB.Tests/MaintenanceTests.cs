using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;
using DecentDB.AdoNet;

namespace DecentDB.Tests;

public class MaintenanceTests
{
    private static string GetDecentDbCliPath()
    {
        // Try well-known Cargo output paths first (works after cargo clean + rebuild)
        var dir = new DirectoryInfo(Directory.GetCurrentDirectory());
        while (dir != null)
        {
            foreach (var profile in new[] { "release", "debug" })
            {
                var cargoPath = Path.Combine(dir.FullName, "target", profile, "decentdb");
                if (File.Exists(cargoPath))
                    return cargoPath;
            }

            // Also check for a loose binary in the directory itself
            var exePath = Path.Combine(dir.FullName, "decentdb");
            if (File.Exists(exePath))
                return exePath;

            dir = dir.Parent;
        }
        
        return "decentdb"; // Fallback to PATH
    }

    [Fact]
    public async Task VacuumAtomicAsync_NonExistentFile_ReturnsFalse()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"test_vacuum_none_{Guid.NewGuid():N}.ddb");
        
        var result = await DecentDBMaintenance.VacuumAtomicAsync(dbPath, GetDecentDbCliPath());
        
        Assert.False(result);
    }

    [Fact]
    public void GetWalStatus_MissingDatabase_ReturnsZeroSizes()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"test_wal_status_none_{Guid.NewGuid():N}.ddb");

        var status = DecentDBMaintenance.GetWalStatus(dbPath);

        Assert.Equal(Path.GetFullPath(dbPath), status.DatabasePath);
        Assert.Equal(0, status.DatabaseBytes);
        Assert.Equal(0, status.TotalWalBytes);
        Assert.False(status.HasWal);
    }

    [Fact]
    public async Task CheckpointAsync_ValidFile_CheckpointsWithoutCli()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"test_checkpoint_binding_{Guid.NewGuid():N}.ddb");

        try
        {
            SeedDatabase(dbPath);

            var result = await DecentDBMaintenance.CheckpointAsync(dbPath);

            Assert.True(result.DatabaseExisted);
            Assert.Equal(Path.GetFullPath(dbPath), result.DatabasePath);
            Assert.True(File.Exists(dbPath));
            Assert.True(result.After.TotalWalBytes <= result.Before.TotalWalBytes);
        }
        finally
        {
            DecentDBConnection.DeleteDatabaseFiles(dbPath);
        }
    }

    [Fact]
    public async Task CompactAsync_ValidFile_CreatesReadableCopyWithoutCli()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"test_compact_binding_{Guid.NewGuid():N}.ddb");
        var compactedPath = dbPath + ".compact";

        try
        {
            SeedDatabase(dbPath);

            var result = await DecentDBMaintenance.CompactAsync(dbPath, compactedPath);

            Assert.True(result.SourceExisted);
            Assert.True(result.SourceBytes > 0);
            Assert.True(result.DestinationBytes > 0);
            Assert.Equal(1L, CountRows(compactedPath));
        }
        finally
        {
            DecentDBConnection.DeleteDatabaseFiles(dbPath);
            DecentDBConnection.DeleteDatabaseFiles(compactedPath);
        }
    }

    [Fact]
    public async Task CompactAsync_SameSourceAndDestination_Throws()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"test_compact_same_{Guid.NewGuid():N}.ddb");

        try
        {
            SeedDatabase(dbPath);

            await Assert.ThrowsAsync<ArgumentException>(() =>
                DecentDBMaintenance.CompactAsync(dbPath, dbPath, overwrite: true));
        }
        finally
        {
            DecentDBConnection.DeleteDatabaseFiles(dbPath);
        }
    }

    [Fact]
    public async Task CompactAsync_DestinationSidecarExists_RequiresOverwrite()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"test_compact_sidecar_{Guid.NewGuid():N}.ddb");
        var compactedPath = dbPath + ".compact";

        try
        {
            SeedDatabase(dbPath);
            await File.WriteAllTextAsync(compactedPath + "-wal", "stale");

            await Assert.ThrowsAsync<IOException>(() =>
                DecentDBMaintenance.CompactAsync(dbPath, compactedPath));

            var result = await DecentDBMaintenance.CompactAsync(dbPath, compactedPath, overwrite: true);

            Assert.True(result.SourceExisted);
            Assert.Equal(1L, CountRows(compactedPath));
        }
        finally
        {
            DecentDBConnection.DeleteDatabaseFiles(dbPath);
            DecentDBConnection.DeleteDatabaseFiles(compactedPath);
        }
    }

    [Fact]
    public async Task VacuumAsync_ValidFile_ReplacesDatabaseAndCanCreateBackupWithoutCli()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"test_vacuum_binding_{Guid.NewGuid():N}.ddb");

        try
        {
            SeedDatabase(dbPath);

            var result = await DecentDBMaintenance.VacuumAsync(dbPath, createBackup: true);

            Assert.True(result.DatabaseExisted);
            Assert.True(result.BackupCreated);
            Assert.True(File.Exists(dbPath));
            Assert.NotNull(result.BackupPath);
            Assert.True(File.Exists(result.BackupPath));
            Assert.Equal(1L, CountRows(dbPath));
        }
        finally
        {
            DecentDBConnection.DeleteDatabaseFiles(dbPath);
            if (File.Exists(dbPath + ".bak")) File.Delete(dbPath + ".bak");
        }
    }

    [Fact]
    public async Task VacuumAtomicAsync_ValidFile_PerformsVacuum()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"test_vacuum_{Guid.NewGuid():N}.ddb");
        var cliPath = GetDecentDbCliPath();

        try
        {
            // Seed a database
            using (var conn = new DecentDBConnection($"Data Source={dbPath}"))
            {
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "CREATE TABLE VacuumTest (Id INTEGER PRIMARY KEY, Val TEXT);";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "INSERT INTO VacuumTest (Id, Val) VALUES (1, 'Hello');";
                cmd.ExecuteNonQuery();
            }

            // File should exist now
            Assert.True(File.Exists(dbPath));
            
            // Perform vacuum without backup
            var result = await DecentDBMaintenance.VacuumAtomicAsync(dbPath, cliPath, createBackup: false);
            
            Assert.True(result);
            Assert.True(File.Exists(dbPath));
            Assert.False(File.Exists(dbPath + ".bak"));
            
            // Perform vacuum with backup
            var resultWithBackup = await DecentDBMaintenance.VacuumAtomicAsync(dbPath, cliPath, createBackup: true);
            
            Assert.True(resultWithBackup);
            Assert.True(File.Exists(dbPath));
            Assert.True(File.Exists(dbPath + ".bak"));

            // Verify data is still intact
            using (var conn = new DecentDBConnection($"Data Source={dbPath}"))
            {
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT COUNT(*) FROM VacuumTest;";
                var count = Convert.ToInt64(cmd.ExecuteScalar());
                Assert.Equal(1L, count);
            }
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(dbPath + ".bak")) File.Delete(dbPath + ".bak");
        }
    }

    private static void SeedDatabase(string dbPath)
    {
        using var conn = new DecentDBConnection($"Data Source={dbPath};WAL Auto Checkpoint=0");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE MaintenanceTest (Id INTEGER PRIMARY KEY, Val TEXT);";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO MaintenanceTest (Id, Val) VALUES (1, 'Hello');";
        cmd.ExecuteNonQuery();
    }

    private static long CountRows(string dbPath)
    {
        using var conn = new DecentDBConnection($"Data Source={dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM MaintenanceTest;";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }
}
