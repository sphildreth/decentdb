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
}
