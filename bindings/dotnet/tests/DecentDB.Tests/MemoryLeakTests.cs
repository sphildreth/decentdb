using System;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

public class MemoryLeakTests
{
    [Fact]
    public void RepeatedOpenQueryCloseKeepsWorkingSetBounded()
    {
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            return;
        }

        var dbPath = Path.Combine(Path.GetTempPath(), $"memory_leak_{Guid.NewGuid():N}.ddb");
        try
        {
            using (var connection = new DecentDBConnection($"Data Source={dbPath}"))
            {
                connection.Open();
                using var command = connection.CreateCommand();
                command.CommandText = "CREATE TABLE leak_probe (id INT64 PRIMARY KEY, payload TEXT)";
                command.ExecuteNonQuery();
                command.CommandText = "INSERT INTO leak_probe (id, payload) VALUES (1, 'probe')";
                command.ExecuteNonQuery();
            }

            for (var i = 0; i < 25; i++)
            {
                RunLeakIteration(dbPath);
            }

            TrimManagedHeap();
            var before = ReadWorkingSetBytes();

            for (var i = 0; i < 160; i++)
            {
                RunLeakIteration(dbPath);
                if (i % 10 == 0)
                {
                    TrimManagedHeap();
                }
            }

            TrimManagedHeap();
            var after = ReadWorkingSetBytes();
            var diff = after - before;

            Assert.True(
                diff < 16 * 1024 * 1024,
                $"Working set grew by {diff} bytes (before={before}, after={after})");
        }
        finally
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + "-wal"))
                File.Delete(dbPath + "-wal");
        }
    }

    private static void RunLeakIteration(string dbPath)
    {
        using var connection = new DecentDBConnection($"Data Source={dbPath}");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM leak_probe";
        var result = command.ExecuteScalar();
        Assert.Equal(1L, Convert.ToInt64(result));
        Assert.Equal(ConnectionState.Open, connection.State);
    }

    private static void TrimManagedHeap()
    {
        for (var i = 0; i < 3; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            Thread.Sleep(10);
        }
    }

    private static long ReadWorkingSetBytes()
    {
        using var process = Process.GetCurrentProcess();
        process.Refresh();
        return process.WorkingSet64;
    }
}
