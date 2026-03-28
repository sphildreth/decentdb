using System;
using System.Data;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class MemoryLeakCollectionDefinition
{
    public const string Name = "MemoryLeakTests";
}

[Collection(MemoryLeakCollectionDefinition.Name)]
public class MemoryLeakTests
{
    [Fact]
    public void RepeatedOpenQueryCloseKeepsRssBounded()
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

            TrimProcessMemory();
            var before = ReadRssBytes();

            for (var i = 0; i < 160; i++)
            {
                RunLeakIteration(dbPath);
                if (i % 10 == 0)
                {
                    TrimProcessMemory();
                }
            }

            TrimProcessMemory();
            var after = ReadRssBytes();
            var diff = after - before;

            Assert.True(
                diff < 16 * 1024 * 1024,
                $"RSS grew by {diff} bytes (before={before}, after={after})");
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

    private static void TrimProcessMemory()
    {
        for (var i = 0; i < 3; i++)
        {
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
            GC.WaitForPendingFinalizers();
            MallocTrim(0);
            Thread.Sleep(10);
        }
    }

    private static long ReadRssBytes()
    {
        foreach (var line in File.ReadLines("/proc/self/status"))
        {
            if (!line.StartsWith("VmRSS:", StringComparison.Ordinal))
            {
                continue;
            }

            var parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length >= 2 && long.TryParse(parts[1], out var kb))
            {
                return kb * 1024;
            }

            break;
        }

        throw new InvalidOperationException("VmRSS not found in /proc/self/status");
    }

    [DllImport("libc.so.6", CallingConvention = CallingConvention.Cdecl, EntryPoint = "malloc_trim")]
    private static extern int MallocTrim(nuint pad);
}
