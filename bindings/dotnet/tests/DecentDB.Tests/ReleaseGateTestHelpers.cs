using System.Runtime.InteropServices;
using System.Threading;

namespace DecentDB.Tests;

internal static class ReleaseGateTestHelpers
{
    public static string CreateDbPath(string prefix)
    {
        return Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}.ddb");
    }

    public static void DeleteDbArtifacts(string dbPath)
    {
        foreach (var suffix in new[] { string.Empty, "-wal", ".bak", ".copy", ".copy-wal" })
        {
            var path = dbPath + suffix;
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
    }

    public static bool SupportsLinuxRssMetrics => RuntimeInformation.IsOSPlatform(OSPlatform.Linux);

    public static long ReadRssBytes()
    {
        if (!SupportsLinuxRssMetrics)
        {
            throw new PlatformNotSupportedException("RSS metrics are only available on Linux test hosts.");
        }

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

    public static void TrimProcessMemory()
    {
        if (!SupportsLinuxRssMetrics)
        {
            return;
        }

        for (var i = 0; i < 3; i++)
        {
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
            GC.WaitForPendingFinalizers();
            MallocTrim(0);
            Thread.Sleep(10);
        }
    }

    public static long FileLengthOrZero(string path)
    {
        return File.Exists(path) ? new FileInfo(path).Length : 0;
    }

    public static string FormatBytes(long bytes)
    {
        return $"{bytes / 1024d / 1024d:F1} MiB";
    }

    [DllImport("libc.so.6", CallingConvention = CallingConvention.Cdecl, EntryPoint = "malloc_trim")]
    private static extern int MallocTrim(nuint pad);
}
