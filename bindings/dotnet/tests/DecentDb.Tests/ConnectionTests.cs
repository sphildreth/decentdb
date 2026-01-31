using System;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using Xunit;
using DecentDb.AdoNet;

namespace DecentDb.Tests;

public class ConnectionTests
{
    [Fact]
    public void OpenAndCloseDatabase()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid():N}.db");
        try
        {
            using var conn = new DecentDbConnection($"Data Source={dbPath}");
            Assert.Equal(ConnectionState.Closed, conn.State);
            conn.Open();
            Assert.Equal(ConnectionState.Open, conn.State);
            conn.Close();
            Assert.Equal(ConnectionState.Closed, conn.State);
        }
        finally
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + "-wal"))
                File.Delete(dbPath + "-wal");
        }
    }

    [Fact]
    public void OpenNonExistentDatabase()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid():N}", "subdir", "db.db");
        try
        {
            using var conn = new DecentDbConnection($"Data Source={dbPath}");
            conn.Open();
            Assert.Equal(ConnectionState.Open, conn.State);
            conn.Close();
        }
        finally
        {
            var dir = Path.GetDirectoryName(dbPath);
            if (dir != null && Directory.Exists(dir))
                Directory.Delete(dir, true);
        }
    }
}
