using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using Xunit;
using DecentDB.AdoNet;

namespace DecentDB.Tests;

public class ConnectionTests
{
    [Fact]
    public void OpenAndCloseDatabase()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid():N}.ddb");
        try
        {
            using var conn = new DecentDBConnection($"Data Source={dbPath}");
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
        var dbPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid():N}", "subdir", "db.ddb");
        try
        {
            using var conn = new DecentDBConnection($"Data Source={dbPath}");
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

    [Fact]
    public void OpenWithCacheSizeMb_Succeeds()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid():N}.ddb");
        try
        {
            using var conn = new DecentDBConnection($"Data Source={dbPath};Cache Size=64MB");
            conn.Open();

            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE t (id INT64)";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "INSERT INTO t (id) VALUES (1)";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "SELECT id FROM t";
            var val = cmd.ExecuteScalar();
            Assert.Equal(1L, Convert.ToInt64(val));
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
    public void SqlObservability_EventsFire_WhenHandlersAttached()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid():N}.ddb");
        try
        {
            using var conn = new DecentDBConnection($"Data Source={dbPath};Logging=0");
            conn.Open();

            var executingEvents = new System.Collections.Generic.List<SqlExecutingEventArgs>();
            var executedEvents = new System.Collections.Generic.List<SqlExecutedEventArgs>();

            conn.SqlExecuting += (s, e) => executingEvents.Add(e);
            conn.SqlExecuted += (s, e) => executedEvents.Add(e);

            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE t (id INT64)";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "INSERT INTO t (id) VALUES (1)";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "SELECT id FROM t";
            _ = cmd.ExecuteScalar();

            Assert.True(executingEvents.Count >= 3);
            Assert.Equal(executingEvents.Count, executedEvents.Count);
            Assert.All(executedEvents, e => Assert.Null(e.Exception));
            Assert.All(executedEvents, e => Assert.True(e.Duration >= TimeSpan.Zero));
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
    public void DeleteDatabaseFiles_RemovesAllSidecars()
    {
        var dir = Path.GetTempPath();
        var basePath = Path.Combine(dir, $"testdel_{Guid.NewGuid():N}.ddb");
        // Create dummy files
        File.WriteAllText(basePath, "dummy");
        File.WriteAllText(basePath + ".wal", "wal");
        File.WriteAllText(basePath + "-wal", "d-wal");
        File.WriteAllText(basePath + "-shm", "shm");

        DecentDBConnection.DeleteDatabaseFiles(basePath);

        Assert.False(File.Exists(basePath), "Data file should be deleted");
        Assert.False(File.Exists(basePath + ".wal"), ".wal file should be deleted");
        Assert.False(File.Exists(basePath + "-wal"), "-wal file should be deleted");
        Assert.False(File.Exists(basePath + "-shm"), "-shm file should be deleted");
    }

    [Fact]
    public void DeleteDatabaseFiles_NoOp_WhenMissing()
    {
        var dir = Path.GetTempPath();
        var basePath = Path.Combine(dir, $"testdel_{Guid.NewGuid():N}.ddb");
        // Ensure no files exist
        Assert.False(File.Exists(basePath));
        Assert.False(File.Exists(basePath + ".wal"));
        Assert.False(File.Exists(basePath + "-wal"));
        Assert.False(File.Exists(basePath + "-shm"));

        // Should not throw
        DecentDBConnection.DeleteDatabaseFiles(basePath);
    }

    [Fact]
    public void DeleteDatabaseFiles_DeletesDataFileLast()
    {
        var dir = Path.GetTempPath();
        var basePath = Path.Combine(dir, $"testdel_{Guid.NewGuid():N}.ddb");
        // Create dummy files
        File.WriteAllText(basePath, "dummy");
        File.WriteAllText(basePath + ".wal", "wal");
        File.WriteAllText(basePath + "-wal", "d-wal");
        File.WriteAllText(basePath + "-shm", "shm");

        // Verify all files exist before deletion
        Assert.True(File.Exists(basePath));
        Assert.True(File.Exists(basePath + ".wal"));
        Assert.True(File.Exists(basePath + "-wal"));
        Assert.True(File.Exists(basePath + "-shm"));

        DecentDBConnection.DeleteDatabaseFiles(basePath);

        // Verify all files are deleted
        Assert.False(File.Exists(basePath), "Data file should be deleted");
        Assert.False(File.Exists(basePath + ".wal"), ".wal file should be deleted");
        Assert.False(File.Exists(basePath + "-wal"), "-wal file should be deleted");
        Assert.False(File.Exists(basePath + "-shm"), "-shm file should be deleted");
    }
}
