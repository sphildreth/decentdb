using System;
using System.Data;
using System.IO;
using System.Threading.Tasks;
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
}
