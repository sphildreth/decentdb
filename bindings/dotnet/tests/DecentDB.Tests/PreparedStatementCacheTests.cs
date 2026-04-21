using System.Reflection;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

[Collection(MemoryLeakCollectionDefinition.Name)]
public sealed class PreparedStatementCacheTests : IDisposable
{
    private readonly string _dbPath = ReleaseGateTestHelpers.CreateDbPath("prepared_statement_cache");

    public void Dispose()
    {
        ReleaseGateTestHelpers.DeleteDbArtifacts(_dbPath);
    }

    [Fact]
    public void ManyUniquePreparedStatements_DoNotExceedConnectionCacheCapacity()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        for (var i = 0; i < 400; i++)
        {
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT {i} AS value";
            command.Prepare();
        }

        var cacheCount = GetPreparedStatementCacheCount(connection);

        Assert.InRange(cacheCount, 1, 256);
    }

    [Fact]
    public void ConnectionClose_ClearsPreparedStatementCache_And_ReleasesMemory()
    {
        if (ReleaseGateTestHelpers.SupportsLinuxRssMetrics)
        {
            ReleaseGateTestHelpers.TrimProcessMemory();
        }

        var rssBefore = ReleaseGateTestHelpers.SupportsLinuxRssMetrics
            ? ReleaseGateTestHelpers.ReadRssBytes()
            : 0;

        using (var connection = new DecentDBConnection($"Data Source={_dbPath}"))
        {
            connection.Open();

            for (var i = 0; i < 400; i++)
            {
                using var command = connection.CreateCommand();
                command.CommandText = $"SELECT {i}, {i + 1}, {i + 2}";
                command.Prepare();
            }

            Assert.InRange(GetPreparedStatementCacheCount(connection), 1, 256);
            connection.Close();
            Assert.Equal(0, GetPreparedStatementCacheCount(connection));
        }

        if (!ReleaseGateTestHelpers.SupportsLinuxRssMetrics)
        {
            return;
        }

        ReleaseGateTestHelpers.TrimProcessMemory();
        var rssAfter = ReleaseGateTestHelpers.ReadRssBytes();
        var rssDiff = rssAfter - rssBefore;

        Assert.True(
            rssDiff < 16 * 1024 * 1024,
            $"Prepared statement cache retained {ReleaseGateTestHelpers.FormatBytes(rssDiff)} after connection close (before={ReleaseGateTestHelpers.FormatBytes(rssBefore)}, after={ReleaseGateTestHelpers.FormatBytes(rssAfter)})");
    }

    private static int GetPreparedStatementCacheCount(DecentDBConnection connection)
    {
        var field = typeof(DecentDBConnection).GetField("_preparedStatementCache",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);

        var value = field!.GetValue(connection);
        Assert.NotNull(value);

        var countProperty = value!.GetType().GetProperty("Count");
        Assert.NotNull(countProperty);

        return (int)(countProperty!.GetValue(value) ?? 0);
    }
}
