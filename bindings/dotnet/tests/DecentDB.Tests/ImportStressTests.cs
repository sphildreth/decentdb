using System.Data;
using System.Data.Common;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

[Collection(MemoryLeakCollectionDefinition.Name)]
public sealed class ImportStressTests : IDisposable
{
    private readonly string _dbPath = ReleaseGateTestHelpers.CreateDbPath("import_stress");

    public void Dispose()
    {
        ReleaseGateTestHelpers.DeleteDbArtifacts(_dbPath);
    }

    [Theory]
    [InlineData(10_000, 1_000, false, 0)]
    [InlineData(100_000, 5_000, true, 4)]
    [InlineData(250_000, 10_000, true, 2)]
    public void CommandReuse_PreparedAndUnprepared_InsertStress_CompletesAndBoundsWal(
        int rowCount,
        int chunkSize,
        bool prepare,
        int checkpointEveryChunks)
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE import_probe (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)";
            create.ExecuteNonQuery();
        }

        var peakRss = ReleaseGateTestHelpers.SupportsLinuxRssMetrics
            ? ReleaseGateTestHelpers.ReadRssBytes()
            : 0;
        var peakWal = 0L;
        var nextId = 1;
        var chunksCompleted = 0;

        while (nextId <= rowCount)
        {
            using var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "INSERT INTO import_probe (id, payload) VALUES (@p0, @p1)";
            AddParameter(command, "@p0");
            AddParameter(command, "@p1");

            if (prepare)
            {
                command.Prepare();
            }

            var upperExclusive = Math.Min(rowCount + 1, nextId + chunkSize);
            for (var id = nextId; id < upperExclusive; id++)
            {
                command.Parameters[0].Value = id;
                command.Parameters[1].Value = $"payload-{id:D8}";
                command.ExecuteNonQuery();
            }

            transaction.Commit();
            chunksCompleted++;

            if (checkpointEveryChunks > 0 && chunksCompleted % checkpointEveryChunks == 0)
            {
                connection.Checkpoint();
            }

            peakWal = Math.Max(peakWal, ReleaseGateTestHelpers.FileLengthOrZero(_dbPath + "-wal"));
            if (ReleaseGateTestHelpers.SupportsLinuxRssMetrics)
            {
                ReleaseGateTestHelpers.TrimProcessMemory();
                peakRss = Math.Max(peakRss, ReleaseGateTestHelpers.ReadRssBytes());
            }

            nextId = upperExclusive;
        }

        connection.Checkpoint();

        using (var verify = connection.CreateCommand())
        {
            verify.CommandText = "SELECT COUNT(*) FROM import_probe";
            Assert.Equal((long)rowCount, Convert.ToInt64(verify.ExecuteScalar()));
        }

        var walAfterCheckpoint = ReleaseGateTestHelpers.FileLengthOrZero(_dbPath + "-wal");
        Assert.True(
            walAfterCheckpoint < Math.Max(1024 * 1024, peakWal / 4),
            $"WAL remained too large after import checkpoint (peak={ReleaseGateTestHelpers.FormatBytes(peakWal)}, after={ReleaseGateTestHelpers.FormatBytes(walAfterCheckpoint)})");

        if (!ReleaseGateTestHelpers.SupportsLinuxRssMetrics)
        {
            return;
        }

        ReleaseGateTestHelpers.TrimProcessMemory();
        var rssAfter = ReleaseGateTestHelpers.ReadRssBytes();

        Assert.True(
            rssAfter <= peakRss,
            $"RSS did not recover after import stress (peak={ReleaseGateTestHelpers.FormatBytes(peakRss)}, after={ReleaseGateTestHelpers.FormatBytes(rssAfter)})");
    }

    private static void AddParameter(DbCommand command, string name)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        command.Parameters.Add(parameter);
    }
}
