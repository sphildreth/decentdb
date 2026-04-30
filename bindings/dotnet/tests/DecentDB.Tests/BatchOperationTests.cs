using System.Data;
using System.Data.Common;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

[Collection(MemoryLeakCollectionDefinition.Name)]
public sealed class BatchOperationTests : IDisposable
{
    private readonly string _dbPath = ReleaseGateTestHelpers.CreateDbPath("batch_operations");

    public void Dispose()
    {
        ReleaseGateTestHelpers.DeleteDbArtifacts(_dbPath);
    }

    [Fact]
    public void ExecuteNonQuery_1000RowsInSingleTransaction_CompletesSuccessfully()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE batch_items (id INTEGER PRIMARY KEY, value TEXT NOT NULL, quantity INTEGER)";
            create.ExecuteNonQuery();
        }

        using var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "INSERT INTO batch_items (id, value, quantity) VALUES (@p0, @p1, @p2)";
        AddParameter(command, "@p0");
        AddParameter(command, "@p1");
        AddParameter(command, "@p2");
        command.Prepare();

        for (int i = 1; i <= 1000; i++)
        {
            command.Parameters[0].Value = i;
            command.Parameters[1].Value = $"item_{i}";
            command.Parameters[2].Value = i * 10;
            command.ExecuteNonQuery();
        }

        transaction.Commit();

        using var verify = connection.CreateCommand();
        verify.CommandText = "SELECT COUNT(*) FROM batch_items";
        Assert.Equal(1000L, Convert.ToInt64(verify.ExecuteScalar()));
    }

    [Fact]
    public void ExecuteNonQuery_5000Rows_AffectedCount_MatchesExpected()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using var create = connection.CreateCommand();
        create.CommandText = "CREATE TABLE row_count_test (id INTEGER PRIMARY KEY, data TEXT)";
        create.ExecuteNonQuery();

        using var insert = connection.CreateCommand();
        insert.CommandText = "INSERT INTO row_count_test (id, data) VALUES (@id, @data)";
        AddParameter(insert, "@id");
        AddParameter(insert, "@data");
        insert.Prepare();

        int expectedRows = 5000;
        int totalAffected = 0;

        for (int i = 1; i <= expectedRows; i++)
        {
            insert.Parameters[0].Value = i;
            insert.Parameters[1].Value = $"data_{i}";
            totalAffected += insert.ExecuteNonQuery();
        }

        Assert.Equal(expectedRows, totalAffected);

        using var verify = connection.CreateCommand();
        verify.CommandText = "SELECT COUNT(*) FROM row_count_test";
        Assert.Equal(expectedRows, Convert.ToInt64(verify.ExecuteScalar()));
    }

    [Fact]
    public void BatchInsert_AllDataTypes_RoundTrip()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using var create = connection.CreateCommand();
        create.CommandText = """
            CREATE TABLE batch_all_types (
                id INTEGER PRIMARY KEY,
                int_val INTEGER,
                text_val TEXT,
                real_val REAL,
                bool_val BOOLEAN,
                blob_val BLOB,
                uuid_val UUID
            )
            """;
        create.ExecuteNonQuery();

        using var transaction = connection.BeginTransaction();
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "INSERT INTO batch_all_types (id, int_val, text_val, real_val, bool_val, blob_val, uuid_val) VALUES (@p0, @p1, @p2, @p3, @p4, @p5, @p6)";
        AddParameter(command, "@p0");
        AddParameter(command, "@p1");
        AddParameter(command, "@p2");
        AddParameter(command, "@p3");
        AddParameter(command, "@p4");
        AddParameter(command, "@p5");
        AddParameter(command, "@p6");
        command.Prepare();

        for (int i = 1; i <= 100; i++)
        {
            command.Parameters[0].Value = i;
            command.Parameters[1].Value = (long)(i * 100);
            command.Parameters[2].Value = $"text_{i}";
            command.Parameters[3].Value = (double)(i * 1.5);
            command.Parameters[4].Value = i % 2 == 0;
            command.Parameters[5].Value = new byte[] { (byte)i, (byte)(i >> 8), (byte)(i >> 16) };
            command.Parameters[6].Value = Guid.NewGuid();
            command.ExecuteNonQuery();
        }

        transaction.Commit();

        using var verify = connection.CreateCommand();
        verify.CommandText = "SELECT COUNT(*) FROM batch_all_types";
        Assert.Equal(100L, Convert.ToInt64(verify.ExecuteScalar()));
    }

    [Fact]
    public void LargeBlobBatch_InsertsAndRetrievesCorrectly()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using var create = connection.CreateCommand();
        create.CommandText = "CREATE TABLE large_blobs (id INTEGER PRIMARY KEY, data BLOB)";
        create.ExecuteNonQuery();

        using var transaction = connection.BeginTransaction();
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "INSERT INTO large_blobs (id, data) VALUES (@p0, @p1)";
        AddParameter(command, "@p0");
        AddParameter(command, "@p1");
        command.Prepare();

        for (int i = 1; i <= 50; i++)
        {
            var blob = new byte[1024 * 10];
            for (int j = 0; j < blob.Length; j++)
            {
                blob[j] = (byte)((i + j) % 256);
            }

            command.Parameters[0].Value = i;
            command.Parameters[1].Value = blob;
            command.ExecuteNonQuery();
        }

        transaction.Commit();

        using var verify = connection.CreateCommand();
        verify.CommandText = "SELECT data FROM large_blobs WHERE id = 25";
        using var reader = verify.ExecuteReader();
        Assert.True(reader.Read());
        var retrievedBlob = reader.GetFieldValue<byte[]>(0);
        Assert.Equal(1024 * 10, retrievedBlob.Length);
    }

    [Fact]
    public void BatchParameter_LargeStringValue_InsertsSuccessfully()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using var create = connection.CreateCommand();
        create.CommandText = "CREATE TABLE large_strings (id INTEGER PRIMARY KEY, payload TEXT)";
        create.ExecuteNonQuery();

        using var transaction = connection.BeginTransaction();
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "INSERT INTO large_strings (id, payload) VALUES (@p0, @p1)";
        AddParameter(command, "@p0");
        AddParameter(command, "@p1");
        command.Prepare();

        var largeString = new string('X', 50000);

        for (int i = 1; i <= 10; i++)
        {
            command.Parameters[0].Value = i;
            command.Parameters[1].Value = i == 5 ? largeString : $"small_{i}";
            command.ExecuteNonQuery();
        }

        transaction.Commit();

        using var verify = connection.CreateCommand();
        verify.CommandText = "SELECT payload FROM large_strings WHERE id = 5";
        var result = verify.ExecuteScalar();
        Assert.Equal(largeString, result);
    }

    [Fact]
    public void ExecuteNonQuery_MultipleBatchesInSequence_RowsAccumulate()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using var create = connection.CreateCommand();
        create.CommandText = "CREATE TABLE sequential_batches (id INTEGER PRIMARY KEY, value INTEGER)";
        create.ExecuteNonQuery();

        const int batchSize = 500;
        const int numberOfBatches = 3;
        int totalRows = 0;

        for (int batch = 0; batch < numberOfBatches; batch++)
        {
            using var transaction = connection.BeginTransaction();
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "INSERT INTO sequential_batches (id, value) VALUES (@p0, @p1)";
            AddParameter(command, "@p0");
            AddParameter(command, "@p1");
            command.Prepare();

            int baseId = batch * batchSize + 1;
            for (int i = 0; i < batchSize; i++)
            {
                int id = baseId + i;
                command.Parameters[0].Value = id;
                command.Parameters[1].Value = id * 2;
                command.ExecuteNonQuery();
            }

            transaction.Commit();
            totalRows += batchSize;
        }

        using var verify = connection.CreateCommand();
        verify.CommandText = "SELECT COUNT(*) FROM sequential_batches";
        Assert.Equal(totalRows, Convert.ToInt64(verify.ExecuteScalar()));
    }

    [Fact]
    public void Batch_UpdateAndDelete_AffectedRowsCount()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using var create = connection.CreateCommand();
        create.CommandText = "CREATE TABLE batch_updates (id INTEGER PRIMARY KEY, status TEXT, value INTEGER)";
        create.ExecuteNonQuery();

        using var insert = connection.CreateCommand();
        insert.CommandText = "INSERT INTO batch_updates (id, status, value) VALUES (@p0, @p1, @p2)";
        AddParameter(insert, "@p0");
        AddParameter(insert, "@p1");
        AddParameter(insert, "@p2");
        insert.Prepare();

        for (int i = 1; i <= 100; i++)
        {
            insert.Parameters[0].Value = i;
            insert.Parameters[1].Value = i <= 50 ? "pending" : "complete";
            insert.Parameters[2].Value = i * 10;
            insert.ExecuteNonQuery();
        }

        using var update = connection.CreateCommand();
        update.CommandText = "UPDATE batch_updates SET status = 'processed' WHERE status = 'pending'";
        int updatedCount = update.ExecuteNonQuery();
        Assert.Equal(50, updatedCount);

        using var delete = connection.CreateCommand();
        delete.CommandText = "DELETE FROM batch_updates WHERE value > 500";
        int deletedCount = delete.ExecuteNonQuery();
        Assert.Equal(50, deletedCount);

        using var verify = connection.CreateCommand();
        verify.CommandText = "SELECT COUNT(*) FROM batch_updates";
        Assert.Equal(50L, Convert.ToInt64(verify.ExecuteScalar()));
    }

    private static void AddParameter(DbCommand command, string name)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        command.Parameters.Add(parameter);
    }
}