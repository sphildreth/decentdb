using System.Data.Common;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

public sealed class BulkMaterializationTests : IDisposable
{
    private readonly string _dbPath = ReleaseGateTestHelpers.CreateDbPath("bulk_materialization");

    public void Dispose()
    {
        ReleaseGateTestHelpers.DeleteDbArtifacts(_dbPath);
    }

    [Fact]
    public void InsertSelect_Cleanup_And_IndexBuild_WorkOnRealisticStagingSchema()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                                  CREATE TABLE artist_staging (
                                      artist_id INTEGER PRIMARY KEY,
                                      mbid TEXT NOT NULL,
                                      name TEXT NOT NULL,
                                      name_normalized TEXT NOT NULL
                                  );
                                  CREATE TABLE artist_alias_staging (
                                      id INTEGER PRIMARY KEY,
                                      artist_id INTEGER NOT NULL,
                                      alias_normalized TEXT NOT NULL
                                  );
                                  CREATE TABLE materialized_artist (
                                      artist_id INTEGER PRIMARY KEY,
                                      mbid TEXT NOT NULL,
                                      name TEXT NOT NULL,
                                      name_normalized TEXT NOT NULL
                                  );
                                  CREATE TABLE materialized_artist_alias (
                                      artist_id INTEGER NOT NULL,
                                      alias_normalized TEXT NOT NULL
                                  );
                                  """;
            command.ExecuteNonQuery();
        }

        using (var transaction = connection.BeginTransaction())
        using (var insertArtist = connection.CreateCommand())
        using (var insertAlias = connection.CreateCommand())
        {
            insertArtist.Transaction = transaction;
            insertAlias.Transaction = transaction;

            insertArtist.CommandText = "INSERT INTO artist_staging (artist_id, mbid, name, name_normalized) VALUES (@p0, @p1, @p2, @p3)";
            AddParameter(insertArtist, "@p0");
            AddParameter(insertArtist, "@p1");
            AddParameter(insertArtist, "@p2");
            AddParameter(insertArtist, "@p3");
            insertArtist.Prepare();

            insertAlias.CommandText = "INSERT INTO artist_alias_staging (id, artist_id, alias_normalized) VALUES (@p0, @p1, @p2)";
            AddParameter(insertAlias, "@p0");
            AddParameter(insertAlias, "@p1");
            AddParameter(insertAlias, "@p2");
            insertAlias.Prepare();

            var aliasId = 1;
            for (var artistId = 1; artistId <= 5_000; artistId++)
            {
                insertArtist.Parameters[0].Value = artistId;
                insertArtist.Parameters[1].Value = $"mbid-{artistId:D6}";
                insertArtist.Parameters[2].Value = $"Artist {artistId}";
                insertArtist.Parameters[3].Value = $"artist{artistId}";
                insertArtist.ExecuteNonQuery();

                insertAlias.Parameters[0].Value = aliasId++;
                insertAlias.Parameters[1].Value = artistId;
                insertAlias.Parameters[2].Value = $"artist{artistId}";
                insertAlias.ExecuteNonQuery();

                insertAlias.Parameters[0].Value = aliasId++;
                insertAlias.Parameters[1].Value = artistId;
                insertAlias.Parameters[2].Value = $"artist{artistId}-alt";
                insertAlias.ExecuteNonQuery();
            }

            transaction.Commit();
        }

        using (var materialize = connection.CreateCommand())
        {
            materialize.CommandText = """
                                      INSERT INTO materialized_artist (artist_id, mbid, name, name_normalized)
                                      SELECT artist_id, mbid, name, name_normalized
                                      FROM artist_staging
                                      WHERE name <> '';

                                      INSERT INTO materialized_artist_alias (artist_id, alias_normalized)
                                      SELECT DISTINCT artist_id, alias_normalized
                                      FROM artist_alias_staging
                                      WHERE alias_normalized <> '';

                                      CREATE INDEX ix_materialized_artist_name_normalized
                                      ON materialized_artist (name_normalized);

                                      DELETE FROM artist_alias_staging;
                                      DELETE FROM artist_staging;
                                      """;
            materialize.ExecuteNonQuery();
        }

        using var verify = connection.CreateCommand();
        verify.CommandText = """
                             SELECT
                                 (SELECT COUNT(*) FROM materialized_artist),
                                 (SELECT COUNT(*) FROM materialized_artist_alias),
                                 (SELECT COUNT(*) FROM artist_staging),
                                 (SELECT COUNT(*) FROM artist_alias_staging)
                             """;

        using var reader = verify.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(5_000L, reader.GetInt64(0));
        Assert.Equal(10_000L, reader.GetInt64(1));
        Assert.Equal(0L, reader.GetInt64(2));
        Assert.Equal(0L, reader.GetInt64(3));
    }

    private static void AddParameter(DbCommand command, string name)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        command.Parameters.Add(parameter);
    }
}
