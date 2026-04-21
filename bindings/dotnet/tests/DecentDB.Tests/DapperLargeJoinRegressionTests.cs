using System.Data.Common;
using Dapper;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

public sealed class DapperLargeJoinRegressionTests : IDisposable
{
    private readonly string _dbPath = ReleaseGateTestHelpers.CreateDbPath("dapper_large_join");

    public void Dispose()
    {
        ReleaseGateTestHelpers.DeleteDbArtifacts(_dbPath);
    }

    [Fact]
    public void QueryWithLargeJoinAndMultiMapping_ReturnsExpectedGraph()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                                  CREATE TABLE join_artists (
                                      artist_id INTEGER PRIMARY KEY,
                                      artist_name TEXT NOT NULL
                                  );
                                  CREATE TABLE join_albums (
                                      album_id INTEGER PRIMARY KEY,
                                      artist_id INTEGER NOT NULL,
                                      album_name TEXT NOT NULL
                                  );
                                  """;
            command.ExecuteNonQuery();
        }

        using (var transaction = connection.BeginTransaction())
        using (var insertArtist = connection.CreateCommand())
        using (var insertAlbum = connection.CreateCommand())
        {
            insertArtist.Transaction = transaction;
            insertAlbum.Transaction = transaction;

            insertArtist.CommandText = "INSERT INTO join_artists (artist_id, artist_name) VALUES (@p0, @p1)";
            AddParameter(insertArtist, "@p0");
            AddParameter(insertArtist, "@p1");
            insertArtist.Prepare();

            insertAlbum.CommandText = "INSERT INTO join_albums (album_id, artist_id, album_name) VALUES (@p0, @p1, @p2)";
            AddParameter(insertAlbum, "@p0");
            AddParameter(insertAlbum, "@p1");
            AddParameter(insertAlbum, "@p2");
            insertAlbum.Prepare();

            var albumId = 1;
            for (var artistId = 1; artistId <= 500; artistId++)
            {
                insertArtist.Parameters[0].Value = artistId;
                insertArtist.Parameters[1].Value = $"Artist {artistId}";
                insertArtist.ExecuteNonQuery();

                for (var i = 0; i < 20; i++)
                {
                    insertAlbum.Parameters[0].Value = albumId++;
                    insertAlbum.Parameters[1].Value = artistId;
                    insertAlbum.Parameters[2].Value = $"Album {artistId}-{i}";
                    insertAlbum.ExecuteNonQuery();
                }
            }

            transaction.Commit();
        }

        var lookup = new Dictionary<long, ArtistJoinRow>();
        var rows = connection.Query<ArtistJoinRow, AlbumJoinRow, ArtistJoinRow>(
            """
            SELECT
                a.artist_id AS ArtistId,
                a.artist_name AS ArtistName,
                b.album_id AS AlbumId,
                b.artist_id AS AlbumArtistId,
                b.album_name AS AlbumName
            FROM join_artists a
            INNER JOIN join_albums b ON b.artist_id = a.artist_id
            ORDER BY a.artist_id, b.album_id
            """,
            (artist, album) =>
            {
                if (!lookup.TryGetValue(artist.ArtistId, out var entry))
                {
                    entry = artist;
                    lookup.Add(entry.ArtistId, entry);
                }

                entry.Albums.Add(album);
                return entry;
            },
            splitOn: "AlbumId").AsList();

        Assert.NotEmpty(rows);
        Assert.Equal(500, lookup.Count);
        Assert.All(lookup.Values, artist => Assert.Equal(20, artist.Albums.Count));
    }

    private static void AddParameter(DbCommand command, string name)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        command.Parameters.Add(parameter);
    }

    private sealed class ArtistJoinRow
    {
        public long ArtistId { get; set; }
        public string ArtistName { get; set; } = string.Empty;
        public List<AlbumJoinRow> Albums { get; } = new();
    }

    private sealed class AlbumJoinRow
    {
        public long AlbumId { get; set; }
        public long AlbumArtistId { get; set; }
        public string AlbumName { get; set; } = string.Empty;
    }
}
