using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DecentDB.AdoNet;
using DecentDB.MicroOrm;
using Xunit;

namespace DecentDB.Tests;

public class QueryRawAsyncTests : IDisposable
{
    private readonly string _dbPath;

    public QueryRawAsyncTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_query_raw_{Guid.NewGuid():N}.ddb");

        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE artists (id INTEGER PRIMARY KEY, name TEXT)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "CREATE TABLE songs (id INTEGER PRIMARY KEY, title TEXT, artist_id INTEGER, duration_ms INTEGER)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO artists (id, name) VALUES (1, 'Artist A'), (2, 'Artist B')";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO songs (id, title, artist_id, duration_ms) VALUES (1, 'Song 1', 1, 200), (2, 'Song 2', 1, 300), (3, 'Song 3', 2, 250)";
        cmd.ExecuteNonQuery();
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
        var walPath = _dbPath + "-wal";
        if (File.Exists(walPath))
            File.Delete(walPath);
    }

    [Fact]
    public async Task QueryRawAsync_MapsViewRowsWithoutPK()
    {
        using var context = new DecentDBContext(_dbPath);

        var results = await context.QueryRawAsync<ArtistSongCount>(
            "SELECT a.name AS artist_name, COUNT(s.id) AS song_count FROM artists a LEFT JOIN songs s ON s.artist_id = a.id GROUP BY a.name");

        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.ArtistName == "Artist A" && r.SongCount == 2);
        Assert.Contains(results, r => r.ArtistName == "Artist B" && r.SongCount == 1);
    }

    [Fact]
    public async Task QueryAsync_StillRequiresPrimaryKey()
    {
        using var context = new DecentDBContext(_dbPath);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await context.QueryAsync<ArtistSongCount>("SELECT 1"));
    }

    [Fact]
    public async Task QueryRawAsync_WithParameters_Works()
    {
        using var context = new DecentDBContext(_dbPath);

        var results = await context.QueryRawAsync<ArtistSongCount>(
            "SELECT a.name AS artist_name, COUNT(s.id) AS song_count FROM artists a LEFT JOIN songs s ON s.artist_id = a.id WHERE a.name = @p0 GROUP BY a.name",
            "Artist A");

        Assert.Single(results);
        Assert.Equal("Artist A", results[0].ArtistName);
        Assert.Equal(2, results[0].SongCount);
    }

    private class ArtistSongCount
    {
        [Column("artist_name")]
        public string ArtistName { get; set; } = string.Empty;
        [Column("song_count")]
        public long SongCount { get; set; }
    }
}
