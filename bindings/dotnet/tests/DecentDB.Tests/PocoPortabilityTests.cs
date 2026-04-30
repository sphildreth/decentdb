using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DecentDB.AdoNet;
using DecentDB.MicroOrm;
using Xunit;

namespace DecentDB.Tests;

public class PocoPortabilityTests : IDisposable
{
    private readonly string _dbPath;

    public PocoPortabilityTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_poco_portability_{Guid.NewGuid():N}.ddb");

        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE album_with_navigations (id INTEGER PRIMARY KEY, title TEXT, artist_id INTEGER)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "CREATE TABLE artist_with_collections (id INTEGER PRIMARY KEY, name TEXT)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "CREATE TABLE entity_with_enums (id INTEGER PRIMARY KEY, genre INTEGER)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "CREATE TABLE entity_with_nullable_ints (id INTEGER PRIMARY KEY, year INTEGER)";
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
    public async Task Poco_WithReferenceNavigation_InsertsSuccessfully()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<AlbumWithNavigation>();

        var album = new AlbumWithNavigation
        {
            Id = 1,
            Title = "Test Album",
            ArtistId = 42,
            Artist = new ArtistStub { Id = 42, Name = "Test Artist" }
        };

        await set.InsertAsync(album);

        var result = await set.ToListAsync();
        Assert.Single(result);
        Assert.Equal("Test Album", result[0].Title);
        Assert.Equal(42, result[0].ArtistId);
    }

    [Fact]
    public async Task Poco_WithCollectionNavigation_InsertsSuccessfully()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<ArtistWithCollection>();

        var artist = new ArtistWithCollection
        {
            Id = 1,
            Name = "Test Artist",
            Albums = new List<AlbumWithNavigation>
            {
                new AlbumWithNavigation { Id = 1, Title = "Album 1", ArtistId = 1 },
            }
        };

        await set.InsertAsync(artist);

        var result = await set.ToListAsync();
        Assert.Single(result);
        Assert.Equal("Test Artist", result[0].Name);
    }

    [Fact]
    public async Task InsertMany_OnPocoWithNavigations_DoesNotBindNavigation()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<AlbumWithNavigation>();

        var albums = new List<AlbumWithNavigation>
        {
            new AlbumWithNavigation { Id = 1, Title = "Album 1", ArtistId = 10, Artist = new ArtistStub { Id = 10, Name = "Artist A" } },
            new AlbumWithNavigation { Id = 2, Title = "Album 2", ArtistId = 20, Artist = new ArtistStub { Id = 20, Name = "Artist B" } },
            new AlbumWithNavigation { Id = 3, Title = "Album 3", ArtistId = 30, Artist = new ArtistStub { Id = 30, Name = "Artist C" } },
        };

        await set.InsertManyAsync(albums);

        var result = await set.ToListAsync();
        Assert.Equal(3, result.Count);
        Assert.All(result, a => Assert.NotNull(a.Title));
    }

    [Fact]
    public async Task Poco_WithEnumProperty_InsertsSuccessfully()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<EntityWithEnum>();

        await set.InsertAsync(new EntityWithEnum { Id = 1, Genre = Genre.Rock });
        await set.InsertAsync(new EntityWithEnum { Id = 2, Genre = Genre.Jazz });

        var result = await set.ToListAsync();
        Assert.Equal(2, result.Count);
        Assert.Equal(Genre.Rock, result[0].Genre);
        Assert.Equal(Genre.Jazz, result[1].Genre);
    }

    [Fact]
    public async Task Poco_WithNullableInt_InsertsSuccessfully()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<EntityWithNullableInt>();

        await set.InsertAsync(new EntityWithNullableInt { Id = 1, Year = 2020 });
        await set.InsertAsync(new EntityWithNullableInt { Id = 2, Year = null });

        var result = await set.ToListAsync();
        Assert.Equal(2, result.Count);
        Assert.Equal(2020, result[0].Year);
        Assert.Null(result[1].Year);
    }

    // --- Test entity types ---

    private class ArtistStub
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class AlbumWithNavigation
    {
        public int Id { get; set; }
        public string Title { get; set; } = string.Empty;
        public int ArtistId { get; set; }
        public ArtistStub Artist { get; set; } = new ArtistStub();
    }

    private class ArtistWithCollection
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<AlbumWithNavigation> Albums { get; set; } = new List<AlbumWithNavigation>();
    }

    private enum Genre { Rock, Pop, Jazz }

    private class EntityWithEnum
    {
        public int Id { get; set; }
        public Genre Genre { get; set; }
    }

    private class EntityWithNullableInt
    {
        public int Id { get; set; }
        public int? Year { get; set; }
    }
}
