using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Microsoft.Extensions.Logging;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

/// <summary>
/// Integration tests that mirror every EF Core query pattern used by Melodee's
/// ArtistSearchEngineServiceDbContext (Artists + Albums).
/// </summary>
public sealed class MelodeeArtistSearchEngineTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_melodee_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    // ---------------------------------------------------------------
    // 1.  EnsureCreated – Melodee calls Database.EnsureCreatedAsync()
    // ---------------------------------------------------------------

    [Fact]
    public void EnsureCreated_CreatesTablesFromModel()
    {
        using var context = CreateContext();
        context.Database.EnsureCreated();

        // Should be able to query empty tables without error.
        Assert.Equal(0, context.Artists.Count());
        Assert.Equal(0, context.Albums.Count());
    }

    // ---------------------------------------------------------------
    // 2.  Add + SaveChanges – insert artist with nested albums
    // ---------------------------------------------------------------

    [Fact]
    public void Add_ArtistWithAlbums_PersistsAndReturnsGeneratedKeys()
    {
        EnsureSchema();

        using var context = CreateContext();
        var artist = MakeArtist("Beatles", "beatles");
        artist.Albums = new List<MelodeeAlbum>
        {
            MakeAlbum("Abbey Road", "abbey road", 1969),
            MakeAlbum("Let It Be", "let it be", 1970)
        };

        context.Artists.Add(artist);
        context.SaveChanges();

        Assert.True(artist.Id > 0);
        Assert.All(artist.Albums, a => Assert.True(a.Id > 0));
    }

    // ---------------------------------------------------------------
    // 3.  Include (SingleQuery LEFT JOIN) – Melodee never calls
    //     AsSplitQuery on this context, so it uses the default join.
    // ---------------------------------------------------------------

    [Fact]
    public void Include_LoadsAlbumsViaLeftJoin()
    {
        SeedData();

        using var context = CreateContext();
        var artist = context.Artists
            .Include(x => x.Albums)
            .FirstOrDefault(x => x.NameNormalized == "beatles");

        Assert.NotNull(artist);
        Assert.Equal(2, artist!.Albums.Count);
    }

    // ---------------------------------------------------------------
    // 4.  Include + complex WHERE with OR, null checks, Contains
    //     Mirrors the main search query in ArtistSearchEngineService.
    // ---------------------------------------------------------------

    [Fact]
    public void Include_WithComplexOrFilter_ReturnsMatchingArtists()
    {
        SeedData();

        var normalizedName = "beatles";
        var firstTag = $"{normalizedName}|";
        var inTag = $"|{normalizedName}|";
        var outerTag = $"|{normalizedName}";
        var targetMbid = Guid.Parse("11111111-1111-1111-1111-111111111111");
        var targetSpotifyId = "sp-beatles";

        using var context = CreateContext();
        var artists = context.Artists
            .Include(x => x.Albums)
            .Where(x => x.NameNormalized == normalizedName ||
                        (x.MusicBrainzId != null && x.MusicBrainzId == targetMbid) ||
                        (x.AlternateNames != null && (x.AlternateNames.Contains(firstTag) ||
                                                      x.AlternateNames.Contains(inTag) ||
                                                      x.AlternateNames.Contains(outerTag))) ||
                        (x.SpotifyId != null && x.SpotifyId == targetSpotifyId))
            .ToArray();

        Assert.Single(artists);
        Assert.Equal("beatles", artists[0].NameNormalized);
        Assert.Equal(2, artists[0].Albums.Count);
    }

    // ---------------------------------------------------------------
    // 5.  AsNoTracking + CountAsync – Melodee counts before paging
    // ---------------------------------------------------------------

    [Fact]
    public void AsNoTracking_Count_ReturnsCorrectTotal()
    {
        SeedData();

        using var context = CreateContext();
        var count = context.Artists.AsNoTracking().Count();

        Assert.Equal(3, count);
    }

    // ---------------------------------------------------------------
    // 6.  Filter by equality (=) and inequality (!=)
    // ---------------------------------------------------------------

    [Fact]
    public void Where_Equality_FiltersCorrectly()
    {
        SeedData();

        using var context = CreateContext();
        var byName = context.Artists.Where(x => x.Name == "Beatles").ToList();
        Assert.Single(byName);

        var byNormalized = context.Artists.Where(x => x.NameNormalized == "miles davis").ToList();
        Assert.Single(byNormalized);

        var notEqual = context.Artists.Where(x => x.Name != "Beatles").ToList();
        Assert.Equal(2, notEqual.Count);
    }

    // ---------------------------------------------------------------
    // 7.  String LIKE patterns – Contains / StartsWith / EndsWith
    //     with ToLower, mirroring ApplyLikeFilter.
    // ---------------------------------------------------------------

    [Fact]
    public void StringMethods_ContainsStartsWithEndsWith_Work()
    {
        SeedData();

        using var context = CreateContext();

        var contains = context.Artists.Where(x => x.Name.ToLower().Contains("eatle")).ToList();
        Assert.Single(contains);

        var startsWith = context.Artists.Where(x => x.Name.ToLower().StartsWith("beat")).ToList();
        Assert.Single(startsWith);

        var endsWith = context.Artists.Where(x => x.Name.ToLower().EndsWith("les")).ToList();
        Assert.Single(endsWith);
    }

    // ---------------------------------------------------------------
    // 8.  OrderBy / OrderByDescending / ThenBy – multi-column sort
    // ---------------------------------------------------------------

    /// <summary>
    /// Verifies ORDER BY ASC works correctly.
    /// ORDER BY DESC with quoted table-aliased columns is a known DecentDB gap:
    /// SELECT "e"."name" FROM "t" AS "e" ORDER BY "e"."name" DESC returns ASC order.
    /// Melodee uses OrderByDescending in housekeeping (on INTEGER columns, which work)
    /// and in paging (on TEXT columns, which may return wrong order).
    /// </summary>
    [Fact]
    public void OrderBy_MultiColumn_SortsCorrectly()
    {
        SeedData();

        using var context = CreateContext();

        // Ascending by Name — works correctly
        var ascending = context.Artists.OrderBy(x => x.Name).Select(x => x.Name).ToList();
        Assert.Equal(3, ascending.Count);
        Assert.Equal("Beatles", ascending[0]);
        Assert.Equal("Miles Davis", ascending[1]);
        Assert.Equal("Solo", ascending[2]);

        // Multi-column: OrderBy SortName ThenBy Id — works for ASC
        var multi = context.Artists
            .OrderBy(x => x.SortName)
            .ThenBy(x => x.Id)
            .Select(x => x.SortName)
            .ToList();
        Assert.Equal(3, multi.Count);
    }

    // ---------------------------------------------------------------
    // 8b. ORDER BY DESC with quoted aliases — known DecentDB gap
    //     Raw SQL "ORDER BY name DESC" works, but EF-generated
    //     "ORDER BY "e"."name" DESC" returns ASC order.
    // ---------------------------------------------------------------

    [Fact]
    public void OrderByDescending_WithQuotedAlias_WorksCorrectly()
    {
        SeedData();

        using var context = CreateContext();
        var descending = context.Artists
            .OrderByDescending(x => x.Name)
            .Select(x => x.Name)
            .ToList();

        Assert.Equal(3, descending.Count);
        Assert.Equal("Solo", descending[0]);
        Assert.Equal("Miles Davis", descending[1]);
        Assert.Equal("Beatles", descending[2]);
    }

    // ---------------------------------------------------------------
    // 9.  Skip / Take – pagination
    // ---------------------------------------------------------------

    [Fact]
    public void SkipTake_PaginatesCorrectly()
    {
        SeedData();

        using var context = CreateContext();
        var page = context.Artists
            .OrderBy(x => x.Id)
            .Skip(1)
            .Take(1)
            .ToList();

        Assert.Single(page);
    }

    // ---------------------------------------------------------------
    // 10. Select projection with correlated scalar subquery
    //     AlbumCount = scopedContext.Albums.Count(a => a.ArtistId == x.Id)
    // ---------------------------------------------------------------

    [Fact]
    public void Select_WithCorrelatedCountSubquery_ReturnsAlbumCounts()
    {
        SeedData();

        using var context = CreateContext();
        var results = context.Artists
            .OrderBy(x => x.Id)
            .Skip(0)
            .Take(10)
            .Select(x => new
            {
                x.Id,
                x.Name,
                x.NameNormalized,
                x.SortName,
                x.AlternateNames,
                x.ItunesId,
                x.AmgId,
                x.DiscogsId,
                x.WikiDataId,
                x.MusicBrainzId,
                x.LastFmId,
                x.SpotifyId,
                x.IsLocked,
                x.LastRefreshed,
                AlbumCount = context.Albums.Count(a => a.ArtistId == x.Id)
            })
            .ToArray();

        Assert.Equal(3, results.Length);

        var beatles = results.Single(r => r.NameNormalized == "beatles");
        Assert.Equal(2, beatles.AlbumCount);

        var miles = results.Single(r => r.NameNormalized == "miles davis");
        Assert.Equal(1, miles.AlbumCount);

        var noAlbums = results.Single(r => r.NameNormalized == "solo");
        Assert.Equal(0, noAlbums.AlbumCount);
    }

    // ---------------------------------------------------------------
    // 11. Include + FirstAsync by PK – reload after save
    // ---------------------------------------------------------------

    [Fact]
    public void Include_FirstById_LoadsSingleArtistWithAlbums()
    {
        SeedData();

        using var context = CreateContext();
        var artist = context.Artists
            .Include(x => x.Albums)
            .First(x => x.Id == 1);

        Assert.NotNull(artist);
        Assert.Equal(2, artist.Albums.Count);
    }

    // ---------------------------------------------------------------
    // 12. FirstOrDefault by normalized name – dedup check
    // ---------------------------------------------------------------

    [Fact]
    public void FirstOrDefault_ByNameNormalized_FindsExisting()
    {
        SeedData();

        using var context = CreateContext();
        var found = context.Artists.FirstOrDefault(x => x.NameNormalized == "beatles");
        Assert.NotNull(found);

        var missing = context.Artists.FirstOrDefault(x => x.NameNormalized == "nonexistent");
        Assert.Null(missing);
    }

    // ---------------------------------------------------------------
    // 13. Update properties + SaveChanges
    // ---------------------------------------------------------------

    [Fact]
    public void Update_ArtistProperties_Persists()
    {
        SeedData();

        using (var context = CreateContext())
        {
            var artist = context.Artists.First(x => x.NameNormalized == "beatles");
            artist.AlternateNames = "fab four|the beatles";
            artist.LastRefreshed = DateTimeOffset.UtcNow;
            context.SaveChanges();
        }

        using (var verify = CreateContext())
        {
            var artist = verify.Artists.First(x => x.NameNormalized == "beatles");
            Assert.Equal("fab four|the beatles", artist.AlternateNames);
            Assert.NotNull(artist.LastRefreshed);
        }
    }

    // ---------------------------------------------------------------
    // 14. RemoveRange albums + Remove artist – cascade-style delete
    //     Melodee manually deletes albums then artist.
    // ---------------------------------------------------------------

    [Fact]
    public void RemoveRange_Albums_ThenRemoveArtist_DeletesAll()
    {
        SeedData();

        using (var context = CreateContext())
        {
            var artist = context.Artists.First(x => x.NameNormalized == "beatles");
            var albums = context.Albums.Where(a => a.ArtistId == artist.Id).ToList();

            context.Albums.RemoveRange(albums);
            context.Artists.Remove(artist);
            context.SaveChanges();
        }

        using (var verify = CreateContext())
        {
            Assert.Equal(2, verify.Artists.Count()); // Miles + Solo remain
            Assert.Equal(1, verify.Albums.Count());  // Only Miles' album remains
        }
    }

    // ---------------------------------------------------------------
    // 15. DbUpdateException on unique constraint violation
    // ---------------------------------------------------------------

    /// <summary>
    /// Melodee catches DbUpdateException on unique constraint violations as a fallback.
    /// DecentDB unique indexes now correctly enforce uniqueness on INSERT.
    /// </summary>
    [Fact]
    public void Add_DuplicateUniqueId_ThrowsDbUpdateException()
    {
        EnsureSchema();

        using var context = CreateContext();
        var first = MakeArtist("Beatles", "beatles");
        first.SpotifyId = "sp-beatles";
        context.Artists.Add(first);
        context.SaveChanges();

        var dup = MakeArtist("Dup", "dup");
        dup.SpotifyId = "sp-beatles";
        context.Artists.Add(dup);
        Assert.Throws<DbUpdateException>(() => context.SaveChanges());
    }

    // ---------------------------------------------------------------
    // 16. ChangeTracker.Clear – Melodee clears on conflict
    // ---------------------------------------------------------------

    [Fact]
    public void ChangeTrackerClear_AllowsRequery()
    {
        SeedData();

        using var context = CreateContext();
        var artist = context.Artists.First(x => x.NameNormalized == "beatles");
        artist.Name = "The Beatles";

        context.ChangeTracker.Clear();

        // After clear, the modified entity is no longer tracked.
        Assert.Empty(context.ChangeTracker.Entries());
        // Re-query succeeds.
        var fresh = context.Artists.First(x => x.NameNormalized == "beatles");
        Assert.Equal("Beatles", fresh.Name);
    }

    // ---------------------------------------------------------------
    // 17. Housekeeping query – null-safe OR filter + OrderByDescending
    //     + Take (batch refresh of stale artists)
    // ---------------------------------------------------------------

    [Fact]
    public void HousekeepingQuery_NullSafeFilter_OrderByDesc_Take()
    {
        SeedData();

        // Set one artist as recently refreshed.
        using (var context = CreateContext())
        {
            var beatles = context.Artists.First(x => x.NameNormalized == "beatles");
            beatles.LastRefreshed = DateTimeOffset.UtcNow;
            beatles.IsLocked = false;
            context.SaveChanges();
        }

        var refreshCutoff = DateTimeOffset.UtcNow.AddDays(-7);

        using (var context = CreateContext())
        {
            var toRefresh = context.Artists
                .Where(x => (x.IsLocked == null || x.IsLocked == false) &&
                            (x.LastRefreshed == null || x.LastRefreshed <= refreshCutoff))
                .OrderByDescending(x => x.LastRefreshed)
                .Take(10)
                .ToList();

            // Beatles was just refreshed, so excluded. Miles + Solo remain.
            Assert.Equal(2, toRefresh.Count);
        }
    }

    // ---------------------------------------------------------------
    // 18. DateTimeOffset round-trip via DateTimeOffsetToBinaryConverter
    //     (Melodee configures this in OnModelCreating)
    // ---------------------------------------------------------------

    [Fact]
    public void DateTimeOffset_BinaryConverter_RoundTrips()
    {
        EnsureSchema();

        var now = DateTimeOffset.UtcNow;
        int artistId;

        using (var context = CreateContext())
        {
            var artist = MakeArtist("Test", "test");
            artist.LastRefreshed = now;
            context.Artists.Add(artist);
            context.SaveChanges();
            artistId = artist.Id;
        }

        using (var verify = CreateContext())
        {
            var loaded = verify.Artists.First(x => x.Id == artistId);
            Assert.NotNull(loaded.LastRefreshed);
            // Binary converter preserves to-the-tick; allow 1ms tolerance
            // because the binary format includes offset info.
            Assert.True(Math.Abs((loaded.LastRefreshed!.Value - now).TotalMilliseconds) < 2);
        }
    }

    // ---------------------------------------------------------------
    // 19. Guid (UUID) round-trip for MusicBrainzId
    // ---------------------------------------------------------------

    [Fact]
    public void Guid_UUID_RoundTrips()
    {
        EnsureSchema();

        var mbid = Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        int artistId;

        using (var context = CreateContext())
        {
            var artist = MakeArtist("GuidTest", "guidtest");
            artist.MusicBrainzId = mbid;
            context.Artists.Add(artist);
            context.SaveChanges();
            artistId = artist.Id;
        }

        using (var verify = CreateContext())
        {
            var loaded = verify.Artists.First(x => x.Id == artistId);
            Assert.Equal(mbid, loaded.MusicBrainzId);
        }
    }

    // ---------------------------------------------------------------
    // 20. Nullable Guid filter – WHERE MusicBrainzId IS NOT NULL
    // ---------------------------------------------------------------

    [Fact]
    public void Where_NullableGuid_FiltersCorrectly()
    {
        SeedData();

        using var context = CreateContext();
        var withMbid = context.Artists.Where(x => x.MusicBrainzId != null).ToList();
        Assert.Equal(2, withMbid.Count); // Beatles + Miles

        var withoutMbid = context.Artists.Where(x => x.MusicBrainzId == null).ToList();
        Assert.Single(withoutMbid); // Solo
    }

    // ---------------------------------------------------------------
    // 21. Albums query by ArtistId – used before RemoveRange
    // ---------------------------------------------------------------

    [Fact]
    public void Albums_WhereByArtistId_ReturnsCorrectSubset()
    {
        SeedData();

        using var context = CreateContext();
        var beatles = context.Artists.First(x => x.NameNormalized == "beatles");
        var albums = context.Albums.Where(a => a.ArtistId == beatles.Id).ToList();

        Assert.Equal(2, albums.Count);
    }

    // ---------------------------------------------------------------
    // 22. Replace albums – delete old, add new, save
    //     Melodee does this when refreshing album data.
    // ---------------------------------------------------------------

    [Fact]
    public void ReplaceAlbums_DeleteOldAddNew_Works()
    {
        SeedData();

        using (var context = CreateContext())
        {
            var artist = context.Artists
                .Include(x => x.Albums)
                .First(x => x.NameNormalized == "beatles");

            var oldAlbums = context.Albums.Where(a => a.ArtistId == artist.Id).ToList();
            context.Albums.RemoveRange(oldAlbums);
            context.SaveChanges();

            var newAlbum = MakeAlbum("Revolver", "revolver", 1966);
            newAlbum.ArtistId = artist.Id;
            context.Albums.Add(newAlbum);
            context.SaveChanges();
        }

        using (var verify = CreateContext())
        {
            var artist = verify.Artists.Include(x => x.Albums).First(x => x.NameNormalized == "beatles");
            Assert.Single(artist.Albums);
            Assert.Equal("revolver", artist.Albums.First().NameNormalized);
        }
    }

    // ===============================================================
    //  Helpers
    // ===============================================================

    private MelodeeDbContext CreateContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<MelodeeDbContext>();
        optionsBuilder.UseDecentDB($"Data Source={_dbPath}");
        return new MelodeeDbContext(optionsBuilder.Options);
    }

    private MelodeeDbContext CreateContextWithLogging(List<string> logs)
    {
        var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Debug);
            builder.AddProvider(new ListLoggerProvider(logs));
        });

        var optionsBuilder = new DbContextOptionsBuilder<MelodeeDbContext>();
        optionsBuilder.UseDecentDB($"Data Source={_dbPath}");
        optionsBuilder.EnableSensitiveDataLogging();
        optionsBuilder.UseLoggerFactory(loggerFactory);
        return new MelodeeDbContext(optionsBuilder.Options);
    }

    private void EnsureSchema()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();

        cmd.CommandText = "DROP TABLE IF EXISTS ef_melodee_albums";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "DROP TABLE IF EXISTS ef_melodee_artists";
        cmd.ExecuteNonQuery();

        cmd.CommandText = """
            CREATE TABLE ef_melodee_artists (
                id         INTEGER PRIMARY KEY,
                name       TEXT    NOT NULL,
                name_normalized TEXT NOT NULL,
                alternate_names TEXT,
                sort_name  TEXT    NOT NULL,
                itunes_id  TEXT,
                amg_id     TEXT,
                discogs_id TEXT,
                wiki_data_id TEXT,
                music_brainz_id UUID,
                last_fm_id TEXT,
                spotify_id TEXT,
                is_locked  BOOLEAN,
                last_refreshed INTEGER
            )
            """;
        cmd.ExecuteNonQuery();

        cmd.CommandText = "CREATE INDEX ix_ef_melodee_artists_name ON ef_melodee_artists (name)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "CREATE INDEX ix_ef_melodee_artists_name_normalized ON ef_melodee_artists (name_normalized)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "CREATE INDEX ix_ef_melodee_artists_sort_name ON ef_melodee_artists (sort_name)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "CREATE UNIQUE INDEX ux_ef_melodee_artists_itunes ON ef_melodee_artists (itunes_id)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "CREATE UNIQUE INDEX ux_ef_melodee_artists_amg ON ef_melodee_artists (amg_id)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "CREATE UNIQUE INDEX ux_ef_melodee_artists_discogs ON ef_melodee_artists (discogs_id)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "CREATE UNIQUE INDEX ux_ef_melodee_artists_wikidata ON ef_melodee_artists (wiki_data_id)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "CREATE UNIQUE INDEX ux_ef_melodee_artists_mbid ON ef_melodee_artists (music_brainz_id)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "CREATE UNIQUE INDEX ux_ef_melodee_artists_lastfm ON ef_melodee_artists (last_fm_id)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "CREATE UNIQUE INDEX ux_ef_melodee_artists_spotify ON ef_melodee_artists (spotify_id)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = """
            CREATE TABLE ef_melodee_albums (
                id         INTEGER PRIMARY KEY,
                artist_id  INTEGER NOT NULL REFERENCES ef_melodee_artists(id),
                sort_name  TEXT    NOT NULL,
                album_type INTEGER NOT NULL,
                music_brainz_id UUID,
                music_brainz_release_group_id UUID,
                spotify_id TEXT,
                cover_url  TEXT,
                name       TEXT    NOT NULL,
                name_normalized TEXT NOT NULL,
                year       INTEGER NOT NULL
            )
            """;
        cmd.ExecuteNonQuery();

        cmd.CommandText = "CREATE INDEX ix_ef_melodee_albums_composite ON ef_melodee_albums (artist_id, name_normalized, year)";
        cmd.ExecuteNonQuery();
    }

    private void SeedData()
    {
        EnsureSchema();

        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();

        // Artist 1: Beatles (has albums, has MusicBrainzId, SpotifyId)
        cmd.CommandText = """
            INSERT INTO ef_melodee_artists (id, name, name_normalized, sort_name, alternate_names, music_brainz_id, spotify_id)
            VALUES (1, 'Beatles', 'beatles', 'Beatles, The', 'fab four|the beatles', @mbid1, 'sp-beatles')
            """;
        cmd.Parameters.Add(new DecentDBParameter("@mbid1", Guid.Parse("11111111-1111-1111-1111-111111111111").ToByteArray()));
        cmd.ExecuteNonQuery();
        cmd.Parameters.Clear();

        // Artist 2: Miles Davis (has albums, has MusicBrainzId)
        cmd.CommandText = """
            INSERT INTO ef_melodee_artists (id, name, name_normalized, sort_name, music_brainz_id)
            VALUES (2, 'Miles Davis', 'miles davis', 'Davis, Miles', @mbid2)
            """;
        cmd.Parameters.Add(new DecentDBParameter("@mbid2", Guid.Parse("22222222-2222-2222-2222-222222222222").ToByteArray()));
        cmd.ExecuteNonQuery();
        cmd.Parameters.Clear();

        // Artist 3: Solo artist (no albums, no external IDs)
        cmd.CommandText = """
            INSERT INTO ef_melodee_artists (id, name, name_normalized, sort_name)
            VALUES (3, 'Solo', 'solo', 'Solo')
            """;
        cmd.ExecuteNonQuery();

        // Albums for Beatles
        cmd.CommandText = """
            INSERT INTO ef_melodee_albums (id, artist_id, sort_name, album_type, name, name_normalized, year)
            VALUES (1, 1, 'Abbey Road', 1, 'Abbey Road', 'abbey road', 1969)
            """;
        cmd.ExecuteNonQuery();
        cmd.CommandText = """
            INSERT INTO ef_melodee_albums (id, artist_id, sort_name, album_type, name, name_normalized, year)
            VALUES (2, 1, 'Let It Be', 1, 'Let It Be', 'let it be', 1970)
            """;
        cmd.ExecuteNonQuery();

        // Album for Miles Davis
        cmd.CommandText = """
            INSERT INTO ef_melodee_albums (id, artist_id, sort_name, album_type, name, name_normalized, year)
            VALUES (3, 2, 'Kind of Blue', 1, 'Kind of Blue', 'kind of blue', 1959)
            """;
        cmd.ExecuteNonQuery();
    }

    private static MelodeeArtist MakeArtist(string name, string normalized) => new()
    {
        Name = name,
        NameNormalized = normalized,
        SortName = name
    };

    private static MelodeeAlbum MakeAlbum(string name, string normalized, int year) => new()
    {
        Name = name,
        NameNormalized = normalized,
        SortName = name,
        Year = year,
        AlbumType = 1
    };

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    // ===============================================================
    //  DbContext + Entities – mirrors Melodee's model & configuration
    // ===============================================================

    private sealed class MelodeeDbContext : DbContext
    {
        public MelodeeDbContext(DbContextOptions<MelodeeDbContext> options)
            : base(options)
        {
        }

        public DbSet<MelodeeArtist> Artists => Set<MelodeeArtist>();
        public DbSet<MelodeeAlbum> Albums => Set<MelodeeAlbum>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            // DateTimeOffset → Binary converter (exactly as Melodee does it)
            foreach (var entityType in modelBuilder.Model.GetEntityTypes())
            {
                var properties = entityType.ClrType.GetProperties()
                    .Where(p => p.PropertyType == typeof(DateTimeOffset) ||
                                p.PropertyType == typeof(DateTimeOffset?));
                foreach (var property in properties)
                {
                    modelBuilder.Entity(entityType.Name)
                        .Property(property.Name)
                        .HasConversion(new DateTimeOffsetToBinaryConverter());
                }
            }

            modelBuilder.Entity<MelodeeArtist>(entity =>
            {
                entity.ToTable("ef_melodee_artists");
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
                entity.Property(x => x.Name).HasColumnName("name");
                entity.Property(x => x.NameNormalized).HasColumnName("name_normalized");
                entity.Property(x => x.AlternateNames).HasColumnName("alternate_names");
                entity.Property(x => x.SortName).HasColumnName("sort_name");
                entity.Property(x => x.ItunesId).HasColumnName("itunes_id");
                entity.Property(x => x.AmgId).HasColumnName("amg_id");
                entity.Property(x => x.DiscogsId).HasColumnName("discogs_id");
                entity.Property(x => x.WikiDataId).HasColumnName("wiki_data_id");
                entity.Property(x => x.MusicBrainzId).HasColumnName("music_brainz_id");
                entity.Property(x => x.LastFmId).HasColumnName("last_fm_id");
                entity.Property(x => x.SpotifyId).HasColumnName("spotify_id");
                entity.Property(x => x.IsLocked).HasColumnName("is_locked");
                entity.Property(x => x.LastRefreshed).HasColumnName("last_refreshed");

                entity.HasIndex(x => x.Name);
                entity.HasIndex(x => x.NameNormalized);
                entity.HasIndex(x => x.SortName);
                entity.HasIndex(x => x.ItunesId).IsUnique();
                entity.HasIndex(x => x.AmgId).IsUnique();
                entity.HasIndex(x => x.DiscogsId).IsUnique();
                entity.HasIndex(x => x.WikiDataId).IsUnique();
                entity.HasIndex(x => x.MusicBrainzId).IsUnique();
                entity.HasIndex(x => x.LastFmId).IsUnique();
                entity.HasIndex(x => x.SpotifyId).IsUnique();

                entity.HasMany(x => x.Albums).WithOne().HasForeignKey(x => x.ArtistId);

                entity.Ignore(x => x.Rank);
                entity.Ignore(x => x.AlbumCount);
            });

            modelBuilder.Entity<MelodeeAlbum>(entity =>
            {
                entity.ToTable("ef_melodee_albums");
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
                entity.Property(x => x.ArtistId).HasColumnName("artist_id");
                entity.Property(x => x.SortName).HasColumnName("sort_name");
                entity.Property(x => x.AlbumType).HasColumnName("album_type");
                entity.Property(x => x.MusicBrainzId).HasColumnName("music_brainz_id");
                entity.Property(x => x.MusicBrainzReleaseGroupId).HasColumnName("music_brainz_release_group_id");
                entity.Property(x => x.SpotifyId).HasColumnName("spotify_id");
                entity.Property(x => x.CoverUrl).HasColumnName("cover_url");
                entity.Property(x => x.Name).HasColumnName("name");
                entity.Property(x => x.NameNormalized).HasColumnName("name_normalized");
                entity.Property(x => x.Year).HasColumnName("year");

                entity.HasIndex(x => new { x.ArtistId, x.NameNormalized, x.Year });
            });
        }
    }

    private sealed class MelodeeArtist
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string NameNormalized { get; set; } = string.Empty;
        public string? AlternateNames { get; set; }
        public string SortName { get; set; } = string.Empty;
        public string? ItunesId { get; set; }
        public string? AmgId { get; set; }
        public string? DiscogsId { get; set; }
        public string? WikiDataId { get; set; }
        public Guid? MusicBrainzId { get; set; }
        public string? LastFmId { get; set; }
        public string? SpotifyId { get; set; }
        public ICollection<MelodeeAlbum> Albums { get; set; } = new List<MelodeeAlbum>();
        public int Rank { get; set; }
        public int AlbumCount { get; set; }
        public bool? IsLocked { get; set; }
        public DateTimeOffset? LastRefreshed { get; set; }
    }

    private sealed class MelodeeAlbum
    {
        public int Id { get; set; }
        public int ArtistId { get; set; }
        public string SortName { get; set; } = string.Empty;
        public int AlbumType { get; set; }
        public Guid? MusicBrainzId { get; set; }
        public Guid? MusicBrainzReleaseGroupId { get; set; }
        public string? SpotifyId { get; set; }
        public string? CoverUrl { get; set; }
        public string Name { get; set; } = string.Empty;
        public string NameNormalized { get; set; } = string.Empty;
        public int Year { get; set; }
    }

    private sealed class ListLoggerProvider : ILoggerProvider
    {
        private readonly List<string> _logs;
        public ListLoggerProvider(List<string> logs) => _logs = logs;
        public ILogger CreateLogger(string categoryName) => new ListLogger(_logs);
        public void Dispose() { }
    }

    private sealed class ListLogger : ILogger
    {
        private readonly List<string> _logs;
        public ListLogger(List<string> logs) => _logs = logs;
        public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => _logs.Add(formatter(state, exception));
    }

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();
        public void Dispose() { }
    }
}
