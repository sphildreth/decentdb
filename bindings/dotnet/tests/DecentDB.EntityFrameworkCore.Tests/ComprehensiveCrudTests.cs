using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

/// <summary>
/// Comprehensive CRUD tests covering all 17 CLR types supported by the EF Core provider,
/// nullable variants, edge-case values, UPDATE round-trips, and async operations.
/// </summary>
public sealed class ComprehensiveCrudTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_crud_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    // ===============================================================
    //  1. INSERT + UPDATE + SELECT round-trip for ALL 17 CLR types
    // ===============================================================

    [Fact]
    public void AllTypes_InsertUpdateSelect_RoundTrips()
    {
        using var context = CreateAllTypesContext();
        context.Database.EnsureCreated();

        // INSERT
        var now = DateTimeOffset.UtcNow;
        var dt = new DateTime(2024, 6, 15, 12, 30, 45, DateTimeKind.Utc);
        var dateOnly = new DateOnly(2024, 6, 15);
        var timeOnly = new TimeOnly(14, 30, 0);
        var timeSpan = TimeSpan.FromHours(2.5);
        var guid = Guid.Parse("12345678-1234-1234-1234-123456789abc");
        var blob = new byte[] { 0x01, 0x02, 0x03, 0xFF };

        var entity = new AllTypesEntity
        {
            BoolVal = true,
            ByteVal = 42,
            ShortVal = 1234,
            IntVal = 100_000,
            LongVal = 9_876_543_210L,
            FloatVal = 3.14f,
            DoubleVal = 2.718281828,
            DecimalVal = 99999.9999m,
            StringVal = "hello world",
            BlobVal = blob,
            DateTimeVal = dt,
            DateTimeOffsetVal = now,
            DateOnlyVal = dateOnly,
            TimeOnlyVal = timeOnly,
            TimeSpanVal = timeSpan,
            GuidVal = guid,
            EnumVal = TestEnum.Active
        };

        context.AllTypes.Add(entity);
        context.SaveChanges();
        var id = entity.Id;
        Assert.True(id > 0);

        // UPDATE every field
        entity.BoolVal = false;
        entity.ByteVal = 255;
        entity.ShortVal = -32768;
        entity.IntVal = int.MinValue;
        entity.LongVal = long.MinValue;
        entity.FloatVal = -1.0f;
        entity.DoubleVal = double.MaxValue;
        entity.DecimalVal = -0.0001m;
        entity.StringVal = "updated";
        entity.BlobVal = new byte[] { 0xDE, 0xAD };
        entity.DateTimeVal = DateTime.UnixEpoch;
        entity.DateTimeOffsetVal = DateTimeOffset.MinValue;
        entity.DateOnlyVal = DateOnly.MinValue;
        entity.TimeOnlyVal = TimeOnly.MinValue;
        entity.TimeSpanVal = TimeSpan.Zero;
        entity.GuidVal = Guid.Empty;
        entity.EnumVal = TestEnum.Inactive;
        context.SaveChanges();

        // SELECT in fresh context
        using var verify = CreateAllTypesContext();
        var loaded = verify.AllTypes.Single(x => x.Id == id);

        Assert.False(loaded.BoolVal);
        Assert.Equal(255, loaded.ByteVal);
        Assert.Equal(-32768, loaded.ShortVal);
        Assert.Equal(int.MinValue, loaded.IntVal);
        Assert.Equal(long.MinValue, loaded.LongVal);
        Assert.Equal(-1.0f, loaded.FloatVal);
        Assert.Equal(double.MaxValue, loaded.DoubleVal);
        Assert.Equal(-0.0001m, loaded.DecimalVal);
        Assert.Equal("updated", loaded.StringVal);
        Assert.Equal(new byte[] { 0xDE, 0xAD }, loaded.BlobVal);
        Assert.Equal(DateTime.UnixEpoch, loaded.DateTimeVal);
        Assert.True(Math.Abs((loaded.DateTimeOffsetVal - DateTimeOffset.MinValue).TotalMilliseconds) < 2);
        Assert.Equal(DateOnly.MinValue, loaded.DateOnlyVal);
        Assert.Equal(TimeOnly.MinValue, loaded.TimeOnlyVal);
        Assert.Equal(TimeSpan.Zero, loaded.TimeSpanVal);
        Assert.Equal(Guid.Empty, loaded.GuidVal);
        Assert.Equal(TestEnum.Inactive, loaded.EnumVal);
    }

    // ===============================================================
    //  2. Nullable types: null → value → null round-trip
    // ===============================================================

    [Fact]
    public void NullableTypes_NullToValueToNull_RoundTrips()
    {
        using var context = CreateAllTypesContext();
        context.Database.EnsureCreated();

        // INSERT with all nulls
        var entity = new AllTypesEntity
        {
            BoolVal = true,
            ByteVal = 1,
            ShortVal = 1,
            IntVal = 1,
            LongVal = 1,
            FloatVal = 1.0f,
            DoubleVal = 1.0,
            DecimalVal = 1.0m,
            StringVal = "x",
            BlobVal = new byte[] { 1 },
            DateTimeVal = DateTime.UtcNow,
            DateTimeOffsetVal = DateTimeOffset.UtcNow,
            DateOnlyVal = DateOnly.FromDateTime(DateTime.UtcNow),
            TimeOnlyVal = TimeOnly.FromDateTime(DateTime.UtcNow),
            TimeSpanVal = TimeSpan.FromSeconds(1),
            GuidVal = Guid.NewGuid(),
            EnumVal = TestEnum.Active,
            // Nullable fields start as null
            NullableBool = null,
            NullableInt = null,
            NullableLong = null,
            NullableDouble = null,
            NullableGuid = null,
            NullableDateTimeOffset = null,
            NullableEnum = null
        };

        context.AllTypes.Add(entity);
        context.SaveChanges();
        var id = entity.Id;

        // Verify nulls persisted
        using (var v1 = CreateAllTypesContext())
        {
            var loaded = v1.AllTypes.Single(x => x.Id == id);
            Assert.Null(loaded.NullableBool);
            Assert.Null(loaded.NullableInt);
            Assert.Null(loaded.NullableLong);
            Assert.Null(loaded.NullableDouble);
            Assert.Null(loaded.NullableGuid);
            Assert.Null(loaded.NullableDateTimeOffset);
            Assert.Null(loaded.NullableEnum);
        }

        // UPDATE: null → value
        using (var ctx2 = CreateAllTypesContext())
        {
            var e = ctx2.AllTypes.Single(x => x.Id == id);
            e.NullableBool = true;
            e.NullableInt = 42;
            e.NullableLong = 999_999_999_999L;
            e.NullableDouble = 3.14;
            e.NullableGuid = Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
            e.NullableDateTimeOffset = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
            e.NullableEnum = TestEnum.Active;
            ctx2.SaveChanges();
        }

        // Verify values persisted
        using (var v2 = CreateAllTypesContext())
        {
            var loaded = v2.AllTypes.Single(x => x.Id == id);
            Assert.True(loaded.NullableBool);
            Assert.Equal(42, loaded.NullableInt);
            Assert.Equal(999_999_999_999L, loaded.NullableLong);
            Assert.Equal(3.14, loaded.NullableDouble);
            Assert.Equal(Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"), loaded.NullableGuid);
            Assert.NotNull(loaded.NullableDateTimeOffset);
            Assert.Equal(TestEnum.Active, loaded.NullableEnum);
        }

        // UPDATE: value → null
        using (var ctx3 = CreateAllTypesContext())
        {
            var e = ctx3.AllTypes.Single(x => x.Id == id);
            e.NullableBool = null;
            e.NullableInt = null;
            e.NullableLong = null;
            e.NullableDouble = null;
            e.NullableGuid = null;
            e.NullableDateTimeOffset = null;
            e.NullableEnum = null;
            ctx3.SaveChanges();
        }

        // Verify nulls again
        using (var v3 = CreateAllTypesContext())
        {
            var loaded = v3.AllTypes.Single(x => x.Id == id);
            Assert.Null(loaded.NullableBool);
            Assert.Null(loaded.NullableInt);
            Assert.Null(loaded.NullableLong);
            Assert.Null(loaded.NullableDouble);
            Assert.Null(loaded.NullableGuid);
            Assert.Null(loaded.NullableDateTimeOffset);
            Assert.Null(loaded.NullableEnum);
        }
    }

    // ===============================================================
    //  3. Edge-case values
    // ===============================================================

    [Fact]
    public void EdgeCaseValues_PersistCorrectly()
    {
        using var context = CreateAllTypesContext();
        context.Database.EnsureCreated();

        var entity = new AllTypesEntity
        {
            BoolVal = false,
            ByteVal = 0,
            ShortVal = short.MaxValue,
            IntVal = int.MaxValue,
            LongVal = long.MaxValue,
            FloatVal = float.MinValue,
            DoubleVal = double.MinValue,
            DecimalVal = 0m,
            StringVal = "",           // empty string
            BlobVal = Array.Empty<byte>(),  // zero-length blob
            DateTimeVal = DateTime.MaxValue.AddTicks(-(DateTime.MaxValue.Ticks % TimeSpan.TicksPerMillisecond)),
            DateTimeOffsetVal = DateTimeOffset.UtcNow,
            DateOnlyVal = DateOnly.MaxValue,
            TimeOnlyVal = TimeOnly.MaxValue,
            TimeSpanVal = TimeSpan.FromTicks(long.MaxValue),
            GuidVal = Guid.Empty,
            EnumVal = (TestEnum)999   // non-standard enum value
        };

        context.AllTypes.Add(entity);
        context.SaveChanges();
        var id = entity.Id;

        using var verify = CreateAllTypesContext();
        var loaded = verify.AllTypes.Single(x => x.Id == id);

        Assert.False(loaded.BoolVal);
        Assert.Equal(0, loaded.ByteVal);
        Assert.Equal(short.MaxValue, loaded.ShortVal);
        Assert.Equal(int.MaxValue, loaded.IntVal);
        Assert.Equal(long.MaxValue, loaded.LongVal);
        Assert.Equal("", loaded.StringVal);
        Assert.Empty(loaded.BlobVal);
        Assert.Equal(Guid.Empty, loaded.GuidVal);
        Assert.Equal((TestEnum)999, loaded.EnumVal);
    }

    // ===============================================================
    //  4. Async operations (mirrors Melodee's async usage)
    // ===============================================================

    [Fact]
    public async Task Async_InsertUpdateDeleteSelect_Works()
    {
        await using var context = CreateAllTypesContext();
        await context.Database.EnsureCreatedAsync();

        var entity = new AllTypesEntity
        {
            BoolVal = true,
            ByteVal = 1,
            ShortVal = 1,
            IntVal = 1,
            LongVal = 1,
            FloatVal = 1.0f,
            DoubleVal = 1.0,
            DecimalVal = 1.0m,
            StringVal = "async-test",
            BlobVal = new byte[] { 0xAB },
            DateTimeVal = DateTime.UtcNow,
            DateTimeOffsetVal = DateTimeOffset.UtcNow,
            DateOnlyVal = DateOnly.FromDateTime(DateTime.UtcNow),
            TimeOnlyVal = new TimeOnly(12, 0),
            TimeSpanVal = TimeSpan.FromMinutes(5),
            GuidVal = Guid.NewGuid(),
            EnumVal = TestEnum.Active
        };

        // AddAsync + SaveChangesAsync
        await context.AllTypes.AddAsync(entity);
        await context.SaveChangesAsync();
        var id = entity.Id;
        Assert.True(id > 0);

        // FirstOrDefaultAsync
        var found = await context.AllTypes.FirstOrDefaultAsync(x => x.Id == id);
        Assert.NotNull(found);

        // Update + SaveChangesAsync
        found!.StringVal = "async-updated";
        await context.SaveChangesAsync();

        // ToListAsync
        var list = await context.AllTypes.Where(x => x.StringVal == "async-updated").ToListAsync();
        Assert.Single(list);

        // CountAsync
        var count = await context.AllTypes.CountAsync();
        Assert.Equal(1, count);

        // AnyAsync
        var any = await context.AllTypes.AnyAsync(x => x.BoolVal);
        Assert.True(any);

        // Remove + SaveChangesAsync
        context.AllTypes.Remove(found);
        await context.SaveChangesAsync();

        Assert.Equal(0, await context.AllTypes.CountAsync());
    }

    // ===============================================================
    //  5. UPDATE all Melodee Artist properties (mirrors UpdateArtistAsync)
    // ===============================================================

    [Fact]
    public void UpdateArtist_AllProperties_Persists()
    {
        using var context = CreateMelodeeContext();
        context.Database.EnsureCreated();

        // Insert initial artist
        var artist = new MelodeeArtist
        {
            Name = "Beatles",
            NameNormalized = "beatles",
            SortName = "Beatles, The",
            AlternateNames = "fab four",
            ItunesId = "itunes-1",
            AmgId = "amg-1",
            DiscogsId = "discogs-1",
            WikiDataId = "wiki-1",
            MusicBrainzId = Guid.Parse("11111111-1111-1111-1111-111111111111"),
            LastFmId = "lastfm-1",
            SpotifyId = "spotify-1",
            IsLocked = false,
            LastRefreshed = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero)
        };

        context.Artists.Add(artist);
        context.SaveChanges();
        var id = artist.Id;

        // UPDATE every single property (exactly what Melodee UpdateArtistAsync does)
        using (var ctx2 = CreateMelodeeContext())
        {
            var existing = ctx2.Artists.First(x => x.Id == id);
            existing.Name = "The Beatles";
            existing.NameNormalized = "the beatles";
            existing.SortName = "Beatles";
            existing.AlternateNames = "fab four|the beatles|beatles";
            existing.ItunesId = "itunes-updated";
            existing.AmgId = "amg-updated";
            existing.DiscogsId = "discogs-updated";
            existing.WikiDataId = "wiki-updated";
            existing.MusicBrainzId = Guid.Parse("22222222-2222-2222-2222-222222222222");
            existing.LastFmId = "lastfm-updated";
            existing.SpotifyId = "spotify-updated";
            existing.IsLocked = true;
            existing.LastRefreshed = DateTimeOffset.UtcNow;
            ctx2.SaveChanges();
        }

        // Verify all properties updated
        using (var verify = CreateMelodeeContext())
        {
            var loaded = verify.Artists.Single(x => x.Id == id);
            Assert.Equal("The Beatles", loaded.Name);
            Assert.Equal("the beatles", loaded.NameNormalized);
            Assert.Equal("Beatles", loaded.SortName);
            Assert.Equal("fab four|the beatles|beatles", loaded.AlternateNames);
            Assert.Equal("itunes-updated", loaded.ItunesId);
            Assert.Equal("amg-updated", loaded.AmgId);
            Assert.Equal("discogs-updated", loaded.DiscogsId);
            Assert.Equal("wiki-updated", loaded.WikiDataId);
            Assert.Equal(Guid.Parse("22222222-2222-2222-2222-222222222222"), loaded.MusicBrainzId);
            Assert.Equal("lastfm-updated", loaded.LastFmId);
            Assert.Equal("spotify-updated", loaded.SpotifyId);
            Assert.True(loaded.IsLocked);
            Assert.NotNull(loaded.LastRefreshed);
        }
    }

    // ===============================================================
    //  6. UPDATE nullable fields to null (clear external IDs)
    // ===============================================================

    [Fact]
    public void UpdateArtist_ClearNullableFields_Persists()
    {
        using var context = CreateMelodeeContext();
        context.Database.EnsureCreated();

        var artist = new MelodeeArtist
        {
            Name = "Test",
            NameNormalized = "test",
            SortName = "Test",
            MusicBrainzId = Guid.NewGuid(),
            SpotifyId = "sp-test",
            IsLocked = true,
            LastRefreshed = DateTimeOffset.UtcNow
        };

        context.Artists.Add(artist);
        context.SaveChanges();
        var id = artist.Id;

        // Clear all nullable fields
        using (var ctx2 = CreateMelodeeContext())
        {
            var e = ctx2.Artists.Single(x => x.Id == id);
            e.MusicBrainzId = null;
            e.SpotifyId = null;
            e.IsLocked = null;
            e.LastRefreshed = null;
            e.AlternateNames = null;
            e.ItunesId = null;
            e.AmgId = null;
            e.DiscogsId = null;
            e.WikiDataId = null;
            e.LastFmId = null;
            ctx2.SaveChanges();
        }

        using (var verify = CreateMelodeeContext())
        {
            var loaded = verify.Artists.Single(x => x.Id == id);
            Assert.Null(loaded.MusicBrainzId);
            Assert.Null(loaded.SpotifyId);
            Assert.Null(loaded.IsLocked);
            Assert.Null(loaded.LastRefreshed);
            Assert.Null(loaded.AlternateNames);
            Assert.Null(loaded.ItunesId);
            Assert.Null(loaded.AmgId);
            Assert.Null(loaded.DiscogsId);
            Assert.Null(loaded.WikiDataId);
            Assert.Null(loaded.LastFmId);
        }
    }

    // ===============================================================
    //  7. SQLite-imported schema: bool as INT64 (the exact Melodee bug)
    // ===============================================================

    [Fact]
    public void SqliteImportedSchema_BoolAsInt64_UpdateWorks()
    {
        // Create schema with INT64 for is_locked (as SQLite import produces)
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();

        cmd.CommandText = """
            CREATE TABLE ef_import_artists (
                id         INTEGER PRIMARY KEY,
                name       TEXT    NOT NULL,
                name_normalized TEXT NOT NULL,
                sort_name  TEXT    NOT NULL,
                is_locked  INT64,
                last_refreshed INT64
            )
            """;
        cmd.ExecuteNonQuery();

        // Seed via raw SQL (as import tool would)
        cmd.CommandText = "INSERT INTO ef_import_artists (id, name, name_normalized, sort_name, is_locked, last_refreshed) VALUES (1, 'Beatles', 'beatles', 'Beatles', 0, 0)";
        cmd.ExecuteNonQuery();

        // Now use EF Core with bool? mapping for is_locked
        var optionsBuilder = new DbContextOptionsBuilder<ImportedDbContext>();
        optionsBuilder.UseDecentDB($"Data Source={_dbPath}");

        using (var context = new ImportedDbContext(optionsBuilder.Options))
        {
            var artist = context.Artists.First(x => x.Id == 1);
            Assert.NotNull(artist);

            // UPDATE bool? on INT64 column — this was the exact bug
            artist.IsLocked = true;
            artist.LastRefreshed = DateTimeOffset.UtcNow;
            context.SaveChanges();
        }

        // Verify in fresh context
        using (var verify = new ImportedDbContext(optionsBuilder.Options))
        {
            var loaded = verify.Artists.Single(x => x.Id == 1);
            Assert.True(loaded.IsLocked);
            Assert.NotNull(loaded.LastRefreshed);
        }
    }

    // ===============================================================
    //  8. Bulk delete (RemoveRange) — Melodee's RefreshArtistAlbums
    // ===============================================================

    [Fact]
    public void RemoveRange_BulkDelete_Works()
    {
        using var context = CreateMelodeeContext();
        context.Database.EnsureCreated();

        var artist = new MelodeeArtist
        {
            Name = "Test",
            NameNormalized = "test",
            SortName = "Test",
            Albums = new List<MelodeeAlbum>
            {
                new() { Name = "A1", NameNormalized = "a1", SortName = "A1", Year = 2020, AlbumType = 1 },
                new() { Name = "A2", NameNormalized = "a2", SortName = "A2", Year = 2021, AlbumType = 1 },
                new() { Name = "A3", NameNormalized = "a3", SortName = "A3", Year = 2022, AlbumType = 1 }
            }
        };

        context.Artists.Add(artist);
        context.SaveChanges();
        var artistId = artist.Id;

        // RemoveRange — Melodee deletes all albums before re-fetching
        using (var ctx2 = CreateMelodeeContext())
        {
            var albums = ctx2.Albums.Where(a => a.ArtistId == artistId).ToList();
            Assert.Equal(3, albums.Count);

            ctx2.Albums.RemoveRange(albums);
            ctx2.SaveChanges();
        }

        using (var verify = CreateMelodeeContext())
        {
            Assert.Equal(0, verify.Albums.Count(a => a.ArtistId == artistId));
            Assert.Equal(1, verify.Artists.Count()); // artist still exists
        }
    }

    // ===============================================================
    //  9. Pagination with correlated COUNT subquery (the original query)
    // ===============================================================

    [Fact]
    public void Pagination_WithAlbumCount_Works()
    {
        using var context = CreateMelodeeContext();
        context.Database.EnsureCreated();

        // Seed multiple artists with varying album counts
        for (int i = 1; i <= 5; i++)
        {
            var a = new MelodeeArtist
            {
                Name = $"Artist {i}",
                NameNormalized = $"artist {i}",
                SortName = $"Artist {i}",
                Albums = Enumerable.Range(1, i).Select(j => new MelodeeAlbum
                {
                    Name = $"Album {j}",
                    NameNormalized = $"album {j}",
                    SortName = $"Album {j}",
                    Year = 2020 + j,
                    AlbumType = 1
                }).ToList()
            };
            context.Artists.Add(a);
        }
        context.SaveChanges();

        // The exact Melodee query pattern with correlated COUNT subquery
        using var ctx2 = CreateMelodeeContext();
        var results = ctx2.Artists
            .Select(a => new
            {
                a.Id,
                a.Name,
                a.NameNormalized,
                a.SortName,
                AlbumCount = ctx2.Albums.Count(al => al.ArtistId == a.Id)
            })
            .OrderBy(x => x.Id)
            .Skip(0)
            .Take(3)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal(1, results[0].AlbumCount);
        Assert.Equal(2, results[1].AlbumCount);
        Assert.Equal(3, results[2].AlbumCount);
    }

    // ===============================================================
    //  10. ChangeTracker.Clear after constraint violation (Melodee pattern)
    // ===============================================================

    [Fact]
    public void ConstraintViolation_ChangeTrackerClear_AllowsRetry()
    {
        using var context = CreateMelodeeContext();
        context.Database.EnsureCreated();

        var artist = new MelodeeArtist
        {
            Name = "Test",
            NameNormalized = "test",
            SortName = "Test",
            SpotifyId = "sp-unique"
        };
        context.Artists.Add(artist);
        context.SaveChanges();

        // Try to add duplicate (unique index violation)
        var dup = new MelodeeArtist
        {
            Name = "Dup",
            NameNormalized = "dup",
            SortName = "Dup",
            SpotifyId = "sp-unique"
        };
        context.Artists.Add(dup);
        Assert.Throws<DbUpdateException>(() => context.SaveChanges());

        // ChangeTracker.Clear and retry with different SpotifyId
        context.ChangeTracker.Clear();

        var fixed_ = new MelodeeArtist
        {
            Name = "Fixed",
            NameNormalized = "fixed",
            SortName = "Fixed",
            SpotifyId = "sp-different"
        };
        context.Artists.Add(fixed_);
        context.SaveChanges();

        Assert.Equal(2, context.Artists.Count());
    }

    // ===============================================================
    //  11. Multiple updates in sequence (Melodee refresh pattern)
    // ===============================================================

    [Fact]
    public void MultipleSequentialUpdates_AllPersist()
    {
        using var context = CreateMelodeeContext();
        context.Database.EnsureCreated();

        var artist = new MelodeeArtist
        {
            Name = "Test",
            NameNormalized = "test",
            SortName = "Test"
        };
        context.Artists.Add(artist);
        context.SaveChanges();
        var id = artist.Id;

        // Simulate multiple sequential updates (Melodee processes multiple search results)
        for (int i = 1; i <= 5; i++)
        {
            using var ctx = CreateMelodeeContext();
            var e = ctx.Artists.Single(x => x.Id == id);
            e.Name = $"Update {i}";
            e.LastRefreshed = DateTimeOffset.UtcNow;
            ctx.SaveChanges();
        }

        using var verify = CreateMelodeeContext();
        var loaded = verify.Artists.Single(x => x.Id == id);
        Assert.Equal("Update 5", loaded.Name);
        Assert.NotNull(loaded.LastRefreshed);
    }

    // ===============================================================
    //  12. DELETE single entity (Melodee's DeleteArtistsAsync loop)
    // ===============================================================

    [Fact]
    public void DeleteSingleEntity_Works()
    {
        using var context = CreateMelodeeContext();
        context.Database.EnsureCreated();

        var a1 = new MelodeeArtist { Name = "A1", NameNormalized = "a1", SortName = "A1" };
        var a2 = new MelodeeArtist { Name = "A2", NameNormalized = "a2", SortName = "A2" };
        var a3 = new MelodeeArtist { Name = "A3", NameNormalized = "a3", SortName = "A3" };

        context.Artists.AddRange(a1, a2, a3);
        context.SaveChanges();

        // Delete one at a time (Melodee's pattern)
        using (var ctx2 = CreateMelodeeContext())
        {
            var toDelete = ctx2.Artists.First(x => x.NameNormalized == "a2");
            ctx2.Artists.Remove(toDelete);
            ctx2.SaveChanges();
        }

        using (var verify = CreateMelodeeContext())
        {
            Assert.Equal(2, verify.Artists.Count());
            Assert.Null(verify.Artists.FirstOrDefault(x => x.NameNormalized == "a2"));
        }
    }

    // ===============================================================
    //  13. Update with FALSE literal (pg_query protobuf quirk)
    // ===============================================================

    [Fact]
    public void UpdateBoolToFalse_Works()
    {
        using var context = CreateMelodeeContext();
        context.Database.EnsureCreated();

        var artist = new MelodeeArtist
        {
            Name = "Test",
            NameNormalized = "test",
            SortName = "Test",
            IsLocked = true
        };
        context.Artists.Add(artist);
        context.SaveChanges();
        var id = artist.Id;

        // Set IsLocked to false — triggers the pg_query FALSE parsing fix
        using (var ctx2 = CreateMelodeeContext())
        {
            var e = ctx2.Artists.Single(x => x.Id == id);
            Assert.True(e.IsLocked);
            e.IsLocked = false;
            ctx2.SaveChanges();
        }

        using (var verify = CreateMelodeeContext())
        {
            var loaded = verify.Artists.Single(x => x.Id == id);
            Assert.False(loaded.IsLocked);
        }
    }

    // ===============================================================
    //  14. Large DateTimeOffset values (> INT32_MAX, parsed as Float)
    // ===============================================================

    [Fact]
    public void LargeDateTimeOffset_RoundTrips()
    {
        using var context = CreateMelodeeContext();
        context.Database.EnsureCreated();

        // DateTimeOffset.UtcNow.ToBinary() produces values > INT32_MAX
        var futureDate = new DateTimeOffset(2099, 12, 31, 23, 59, 59, TimeSpan.Zero);
        var pastDate = new DateTimeOffset(1900, 1, 1, 0, 0, 0, TimeSpan.Zero);

        var a1 = new MelodeeArtist
        {
            Name = "Future",
            NameNormalized = "future",
            SortName = "Future",
            LastRefreshed = futureDate
        };
        var a2 = new MelodeeArtist
        {
            Name = "Past",
            NameNormalized = "past",
            SortName = "Past",
            LastRefreshed = pastDate
        };

        context.Artists.AddRange(a1, a2);
        context.SaveChanges();

        using var verify = CreateMelodeeContext();
        var loadedFuture = verify.Artists.Single(x => x.NameNormalized == "future");
        var loadedPast = verify.Artists.Single(x => x.NameNormalized == "past");

        Assert.NotNull(loadedFuture.LastRefreshed);
        Assert.NotNull(loadedPast.LastRefreshed);
        Assert.True(Math.Abs((loadedFuture.LastRefreshed!.Value - futureDate).TotalMilliseconds) < 2);
        Assert.True(Math.Abs((loadedPast.LastRefreshed!.Value - pastDate).TotalMilliseconds) < 2);
    }

    // ===============================================================
    //  15. Async full workflow (mirrors Melodee's async service methods)
    // ===============================================================

    [Fact]
    public async Task Async_MelodeeWorkflow_InsertSearchUpdateDelete()
    {
        await using var context = CreateMelodeeContext();
        await context.Database.EnsureCreatedAsync();

        // AddAsync (Melodee AddArtistAsync)
        var artist = new MelodeeArtist
        {
            Name = "Beatles",
            NameNormalized = "beatles",
            SortName = "Beatles, The",
            Albums = new List<MelodeeAlbum>
            {
                new() { Name = "Abbey Road", NameNormalized = "abbey road", SortName = "Abbey Road", Year = 1969, AlbumType = 1 }
            }
        };
        context.Artists.Add(artist);
        await context.SaveChangesAsync();
        var id = artist.Id;

        // FirstOrDefaultAsync (Melodee search/lookup)
        await using var ctx2 = CreateMelodeeContext();
        var found = await ctx2.Artists
            .Include(x => x.Albums)
            .FirstOrDefaultAsync(x => x.NameNormalized == "beatles");
        Assert.NotNull(found);
        Assert.Single(found!.Albums);

        // Update + SaveChangesAsync (Melodee UpdateArtistAsync)
        found.IsLocked = true;
        found.LastRefreshed = DateTimeOffset.UtcNow;
        found.MusicBrainzId = Guid.NewGuid();
        await ctx2.SaveChangesAsync();

        // ToListAsync with Where (Melodee search patterns)
        var results = await ctx2.Artists
            .Where(x => x.IsLocked == true)
            .ToListAsync();
        Assert.Single(results);

        // Delete albums then artist (Melodee DeleteArtistsAsync)
        var albums = await ctx2.Albums.Where(a => a.ArtistId == id).ToListAsync();
        ctx2.Albums.RemoveRange(albums);
        ctx2.Artists.Remove(found);
        await ctx2.SaveChangesAsync();

        Assert.Equal(0, await ctx2.Artists.CountAsync());
        Assert.Equal(0, await ctx2.Albums.CountAsync());
    }

    // ===============================================================
    //  Helpers
    // ===============================================================

    private AllTypesDbContext CreateAllTypesContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<AllTypesDbContext>();
        optionsBuilder.UseDecentDB($"Data Source={_dbPath}");
        return new AllTypesDbContext(optionsBuilder.Options);
    }

    private MelodeeCrudDbContext CreateMelodeeContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<MelodeeCrudDbContext>();
        optionsBuilder.UseDecentDB($"Data Source={_dbPath}");
        return new MelodeeCrudDbContext(optionsBuilder.Options);
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path)) File.Delete(path);
    }

    // ===============================================================
    //  Entity / Context for All-Types testing
    // ===============================================================

    private enum TestEnum { Inactive = 0, Active = 1, Suspended = 2 }

    private sealed class AllTypesEntity
    {
        public int Id { get; set; }
        public bool BoolVal { get; set; }
        public byte ByteVal { get; set; }
        public short ShortVal { get; set; }
        public int IntVal { get; set; }
        public long LongVal { get; set; }
        public float FloatVal { get; set; }
        public double DoubleVal { get; set; }
        public decimal DecimalVal { get; set; }
        public string StringVal { get; set; } = string.Empty;
        public byte[] BlobVal { get; set; } = Array.Empty<byte>();
        public DateTime DateTimeVal { get; set; }
        public DateTimeOffset DateTimeOffsetVal { get; set; }
        public DateOnly DateOnlyVal { get; set; }
        public TimeOnly TimeOnlyVal { get; set; }
        public TimeSpan TimeSpanVal { get; set; }
        public Guid GuidVal { get; set; }
        public TestEnum EnumVal { get; set; }

        // Nullable variants
        public bool? NullableBool { get; set; }
        public int? NullableInt { get; set; }
        public long? NullableLong { get; set; }
        public double? NullableDouble { get; set; }
        public Guid? NullableGuid { get; set; }
        public DateTimeOffset? NullableDateTimeOffset { get; set; }
        public TestEnum? NullableEnum { get; set; }
    }

    private sealed class AllTypesDbContext : DbContext
    {
        public AllTypesDbContext(DbContextOptions<AllTypesDbContext> options) : base(options) { }
        public DbSet<AllTypesEntity> AllTypes => Set<AllTypesEntity>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            // Apply DateTimeOffset → Binary converter (same as Melodee)
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

            modelBuilder.Entity<AllTypesEntity>(entity =>
            {
                entity.ToTable("ef_all_types");
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).ValueGeneratedOnAdd();
            });
        }
    }

    // ===============================================================
    //  Entity / Context for Melodee CRUD testing
    // ===============================================================

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
        public bool? IsLocked { get; set; }
        public DateTimeOffset? LastRefreshed { get; set; }
        public ICollection<MelodeeAlbum> Albums { get; set; } = new List<MelodeeAlbum>();
    }

    private sealed class MelodeeAlbum
    {
        public int Id { get; set; }
        public int ArtistId { get; set; }
        public string Name { get; set; } = string.Empty;
        public string NameNormalized { get; set; } = string.Empty;
        public string SortName { get; set; } = string.Empty;
        public int Year { get; set; }
        public int AlbumType { get; set; }
        public Guid? MusicBrainzId { get; set; }
        public Guid? MusicBrainzReleaseGroupId { get; set; }
        public string? SpotifyId { get; set; }
        public string? CoverUrl { get; set; }
    }

    private sealed class MelodeeCrudDbContext : DbContext
    {
        public MelodeeCrudDbContext(DbContextOptions<MelodeeCrudDbContext> options) : base(options) { }
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
                entity.ToTable("ef_crud_artists");
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).ValueGeneratedOnAdd();
                entity.HasIndex(x => x.NameNormalized);
                entity.HasIndex(x => x.SpotifyId).IsUnique();
                entity.HasIndex(x => x.MusicBrainzId).IsUnique();
                entity.HasMany(x => x.Albums).WithOne().HasForeignKey(x => x.ArtistId);
            });

            modelBuilder.Entity<MelodeeAlbum>(entity =>
            {
                entity.ToTable("ef_crud_albums");
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).ValueGeneratedOnAdd();
                entity.HasIndex(x => new { x.ArtistId, x.NameNormalized, x.Year });
            });
        }
    }

    // ===============================================================
    //  Entity / Context for SQLite-imported schema testing
    // ===============================================================

    private sealed class ImportedArtist
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string NameNormalized { get; set; } = string.Empty;
        public string SortName { get; set; } = string.Empty;
        public bool? IsLocked { get; set; }
        public DateTimeOffset? LastRefreshed { get; set; }
    }

    private sealed class ImportedDbContext : DbContext
    {
        public ImportedDbContext(DbContextOptions<ImportedDbContext> options) : base(options) { }
        public DbSet<ImportedArtist> Artists => Set<ImportedArtist>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
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

            modelBuilder.Entity<ImportedArtist>(entity =>
            {
                entity.ToTable("ef_import_artists");
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
                entity.Property(x => x.Name).HasColumnName("name");
                entity.Property(x => x.NameNormalized).HasColumnName("name_normalized");
                entity.Property(x => x.SortName).HasColumnName("sort_name");
                entity.Property(x => x.IsLocked).HasColumnName("is_locked");
                entity.Property(x => x.LastRefreshed).HasColumnName("last_refreshed");
            });
        }
    }
}
