using EntityFrameworkDemo.Data;
using EntityFrameworkDemo.Models;
using EntityFrameworkDemo.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

var dbPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "music.ddb"));

Console.WriteLine("╔══════════════════════════════════════════════════════════════════════════╗");
Console.WriteLine("║           DecentDB Entity Framework Core Demo Application                ║");
Console.WriteLine("╚══════════════════════════════════════════════════════════════════════════╝");
Console.WriteLine();
Console.WriteLine($"Database: {dbPath}");
Console.WriteLine();

// Setup DI
var services = new ServiceCollection();

services.AddLogging(builder =>
{
    builder.AddConsole();
    builder.SetMinimumLevel(LogLevel.Warning);
});

services.AddDbContextFactory<MusicDbContext>(options =>
{
    options.UseDecentDB($"Data Source={dbPath}");
    options.EnableSensitiveDataLogging(false);
});

services.AddSingleton<PerformanceMetrics>();
services.AddScoped<ArtistService>();
services.AddScoped<AlbumService>();
services.AddScoped<TrackService>();

var serviceProvider = services.BuildServiceProvider();

// Clean start
if (File.Exists(dbPath))
{
    File.Delete(dbPath);
    Console.WriteLine("Cleaned up existing database.");
}

// Ensure database and schema are created
using (var scope = serviceProvider.CreateScope())
{
    var context = scope.ServiceProvider.GetRequiredService<MusicDbContext>();
    await context.Database.EnsureCreatedAsync();
    Console.WriteLine("Database schema created successfully.");
}

// Get services
var metrics = serviceProvider.GetRequiredService<PerformanceMetrics>();
var artistService = serviceProvider.GetRequiredService<ArtistService>();
var albumService = serviceProvider.GetRequiredService<AlbumService>();
var trackService = serviceProvider.GetRequiredService<TrackService>();

// Generate and seed data
Console.WriteLine("\n" + new string('═', 76));
Console.WriteLine("SEEDING DATABASE");
Console.WriteLine(new string('═', 76));

var artists = DataGenerator.GenerateArtists(100);
Console.WriteLine($"Generated {artists.Count} artists");

foreach (var artist in artists)
{
    artist.Albums = DataGenerator.GenerateAlbumsForArtist(artist);
}

var totalAlbums = artists.Sum(a => a.Albums.Count);
var totalTracks = artists.Sum(a => a.Albums.Sum(al => al.Tracks.Count));

Console.WriteLine($"Generated {totalAlbums} albums");
Console.WriteLine($"Generated {totalTracks} tracks");

// Flatten the object graph so we can seed in three distinct phases without EF trying to insert
// the same entities multiple times (each service call uses a new DbContext instance).
var seedAlbums = artists.SelectMany(a => a.Albums).ToList();
var seedTracks = seedAlbums.SelectMany(a => a.Tracks).ToList();

// Detach relationships before seeding Artists (otherwise Artists.AddRange will cascade insert Albums/Tracks too).
foreach (var artist in artists)
{
    artist.Albums.Clear();
}

// Seed artists
await metrics.MeasureAsync("Seed: Create 100 Artists", async () =>
{
    await artistService.CreateManyAsync(artists);
});

// Prepare albums for seeding: keep FK ids, remove navigation properties to prevent EF from inserting Artists/Tracks again.
foreach (var album in seedAlbums)
{
    album.ArtistId = album.Artist.Id;
    album.Artist = null!;
    album.Tracks.Clear();
}

// Seed albums
await metrics.MeasureAsync("Seed: Create All Albums", async () =>
{
    await albumService.CreateManyAsync(seedAlbums);
}, $"{seedAlbums.Count} albums");

// Prepare tracks for seeding: keep FK ids, remove navigation properties to prevent EF from inserting Albums again.
foreach (var track in seedTracks)
{
    track.AlbumId = track.Album.Id;
    track.Album = null!;
}

// Seed tracks
await metrics.MeasureAsync("Seed: Create All Tracks", async () =>
{
    await trackService.CreateManyAsync(seedTracks);
}, $"{seedTracks.Count} tracks");

Console.WriteLine("\n✓ Database seeded successfully!");

// Run performance benchmarks
Console.WriteLine("\n" + new string('═', 76));
Console.WriteLine("PERFORMANCE BENCHMARKS");
Console.WriteLine(new string('═', 76));

// Artist benchmarks
Console.WriteLine("\n--- Artist Operations ---");

var allArtists = await metrics.MeasureAsync(
    "Artist: Get All (Page 1)",
    async () => await artistService.GetAllAsync(1, 20),
    "20 items"
);

await metrics.MeasureAsync(
    "Artist: Get All (Page 2)",
    async () => await artistService.GetAllAsync(2, 20),
    "20 items"
);

var artistCount = await metrics.MeasureAsync(
    "Artist: Count Total",
    async () => await artistService.GetTotalCountAsync()
);

var artistById = await metrics.MeasureAsync(
    "Artist: Get By ID",
    async () => await artistService.GetByIdAsync(allArtists.First().Id)
);

var artistWithAlbums = await metrics.MeasureAsync(
    "Artist: Get By ID With Albums",
    async () => await artistService.GetByIdWithAlbumsAsync(allArtists.First().Id)
);

var rockArtists = await metrics.MeasureAsync(
    "Artist: Search By Genre (Rock)",
    async () => await artistService.GetByGenreAsync("Rock")
);

var searchResults = await metrics.MeasureAsync(
    "Artist: Search By Name ('The')",
    async () => await artistService.SearchByNameAsync("The")
);

// Album benchmarks
Console.WriteLine("\n--- Album Operations ---");

var allAlbums = await metrics.MeasureAsync(
    "Album: Get All (Page 1)",
    async () => await albumService.GetAllAsync(1, 20),
    "20 items"
);

var albumCount = await metrics.MeasureAsync(
    "Album: Count Total",
    async () => await albumService.GetTotalCountAsync()
);

var albumById = await metrics.MeasureAsync(
    "Album: Get By ID",
    async () => await albumService.GetByIdAsync(allAlbums.First().Id)
);

var albumWithTracks = await metrics.MeasureAsync(
    "Album: Get By ID With Tracks",
    async () => await albumService.GetByIdWithTracksAsync(allAlbums.First().Id)
);

var albumsByArtist = await metrics.MeasureAsync(
    "Album: Get By Artist",
    async () => await albumService.GetByArtistAsync(allArtists.First().Id)
);

var albumsByYear = await metrics.MeasureAsync(
    "Album: Get By Year Range (2000-2010)",
    async () => await albumService.GetByYearRangeAsync(2000, 2010)
);

// Track benchmarks
Console.WriteLine("\n--- Track Operations ---");

var allTracks = await metrics.MeasureAsync(
    "Track: Get All (Page 1)",
    async () => await trackService.GetAllAsync(1, 20),
    "20 items"
);

var trackCount = await metrics.MeasureAsync(
    "Track: Count Total",
    async () => await trackService.GetTotalCountAsync()
);

var trackById = await metrics.MeasureAsync(
    "Track: Get By ID",
    async () => await trackService.GetByIdAsync(allTracks.First().Id)
);

var trackWithAlbum = await metrics.MeasureAsync(
    "Track: Get By ID With Album",
    async () => await trackService.GetByIdWithAlbumAsync(allTracks.First().Id)
);

var tracksByAlbum = await metrics.MeasureAsync(
    "Track: Get By Album",
    async () => await trackService.GetByAlbumAsync(allAlbums.First().Id)
);

var rockTracks = await metrics.MeasureAsync(
    "Track: Get By Genre (Rock)",
    async () => await trackService.GetByGenreAsync("Rock")
);

var longTracks = await metrics.MeasureAsync(
    "Track: Get By Duration (5-8 min)",
    async () => await trackService.GetByDurationRangeAsync(300, 480)
);

var topTracks = await metrics.MeasureAsync(
    "Track: Get Top 10 By Play Count",
    async () => await trackService.GetTopByPlayCountAsync(10)
);

// Modification operations
Console.WriteLine("\n--- Modification Operations ---");

var newArtist = new Artist
{
    Name = "Test Artist",
    Genre = "Test Genre",
    Country = "Test Country"
};

var createdArtist = await metrics.MeasureAsync(
    "Artist: Create New",
    async () => await artistService.CreateAsync(newArtist)
);

createdArtist.Name = "Updated Test Artist";
await metrics.MeasureAsync(
    "Artist: Update",
    async () => await artistService.UpdateAsync(createdArtist)
);

await metrics.MeasureAsync(
    "Artist: Delete",
    async () => await artistService.DeleteAsync(createdArtist.Id)
);

// Complex queries
Console.WriteLine("\n--- Complex Queries ---");

var topArtists = await metrics.MeasureAsync(
    "Artist: Get Top 10 By Album Count",
    async () => await artistService.GetTopByAlbumCountAsync(10)
);

var avgDuration = await metrics.MeasureAsync(
    "Album: Get Average Duration",
    async () => await albumService.GetAverageDurationAsync()
);

var totalPlayCount = await metrics.MeasureAsync(
    "Track: Get Total Play Count",
    async () => await trackService.GetTotalPlayCountAsync()
);

// Print performance report
metrics.PrintReport();

// Summary
Console.WriteLine("\n" + new string('═', 76));
Console.WriteLine("SUMMARY");
Console.WriteLine(new string('═', 76));

Console.WriteLine($"\nDatabase Statistics:");
Console.WriteLine($"  Artists: {artistCount}");
Console.WriteLine($"  Albums: {albumCount}");
Console.WriteLine($"  Tracks: {trackCount}");
Console.WriteLine($"  Average Album Duration: {TimeSpan.FromSeconds(avgDuration):mm\\:ss}");
Console.WriteLine($"  Total Track Plays: {totalPlayCount:N0}");

Console.WriteLine($"\nTop 5 Artists by Album Count:");
foreach (var artist in topArtists.Take(5))
{
    Console.WriteLine($"  {artist.Name}: {artist.GetTotalAlbumCount()} albums, {artist.GetTotalTrackCount()} tracks");
}

Console.WriteLine($"\nTop 5 Most Played Tracks:");
foreach (var track in topTracks.Take(5))
{
    Console.WriteLine($"  {track.Title}: {track.PlayCount:N0} plays ({track.GetFormattedDuration()})");
}

Console.WriteLine("\n✓ Demo completed successfully!");

// Cleanup
Console.WriteLine("\nCleaning up...");
if (File.Exists(dbPath))
{
    File.Delete(dbPath);
    Console.WriteLine("Database file deleted.");
}

Console.WriteLine("\nPress any key to exit...");
Console.ReadKey();
