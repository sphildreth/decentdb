using EntityFrameworkDemo.Data;
using EntityFrameworkDemo.Models;
using EntityFrameworkDemo.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NodaTime;
using DecentDB.EntityFrameworkCore;

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
    options.UseDecentDB($"Data Source={dbPath}", x => x.UseNodaTime());
    options.UseLazyLoadingProxies();
    options.EnableSensitiveDataLogging(false);
});

services.AddSingleton<PerformanceMetrics>();
services.AddScoped<ArtistService>();
services.AddScoped<AlbumService>();
services.AddScoped<TrackService>();
services.AddScoped<EventService>();

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
var eventService = serviceProvider.GetRequiredService<EventService>();

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

// Generate and seed events (NodaTime entities)
var seedEvents = DataGenerator.GenerateEventsForArtists(artists);
foreach (var ev in seedEvents)
{
    ev.Artist = null!;
}
Console.WriteLine($"Generated {seedEvents.Count} events");

await metrics.MeasureAsync("Seed: Create All Events", async () =>
{
    await eventService.CreateManyAsync(seedEvents);
}, $"{seedEvents.Count} events");

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

// ── Showcase: GroupBy ──
Console.WriteLine("\n--- GroupBy & Aggregation ---");

var genreCounts = await metrics.MeasureAsync(
    "GroupBy: Artists Per Genre",
    async () => await artistService.GetArtistCountsByGenreAsync()
);

var decadeCounts = await metrics.MeasureAsync(
    "GroupBy: Albums Per Decade",
    async () => await albumService.GetAlbumCountByDecadeAsync()
);

var trackStatsByGenre = await metrics.MeasureAsync(
    "GroupBy: Track Stats Per Genre",
    async () => await trackService.GetTrackStatsByGenreAsync()
);

// ── Showcase: Projections ──
Console.WriteLine("\n--- Select Projections ---");

var artistSummaries = await metrics.MeasureAsync(
    "Projection: Artist Summaries (DTO)",
    async () => await artistService.GetArtistSummariesAsync(10)
);

var albumSizeLabels = await metrics.MeasureAsync(
    "Projection: Album Size Labels (CASE)",
    async () => await albumService.GetAlbumSizeLabelsAsync(10)
);

var trackDurationLabels = await metrics.MeasureAsync(
    "Projection: Track Duration Labels (CASE)",
    async () => await trackService.GetTrackDurationLabelsAsync(10)
);

// ── Showcase: DISTINCT ──
Console.WriteLine("\n--- Distinct Queries ---");

var distinctCountries = await metrics.MeasureAsync(
    "Distinct: Artist Countries",
    async () => await artistService.GetDistinctCountriesAsync()
);

var distinctGenres = await metrics.MeasureAsync(
    "Distinct: Track Genres",
    async () => await trackService.GetDistinctGenresAsync()
);

// ── Showcase: String Operations ──
Console.WriteLine("\n--- String Operations ---");

var upperNames = await metrics.MeasureAsync(
    "String: Artist Names ToUpper",
    async () => await artistService.GetArtistNamesUpperAsync(5)
);

var trimmedTitles = await metrics.MeasureAsync(
    "String: Track Titles Trim+Upper",
    async () => await trackService.GetTrimmedTitlesAsync(5)
);

var maxTitleLen = await metrics.MeasureAsync(
    "String: Max Track Title Length",
    async () => await trackService.GetMaxTitleLengthAsync()
);

var searchCaseInsensitive = await metrics.MeasureAsync(
    "String: Case-Insensitive Album Search",
    async () => await albumService.SearchByTitleCaseInsensitiveAsync("dark")
);

// ── Showcase: Any / All / Min / Max ──
Console.WriteLine("\n--- Any / Min / Max ---");

var anyRock = await metrics.MeasureAsync(
    "Any: Artists In Rock",
    async () => await artistService.AnyArtistInGenreAsync("Rock")
);

var anyExplicit = await metrics.MeasureAsync(
    "Any: Explicit Tracks Exist",
    async () => await trackService.AnyExplicitTracksAsync()
);

var earliestYear = await metrics.MeasureAsync(
    "Min: Earliest Formed Year",
    async () => await artistService.GetEarliestFormedYearAsync()
);

var latestYear = await metrics.MeasureAsync(
    "Max: Latest Formed Year",
    async () => await artistService.GetLatestFormedYearAsync()
);

var maxTracks = await metrics.MeasureAsync(
    "Max: Most Tracks On Album",
    async () => await albumService.GetMaxTracksOnAlbumAsync()
);

// ── Showcase: Math / Rounding ──
Console.WriteLine("\n--- Math Operations ---");

var avgRatingRounded = await metrics.MeasureAsync(
    "Math: Avg Rating Rounded",
    async () => await trackService.GetAverageRatingRoundedAsync()
);

// ── Showcase: Split Query / Filtered Include / Raw SQL ──
Console.WriteLine("\n--- Advanced Loading ---");

var splitAlbum = await metrics.MeasureAsync(
    "Split: Album With Artist+Tracks",
    async () => await albumService.GetByIdWithAllDetailsSplitAsync(allAlbums.First().Id)
);

var filteredInclude = await metrics.MeasureAsync(
    "Filtered: Albums With Long Tracks",
    async () => await trackService.GetAlbumsWithLongTracksAsync(5)
);

var rawSqlAlbums = await metrics.MeasureAsync(
    "RawSQL: Recent Albums (>2005)",
    async () => await albumService.GetRecentAlbumsRawSqlAsync(2005)
);

// ── Showcase: Artist Eras (CASE WHEN) ──
Console.WriteLine("\n--- Conditional Queries ---");

var artistEras = await metrics.MeasureAsync(
    "Conditional: Artists By Era (CASE)",
    async () => await artistService.GetArtistsByEraAsync()
);

// ── Showcase: NodaTime (Instant + LocalDate + DateTime coexistence) ──
Console.WriteLine("\n--- NodaTime: Instant & LocalDate ---");

var eventCount = await metrics.MeasureAsync(
    "NodaTime: Count All Events",
    async () => await eventService.GetTotalCountAsync()
);

var allEvents = await metrics.MeasureAsync(
    "NodaTime: Get Events (Page 1)",
    async () => await eventService.GetAllAsync(1, 20),
    "20 items"
);

var eventWithArtist = await metrics.MeasureAsync(
    "NodaTime: Event With Artist (Include)",
    async () => await eventService.GetByIdWithArtistAsync(allEvents.First().Id)
);

// Instant range query
var saleFrom = Instant.FromUtc(2024, 1, 1, 0, 0);
var saleTo = Instant.FromUtc(2024, 6, 30, 23, 59);
var salePeriodEvents = await metrics.MeasureAsync(
    "NodaTime: Instant Range (H1 2024)",
    async () => await eventService.GetByTicketSaleRangeAsync(saleFrom, saleTo)
);

var doorsOpenCount = await metrics.MeasureAsync(
    "NodaTime: Count Non-Null Instant",
    async () => await eventService.CountWithDoorsOpenSetAsync()
);

// LocalDate range query
var dateFrom = new LocalDate(2024, 6, 1);
var dateTo = new LocalDate(2024, 8, 31);
var summerEvents = await metrics.MeasureAsync(
    "NodaTime: LocalDate Range (Summer 24)",
    async () => await eventService.GetByDateRangeAsync(dateFrom, dateTo)
);

var nextSales = await metrics.MeasureAsync(
    "NodaTime: Order By Instant (First 10)",
    async () => await eventService.GetNextUpcomingSalesAsync(10)
);

var artistEvents = await metrics.MeasureAsync(
    "NodaTime: Events By Artist",
    async () => await eventService.GetByArtistAsync(allArtists.First().Id)
);

Console.WriteLine("\n--- NodaTime: Aggregation & Projection ---");

var venueStats = await metrics.MeasureAsync(
    "NodaTime: GroupBy Venue Stats",
    async () => await eventService.GetVenueStatsAsync()
);

var topRevenue = await metrics.MeasureAsync(
    "NodaTime: Top 5 Artists By Revenue",
    async () => await eventService.GetTopArtistsByRevenueAsync(5)
);

var eventSummaries = await metrics.MeasureAsync(
    "NodaTime: Event Summaries (DTO)",
    async () => await eventService.GetEventSummariesAsync(10)
);

var totalTickets = await metrics.MeasureAsync(
    "NodaTime: Total Tickets Sold",
    async () => await eventService.GetTotalTicketsSoldAsync()
);

var avgPrice = await metrics.MeasureAsync(
    "NodaTime: Average Ticket Price",
    async () => await eventService.GetAverageTicketPriceAsync()
);

var eventCities = await metrics.MeasureAsync(
    "NodaTime: Distinct Cities",
    async () => await eventService.GetDistinctCitiesAsync()
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
Console.WriteLine($"  Distinct Countries: {distinctCountries.Count}");
Console.WriteLine($"  Distinct Genres: {distinctGenres.Count}");
Console.WriteLine($"  Earliest Formed: {earliestYear}");
Console.WriteLine($"  Latest Formed: {latestYear}");
Console.WriteLine($"  Max Tracks On Album: {maxTracks}");
Console.WriteLine($"  Max Track Title Length: {maxTitleLen} chars");
Console.WriteLine($"  Avg Rating (rounded): {avgRatingRounded:F1}");

Console.WriteLine($"\nArtists By Genre:");
foreach (var gc in genreCounts.Take(5))
{
    Console.WriteLine($"  {gc.Genre}: {gc.Count} artists");
}

Console.WriteLine($"\nAlbums By Decade:");
foreach (var d in decadeCounts)
{
    Console.WriteLine($"  {d.Decade}s: {d.Count} albums (avg {d.AvgTracks:F1} tracks)");
}

Console.WriteLine($"\nTop 5 Genres By Plays:");
foreach (var ts in trackStatsByGenre.Take(5))
{
    Console.WriteLine($"  {ts.Genre}: {ts.TotalPlays:N0} plays ({ts.TrackCount} tracks, avg {ts.AvgDuration:F0}s)");
}

Console.WriteLine($"\nArtist Summaries (DTO Projection):");
foreach (var s in artistSummaries.Take(5))
{
    Console.WriteLine($"  {s.Name} [{s.Genre}] — {s.Country}, {s.AlbumCount} albums");
}

Console.WriteLine($"\nArtist Eras (CASE WHEN):");
foreach (var e in artistEras.Take(5))
{
    Console.WriteLine($"  {e.Name} ({e.FormedYear}) — {e.Era}");
}

Console.WriteLine($"\nString Operations:");
Console.Write("  ToUpper: ");
Console.WriteLine(string.Join(", ", upperNames.Take(3)));
Console.Write("  Trim+Upper: ");
Console.WriteLine(string.Join(", ", trimmedTitles.Take(3)));

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

// NodaTime summary
Console.WriteLine($"\nNodaTime Statistics (Instant + LocalDate + DateTime):");
Console.WriteLine($"  Total Events: {eventCount}");
Console.WriteLine($"  Total Tickets Sold: {totalTickets:N0}");
Console.WriteLine($"  Avg Ticket Price: ${avgPrice:F2}");
Console.WriteLine($"  Events with DoorsOpen set: {doorsOpenCount}");
Console.WriteLine($"  Events in H1 2024 (Instant range): {salePeriodEvents.Count}");
Console.WriteLine($"  Summer 2024 Events (LocalDate range): {summerEvents.Count}");
Console.WriteLine($"  Distinct Event Cities: {eventCities.Count}");

Console.WriteLine($"\nTop 5 Venues (GroupBy on NodaTime entity):");
foreach (var v in venueStats.Take(5))
{
    Console.WriteLine($"  {v.Venue}: {v.EventCount} events, {v.TotalTicketsSold:N0} tickets, avg ${v.AvgTicketPrice:F2}");
}

Console.WriteLine($"\nTop 5 Artists By Event Revenue:");
foreach (var (name, revenue, count) in topRevenue)
{
    Console.WriteLine($"  {name}: ${revenue:N0} ({count} events)");
}

Console.WriteLine($"\nEvent Summaries (DTO mixing Instant/DateTime):");
foreach (var es in eventSummaries.Take(3))
{
    var eventDate = new NodaTime.LocalDate(1970, 1, 1).PlusDays((int)es.EventDateDays);
    Console.WriteLine($"  {es.Name} — {es.Venue}, {eventDate}, {es.TicketsSold} tickets (${es.Revenue:N0})");
}

Console.WriteLine("\n✓ Demo completed successfully!");

// Cleanup
Console.WriteLine("\nCleaning up...");
if (File.Exists(dbPath))
{
    File.Delete(dbPath);
    Console.WriteLine("Database file deleted.");
}
