using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using DecentDb.AdoNet;
using DecentDb.MicroOrm;
using Xunit;
using Xunit.Abstractions;

namespace DecentDb.Tests;

public sealed class MelodeeArtistSearchEngineIntegrationTests
{
    private readonly ITestOutputHelper _output;

    public MelodeeArtistSearchEngineIntegrationTests(ITestOutputHelper output)
    {
        _output = output;
    }

    private static bool TryGetExternalPath(string envVar, string description, out string path, out string reason)
    {
        path = (Environment.GetEnvironmentVariable(envVar) ?? "").Trim();
        if (path.Length == 0)
        {
            reason = $"set {envVar} to a {description} path to enable this test";
            return false;
        }

        if (!File.Exists(path))
        {
            reason = $"missing file at {envVar}='{path}'";
            return false;
        }

        reason = "";
        return true;
    }

    private static bool TryStageDbForReadOnly(string sourcePath, out string stagedPath)
    {
        stagedPath = string.Empty;

        if (!File.Exists(sourcePath))
        {
            return false;
        }

        var tempDir = Path.Combine(Path.GetTempPath(), "decentdb_melodee");
        Directory.CreateDirectory(tempDir);

        stagedPath = Path.Combine(tempDir, $"{Path.GetFileName(sourcePath)}.{Guid.NewGuid():N}");
        File.Copy(sourcePath, stagedPath, overwrite: true);

        var sourceWal = sourcePath + "-wal";
        if (File.Exists(sourceWal))
        {
            File.Copy(sourceWal, stagedPath + "-wal", overwrite: true);
        }

        return true;
    }

    private sealed record SampleQuery(
        string DisplayName,
        string NameNormalized,
        string? MusicBrainzId,
        string? SpotifyId,
        double LoggedElapsedMs);

    // These are the 10 log samples from the user prompt (Elapsed is the Melodee-app timing).
    private static readonly SampleQuery[] Samples =
    [
        new("Anyma, Sevdaliza", "ANYMASEVDALIZA", null, null, 205.43),
        new("Gate Crashers", "GATECRASHERS", null, null, 3386.4951),
        new("Chet Faker", "CHETFAKER", null, null, 541.2957),
        new("Vel Nine", "VELNINE", null, null, 2840.7465),
        new("Your Inland Empire", "YOURINLANDEMPIRE", null, null, 522.6921),
        new("Edge of Sanity", "EDGEOFSANITY", null, null, 2.8725),
        new("Lunar C", "LUNARC", null, null, 844.0219),
        new("The Prophets", "THEPROPHETS", null, null, 1094.1006),
        new("San Francisco Street Music", "SANFRANCISCOSTREETMUSIC", null, null, 1467.1541),
        new("Pink Floyd", "PINKFLOYD", null, null, 15.5249),
    ];

    [DecentDb.MicroOrm.Table("Artists")]
    private sealed class ArtistRow
    {
        [Column("id")]
        public long Id { get; set; }

        [Column("name")]
        public string Name { get; set; } = "";

        [Column("namenormalized")]
        public string NameNormalized { get; set; } = "";

        [Column("alternatenames")]
        public string? AlternateNames { get; set; }

        [Column("sortname")]
        public string SortName { get; set; } = "";

        [Column("itunesid")]
        public string? ItunesId { get; set; }

        [Column("amgid")]
        public string? AmgId { get; set; }

        [Column("discogsid")]
        public string? DiscogsId { get; set; }

        [Column("wikidataid")]
        public string? WikiDataId { get; set; }

        [Column("musicbrainzid")]
        public string? MusicBrainzId { get; set; }

        [Column("lastfmid")]
        public string? LastFmId { get; set; }

        [Column("spotifyid")]
        public string? SpotifyId { get; set; }

        [Column("islocked")]
        public long? IsLocked { get; set; }

        [Column("lastrefreshed")]
        public long? LastRefreshed { get; set; }
    }

    [DecentDb.MicroOrm.Table("Albums")]
    private sealed class AlbumRow
    {
        [Column("id")]
        public long Id { get; set; }

        [Column("artistid")]
        public long ArtistId { get; set; }

        [Column("sortname")]
        public string SortName { get; set; } = "";

        [Column("albumtype")]
        public long AlbumType { get; set; }

        [Column("musicbrainzid")]
        public string? MusicBrainzId { get; set; }

        [Column("musicbrainzreleasegroupid")]
        public string? MusicBrainzReleaseGroupId { get; set; }

        [Column("spotifyid")]
        public string? SpotifyId { get; set; }

        [Column("coverurl")]
        public string? CoverUrl { get; set; }

        [Column("name")]
        public string Name { get; set; } = "";

        [Column("namenormalized")]
        public string NameNormalized { get; set; } = "";

        [Column("year")]
        public long Year { get; set; }
    }

    private sealed class ArtistWithAlbums
    {
        public ArtistRow Artist { get; }
        public List<AlbumRow> Albums { get; } = new();

        public ArtistWithAlbums(ArtistRow artist)
        {
            Artist = artist;
        }
    }

    private static (string firstTag, string inTag, string outerTag) BuildTags(string nameNormalized)
    {
        const string sep = "|";
        return (
            firstTag: $"{nameNormalized}{sep}",
            inTag: $"{sep}{nameNormalized}{sep}",
            outerTag: $"{sep}{nameNormalized}"
        );
    }

    private static object BuildParams(SampleQuery q)
    {
        var (firstTag, inTag, outerTag) = BuildTags(q.NameNormalized);

        // EF Core uses string.Contains() on AlternateNames which is a substring match.
        // We model that with LIKE and a pattern that contains '%' on both ends.
        return new
        {
            NameNormalized = q.NameNormalized,
            MusicBrainzId = q.MusicBrainzId,
            SpotifyId = q.SpotifyId,
            FirstTagPattern = $"%{firstTag}%",
            InTagPattern = $"%{inTag}%",
            OuterTagPattern = $"%{outerTag}%",
        };
    }

    private static void EnsureAlternateNamesTrigramIndex(DecentDbConnection conn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE INDEX ix_artists_alternatenames_trgm ON Artists USING trigram (AlternateNames)";
        try
        {
            _ = cmd.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            // Best-effort. Index may already exist in the staged DB.
            var msg = ex.Message ?? "";
            if (msg.Contains("already exists", StringComparison.OrdinalIgnoreCase)
                || msg.Contains("exists", StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            throw;
        }
    }

        // NOTE: Melodee uses EF Core Include(Albums). EF may execute this as a split-query.
        // We intentionally do that here (artist query + albums query) to match behavior and avoid
        // Dapper multi-mapping paths that currently crash the native host process under large joins.
        private const string DapperArtistsSql = @"
SELECT
    a.Id,
    a.Name,
    a.NameNormalized,
    a.AlternateNames,
    a.SortName,
    a.ItunesId,
    a.AmgId,
    a.DiscogsId,
    a.WikiDataId,
    a.MusicBrainzId,
    a.LastFmId,
    a.SpotifyId,
    a.IsLocked,
    a.LastRefreshed
FROM Artists a
WHERE
    a.NameNormalized = @NameNormalized
    OR (a.MusicBrainzId IS NOT NULL AND @MusicBrainzId IS NOT NULL AND a.MusicBrainzId = @MusicBrainzId)
    OR (a.AlternateNames IS NOT NULL AND (
                a.AlternateNames LIKE @FirstTagPattern
                OR a.AlternateNames LIKE @InTagPattern
                OR a.AlternateNames LIKE @OuterTagPattern
    ))
    OR (a.SpotifyId IS NOT NULL AND @SpotifyId IS NOT NULL AND a.SpotifyId = @SpotifyId)
";

        private const string DapperArtistsByNameNormalizedSql = @"
SELECT
    a.Id,
    a.Name,
    a.NameNormalized,
    a.AlternateNames,
    a.SortName,
    a.ItunesId,
    a.AmgId,
    a.DiscogsId,
    a.WikiDataId,
    a.MusicBrainzId,
    a.LastFmId,
    a.SpotifyId,
    a.IsLocked,
    a.LastRefreshed
FROM Artists a
WHERE a.NameNormalized = @NameNormalized
";

        private const string DapperArtistsByMusicBrainzIdSql = @"
SELECT
    a.Id,
    a.Name,
    a.NameNormalized,
    a.AlternateNames,
    a.SortName,
    a.ItunesId,
    a.AmgId,
    a.DiscogsId,
    a.WikiDataId,
    a.MusicBrainzId,
    a.LastFmId,
    a.SpotifyId,
    a.IsLocked,
    a.LastRefreshed
FROM Artists a
WHERE a.MusicBrainzId = @MusicBrainzId
";

        private const string DapperArtistsBySpotifyIdSql = @"
SELECT
    a.Id,
    a.Name,
    a.NameNormalized,
    a.AlternateNames,
    a.SortName,
    a.ItunesId,
    a.AmgId,
    a.DiscogsId,
    a.WikiDataId,
    a.MusicBrainzId,
    a.LastFmId,
    a.SpotifyId,
    a.IsLocked,
    a.LastRefreshed
FROM Artists a
WHERE a.SpotifyId = @SpotifyId
";

        private const string DapperArtistsByAlternateNamesLikeSql = @"
SELECT
    a.Id,
    a.Name,
    a.NameNormalized,
    a.AlternateNames,
    a.SortName,
    a.ItunesId,
    a.AmgId,
    a.DiscogsId,
    a.WikiDataId,
    a.MusicBrainzId,
    a.LastFmId,
    a.SpotifyId,
    a.IsLocked,
    a.LastRefreshed
FROM Artists a
WHERE a.AlternateNames IS NOT NULL AND a.AlternateNames LIKE @Pattern
";

        private const string DapperAlbumsSelectPrefix = @"
SELECT
    Id,
    ArtistId,
    SortName,
    AlbumType,
    MusicBrainzId,
    MusicBrainzReleaseGroupId,
    SpotifyId,
    CoverUrl,
    Name,
    NameNormalized,
    Year
FROM Albums
WHERE
";

        private const string JoinSqlFull = @"
SELECT
    a.Id,
    a.Name,
    a.NameNormalized,
    a.AlternateNames,
    a.SortName,
    a.ItunesId,
    a.AmgId,
    a.DiscogsId,
    a.WikiDataId,
    a.MusicBrainzId,
    a.LastFmId,
    a.SpotifyId,
    a.IsLocked,
    a.LastRefreshed,

    al.Id AS AlbumId,
    al.ArtistId,
    al.SortName,
    al.AlbumType,
    al.MusicBrainzId,
    al.MusicBrainzReleaseGroupId,
    al.SpotifyId,
    al.CoverUrl,
    al.Name,
    al.NameNormalized,
    al.Year
FROM Artists a
LEFT JOIN Albums al ON al.ArtistId = a.Id
WHERE
    a.NameNormalized = @NameNormalized
    OR (a.MusicBrainzId IS NOT NULL AND @MusicBrainzId IS NOT NULL AND a.MusicBrainzId = @MusicBrainzId)
    OR (a.AlternateNames IS NOT NULL AND (
                a.AlternateNames LIKE @FirstTagPattern
                OR a.AlternateNames LIKE @InTagPattern
                OR a.AlternateNames LIKE @OuterTagPattern
    ))
    OR (a.SpotifyId IS NOT NULL AND @SpotifyId IS NOT NULL AND a.SpotifyId = @SpotifyId)
";

        private const string DapperArtistIdsSql = @"
SELECT DISTINCT a.Id
FROM Artists a
WHERE
    a.NameNormalized = @NameNormalized
    OR (a.MusicBrainzId IS NOT NULL AND @MusicBrainzId IS NOT NULL AND a.MusicBrainzId = @MusicBrainzId)
    OR (a.AlternateNames IS NOT NULL AND (
                a.AlternateNames LIKE @FirstTagPattern
                OR a.AlternateNames LIKE @InTagPattern
                OR a.AlternateNames LIKE @OuterTagPattern
    ))
    OR (a.SpotifyId IS NOT NULL AND @SpotifyId IS NOT NULL AND a.SpotifyId = @SpotifyId)
";

        private const string AdoNetJoinSmokeSql = @"
SELECT
    a.Id,
    a.Name,
    a.NameNormalized,
    al.Id AS AlbumId,
    al.NameNormalized AS AlbumNameNormalized,
    al.Year
FROM Artists a
LEFT JOIN Albums al ON al.ArtistId = a.Id
WHERE a.NameNormalized = @NameNormalized
";

    private static async Task<List<ArtistWithAlbums>> RunDapperAsync(DecentDbConnection conn, SampleQuery q)
    {
        var artists = (await conn.QueryAsync<ArtistRow>(DapperArtistsSql, BuildParams(q))).ToList();
        return await AttachAlbumsAsync(conn, artists);
    }

    private static async Task<List<ArtistWithAlbums>> RunDapperIndexFriendlyAsync(DecentDbConnection conn, SampleQuery q)
    {
        // Run each predicate branch as its own query so the engine can use the most appropriate index.
        // In particular, a single OR-heavy query tends to defeat btree index usage.
        var found = new Dictionary<long, ArtistRow>();

        foreach (var a in await conn.QueryAsync<ArtistRow>(DapperArtistsByNameNormalizedSql, new { NameNormalized = q.NameNormalized }))
        {
            found.TryAdd(a.Id, a);
        }

        if (!string.IsNullOrWhiteSpace(q.MusicBrainzId))
        {
            foreach (var a in await conn.QueryAsync<ArtistRow>(DapperArtistsByMusicBrainzIdSql, new { MusicBrainzId = q.MusicBrainzId }))
            {
                found.TryAdd(a.Id, a);
            }
        }

        if (!string.IsNullOrWhiteSpace(q.SpotifyId))
        {
            foreach (var a in await conn.QueryAsync<ArtistRow>(DapperArtistsBySpotifyIdSql, new { SpotifyId = q.SpotifyId }))
            {
                found.TryAdd(a.Id, a);
            }
        }

        var p = BuildParams(q);
        var pType = p.GetType();
        var firstTagPattern = (string)pType.GetProperty("FirstTagPattern")!.GetValue(p)!;
        var inTagPattern = (string)pType.GetProperty("InTagPattern")!.GetValue(p)!;
        var outerTagPattern = (string)pType.GetProperty("OuterTagPattern")!.GetValue(p)!;

        foreach (var pattern in new[] { firstTagPattern, inTagPattern, outerTagPattern })
        {
            foreach (var a in await conn.QueryAsync<ArtistRow>(DapperArtistsByAlternateNamesLikeSql, new { Pattern = pattern }))
            {
                found.TryAdd(a.Id, a);
            }
        }

        return await AttachAlbumsAsync(conn, found.Values.ToList());
    }

    private static async Task<List<ArtistWithAlbums>> AttachAlbumsAsync(DecentDbConnection conn, List<ArtistRow> artists)
    {
        if (artists.Count == 0)
        {
            return new List<ArtistWithAlbums>();
        }

        var artistIds = artists.Select(a => a.Id).Distinct().ToArray();

        // DecentDB currently does not support parameterized IN-lists (e.g. `WHERE x IN (@p1, @p2, ...)`).
        // Generate a small OR-chain instead.
        var albumsSql = DapperAlbumsSelectPrefix + string.Join(" OR ", artistIds.Select((_, i) => $"ArtistId = @ArtistId{i}"));
        var albumsParams = new DynamicParameters();
        for (var i = 0; i < artistIds.Length; i++)
        {
            albumsParams.Add($"ArtistId{i}", artistIds[i]);
        }

        var albums = (await conn.QueryAsync<AlbumRow>(albumsSql, albumsParams)).ToList();
        var byArtist = albums.GroupBy(a => a.ArtistId).ToDictionary(g => g.Key, g => g.ToList());

        var result = new List<ArtistWithAlbums>(artists.Count);
        foreach (var a in artists)
        {
            var row = new ArtistWithAlbums(a);
            if (byArtist.TryGetValue(a.Id, out var als))
            {
                row.Albums.AddRange(als);
            }
            result.Add(row);
        }

        return result;
    }

    private static async Task<HashSet<long>> RunDapperArtistIdsAsync(DecentDbConnection conn, SampleQuery q)
    {
        var ids = await conn.QueryAsync<long>(DapperArtistIdsSql, BuildParams(q));
        return ids.ToHashSet();
    }

    private static bool TryGetSqlite3Path(out string sqlite3)
    {
        sqlite3 = "sqlite3";
        try
        {
            var psi = new ProcessStartInfo
            {
                FileName = sqlite3,
                Arguments = "--version",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };
            using var p = Process.Start(psi);
            if (p is null) return false;
            p.WaitForExit(2000);
            return p.ExitCode == 0;
        }
        catch
        {
            return false;
        }
    }

    private static HashSet<long> QuerySqliteArtistIds(string sqliteDbPath, SampleQuery q)
    {
        var p = BuildParams(q);
        var pType = p.GetType();
        var nameNormalized = (string)pType.GetProperty("NameNormalized")!.GetValue(p)!;
        var musicBrainzId = (string?)pType.GetProperty("MusicBrainzId")!.GetValue(p);
        var spotifyId = (string?)pType.GetProperty("SpotifyId")!.GetValue(p);
        var firstTagPattern = (string)pType.GetProperty("FirstTagPattern")!.GetValue(p)!;
        var inTagPattern = (string)pType.GetProperty("InTagPattern")!.GetValue(p)!;
        var outerTagPattern = (string)pType.GetProperty("OuterTagPattern")!.GetValue(p)!;

        static string Q(string s) => "'" + s.Replace("'", "''") + "'";
        static string Qn(string? s) => s is null ? "NULL" : Q(s);

        // Keep this SQL text in sync with DapperArtistIdsSql.
        var sql = $@"
SELECT DISTINCT a.Id
FROM Artists a
WHERE
  a.NameNormalized = {Q(nameNormalized)}
  OR (a.MusicBrainzId IS NOT NULL AND {Qn(musicBrainzId)} IS NOT NULL AND a.MusicBrainzId = {Qn(musicBrainzId)})
  OR (a.AlternateNames IS NOT NULL AND (
        a.AlternateNames LIKE {Q(firstTagPattern)}
        OR a.AlternateNames LIKE {Q(inTagPattern)}
        OR a.AlternateNames LIKE {Q(outerTagPattern)}
  ))
  OR (a.SpotifyId IS NOT NULL AND {Qn(spotifyId)} IS NOT NULL AND a.SpotifyId = {Qn(spotifyId)})
";

        var psi = new ProcessStartInfo
        {
            FileName = "sqlite3",
            Arguments = $"\"{sqliteDbPath}\" \"{sql.Replace("\n", " ").Replace("\r", " ")}\"",
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };

        using var proc = Process.Start(psi);
        if (proc is null) throw new InvalidOperationException("Failed to start sqlite3");
        var output = proc.StandardOutput.ReadToEnd();
        var err = proc.StandardError.ReadToEnd();
        proc.WaitForExit();
        if (proc.ExitCode != 0)
        {
            throw new InvalidOperationException($"sqlite3 exited with {proc.ExitCode}: {err}");
        }

        var ids = new HashSet<long>();
        foreach (var line in output.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
        {
            if (long.TryParse(line.Trim(), out var id)) ids.Add(id);
        }

        return ids;
    }

    private static async Task<List<ArtistWithAlbums>> RunMicroOrmAsync(DecentDbContext ctx, SampleQuery q)
    {
        var (firstTag, inTag, outerTag) = BuildTags(q.NameNormalized);
        var artists = ctx.Set<ArtistRow>();

        // This mirrors the predicate in Melodee's DoSearchAsync.
        var foundArtists = await artists
            .Where(x => x.NameNormalized == q.NameNormalized
                        || x.MusicBrainzId == q.MusicBrainzId
                        || (x.AlternateNames != null && (
                                x.AlternateNames.Contains(firstTag)
                                || x.AlternateNames.Contains(inTag)
                                || x.AlternateNames.Contains(outerTag)
                            ))
                        || x.SpotifyId == q.SpotifyId
            )
            .ToListAsync();

        if (foundArtists.Count == 0)
        {
            return new List<ArtistWithAlbums>();
        }

        var ids = foundArtists.Select(a => a.Id).Distinct().ToArray();

        // DecentDB currently does not support parameterized IN-lists, and MicroORM translates
        // `ids.Contains(x.ArtistId)` into an IN expression. Fetch per-artist instead.
        var allAlbums = new List<AlbumRow>();
        var albumsSet = ctx.Set<AlbumRow>();
        foreach (var id in ids)
        {
            var perArtist = await albumsSet.Where(a => a.ArtistId == id).ToListAsync();
            if (perArtist.Count > 0) allAlbums.AddRange(perArtist);
        }

        var byArtist = allAlbums.GroupBy(a => a.ArtistId).ToDictionary(g => g.Key, g => g.ToList());

        var result = new List<ArtistWithAlbums>(foundArtists.Count);
        foreach (var a in foundArtists)
        {
            var row = new ArtistWithAlbums(a);
            if (byArtist.TryGetValue(a.Id, out var als))
            {
                row.Albums.AddRange(als);
            }
            result.Add(row);
        }

        return result;
    }

    private static async Task<List<ArtistRow>> RunMicroOrmIndexFriendlyArtistsAsync(DecentDbContext ctx, SampleQuery q)
    {
        // Same predicate as Melodee, but executed as separate queries so the engine can use
        // the relevant index for each branch instead of falling back to a table scan for a big OR.
        var found = new Dictionary<long, ArtistRow>();
        var artists = ctx.Set<ArtistRow>();

        foreach (var a in await artists.Where(x => x.NameNormalized == q.NameNormalized).ToListAsync())
        {
            found.TryAdd(a.Id, a);
        }

        if (!string.IsNullOrWhiteSpace(q.MusicBrainzId))
        {
            foreach (var a in await artists.Where(x => x.MusicBrainzId == q.MusicBrainzId).ToListAsync())
            {
                found.TryAdd(a.Id, a);
            }
        }

        if (!string.IsNullOrWhiteSpace(q.SpotifyId))
        {
            foreach (var a in await artists.Where(x => x.SpotifyId == q.SpotifyId).ToListAsync())
            {
                found.TryAdd(a.Id, a);
            }
        }

        var (firstTag, inTag, outerTag) = BuildTags(q.NameNormalized);
        foreach (var a in await artists.Where(x => x.AlternateNames != null && x.AlternateNames.Contains(firstTag)).ToListAsync())
        {
            found.TryAdd(a.Id, a);
        }

        foreach (var a in await artists.Where(x => x.AlternateNames != null && x.AlternateNames.Contains(inTag)).ToListAsync())
        {
            found.TryAdd(a.Id, a);
        }

        foreach (var a in await artists.Where(x => x.AlternateNames != null && x.AlternateNames.Contains(outerTag)).ToListAsync())
        {
            found.TryAdd(a.Id, a);
        }

        return found.Values.ToList();
    }

    private static async Task<List<ArtistWithAlbums>> RunMicroOrmIndexFriendlyAsync(DecentDbConnection conn, DecentDbContext ctx, SampleQuery q)
    {
        var artists = await RunMicroOrmIndexFriendlyArtistsAsync(ctx, q);
        return await AttachAlbumsAsync(conn, artists);
    }

    private static double MeasureMs(Action action)
    {
        var start = Stopwatch.GetTimestamp();
        action();
        return Stopwatch.GetElapsedTime(start).TotalMilliseconds;
    }

    private static (double p50, double p95) Percentiles(IReadOnlyList<double> sorted)
    {
        if (sorted.Count == 0) return (0, 0);

        static double At(IReadOnlyList<double> s, int pct)
        {
            var idx = (int)Math.Round((pct / 100.0) * (s.Count - 1));
            idx = Math.Clamp(idx, 0, s.Count - 1);
            return s[idx];
        }

        return (At(sorted, 50), At(sorted, 95));
    }

    [Fact]
    public async Task MelodeeArtistSearch_Dapper_ArtistIdsOnly_CanExecuteSampleQueries()
    {
        if (!TryGetExternalPath("MELODEE_ARTIST_DDB", "Melodee-converted .ddb", out var decentDbPath, out var reason))
        {
            _output.WriteLine($"SKIP: {reason}");
            return;
        }
        if (!TryStageDbForReadOnly(decentDbPath, out var staged))
        {
            _output.WriteLine($"SKIP: failed to stage DecentDB file: {decentDbPath}");
            return;
        }

        try
        {
            using var conn = new DecentDbConnection($"Data Source={staged}");
            conn.Open();

            foreach (var sample in Samples)
            {
                var ids = await RunDapperArtistIdsAsync(conn, sample);
                _output.WriteLine($"DapperIds [{sample.DisplayName}] => artists={ids.Count}");
            }
        }
        finally
        {
            TryDelete(staged);
            TryDelete(staged + "-wal");
        }
    }

    [Fact]
    public void MelodeeArtistSearch_AdoNet_JoinQuery_CanStreamRows_NoCrash()
    {
        if (!TryGetExternalPath("MELODEE_ARTIST_DDB", "Melodee-converted .ddb", out var decentDbPath, out var reason))
        {
            _output.WriteLine($"SKIP: {reason}");
            return;
        }
        if (!TryStageDbForReadOnly(decentDbPath, out var staged))
        {
            _output.WriteLine($"SKIP: failed to stage DecentDB file: {decentDbPath}");
            return;
        }

        try
        {
            using var conn = new DecentDbConnection($"Data Source={staged}");
            conn.Open();

            var sample = Samples.Last(s => s.NameNormalized == "PINKFLOYD");

            using var cmd = conn.CreateCommand();
            cmd.CommandText = AdoNetJoinSmokeSql;
            var p = cmd.CreateParameter();
            p.ParameterName = "@NameNormalized";
            p.Value = sample.NameNormalized;
            cmd.Parameters.Add(p);

            using var reader = cmd.ExecuteReader();
            long rows = 0;
            while (reader.Read())
            {
                // Touch a mix of types.
                _ = reader.GetInt64(0);
                _ = reader.GetString(1);
                _ = reader.GetString(2);

                if (!reader.IsDBNull(3))
                {
                    _ = reader.GetInt64(3);
                    _ = reader.GetString(4);
                    _ = reader.GetInt64(5);
                }

                rows++;
            }

            _output.WriteLine($"ADO.NET join rows={rows}");
            Assert.True(rows > 0);
        }
        finally
        {
            TryDelete(staged);
            TryDelete(staged + "-wal");
        }
    }

    [Fact]
    public void MelodeeArtistSearch_AdoNet_FullSelect_CanReadValues_NoCrash()
    {
        if (!TryGetExternalPath("MELODEE_ARTIST_DDB", "Melodee-converted .ddb", out var decentDbPath, out var reason))
        {
            _output.WriteLine($"SKIP: {reason}");
            return;
        }
        if (!TryStageDbForReadOnly(decentDbPath, out var staged))
        {
            _output.WriteLine($"SKIP: failed to stage DecentDB file: {decentDbPath}");
            return;
        }

        try
        {
            using var conn = new DecentDbConnection($"Data Source={staged}");
            conn.Open();

            var sample = Samples.Last(s => s.NameNormalized == "PINKFLOYD");

            using var cmd = conn.CreateCommand();
            cmd.CommandText = JoinSqlFull;

            var p = cmd.CreateParameter();
            p.ParameterName = "@NameNormalized";
            p.Value = sample.NameNormalized;
            cmd.Parameters.Add(p);

            var pMb = cmd.CreateParameter();
            pMb.ParameterName = "@MusicBrainzId";
            pMb.Value = DBNull.Value;
            cmd.Parameters.Add(pMb);

            var pSp = cmd.CreateParameter();
            pSp.ParameterName = "@SpotifyId";
            pSp.Value = DBNull.Value;
            cmd.Parameters.Add(pSp);

            // Patterns won't match for this sample (NameNormalized path hits first), but must be bound.
            var (firstTag, inTag, outerTag) = BuildTags(sample.NameNormalized);

            var pF = cmd.CreateParameter();
            pF.ParameterName = "@FirstTagPattern";
            pF.Value = $"%{firstTag}%";
            cmd.Parameters.Add(pF);

            var pI = cmd.CreateParameter();
            pI.ParameterName = "@InTagPattern";
            pI.Value = $"%{inTag}%";
            cmd.Parameters.Add(pI);

            var pO = cmd.CreateParameter();
            pO.ParameterName = "@OuterTagPattern";
            pO.Value = $"%{outerTag}%";
            cmd.Parameters.Add(pO);

            using var reader = cmd.ExecuteReader();
            var fieldCount = reader.FieldCount;

            for (var i = 0; i < fieldCount; i++)
            {
                _ = reader.GetName(i);
            }
            long rows = 0;

            while (reader.Read())
            {
                for (var i = 0; i < fieldCount; i++)
                {
                    _ = reader.GetValue(i);
                }

                rows++;
                if (rows >= 500) break; // enough to exercise repeated stepping
            }

            _output.WriteLine($"ADO.NET full-select rowsRead={rows} fieldCount={fieldCount}");
            Assert.True(rows > 0);
            Assert.True(fieldCount >= 10);
        }
        finally
        {
            TryDelete(staged);
            TryDelete(staged + "-wal");
        }
    }

    [Fact]
    public async Task MelodeeArtistSearch_Dapper_CanExecuteSampleQueries()
    {
        if (!TryGetExternalPath("MELODEE_ARTIST_DDB", "Melodee-converted .ddb", out var decentDbPath, out var reason))
        {
            _output.WriteLine($"SKIP: {reason}");
            return;
        }
        if (!TryStageDbForReadOnly(decentDbPath, out var staged))
        {
            _output.WriteLine($"SKIP: failed to stage DecentDB file: {decentDbPath}");
            return;
        }

        try
        {
            using var conn = new DecentDbConnection($"Data Source={staged}");
            conn.Open();

            foreach (var sample in Samples)
            {
                var rows = await RunDapperAsync(conn, sample);
                _output.WriteLine($"Dapper [{sample.DisplayName}] => artists={rows.Count} albums={rows.Sum(r => r.Albums.Count)}");
            }
        }
        finally
        {
            TryDelete(staged);
            TryDelete(staged + "-wal");
        }
    }

    [Fact]
    public async Task MelodeeArtistSearch_MicroOrm_CanExecuteSampleQueries()
    {
        if (!TryGetExternalPath("MELODEE_ARTIST_DDB", "Melodee-converted .ddb", out var decentDbPath, out var reason))
        {
            _output.WriteLine($"SKIP: {reason}");
            return;
        }
        if (!TryStageDbForReadOnly(decentDbPath, out var staged))
        {
            _output.WriteLine($"SKIP: failed to stage DecentDB file: {decentDbPath}");
            return;
        }

        try
        {
            using var ctx = new DecentDbContext(staged, pooling: true);

            string? lastSql = null;
            ctx.SqlExecuting += (_, e) => { lastSql = e.Sql; };

            foreach (var sample in Samples)
            {
                try
                {
                    var rows = await RunMicroOrmAsync(ctx, sample);
                    _output.WriteLine($"MicroORM [{sample.DisplayName}] => artists={rows.Count} albums={rows.Sum(r => r.Albums.Count)}");
                }
                catch (Exception)
                {
                    _output.WriteLine($"MicroORM failed for '{sample.DisplayName}'. Last SQL: {lastSql}");
                    throw;
                }
            }
        }
        finally
        {
            TryDelete(staged);
            TryDelete(staged + "-wal");
        }
    }

    [Fact]
    public async Task MelodeeArtistSearch_Dapper_PerfSamples_PrintsPercentiles_OptionalAssertion()
    {
        if (!TryGetExternalPath("MELODEE_ARTIST_DDB", "Melodee-converted .ddb", out var decentDbPath, out var reason))
        {
            _output.WriteLine($"SKIP: {reason}");
            return;
        }
        if (!TryStageDbForReadOnly(decentDbPath, out var staged))
        {
            _output.WriteLine($"SKIP: failed to stage DecentDB file: {decentDbPath}");
            return;
        }

        var strict = (Environment.GetEnvironmentVariable("DECENTDB_ASSERT_MELODEE_PERF")?.Trim() ?? "") == "1";

        try
        {
            using var conn = new DecentDbConnection($"Data Source={staged}");
            conn.Open();

            // Makes AlternateNames substring search indexable (LIKE '%...%').
            // With the engine-level OR-to-UnionDistinct rewrite, this also enables a fully index-driven plan
            // for the monolithic Melodee-style OR predicate.
            EnsureAlternateNamesTrigramIndex(conn);

            const int warmup = 3;
            const int iterations = 15;

            foreach (var sample in Samples)
            {
                // Warmup
                for (var i = 0; i < warmup; i++)
                {
                    _ = await RunDapperAsync(conn, sample);
                }

                var samples = new List<double>(iterations);
                for (var i = 0; i < iterations; i++)
                {
                    var ms = MeasureMs(() => RunDapperAsync(conn, sample).GetAwaiter().GetResult());
                    samples.Add(ms);
                }

                samples.Sort();
                var (p50, p95) = Percentiles(samples);
                Console.WriteLine($"Perf [{sample.DisplayName}] p50={p50:0.000}ms p95={p95:0.000}ms (Melodee log {sample.LoggedElapsedMs:0.000}ms)");

                if (strict)
                {
                    Assert.True(p50 <= sample.LoggedElapsedMs, $"p50 {p50:0.000}ms exceeded Melodee log {sample.LoggedElapsedMs:0.000}ms for '{sample.DisplayName}'");
                }
            }
        }
        finally
        {
            TryDelete(staged);
            TryDelete(staged + "-wal");
        }
    }

    [Fact]
    public async Task MelodeeArtistSearch_MicroOrm_PerfSamples_PrintsPercentiles_OptionalAssertion()
    {
        if (!TryGetExternalPath("MELODEE_ARTIST_DDB", "Melodee-converted .ddb", out var decentDbPath, out var reason))
        {
            _output.WriteLine($"SKIP: {reason}");
            return;
        }
        if (!TryStageDbForReadOnly(decentDbPath, out var staged))
        {
            _output.WriteLine($"SKIP: failed to stage DecentDB file: {decentDbPath}");
            return;
        }

        var strict = (Environment.GetEnvironmentVariable("DECENTDB_ASSERT_MELODEE_PERF")?.Trim() ?? "") == "1";

        try
        {
            // Makes AlternateNames substring search indexable (LIKE '%...%'), which is required for
            // the engine to turn the full OR predicate into a UnionDistinct of index seeks.
            using (var conn = new DecentDbConnection($"Data Source={staged}"))
            {
                conn.Open();
                EnsureAlternateNamesTrigramIndex(conn);
            }

            using var ctx = new DecentDbContext(staged, pooling: true);

            const int warmup = 3;
            const int iterations = 15;

            foreach (var sample in Samples)
            {
                // Warmup
                for (var i = 0; i < warmup; i++)
                {
                    _ = await RunMicroOrmAsync(ctx, sample);
                }

                var samples = new List<double>(iterations);
                for (var i = 0; i < iterations; i++)
                {
                    var ms = MeasureMs(() => RunMicroOrmAsync(ctx, sample).GetAwaiter().GetResult());
                    samples.Add(ms);
                }

                samples.Sort();
                var (p50, p95) = Percentiles(samples);
                Console.WriteLine($"PerfMicroOrm [{sample.DisplayName}] p50={p50:0.000}ms p95={p95:0.000}ms (Melodee log {sample.LoggedElapsedMs:0.000}ms)");

                if (strict)
                {
                    Assert.True(p50 <= sample.LoggedElapsedMs, $"p50 {p50:0.000}ms exceeded Melodee log {sample.LoggedElapsedMs:0.000}ms for '{sample.DisplayName}'");
                }
            }
        }
        finally
        {
            TryDelete(staged);
            TryDelete(staged + "-wal");
        }
    }

    [Fact]
    public async Task MelodeeArtistSearch_Dapper_PerfSamples_IndexFriendly_PrintsPercentiles_OptionalAssertion()
    {
        if (!TryGetExternalPath("MELODEE_ARTIST_DDB", "Melodee-converted .ddb", out var decentDbPath, out var reason))
        {
            _output.WriteLine($"SKIP: {reason}");
            return;
        }
        if (!TryStageDbForReadOnly(decentDbPath, out var staged))
        {
            _output.WriteLine($"SKIP: failed to stage DecentDB file: {decentDbPath}");
            return;
        }

        var strict = (Environment.GetEnvironmentVariable("DECENTDB_ASSERT_MELODEE_PERF")?.Trim() ?? "") == "1";

        try
        {
            using var conn = new DecentDbConnection($"Data Source={staged}");
            conn.Open();

            // Makes AlternateNames substring search indexable (LIKE '%...%').
            EnsureAlternateNamesTrigramIndex(conn);

            const int warmup = 3;
            const int iterations = 15;

            foreach (var sample in Samples)
            {
                for (var i = 0; i < warmup; i++)
                {
                    _ = await RunDapperIndexFriendlyAsync(conn, sample);
                }

                var samples = new List<double>(iterations);
                for (var i = 0; i < iterations; i++)
                {
                    var ms = MeasureMs(() => RunDapperIndexFriendlyAsync(conn, sample).GetAwaiter().GetResult());
                    samples.Add(ms);
                }

                samples.Sort();
                var (p50, p95) = Percentiles(samples);
                Console.WriteLine($"PerfIndexFriendly [{sample.DisplayName}] p50={p50:0.000}ms p95={p95:0.000}ms (Melodee log {sample.LoggedElapsedMs:0.000}ms)");

                if (strict)
                {
                    Assert.True(p50 <= sample.LoggedElapsedMs, $"p50 {p50:0.000}ms exceeded Melodee log {sample.LoggedElapsedMs:0.000}ms for '{sample.DisplayName}'");
                }
            }
        }
        finally
        {
            TryDelete(staged);
            TryDelete(staged + "-wal");
        }
    }

    [Fact]
    public async Task MelodeeArtistSearch_Dapper_MatchesSqlite_WhenEnabled()
    {
        var compare = (Environment.GetEnvironmentVariable("DECENTDB_COMPARE_MELODEE_SQLITE")?.Trim() ?? "") == "1";
        if (!compare)
        {
            _output.WriteLine("SKIP: set DECENTDB_COMPARE_MELODEE_SQLITE=1 to enable SQLite comparisons");
            return;
        }

        if (!TryGetSqlite3Path(out _))
        {
            _output.WriteLine("SKIP: sqlite3 not found on PATH");
            return;
        }

        if (!TryGetExternalPath("MELODEE_ARTIST_SQLITE", "Melodee SQLite .db", out var sqlitePath, out var sqliteReason))
        {
            _output.WriteLine($"SKIP: {sqliteReason}");
            return;
        }

        if (!TryGetExternalPath("MELODEE_ARTIST_DDB", "Melodee-converted .ddb", out var decentDbPath, out var ddbReason))
        {
            _output.WriteLine($"SKIP: {ddbReason}");
            return;
        }
        if (!TryStageDbForReadOnly(decentDbPath, out var staged))
        {
            _output.WriteLine($"SKIP: failed to stage DecentDB file: {decentDbPath}");
            return;
        }

        try
        {
            using var conn = new DecentDbConnection($"Data Source={staged}");
            conn.Open();

            foreach (var sample in Samples)
            {
                var decentIds = await RunDapperArtistIdsAsync(conn, sample);
                var sqliteIds = QuerySqliteArtistIds(sqlitePath, sample);

                _output.WriteLine($"Compare [{sample.DisplayName}] DecentDB={decentIds.Count} SQLite={sqliteIds.Count}");
                Assert.True(decentIds.SetEquals(sqliteIds), $"Mismatched artist id set for '{sample.DisplayName}'");
            }
        }
        finally
        {
            TryDelete(staged);
            TryDelete(staged + "-wal");
        }
    }

    [Fact]
    public async Task MelodeeArtistSearch_MicroOrm_PerfSamples_IndexFriendly_PrintsPercentiles_OptionalAssertion()
    {
        if (!TryGetExternalPath("MELODEE_ARTIST_DDB", "Melodee-converted .ddb", out var decentDbPath, out var reason))
        {
            _output.WriteLine($"SKIP: {reason}");
            return;
        }
        if (!TryStageDbForReadOnly(decentDbPath, out var staged))
        {
            _output.WriteLine($"SKIP: failed to stage DecentDB file: {decentDbPath}");
            return;
        }

        var strict = (Environment.GetEnvironmentVariable("DECENTDB_ASSERT_MELODEE_PERF")?.Trim() ?? "") == "1";

        try
        {
            using var conn = new DecentDbConnection($"Data Source={staged}");
            conn.Open();
            EnsureAlternateNamesTrigramIndex(conn);

            using var ctx = new DecentDbContext(staged, pooling: true);

            const int warmup = 3;
            const int iterations = 15;

            foreach (var sample in Samples)
            {
                for (var i = 0; i < warmup; i++)
                {
                    _ = await RunMicroOrmIndexFriendlyAsync(conn, ctx, sample);
                }

                var samples = new List<double>(iterations);
                for (var i = 0; i < iterations; i++)
                {
                    var ms = MeasureMs(() => RunMicroOrmIndexFriendlyAsync(conn, ctx, sample).GetAwaiter().GetResult());
                    samples.Add(ms);
                }

                samples.Sort();
                var (p50, p95) = Percentiles(samples);
                Console.WriteLine($"PerfMicroOrmIndexFriendly [{sample.DisplayName}] p50={p50:0.000}ms p95={p95:0.000}ms (Melodee log {sample.LoggedElapsedMs:0.000}ms)");

                if (strict)
                {
                    Assert.True(p50 <= sample.LoggedElapsedMs, $"p50 {p50:0.000}ms exceeded Melodee log {sample.LoggedElapsedMs:0.000}ms for '{sample.DisplayName}'");
                }
            }
        }
        finally
        {
            TryDelete(staged);
            TryDelete(staged + "-wal");
        }
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path)) File.Delete(path);
        }
        catch
        {
            // best-effort cleanup
        }
    }
}
