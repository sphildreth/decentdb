using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

/// <summary>
/// N14: Tests for the correlated aggregate rewrite infrastructure.
/// </summary>
public sealed class CorrelatedAggregateRewriteTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_corr_agg_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path)) File.Delete(path);
    }

    private sealed class Artist
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private sealed class Song
    {
        public int Id { get; set; }
        public int ArtistId { get; set; }
        public string Title { get; set; } = string.Empty;
    }

    private sealed class TestContext : DbContext
    {
        public TestContext(DbContextOptions<TestContext> options) : base(options) { }
        public DbSet<Artist> Artists => Set<Artist>();
        public DbSet<Song> Songs => Set<Song>();
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Artist>(e =>
            {
                e.ToTable("artists");
                e.HasKey(x => x.Id);
                e.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
                e.Property(x => x.Name).HasColumnName("name");
            });
            modelBuilder.Entity<Song>(e =>
            {
                e.ToTable("songs");
                e.HasKey(x => x.Id);
                e.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
                e.Property(x => x.ArtistId).HasColumnName("artist_id");
                e.Property(x => x.Title).HasColumnName("title");
            });
        }
    }

    [Fact]
    public void DisableCorrelatedAggregateRewrite_OptOut()
    {
        // Verify that the opt-out option can be set without errors
        var options = new DbContextOptionsBuilder<TestContext>()
            .UseDecentDB(_dbPath, o => o.DisableCorrelatedAggregateRewrite())
            .Options;

        using var ctx = new TestContext(options);
        ctx.Database.EnsureCreated();
        // If we get here without exception, the opt-out infrastructure works
        Assert.True(true);
    }

    [Fact]
    public void CorrelatedCount_ProducesCorrectResults()
    {
        var options = new DbContextOptionsBuilder<TestContext>()
            .UseDecentDB(_dbPath)
            .Options;

        using (var ctx = new TestContext(options))
        {
            ctx.Database.EnsureCreated();
            ctx.Artists.AddRange(
                new Artist { Name = "A" },
                new Artist { Name = "B" });
            ctx.SaveChanges();

            var artistA = ctx.Artists.First(a => a.Name == "A");
            var artistB = ctx.Artists.First(a => a.Name == "B");

            ctx.Songs.AddRange(
                new Song { ArtistId = artistA.Id, Title = "Song1" },
                new Song { ArtistId = artistA.Id, Title = "Song2" },
                new Song { ArtistId = artistB.Id, Title = "Song3" });
            ctx.SaveChanges();
        }

        using (var ctx = new TestContext(options))
        {
            // Correlated count query
            var results = ctx.Artists
                .Select(a => new { a.Name, SongCount = ctx.Songs.Count(s => s.ArtistId == a.Id) })
                .OrderBy(x => x.Name)
                .ToList();

            Assert.Equal(2, results.Count);
            Assert.Equal("A", results[0].Name);
            Assert.Equal(2, results[0].SongCount);
            Assert.Equal("B", results[1].Name);
            Assert.Equal(1, results[1].SongCount);
        }
    }
}
