using EntityFrameworkDemo.Models;
using Microsoft.EntityFrameworkCore;

namespace EntityFrameworkDemo.Data;

public class MusicDbContext : DbContext
{
    public MusicDbContext(DbContextOptions<MusicDbContext> options)
        : base(options)
    {
    }

    public DbSet<Artist> Artists => Set<Artist>();
    public DbSet<Album> Albums => Set<Album>();
    public DbSet<Track> Tracks => Set<Track>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        ConfigureArtist(modelBuilder);
        ConfigureAlbum(modelBuilder);
        ConfigureTrack(modelBuilder);
    }

    private void ConfigureArtist(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Artist>(entity =>
        {
            entity.ToTable("artists");

            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id)
                .HasColumnName("id")
                .ValueGeneratedOnAdd();

            entity.Property(e => e.Name)
                .HasColumnName("name")
                .IsRequired()
                .HasMaxLength(200);

            entity.Property(e => e.Bio)
                .HasColumnName("bio");

            entity.Property(e => e.Genre)
                .HasColumnName("genre")
                .HasMaxLength(100);

            entity.Property(e => e.Country)
                .HasColumnName("country")
                .HasMaxLength(100);

            entity.Property(e => e.FormedYear)
                .HasColumnName("formed_year");

            entity.Property(e => e.Website)
                .HasColumnName("website")
                .HasMaxLength(500);

            entity.Property(e => e.CreatedAt)
                .HasColumnName("created_at")
                .IsRequired();

            entity.Property(e => e.ModifiedAt)
                .HasColumnName("modified_at");

            entity.HasIndex(e => e.Name)
                .HasDatabaseName("ix_artists_name");

            entity.HasIndex(e => e.Genre)
                .HasDatabaseName("ix_artists_genre");

            entity.HasMany(e => e.Albums)
                .WithOne(a => a.Artist)
                .HasForeignKey(a => a.ArtistId)
                .OnDelete(DeleteBehavior.Cascade);
        });
    }

    private void ConfigureAlbum(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Album>(entity =>
        {
            entity.ToTable("albums");

            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id)
                .HasColumnName("id")
                .ValueGeneratedOnAdd();

            entity.Property(e => e.Title)
                .HasColumnName("title")
                .IsRequired()
                .HasMaxLength(200);

            entity.Property(e => e.Description)
                .HasColumnName("description");

            entity.Property(e => e.ReleaseYear)
                .HasColumnName("release_year")
                .IsRequired();

            entity.Property(e => e.RecordLabel)
                .HasColumnName("record_label")
                .HasMaxLength(200);

            entity.Property(e => e.TotalTracks)
                .HasColumnName("total_tracks")
                .IsRequired();

            entity.Property(e => e.TotalDurationSeconds)
                .HasColumnName("total_duration_seconds")
                .IsRequired();

            entity.Property(e => e.CoverArtUrl)
                .HasColumnName("cover_art_url")
                .HasMaxLength(500);

            entity.Property(e => e.CreatedAt)
                .HasColumnName("created_at")
                .IsRequired();

            entity.Property(e => e.ModifiedAt)
                .HasColumnName("modified_at");

            entity.Property(e => e.ArtistId)
                .HasColumnName("artist_id")
                .IsRequired();

            entity.HasIndex(e => e.Title)
                .HasDatabaseName("ix_albums_title");

            entity.HasIndex(e => e.ReleaseYear)
                .HasDatabaseName("ix_albums_release_year");

            entity.HasIndex(e => e.ArtistId)
                .HasDatabaseName("ix_albums_artist_id");

            entity.HasMany(e => e.Tracks)
                .WithOne(t => t.Album)
                .HasForeignKey(t => t.AlbumId)
                .OnDelete(DeleteBehavior.Cascade);
        });
    }

    private void ConfigureTrack(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Track>(entity =>
        {
            entity.ToTable("tracks");

            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id)
                .HasColumnName("id")
                .ValueGeneratedOnAdd();

            entity.Property(e => e.Title)
                .HasColumnName("title")
                .IsRequired()
                .HasMaxLength(200);

            entity.Property(e => e.TrackNumber)
                .HasColumnName("track_number")
                .IsRequired();

            entity.Property(e => e.DurationSeconds)
                .HasColumnName("duration_seconds")
                .IsRequired();

            entity.Property(e => e.Genre)
                .HasColumnName("genre")
                .HasMaxLength(50);

            entity.Property(e => e.Lyrics)
                .HasColumnName("lyrics");

            entity.Property(e => e.IsExplicit)
                .HasColumnName("is_explicit")
                .IsRequired();

            entity.Property(e => e.PlayCount)
                .HasColumnName("play_count")
                .IsRequired();

            entity.Property(e => e.Rating)
                .HasColumnName("rating");

            entity.Property(e => e.CreatedAt)
                .HasColumnName("created_at")
                .IsRequired();

            entity.Property(e => e.ModifiedAt)
                .HasColumnName("modified_at");

            entity.Property(e => e.AlbumId)
                .HasColumnName("album_id")
                .IsRequired();

            entity.HasIndex(e => e.AlbumId)
                .HasDatabaseName("ix_tracks_album_id");

            entity.HasIndex(e => e.TrackNumber)
                .HasDatabaseName("ix_tracks_track_number");

            entity.HasIndex(e => e.DurationSeconds)
                .HasDatabaseName("ix_tracks_duration");

            entity.HasOne(e => e.Album)
                .WithMany(a => a.Tracks)
                .HasForeignKey(e => e.AlbumId)
                .OnDelete(DeleteBehavior.Cascade);
        });
    }
}
