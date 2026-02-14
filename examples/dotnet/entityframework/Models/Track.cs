using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace EntityFrameworkDemo.Models;

[Table("tracks")]
public class Track
{
    [Key]
    [Column("id")]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }

    [Required]
    [Column("title")]
    [MaxLength(200)]
    public string Title { get; set; } = string.Empty;

    [Column("track_number")]
    public int TrackNumber { get; set; }

    [Column("duration_seconds")]
    public int DurationSeconds { get; set; }

    [Column("genre")]
    [MaxLength(50)]
    public string? Genre { get; set; }

    [Column("lyrics")]
    public string? Lyrics { get; set; }

    [Column("is_explicit")]
    public bool IsExplicit { get; set; }

    [Column("play_count")]
    public long PlayCount { get; set; }

    [Column("rating")]
    public double? Rating { get; set; }

    [Column("created_at")]
    public DateTime CreatedAt { get; set; }

    [Column("modified_at")]
    public DateTime? ModifiedAt { get; set; }

    [Column("album_id")]
    [ForeignKey(nameof(Album))]
    public int AlbumId { get; set; }

    public virtual Album Album { get; set; } = null!;

    public TimeSpan GetDuration()
    {
        return TimeSpan.FromSeconds(DurationSeconds);
    }

    public string GetFormattedDuration()
    {
        var duration = GetDuration();
        return $"{duration.Minutes:D2}:{duration.Seconds:D2}";
    }
}
