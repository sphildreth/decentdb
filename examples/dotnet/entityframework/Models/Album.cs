namespace EntityFrameworkDemo.Models;

public class Album
{
    public int Id { get; set; }
    public string Title { get; set; } = string.Empty;
    public string? Description { get; set; }
    public int ReleaseYear { get; set; }
    public string? RecordLabel { get; set; }
    public int TotalTracks { get; set; }
    public int TotalDurationSeconds { get; set; }
    public string? CoverArtUrl { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? ModifiedAt { get; set; }
    public int ArtistId { get; set; }

    public virtual Artist Artist { get; set; } = null!;
    public virtual ICollection<Track> Tracks { get; set; } = new List<Track>();

    public TimeSpan GetTotalDuration()
    {
        return TimeSpan.FromSeconds(TotalDurationSeconds);
    }

    public string GetFormattedTotalDuration()
    {
        var duration = GetTotalDuration();
        if (duration.TotalHours >= 1)
        {
            return $"{duration.Hours}:{duration.Minutes:D2}:{duration.Seconds:D2}";
        }
        return $"{duration.Minutes:D2}:{duration.Seconds:D2}";
    }
}
