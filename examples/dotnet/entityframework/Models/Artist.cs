namespace EntityFrameworkDemo.Models;

public class Artist
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string? Bio { get; set; }
    public string? Genre { get; set; }
    public string? Country { get; set; }
    public int? FormedYear { get; set; }
    public string? Website { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? ModifiedAt { get; set; }

    public virtual ICollection<Album> Albums { get; set; } = new List<Album>();

    public int GetTotalAlbumCount()
    {
        return Albums?.Count ?? 0;
    }

    public int GetTotalTrackCount()
    {
        return Albums?.Sum(a => a.Tracks?.Count ?? 0) ?? 0;
    }

    public TimeSpan GetTotalDuration()
    {
        var totalSeconds = Albums?.Sum(a => a.TotalDurationSeconds) ?? 0;
        return TimeSpan.FromSeconds(totalSeconds);
    }
}
