namespace EntityFrameworkDemo.Models;

public class GenreCount
{
    public string Genre { get; set; } = string.Empty;
    public int Count { get; set; }
}

public class ArtistSummary
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Genre { get; set; } = string.Empty;
    public string Country { get; set; } = string.Empty;
    public int AlbumCount { get; set; }
}

public class ArtistEra
{
    public string Name { get; set; } = string.Empty;
    public int FormedYear { get; set; }
    public string Era { get; set; } = string.Empty;
}

public class AlbumsByDecade
{
    public int Decade { get; set; }
    public int Count { get; set; }
    public double AvgTracks { get; set; }
}

public class TrackStats
{
    public string Genre { get; set; } = string.Empty;
    public int TrackCount { get; set; }
    public double AvgDuration { get; set; }
    public long TotalPlays { get; set; }
}

public class EventSummary
{
    public string Name { get; set; } = string.Empty;
    public string ArtistName { get; set; } = string.Empty;
    public string Venue { get; set; } = string.Empty;
    public long EventDateDays { get; set; }
    public int TicketsSold { get; set; }
    public double Revenue { get; set; }
}

public class VenueStats
{
    public string Venue { get; set; } = string.Empty;
    public int EventCount { get; set; }
    public int TotalTicketsSold { get; set; }
    public double AvgTicketPrice { get; set; }
}
