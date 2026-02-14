using NodaTime;

namespace EntityFrameworkDemo.Models;

/// <summary>
/// Demonstrates NodaTime types (Instant, LocalDate) alongside standard DateTime properties.
/// </summary>
public class Event
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string? Venue { get; set; }
    public string? City { get; set; }
    public string? Country { get; set; }

    // NodaTime types — stored as INTEGER (Unix milliseconds / days-since-epoch)
    public Instant TicketSaleStart { get; set; }
    public Instant? DoorsOpen { get; set; }
    public LocalDate EventDate { get; set; }

    // Standard DateTime — coexists with NodaTime in the same entity
    public DateTime CreatedAt { get; set; }

    public int CapacityTotal { get; set; }
    public int TicketsSold { get; set; }
    public double TicketPrice { get; set; }

    public int ArtistId { get; set; }
    public virtual Artist Artist { get; set; } = null!;
}
