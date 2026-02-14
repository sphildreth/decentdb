using EntityFrameworkDemo.Data;
using EntityFrameworkDemo.Models;
using Microsoft.EntityFrameworkCore;
using NodaTime;

namespace EntityFrameworkDemo.Services;

public class EventService
{
    private readonly IDbContextFactory<MusicDbContext> _contextFactory;

    public EventService(IDbContextFactory<MusicDbContext> contextFactory)
    {
        _contextFactory = contextFactory;
    }

    // ── Basic CRUD ──

    public async Task CreateManyAsync(List<Event> events)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        context.Events.AddRange(events);
        await context.SaveChangesAsync();
    }

    public async Task<int> GetTotalCountAsync()
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Events.CountAsync();
    }

    public async Task<List<Event>> GetAllAsync(int page, int pageSize)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Events
            .OrderBy(e => e.EventDate)
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .ToListAsync();
    }

    public async Task<Event?> GetByIdWithArtistAsync(int id)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Events
            .Include(e => e.Artist)
            .FirstOrDefaultAsync(e => e.Id == id);
    }

    // ── NodaTime Instant Queries ──

    /// <summary>
    /// Filter events by Instant range — demonstrates Instant comparison in LINQ.
    /// </summary>
    public async Task<List<Event>> GetByTicketSaleRangeAsync(Instant from, Instant to)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Events
            .Where(e => e.TicketSaleStart >= from && e.TicketSaleStart <= to)
            .OrderBy(e => e.TicketSaleStart)
            .ToListAsync();
    }

    /// <summary>
    /// Get events where doors have been set (non-null Instant).
    /// </summary>
    public async Task<int> CountWithDoorsOpenSetAsync()
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Events
            .Where(e => e.DoorsOpen != null)
            .CountAsync();
    }

    // ── NodaTime LocalDate Queries ──

    /// <summary>
    /// Filter events by LocalDate range — demonstrates LocalDate comparison in LINQ.
    /// </summary>
    public async Task<List<Event>> GetByDateRangeAsync(LocalDate from, LocalDate to)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Events
            .Where(e => e.EventDate >= from && e.EventDate <= to)
            .OrderBy(e => e.EventDate)
            .ToListAsync();
    }

    /// <summary>
    /// Get the earliest event date — demonstrates Min on LocalDate.
    /// </summary>
    public async Task<long> GetEarliestEventDateAsync()
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        // LocalDate maps to long (days since epoch), so Min returns long
        return await context.Events.MinAsync(e => (long)(object)e.EventDate);
    }

    // ── Mixed DateTime + NodaTime Queries ──

    /// <summary>
    /// Project a DTO that includes both NodaTime (EventDate as long) and DateTime (CreatedAt) values.
    /// Shows both type systems coexisting in the same projection.
    /// </summary>
    public async Task<List<EventSummary>> GetEventSummariesAsync(int limit)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Events
            .Include(e => e.Artist)
            .OrderByDescending(e => e.TicketsSold)
            .Take(limit)
            .Select(e => new EventSummary
            {
                Name = e.Name,
                ArtistName = e.Artist.Name,
                Venue = e.Venue ?? "",
                EventDateDays = (long)(object)e.EventDate,
                TicketsSold = e.TicketsSold,
                Revenue = e.TicketsSold * e.TicketPrice
            })
            .ToListAsync();
    }

    // ── GroupBy / Aggregation ──

    /// <summary>
    /// Group events by venue — demonstrates aggregation with NodaTime entities.
    /// </summary>
    public async Task<List<VenueStats>> GetVenueStatsAsync()
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Events
            .GroupBy(e => e.Venue)
            .Select(g => new VenueStats
            {
                Venue = g.Key ?? "Unknown",
                EventCount = g.Count(),
                TotalTicketsSold = g.Sum(e => e.TicketsSold),
                AvgTicketPrice = g.Average(e => e.TicketPrice)
            })
            .OrderByDescending(v => v.EventCount)
            .ToListAsync();
    }

    /// <summary>
    /// Get top artists by total ticket revenue — join across NodaTime and DateTime entities.
    /// </summary>
    public async Task<List<(string ArtistName, double TotalRevenue, int EventCount)>> GetTopArtistsByRevenueAsync(int limit)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        var results = await context.Events
            .Include(e => e.Artist)
            .GroupBy(e => e.Artist.Name)
            .Select(g => new
            {
                ArtistName = g.Key,
                TotalRevenue = g.Sum(e => e.TicketsSold * e.TicketPrice),
                EventCount = g.Count()
            })
            .OrderByDescending(x => x.TotalRevenue)
            .Take(limit)
            .ToListAsync();

        return results.Select(r => (r.ArtistName, r.TotalRevenue, r.EventCount)).ToList();
    }

    // ── Ordering by NodaTime types ──

    /// <summary>
    /// Order events by Instant (ticket sale start) — demonstrates Instant ordering.
    /// </summary>
    public async Task<List<Event>> GetNextUpcomingSalesAsync(int limit)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Events
            .OrderBy(e => e.TicketSaleStart)
            .Take(limit)
            .ToListAsync();
    }

    /// <summary>
    /// Get events for a specific artist, ordered by LocalDate.
    /// </summary>
    public async Task<List<Event>> GetByArtistAsync(int artistId)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Events
            .Where(e => e.ArtistId == artistId)
            .OrderBy(e => e.EventDate)
            .ToListAsync();
    }

    // ── Capacity / Utilization ──

    /// <summary>
    /// Get total tickets sold across all events.
    /// </summary>
    public async Task<long> GetTotalTicketsSoldAsync()
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Events.SumAsync(e => (long)e.TicketsSold);
    }

    /// <summary>
    /// Get average ticket price.
    /// </summary>
    public async Task<double> GetAverageTicketPriceAsync()
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Events.AverageAsync(e => e.TicketPrice);
    }

    /// <summary>
    /// Get distinct cities hosting events.
    /// </summary>
    public async Task<List<string>> GetDistinctCitiesAsync()
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Events
            .Where(e => e.City != null)
            .Select(e => e.City!)
            .Distinct()
            .OrderBy(c => c)
            .ToListAsync();
    }
}
