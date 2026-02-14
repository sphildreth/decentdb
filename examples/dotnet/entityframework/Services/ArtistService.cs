using EntityFrameworkDemo.Data;
using EntityFrameworkDemo.Models;
using Microsoft.EntityFrameworkCore;

namespace EntityFrameworkDemo.Services;

public class ArtistService
{
    private readonly IDbContextFactory<MusicDbContext> _contextFactory;

    public ArtistService(IDbContextFactory<MusicDbContext> contextFactory)
    {
        _contextFactory = contextFactory;
    }

    public async Task<Artist?> GetByIdAsync(int id)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Artists
            .AsNoTracking()
            .FirstOrDefaultAsync(a => a.Id == id);
    }

    public async Task<Artist?> GetByIdWithAlbumsAsync(int id)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Artists
            .AsNoTracking()
            .Include(a => a.Albums)
            .FirstOrDefaultAsync(a => a.Id == id);
    }

    public async Task<List<Artist>> GetAllAsync(int page = 1, int pageSize = 20)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Artists
            .AsNoTracking()
            .OrderBy(a => a.Name)
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .ToListAsync();
    }

    public async Task<List<Artist>> GetByGenreAsync(string genre)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Artists
            .AsNoTracking()
            .Where(a => a.Genre != null && a.Genre.Contains(genre))
            .OrderBy(a => a.Name)
            .ToListAsync();
    }

    public async Task<List<Artist>> GetByCountryAsync(string country)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Artists
            .AsNoTracking()
            .Where(a => a.Country == country)
            .OrderBy(a => a.Name)
            .ToListAsync();
    }

    public async Task<List<Artist>> SearchByNameAsync(string searchTerm)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Artists
            .AsNoTracking()
            .Where(a => a.Name.Contains(searchTerm))
            .OrderBy(a => a.Name)
            .ToListAsync();
    }

    public async Task<int> GetTotalCountAsync()
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Artists.CountAsync();
    }

    public async Task<int> GetCountByGenreAsync(string genre)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Artists
            .CountAsync(a => a.Genre == genre);
    }

    public async Task<Artist> CreateAsync(Artist artist)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        artist.CreatedAt = DateTime.UtcNow;
        context.Artists.Add(artist);
        await context.SaveChangesAsync();
        return artist;
    }

    public async Task<List<Artist>> CreateManyAsync(List<Artist> artists)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        var now = DateTime.UtcNow;
        foreach (var artist in artists)
        {
            artist.CreatedAt = now;
        }
        context.Artists.AddRange(artists);
        await context.SaveChangesAsync();
        return artists;
    }

    public async Task<Artist> UpdateAsync(Artist artist)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        artist.ModifiedAt = DateTime.UtcNow;
        context.Artists.Update(artist);
        await context.SaveChangesAsync();
        return artist;
    }

    public async Task<bool> DeleteAsync(int id)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        var artist = await context.Artists.FindAsync(id);
        if (artist == null) return false;

        context.Artists.Remove(artist);
        await context.SaveChangesAsync();
        return true;
    }

    public async Task<List<Artist>> GetTopByAlbumCountAsync(int count = 10)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Artists
            .AsNoTracking()
            .Include(a => a.Albums)
                .ThenInclude(a => a.Tracks)
            .OrderByDescending(a => a.Albums.Count)
            .Take(count)
            .ToListAsync();
    }

    // --- Showcase: GroupBy ---
    public async Task<List<GenreCount>> GetArtistCountsByGenreAsync()
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Artists
            .AsNoTracking()
            .Where(a => a.Genre != null)
            .GroupBy(a => a.Genre!)
            .Select(g => new GenreCount { Genre = g.Key, Count = g.Count() })
            .OrderByDescending(g => g.Count)
            .ToListAsync();
    }

    // --- Showcase: Select Projection (DTO) ---
    public async Task<List<ArtistSummary>> GetArtistSummariesAsync(int count = 10)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Artists
            .AsNoTracking()
            .OrderBy(a => a.Name)
            .Take(count)
            .Select(a => new ArtistSummary
            {
                Id = a.Id,
                Name = a.Name,
                Genre = a.Genre ?? "Unknown",
                Country = a.Country ?? "Unknown",
                AlbumCount = a.Albums.Count
            })
            .ToListAsync();
    }

    // --- Showcase: DISTINCT ---
    public async Task<List<string>> GetDistinctCountriesAsync()
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Artists
            .AsNoTracking()
            .Where(a => a.Country != null)
            .Select(a => a.Country!)
            .Distinct()
            .OrderBy(c => c)
            .ToListAsync();
    }

    // --- Showcase: Conditional (CASE WHEN via ternary) ---
    public async Task<List<ArtistEra>> GetArtistsByEraAsync()
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Artists
            .AsNoTracking()
            .Where(a => a.FormedYear != null)
            .Select(a => new ArtistEra
            {
                Name = a.Name,
                FormedYear = a.FormedYear!.Value,
                Era = a.FormedYear < 1970 ? "Classic"
                    : a.FormedYear < 1990 ? "Golden Age"
                    : a.FormedYear < 2010 ? "Modern"
                    : "Contemporary"
            })
            .OrderBy(a => a.FormedYear)
            .Take(10)
            .ToListAsync();
    }

    // --- Showcase: String manipulation ---
    public async Task<List<string>> GetArtistNamesUpperAsync(int count = 5)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Artists
            .AsNoTracking()
            .OrderBy(a => a.Name)
            .Take(count)
            .Select(a => a.Name.ToUpper())
            .ToListAsync();
    }

    // --- Showcase: Any() / All() ---
    public async Task<bool> AnyArtistInGenreAsync(string genre)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Artists.AnyAsync(a => a.Genre == genre);
    }

    // --- Showcase: Min / Max ---
    public async Task<int?> GetEarliestFormedYearAsync()
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Artists
            .Where(a => a.FormedYear != null)
            .MinAsync(a => a.FormedYear);
    }

    public async Task<int?> GetLatestFormedYearAsync()
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Artists
            .Where(a => a.FormedYear != null)
            .MaxAsync(a => a.FormedYear);
    }
}
