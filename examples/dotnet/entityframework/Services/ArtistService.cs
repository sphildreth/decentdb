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
            .OrderByDescending(a => a.Albums.Count)
            .Take(count)
            .ToListAsync();
    }
}
