using EntityFrameworkDemo.Data;
using EntityFrameworkDemo.Models;
using Microsoft.EntityFrameworkCore;

namespace EntityFrameworkDemo.Services;

public class AlbumService
{
    private readonly IDbContextFactory<MusicDbContext> _contextFactory;

    public AlbumService(IDbContextFactory<MusicDbContext> contextFactory)
    {
        _contextFactory = contextFactory;
    }

    public async Task<Album?> GetByIdAsync(int id)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Albums
            .AsNoTracking()
            .FirstOrDefaultAsync(a => a.Id == id);
    }

    public async Task<Album?> GetByIdWithTracksAsync(int id)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Albums
            .AsNoTracking()
            .Include(a => a.Tracks)
            .FirstOrDefaultAsync(a => a.Id == id);
    }

    public async Task<Album?> GetByIdWithArtistAndTracksAsync(int id)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Albums
            .AsNoTracking()
            .Include(a => a.Artist)
            .Include(a => a.Tracks)
            .FirstOrDefaultAsync(a => a.Id == id);
    }

    public async Task<List<Album>> GetAllAsync(int page = 1, int pageSize = 20)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Albums
            .AsNoTracking()
            .OrderBy(a => a.ReleaseYear)
            .ThenBy(a => a.Title)
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .ToListAsync();
    }

    public async Task<List<Album>> GetByArtistAsync(int artistId)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Albums
            .AsNoTracking()
            .Where(a => a.ArtistId == artistId)
            .OrderBy(a => a.ReleaseYear)
            .ToListAsync();
    }

    public async Task<List<Album>> GetByArtistWithTracksAsync(int artistId)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Albums
            .AsNoTracking()
            .Include(a => a.Tracks)
            .Where(a => a.ArtistId == artistId)
            .OrderBy(a => a.ReleaseYear)
            .ToListAsync();
    }

    public async Task<List<Album>> GetByYearAsync(int year)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Albums
            .AsNoTracking()
            .Where(a => a.ReleaseYear == year)
            .OrderBy(a => a.Title)
            .ToListAsync();
    }

    public async Task<List<Album>> GetByYearRangeAsync(int startYear, int endYear)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Albums
            .AsNoTracking()
            .Where(a => a.ReleaseYear >= startYear && a.ReleaseYear <= endYear)
            .OrderBy(a => a.ReleaseYear)
            .ThenBy(a => a.Title)
            .ToListAsync();
    }

    public async Task<List<Album>> SearchByTitleAsync(string searchTerm)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Albums
            .AsNoTracking()
            .Where(a => a.Title.Contains(searchTerm))
            .OrderBy(a => a.Title)
            .ToListAsync();
    }

    public async Task<int> GetTotalCountAsync()
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Albums.CountAsync();
    }

    public async Task<int> GetCountByArtistAsync(int artistId)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Albums
            .CountAsync(a => a.ArtistId == artistId);
    }

    public async Task<double> GetAverageDurationAsync()
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Albums
            .AverageAsync(a => (double?)a.TotalDurationSeconds) ?? 0;
    }

    public async Task<Album> CreateAsync(Album album)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        album.CreatedAt = DateTime.UtcNow;
        context.Albums.Add(album);
        await context.SaveChangesAsync();
        return album;
    }

    public async Task<List<Album>> CreateManyAsync(List<Album> albums)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        var now = DateTime.UtcNow;
        foreach (var album in albums)
        {
            album.CreatedAt = now;
        }
        context.Albums.AddRange(albums);
        await context.SaveChangesAsync();
        return albums;
    }

    public async Task<Album> UpdateAsync(Album album)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        album.ModifiedAt = DateTime.UtcNow;
        context.Albums.Update(album);
        await context.SaveChangesAsync();
        return album;
    }

    public async Task<bool> DeleteAsync(int id)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        var album = await context.Albums.FindAsync(id);
        if (album == null) return false;

        context.Albums.Remove(album);
        await context.SaveChangesAsync();
        return true;
    }

    public async Task<List<Album>> GetTopByTrackCountAsync(int count = 10)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Albums
            .AsNoTracking()
            .OrderByDescending(a => a.TotalTracks)
            .Take(count)
            .ToListAsync();
    }

    public async Task<List<Album>> GetTopByDurationAsync(int count = 10)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Albums
            .AsNoTracking()
            .OrderByDescending(a => a.TotalDurationSeconds)
            .Take(count)
            .ToListAsync();
    }
}
