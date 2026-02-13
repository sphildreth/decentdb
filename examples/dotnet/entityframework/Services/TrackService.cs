using EntityFrameworkDemo.Data;
using EntityFrameworkDemo.Models;
using Microsoft.EntityFrameworkCore;

namespace EntityFrameworkDemo.Services;

public class TrackService
{
    private readonly IDbContextFactory<MusicDbContext> _contextFactory;

    public TrackService(IDbContextFactory<MusicDbContext> contextFactory)
    {
        _contextFactory = contextFactory;
    }

    public async Task<Track?> GetByIdAsync(int id)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Tracks
            .AsNoTracking()
            .FirstOrDefaultAsync(t => t.Id == id);
    }

    public async Task<Track?> GetByIdWithAlbumAsync(int id)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Tracks
            .AsNoTracking()
            .Include(t => t.Album)
            .FirstOrDefaultAsync(t => t.Id == id);
    }

    public async Task<List<Track>> GetAllAsync(int page = 1, int pageSize = 20)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Tracks
            .AsNoTracking()
            .OrderBy(t => t.AlbumId)
            .ThenBy(t => t.TrackNumber)
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .ToListAsync();
    }

    public async Task<List<Track>> GetByAlbumAsync(int albumId)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Tracks
            .AsNoTracking()
            .Where(t => t.AlbumId == albumId)
            .OrderBy(t => t.TrackNumber)
            .ToListAsync();
    }

    public async Task<List<Track>> GetByAlbumWithDetailsAsync(int albumId)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Tracks
            .AsNoTracking()
            .Include(t => t.Album)
            .ThenInclude(a => a.Artist)
            .Where(t => t.AlbumId == albumId)
            .OrderBy(t => t.TrackNumber)
            .ToListAsync();
    }

    public async Task<List<Track>> GetByGenreAsync(string genre)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Tracks
            .AsNoTracking()
            .Where(t => t.Genre == genre)
            .OrderBy(t => t.Title)
            .ToListAsync();
    }

    public async Task<List<Track>> GetExplicitTracksAsync()
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Tracks
            .AsNoTracking()
            .Where(t => t.IsExplicit)
            .OrderBy(t => t.Title)
            .ToListAsync();
    }

    public async Task<List<Track>> SearchByTitleAsync(string searchTerm)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Tracks
            .AsNoTracking()
            .Where(t => t.Title.Contains(searchTerm))
            .OrderBy(t => t.Title)
            .ToListAsync();
    }

    public async Task<List<Track>> GetByDurationRangeAsync(int minSeconds, int maxSeconds)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Tracks
            .AsNoTracking()
            .Where(t => t.DurationSeconds >= minSeconds && t.DurationSeconds <= maxSeconds)
            .OrderBy(t => t.DurationSeconds)
            .ToListAsync();
    }

    public async Task<int> GetTotalCountAsync()
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Tracks.CountAsync();
    }

    public async Task<int> GetCountByAlbumAsync(int albumId)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Tracks
            .CountAsync(t => t.AlbumId == albumId);
    }

    public async Task<double> GetAverageDurationAsync()
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Tracks
            .AverageAsync(t => (double?)t.DurationSeconds) ?? 0;
    }

    public async Task<long> GetTotalPlayCountAsync()
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Tracks
            .SumAsync(t => (long?)t.PlayCount) ?? 0;
    }

    public async Task<Track> CreateAsync(Track track)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        track.CreatedAt = DateTime.UtcNow;
        context.Tracks.Add(track);
        await context.SaveChangesAsync();
        return track;
    }

    public async Task<List<Track>> CreateManyAsync(List<Track> tracks)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        var now = DateTime.UtcNow;
        foreach (var track in tracks)
        {
            track.CreatedAt = now;
        }
        context.Tracks.AddRange(tracks);
        await context.SaveChangesAsync();
        return tracks;
    }

    public async Task<Track> UpdateAsync(Track track)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        track.ModifiedAt = DateTime.UtcNow;
        context.Tracks.Update(track);
        await context.SaveChangesAsync();
        return track;
    }

    public async Task<bool> DeleteAsync(int id)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        var track = await context.Tracks.FindAsync(id);
        if (track == null) return false;

        context.Tracks.Remove(track);
        await context.SaveChangesAsync();
        return true;
    }

    public async Task<bool> IncrementPlayCountAsync(int id)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        var track = await context.Tracks.FindAsync(id);
        if (track == null) return false;

        track.PlayCount++;
        track.ModifiedAt = DateTime.UtcNow;
        await context.SaveChangesAsync();
        return true;
    }

    public async Task<List<Track>> GetTopByPlayCountAsync(int count = 10)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Tracks
            .AsNoTracking()
            .OrderByDescending(t => t.PlayCount)
            .Take(count)
            .ToListAsync();
    }

    public async Task<List<Track>> GetTopByRatingAsync(int count = 10)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Tracks
            .AsNoTracking()
            .Where(t => t.Rating.HasValue)
            .OrderByDescending(t => t.Rating)
            .Take(count)
            .ToListAsync();
    }

    public async Task<List<Track>> GetLongestTracksAsync(int count = 10)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.Tracks
            .AsNoTracking()
            .OrderByDescending(t => t.DurationSeconds)
            .Take(count)
            .ToListAsync();
    }
}
