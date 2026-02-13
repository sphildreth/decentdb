# Entity Framework Core + DecentDB Demo

A comprehensive .NET 10 console application demonstrating the DecentDB Entity Framework Core provider with a music catalog scenario (Artists → Albums → Tracks).

## Features Demonstrated

### Entity Framework Core Features
- **DbContextFactory** pattern for dependency injection
- **Service Collection** configuration
- **Fluent API** configuration (Album.cs)
- **Data Annotations** (Track.cs)
- **Entity Relationships**
  - One-to-Many: Artist → Albums
  - One-to-Many: Album → Tracks
  - Cascade delete configuration
- **Indexes** on frequently queried columns
- **LINQ Operations**
  - Filtering (Where)
  - Ordering (OrderBy, ThenBy)
  - Paging (Skip, Take)
  - Projection (Select)
  - Aggregation (Count, Average, Sum)
  - Include/Eager loading
- **CRUD Operations**
  - Create (single and batch)
  - Read (with various query patterns)
  - Update
  - Delete

### DecentDB Provider Features
- `UseDecentDB()` configuration
- Connection string configuration
- Database creation and migrations
- Transaction support
- Query translation to DecentDB SQL

## Project Structure

```
entityframework/
├── Data/
│   └── MusicDbContext.cs         # EF Core DbContext with fluent configuration
├── Models/
│   ├── Artist.cs                 # Artist entity (semantic mapping)
│   ├── Album.cs                  # Album entity (semantic mapping)
│   └── Track.cs                  # Track entity (attribute mapping)
├── Services/
│   ├── ArtistService.cs          # Business logic for artists
│   ├── AlbumService.cs           # Business logic for albums
│   ├── TrackService.cs           # Business logic for tracks
│   ├── DataGenerator.cs          # Seed data generation
│   └── PerformanceMetrics.cs     # Performance measurement utilities
├── Program.cs                    # Main application entry point
└── README.md                     # This file
```

## Mapping Approaches

### Track.cs - Data Annotations Approach
Uses attributes to configure the database schema:
```csharp
[Table("tracks")]
[Index(nameof(AlbumId), Name = "ix_tracks_album_id")]
public class Track
{
    [Key]
    [Column("id")]
    public int Id { get; set; }
    
    [Required]
    [Column("title")]
    public string Title { get; set; } = string.Empty;
    // ...
}
```

### Album.cs & Artist.cs - Fluent API Approach
Configuration done in `MusicDbContext.OnModelCreating()`:
```csharp
modelBuilder.Entity<Album>(entity =>
{
    entity.ToTable("albums");
    entity.HasKey(e => e.Id);
    entity.Property(e => e.Title)
        .HasColumnName("title")
        .IsRequired()
        .HasMaxLength(200);
    // ...
});
```

## Running the Demo

### Prerequisites
- .NET 10 SDK
- DecentDB EF Core provider (built from source)

### Build and Run

```bash
cd examples/dotnet/entityframework

dotnet build
dotnet run
```

The application will:
1. Create a new DecentDB database file (`music.ddb`)
2. Generate seed data:
   - 100 Artists
   - 3-20 Albums per artist (random)
   - 5-20 Tracks per album (random)
3. Run performance benchmarks
4. Display statistics and metrics
5. Clean up the database file

### Performance Benchmarks

The demo measures performance for:
- **Seeding Operations**: Bulk insert of artists, albums, and tracks
- **Query Operations**:
  - Get all (paged)
  - Get by ID
  - Get with relationships (Include)
  - Search/filter operations
  - Aggregation (Count, Average, Sum)
- **Modification Operations**:
  - Create
  - Update
  - Delete
- **Complex Queries**:
  - Top N queries
  - Range queries
  - Multi-level includes

## Example Output

```
╔══════════════════════════════════════════════════════════════════════════╗
║                    PERFORMANCE METRICS REPORT                            ║
═══════════════════════════════════════════════════════════════════════════

Seed Operations:
----------------------------------------------------------------------
  Seed: Create 100 Artists                                   45ms
  Seed: Create All Albums (1,247 albums)                    234ms
  Seed: Create All Tracks (8,932 tracks)                  1,456ms
                                                            Avg: 578.3ms | Total: 1735ms

Artist Operations:
----------------------------------------------------------------------
  Artist: Get All (Page 1, 20 items)                          3ms
  Artist: Get All (Page 2, 20 items)                          2ms
  Artist: Count Total                                         1ms
  Artist: Get By ID                                           1ms
  Artist: Get By ID With Albums                               5ms
  Artist: Search By Genre (Rock)                             12ms
                                                            Avg: 4.0ms | Total: 24ms

...

Database Statistics:
  Artists: 100
  Albums: 1,247
  Tracks: 8,932
  Average Album Duration: 42:15
  Total Track Plays: 4,234,567,890

Top 5 Artists by Album Count:
  The Midnight Collective: 20 albums, 287 tracks
  Rock Revolution: 19 albums, 265 tracks
  ...
```

## Dependencies

- `DecentDB.EntityFrameworkCore` - EF Core provider for DecentDB
- `Microsoft.EntityFrameworkCore` - EF Core runtime
- `Microsoft.Extensions.DependencyInjection` - DI container
- `Microsoft.Extensions.Logging.Console` - Logging

## Notes

- The demo uses `AsNoTracking()` for read-only queries to improve performance
- `AddDbContextFactory` is used instead of `AddDbContext` to support multiple simultaneous operations
- The database file is automatically cleaned up after the demo runs
- All operations are async/await for optimal performance
