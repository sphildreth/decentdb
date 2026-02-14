using EntityFrameworkDemo.Models;
using NodaTime;

namespace EntityFrameworkDemo.Services;

public class DataGenerator
{
    private static readonly Random Random = new Random();

    private static readonly string[] FirstNames = new[]
    {
        "The", "A", "John", "Jane", "Bob", "Alice", "Rock", "Metal", "Jazz", "Blues",
        "Funk", "Soul", "Pop", "Indie", "Alternative", "Progressive", "Classic", "Modern",
        "Electric", "Acoustic", "Digital", "Analog", "Neon", "Cyber", "Retro", "Vintage"
    };

    private static readonly string[] LastNames = new[]
    {
        "Band", "Collective", "Orchestra", "Quartet", "Trio", "Ensemble", "Project",
        "Experience", "Revolution", "Movement", "Syndrome", "Theory", "Experiment",
        "Conspiracy", "Syndicate", "Alliance", "Coalition", "Federation", "Society"
    };

    private static readonly string[] AlbumPrefixes = new[]
    {
        "Greatest Hits", "The Best of", "Ultimate Collection", "Volume", "Episode",
        "Chapter", "Part", "Sessions", "Live at", "Unplugged", "Acoustic", "Remixes",
        "B-Sides", "Rarities", "Demos", "Anniversary Edition", "Deluxe Edition"
    };

    private static readonly string[] TrackAdjectives = new[]
    {
        "Beautiful", "Dark", "Light", "Heavy", "Soft", "Loud", "Fast", "Slow",
        "Wild", "Calm", "Crazy", "Peaceful", "Angry", "Happy", "Sad", "Epic",
        "Secret", "Hidden", "Lost", "Found", "Broken", "Fixed", "New", "Old"
    };

    private static readonly string[] TrackNouns = new[]
    {
        "Love", "Heart", "Dream", "Night", "Day", "World", "Life", "Time",
        "Space", "Mind", "Soul", "Spirit", "Fire", "Water", "Earth", "Wind",
        "Shadow", "Light", "Hope", "Fear", "Joy", "Pain", "Smile", "Tear"
    };

    private static readonly string[] Genres = new[]
    {
        "Rock", "Pop", "Jazz", "Blues", "Metal", "Hip Hop", "Electronic", "Classical",
        "Folk", "Country", "R&B", "Soul", "Funk", "Reggae", "Punk", "Indie"
    };

    private static readonly string[] Countries = new[]
    {
        "USA", "UK", "Canada", "Germany", "France", "Japan", "Australia", "Sweden",
        "Brazil", "Italy", "Spain", "Netherlands", "Norway", "Denmark", "Ireland"
    };

    public static List<Artist> GenerateArtists(int count = 100)
    {
        var artists = new List<Artist>();
        var usedNames = new HashSet<string>();

        for (int i = 0; i < count; i++)
        {
            string name;
            int attempts = 0;
            do
            {
                name = GenerateUniqueArtistName();
                attempts++;
            } while (usedNames.Contains(name) && attempts < 10);

            usedNames.Add(name);

            artists.Add(new Artist
            {
                Name = name,
                Bio = GenerateBio(),
                Genre = Genres[Random.Next(Genres.Length)],
                Country = Countries[Random.Next(Countries.Length)],
                FormedYear = Random.Next(1950, 2020),
                Website = $"https://www.{name.ToLower().Replace(" ", "")}.com",
                Albums = new List<Album>()
            });
        }

        return artists;
    }

    public static List<Album> GenerateAlbumsForArtist(Artist artist)
    {
        var albums = new List<Album>();
        int albumCount = Random.Next(3, 21); // 3 to 20 albums

        for (int i = 0; i < albumCount; i++)
        {
            var album = new Album
            {
                Title = GenerateAlbumTitle(artist.Name, i),
                Description = GenerateAlbumDescription(),
                ReleaseYear = artist.FormedYear!.Value + Random.Next(0, 20),
                RecordLabel = GenerateRecordLabel(),
                CoverArtUrl = $"https://example.com/covers/{artist.Name.ToLower().Replace(" ", "-")}-{i}.jpg",
                Artist = artist,
                ArtistId = artist.Id,
                Tracks = new List<Track>()
            };

            album.Tracks = GenerateTracksForAlbum(album);
            album.TotalTracks = album.Tracks.Count;
            album.TotalDurationSeconds = album.Tracks.Sum(t => t.DurationSeconds);

            albums.Add(album);
        }

        return albums;
    }

    private static List<Track> GenerateTracksForAlbum(Album album)
    {
        var tracks = new List<Track>();
        int trackCount = Random.Next(5, 21); // 5 to 20 tracks

        for (int i = 0; i < trackCount; i++)
        {
            tracks.Add(new Track
            {
                Title = GenerateTrackTitle(),
                TrackNumber = i + 1,
                DurationSeconds = Random.Next(120, 600), // 2 to 10 minutes
                Genre = album.Artist.Genre,
                Lyrics = GenerateLyrics(),
                IsExplicit = Random.Next(10) == 0, // 10% chance
                PlayCount = Random.Next(0, 1000000),
                Rating = Random.NextDouble() * 5,
                Album = album,
                AlbumId = album.Id
            });
        }

        return tracks;
    }

    private static string GenerateUniqueArtistName()
    {
        var first = FirstNames[Random.Next(FirstNames.Length)];
        var last = LastNames[Random.Next(LastNames.Length)];

        if (Random.Next(2) == 0)
        {
            return $"{first} {last}";
        }
        else
        {
            var number = Random.Next(1, 100);
            return $"{first} {last} {number}";
        }
    }

    private static string GenerateAlbumTitle(string artistName, int index)
    {
        if (Random.Next(3) == 0)
        {
            var prefix = AlbumPrefixes[Random.Next(AlbumPrefixes.Length)];
            if (prefix.Contains("Volume") || prefix.Contains("Part") || prefix.Contains("Chapter"))
            {
                return $"{prefix} {index + 1}";
            }
            return prefix;
        }

        var adj = TrackAdjectives[Random.Next(TrackAdjectives.Length)];
        var noun = TrackNouns[Random.Next(TrackNouns.Length)];
        return $"{adj} {noun}";
    }

    private static string GenerateTrackTitle()
    {
        var adj = TrackAdjectives[Random.Next(TrackAdjectives.Length)];
        var noun = TrackNouns[Random.Next(TrackNouns.Length)];

        if (Random.Next(2) == 0)
        {
            return $"{adj} {noun}";
        }
        else
        {
            var secondNoun = TrackNouns[Random.Next(TrackNouns.Length)];
            return $"{adj} {noun} and {secondNoun}";
        }
    }

    private static string GenerateBio()
    {
        var bios = new[]
        {
            "An innovative band pushing the boundaries of modern music.",
            "Legends in their genre with decades of influential work.",
            "A fresh new voice in the contemporary music scene.",
            "Known for their energetic live performances and catchy melodies.",
            "Critically acclaimed artists with multiple award-winning albums.",
            "Underground heroes who rose to mainstream success.",
            "Pioneers of their unique sound that defined a generation.",
            "A collaborative project bringing together world-class musicians."
        };

        return bios[Random.Next(bios.Length)];
    }

    private static string GenerateAlbumDescription()
    {
        var descriptions = new[]
        {
            "Their most ambitious project yet, exploring new sonic territories.",
            "A return to their roots with a modern twist.",
            "A concept album that tells a compelling story through music.",
            "Featuring collaborations with some of the biggest names in music.",
            "Recorded in legendary studios with state-of-the-art production.",
            "An intimate collection of songs showcasing their growth as artists.",
            "A genre-defying masterpiece that challenges conventions."
        };

        return descriptions[Random.Next(descriptions.Length)];
    }

    private static string GenerateRecordLabel()
    {
        var labels = new[]
        {
            "Universal Music", "Sony Music", "Warner Music", "EMI", "Atlantic Records",
            "Columbia Records", "Capitol Records", "Elektra Records", "Virgin Records",
            "Sub Pop", "Merge Records", "Matador Records", "4AD", "Domino Records"
        };

        return labels[Random.Next(labels.Length)];
    }

    private static string GenerateLyrics()
    {
        var lines = new[]
        {
            "Walking down the endless road",
            "Searching for a place to call home",
            "The stars above guide my way",
            "Through the night into the day",
            "Memories fade like morning dew",
            "But my love remains forever true",
            "In this world of chaos and strife",
            "Music is the soundtrack of life"
        };

        var selectedLines = lines.OrderBy(x => Random.Next()).Take(4);
        return string.Join("\n", selectedLines);
    }

    private static readonly string[] Venues = new[]
    {
        "Madison Square Garden", "The O2 Arena", "Wembley Stadium", "Red Rocks Amphitheatre",
        "Sydney Opera House", "Royal Albert Hall", "Hollywood Bowl", "Ryman Auditorium",
        "Fillmore", "Brixton Academy", "Olympia", "Budokan", "Paradiso", "Melkweg",
        "Berghain", "The Roundhouse", "Forum", "Palladium"
    };

    private static readonly string[] Cities = new[]
    {
        "New York", "London", "Los Angeles", "Nashville", "Sydney", "Tokyo",
        "Berlin", "Amsterdam", "Paris", "Melbourne", "Chicago", "Austin",
        "Manchester", "Stockholm", "Toronto", "Seattle"
    };

    public static List<Event> GenerateEventsForArtists(List<Artist> artists, int eventsPerArtist = 5)
    {
        var events = new List<Event>();
        var baseDate = new LocalDate(2024, 1, 1);

        foreach (var artist in artists)
        {
            int count = Random.Next(2, eventsPerArtist + 1);
            for (int i = 0; i < count; i++)
            {
                int daysOffset = Random.Next(0, 730); // within 2 years
                var eventDate = baseDate.PlusDays(daysOffset);

                // Ticket sales start 30-90 days before event
                int saleLeadDays = Random.Next(30, 91);
                var saleDate = eventDate.PlusDays(-saleLeadDays);
                var ticketSaleStart = saleDate.AtMidnight()
                    .InZoneLeniently(DateTimeZone.Utc)
                    .ToInstant()
                    .Plus(Duration.FromHours(Random.Next(8, 18)));

                // Doors open on event day, 1-3 hours before show
                var doorsOpen = eventDate.AtMidnight()
                    .InZoneLeniently(DateTimeZone.Utc)
                    .ToInstant()
                    .Plus(Duration.FromHours(Random.Next(17, 20)));

                var capacity = Random.Next(500, 20001);
                var sold = Random.Next((int)(capacity * 0.4), capacity + 1);

                events.Add(new Event
                {
                    Name = $"{artist.Name} Live at {Venues[Random.Next(Venues.Length)]}",
                    Venue = Venues[Random.Next(Venues.Length)],
                    City = Cities[Random.Next(Cities.Length)],
                    Country = Countries[Random.Next(Countries.Length)],
                    TicketSaleStart = ticketSaleStart,
                    DoorsOpen = doorsOpen,
                    EventDate = eventDate,
                    CreatedAt = DateTime.UtcNow,
                    CapacityTotal = capacity,
                    TicketsSold = sold,
                    TicketPrice = Math.Round(Random.NextDouble() * 200 + 25, 2),
                    ArtistId = artist.Id,
                    Artist = artist
                });
            }
        }

        return events;
    }
}
