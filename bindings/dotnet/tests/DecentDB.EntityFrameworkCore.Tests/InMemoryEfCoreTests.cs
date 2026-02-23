using System;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Linq;
using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class InMemoryEfCoreTests : IDisposable
{
    private readonly DecentDBConnection _connection;

    public InMemoryEfCoreTests()
    {
        _connection = new DecentDBConnection("Data Source=:memory:");
        _connection.Open();
    }

    public void Dispose()
    {
        _connection.Dispose();
    }

    private DbContextOptions<TContext> CreateOptions<TContext>() where TContext : DbContext
    {
        var builder = new DbContextOptionsBuilder<TContext>();
        builder.UseDecentDB(_connection, contextOwnsConnection: false);
        return builder.Options;
    }

    // --- DbContext definitions ---

    private sealed class SimpleItem
    {
        [Key]
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public long Value { get; set; }
    }

    private sealed class SimpleDbContext : DbContext
    {
        public SimpleDbContext(DbContextOptions<SimpleDbContext> options) : base(options) { }
        public DbSet<SimpleItem> Items => Set<SimpleItem>();
    }

    private sealed class Author
    {
        [Key]
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<Book> Books { get; set; } = new();
    }

    private sealed class Book
    {
        [Key]
        public long Id { get; set; }
        public string Title { get; set; } = string.Empty;
        public long AuthorId { get; set; }
        public Author Author { get; set; } = null!;
    }

    private sealed class LibraryDbContext : DbContext
    {
        public LibraryDbContext(DbContextOptions<LibraryDbContext> options) : base(options) { }
        public DbSet<Author> Authors => Set<Author>();
        public DbSet<Book> Books => Set<Book>();
    }

    // --- Tests ---

    [Fact]
    public void EnsureCreated_Succeeds()
    {
        using var context = new SimpleDbContext(CreateOptions<SimpleDbContext>());
        var created = context.Database.EnsureCreated();
        Assert.True(created);
    }

    [Fact]
    public void EnsureCreated_SecondCall_ReturnsFalse()
    {
        using var ctx1 = new SimpleDbContext(CreateOptions<SimpleDbContext>());
        Assert.True(ctx1.Database.EnsureCreated());

        using var ctx2 = new SimpleDbContext(CreateOptions<SimpleDbContext>());
        Assert.False(ctx2.Database.EnsureCreated());
    }

    [Fact]
    public void Insert_And_Query()
    {
        using var context = new SimpleDbContext(CreateOptions<SimpleDbContext>());
        context.Database.EnsureCreated();

        context.Items.Add(new SimpleItem { Id = 1, Name = "alpha", Value = 10 });
        context.SaveChanges();

        var item = context.Items.Single(i => i.Id == 1);
        Assert.Equal("alpha", item.Name);
        Assert.Equal(10, item.Value);
    }

    [Fact]
    public void Insert_Multiple_And_Count()
    {
        using var context = new SimpleDbContext(CreateOptions<SimpleDbContext>());
        context.Database.EnsureCreated();

        for (int i = 1; i <= 20; i++)
            context.Items.Add(new SimpleItem { Id = i, Name = $"item_{i}", Value = i * 10 });

        context.SaveChanges();
        Assert.Equal(20, context.Items.Count());
    }

    [Fact]
    public void Update_Entity()
    {
        using var context = new SimpleDbContext(CreateOptions<SimpleDbContext>());
        context.Database.EnsureCreated();

        context.Items.Add(new SimpleItem { Id = 1, Name = "before", Value = 1 });
        context.SaveChanges();

        var item = context.Items.Single(i => i.Id == 1);
        item.Name = "after";
        item.Value = 99;
        context.SaveChanges();

        // Re-query via a new context sharing the same connection.
        using var ctx2 = new SimpleDbContext(CreateOptions<SimpleDbContext>());
        var updated = ctx2.Items.Single(i => i.Id == 1);
        Assert.Equal("after", updated.Name);
        Assert.Equal(99, updated.Value);
    }

    [Fact]
    public void Delete_Entity()
    {
        using var context = new SimpleDbContext(CreateOptions<SimpleDbContext>());
        context.Database.EnsureCreated();

        context.Items.Add(new SimpleItem { Id = 1, Name = "doomed", Value = 0 });
        context.SaveChanges();

        var item = context.Items.Single(i => i.Id == 1);
        context.Items.Remove(item);
        context.SaveChanges();

        Assert.Equal(0, context.Items.Count());
    }

    [Fact]
    public void Where_Filter()
    {
        using var context = new SimpleDbContext(CreateOptions<SimpleDbContext>());
        context.Database.EnsureCreated();

        context.Items.Add(new SimpleItem { Id = 1, Name = "low", Value = 5 });
        context.Items.Add(new SimpleItem { Id = 2, Name = "high", Value = 100 });
        context.Items.Add(new SimpleItem { Id = 3, Name = "mid", Value = 50 });
        context.SaveChanges();

        var highItems = context.Items.Where(i => i.Value >= 50).OrderBy(i => i.Value).ToList();
        Assert.Equal(2, highItems.Count);
        Assert.Equal("mid", highItems[0].Name);
        Assert.Equal("high", highItems[1].Name);
    }

    [Fact]
    public void OrderBy_And_Limit()
    {
        using var context = new SimpleDbContext(CreateOptions<SimpleDbContext>());
        context.Database.EnsureCreated();

        for (int i = 1; i <= 10; i++)
            context.Items.Add(new SimpleItem { Id = i, Name = $"n{i}", Value = 100 - i });

        context.SaveChanges();

        var top3 = context.Items.OrderByDescending(i => i.Value).Take(3).ToList();
        Assert.Equal(3, top3.Count);
        Assert.Equal(99, top3[0].Value);
    }

    [Fact]
    public void CrossContext_SharesData_Via_SharedConnection()
    {
        using var ctx1 = new SimpleDbContext(CreateOptions<SimpleDbContext>());
        ctx1.Database.EnsureCreated();
        ctx1.Items.Add(new SimpleItem { Id = 1, Name = "shared", Value = 42 });
        ctx1.SaveChanges();

        // Second context on same connection should see the data.
        using var ctx2 = new SimpleDbContext(CreateOptions<SimpleDbContext>());
        var item = ctx2.Items.Single(i => i.Id == 1);
        Assert.Equal("shared", item.Name);
    }

    [Fact]
    public void RelatedEntities_Insert_And_Query()
    {
        using var context = new LibraryDbContext(CreateOptions<LibraryDbContext>());
        context.Database.EnsureCreated();

        var author = new Author { Id = 1, Name = "Tolkien" };
        context.Authors.Add(author);
        context.SaveChanges();

        context.Books.Add(new Book { Id = 1, Title = "The Hobbit", AuthorId = 1 });
        context.Books.Add(new Book { Id = 2, Title = "The Silmarillion", AuthorId = 1 });
        context.SaveChanges();

        var books = context.Books.Where(b => b.AuthorId == 1).OrderBy(b => b.Title).ToList();
        Assert.Equal(2, books.Count);
        Assert.Equal("The Hobbit", books[0].Title);
        Assert.Equal("The Silmarillion", books[1].Title);
    }

    [Fact]
    public void EnsureDeleted_IsNoOp_ForInMemory()
    {
        using var context = new SimpleDbContext(CreateOptions<SimpleDbContext>());
        context.Database.EnsureCreated();
        // EnsureDeleted should not throw for in-memory databases.
        context.Database.EnsureDeleted();
    }

    [Fact]
    public void HasTables_ReturnsTrueAfterCreation()
    {
        using var context = new SimpleDbContext(CreateOptions<SimpleDbContext>());
        context.Database.EnsureCreated();

        // The database creator reports tables exist.
        var creator = context.GetService<Microsoft.EntityFrameworkCore.Storage.IRelationalDatabaseCreator>();
        Assert.True(creator.HasTables());
    }
}
