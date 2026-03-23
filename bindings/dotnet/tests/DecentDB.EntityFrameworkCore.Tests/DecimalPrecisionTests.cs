using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

/// <summary>
/// Verifies that the EF Core provider respects decimal precision and scale
/// from ConfigureConventions, HasPrecision, and HasColumnType.
/// </summary>
public sealed class DecimalPrecisionTests : IDisposable
{
    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), $"decentdb_prec_{Guid.NewGuid():N}");

    public DecimalPrecisionTests()
    {
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, true);
        }
    }

    [Fact]
    public void DefaultDecimalMapping_UsesDefaultPrecisionAndScale()
    {
        var dbPath = Path.Combine(_tempDir, "default.ddb");
        using var context = CreateContext<DefaultDecimalContext>(dbPath);
        var mappingSource = context.GetService<IRelationalTypeMappingSource>();

        var mapping = (RelationalTypeMapping)mappingSource.FindMapping(typeof(decimal))!;
        Assert.Equal("DECIMAL(18,4)", mapping.StoreType);
    }

    [Fact]
    public void ConfigureConventions_HavePrecision_IsRespected()
    {
        var dbPath = Path.Combine(_tempDir, "conventions.ddb");
        using var context = CreateContext<CustomPrecisionConventionContext>(dbPath);

        context.Database.EnsureCreated();

        context.Items.Add(new PrecisionItem { Value = 123.456789m });
        context.SaveChanges();

        var item = context.Items.First();
        Assert.Equal(123.456789m, item.Value);
    }

    [Fact]
    public void HasPrecision_OnProperty_IsRespected()
    {
        var dbPath = Path.Combine(_tempDir, "hasprecision.ddb");
        using var context = CreateContext<PropertyPrecisionContext>(dbPath);

        context.Database.EnsureCreated();

        context.Items.Add(new PrecisionItem { Value = 99.12m });
        context.SaveChanges();

        var item = context.Items.First();
        Assert.Equal(99.12m, item.Value);
    }

    [Fact]
    public void HasColumnType_WithPrecisionScale_IsRespected()
    {
        var dbPath = Path.Combine(_tempDir, "columntype.ddb");
        using var context = CreateContext<ColumnTypeContext>(dbPath);

        context.Database.EnsureCreated();

        context.Items.Add(new PrecisionItem { Value = 12345.67m });
        context.SaveChanges();

        var item = context.Items.First();
        Assert.Equal(12345.67m, item.Value);
    }

    [Fact]
    public void NullableDecimal_WithPrecision_IsRespected()
    {
        var dbPath = Path.Combine(_tempDir, "nullable.ddb");
        using var context = CreateContext<NullableDecimalContext>(dbPath);

        context.Database.EnsureCreated();

        context.Items.Add(new NullableDecimalItem { Value = 42.123456m });
        context.Items.Add(new NullableDecimalItem { Value = null });
        context.SaveChanges();

        var items = context.Items.OrderBy(i => i.Id).ToList();
        Assert.Equal(42.123456m, items[0].Value);
        Assert.Null(items[1].Value);
    }

    [Fact]
    public void MultipleDecimalProperties_WithDifferentPrecisions()
    {
        var dbPath = Path.Combine(_tempDir, "multi.ddb");
        using var context = CreateContext<MultiPrecisionContext>(dbPath);

        context.Database.EnsureCreated();

        context.Items.Add(new MultiDecimalItem
        {
            Price = 99.99m,
            TaxRate = 0.0825m,
            Weight = 1234.5m
        });
        context.SaveChanges();

        var item = context.Items.First();
        Assert.Equal(99.99m, item.Price);
        Assert.Equal(0.0825m, item.TaxRate);
        Assert.Equal(1234.5m, item.Weight);
    }

    private static TContext CreateContext<TContext>(string dbPath) where TContext : DbContext
    {
        var options = new DbContextOptionsBuilder<TContext>()
            .UseDecentDB($"Data Source={dbPath}")
            .Options;
        return (TContext)Activator.CreateInstance(typeof(TContext), options)!;
    }

    #region Test entities and contexts

    public class PrecisionItem
    {
        public int Id { get; set; }
        public decimal Value { get; set; }
    }

    public class NullableDecimalItem
    {
        public int Id { get; set; }
        public decimal? Value { get; set; }
    }

    public class MultiDecimalItem
    {
        public int Id { get; set; }
        public decimal Price { get; set; }
        public decimal TaxRate { get; set; }
        public decimal Weight { get; set; }
    }

    private sealed class DefaultDecimalContext : DbContext
    {
        public DefaultDecimalContext(DbContextOptions<DefaultDecimalContext> options) : base(options) { }
        public DbSet<PrecisionItem> Items { get; set; } = null!;
    }

    private sealed class CustomPrecisionConventionContext : DbContext
    {
        public CustomPrecisionConventionContext(DbContextOptions<CustomPrecisionConventionContext> options) : base(options) { }
        public DbSet<PrecisionItem> Items { get; set; } = null!;

        protected override void ConfigureConventions(ModelConfigurationBuilder configurationBuilder)
        {
            configurationBuilder.Properties<decimal>().HavePrecision(18, 6);
        }
    }

    private sealed class PropertyPrecisionContext : DbContext
    {
        public PropertyPrecisionContext(DbContextOptions<PropertyPrecisionContext> options) : base(options) { }
        public DbSet<PrecisionItem> Items { get; set; } = null!;

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<PrecisionItem>().Property(e => e.Value).HasPrecision(10, 2);
        }
    }

    private sealed class ColumnTypeContext : DbContext
    {
        public ColumnTypeContext(DbContextOptions<ColumnTypeContext> options) : base(options) { }
        public DbSet<PrecisionItem> Items { get; set; } = null!;

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<PrecisionItem>().Property(e => e.Value).HasColumnType("DECIMAL(10,2)");
        }
    }

    private sealed class NullableDecimalContext : DbContext
    {
        public NullableDecimalContext(DbContextOptions<NullableDecimalContext> options) : base(options) { }
        public DbSet<NullableDecimalItem> Items { get; set; } = null!;

        protected override void ConfigureConventions(ModelConfigurationBuilder configurationBuilder)
        {
            configurationBuilder.Properties<decimal>().HavePrecision(18, 6);
        }
    }

    private sealed class MultiPrecisionContext : DbContext
    {
        public MultiPrecisionContext(DbContextOptions<MultiPrecisionContext> options) : base(options) { }
        public DbSet<MultiDecimalItem> Items { get; set; } = null!;

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<MultiDecimalItem>().Property(e => e.Price).HasPrecision(10, 2);
            modelBuilder.Entity<MultiDecimalItem>().Property(e => e.TaxRate).HasPrecision(8, 4);
            modelBuilder.Entity<MultiDecimalItem>().Property(e => e.Weight).HasPrecision(12, 1);
        }
    }

    #endregion
}
