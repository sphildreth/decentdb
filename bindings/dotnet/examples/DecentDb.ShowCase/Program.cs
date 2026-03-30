using System.Diagnostics;
using System.Text;
using DecentDB.AdoNet;
using DecentDb.ShowCase;
using DecentDb.ShowCase.Entities;
using Microsoft.EntityFrameworkCore;
using NodaTime;

class Program
{
    private static readonly string DbPath = Path.Combine(Path.GetTempPath(), $"decentdb_showcase_{Guid.NewGuid()}.db");

    static async Task Main(string[] args)
    {
        Console.WriteLine("═══════════════════════════════════════════════════════════════════════════════════");
        Console.WriteLine("              DecentDB EntityFramework Core Showcase");
        Console.WriteLine("═══════════════════════════════════════════════════════════════════════════════════");
        Console.WriteLine();

        try
        {
            await DemonstrateDecentDBMetadata();
            await DemonstrateDatabaseOperations();
            await DemonstrateEFCoreBasicCRUD();
            await DemonstrateNullableComparisons();
            await DemonstrateLinqQueries();
            await DemonstrateStringOperations();
            await DemonstrateMathOperations();
            await DemonstrateDateTimeOperations();
            await DemonstrateNodaTimeOperations();
            await DemonstratePrimitiveCollections();
            await DemonstrateTransactions();
            await DemonstrateConcurrencyControl();
            await DemonstrateSchemaIntrospection();
            await DemonstrateRawSql();
            await DemonstrateChangeTracking();
            await DemonstrateBulkOperations();
            await DemonstrateLikePatternMatching();
            await DemonstrateSetOperations();
            await DemonstrateExplicitJoins();
            await DemonstrateSubqueries();
            await DemonstrateIncludeThenInclude();
            await DemonstrateExistenceAndChildFilters();
            await DemonstrateConditionalLogic();
            await DemonstrateQueryComposition();
            await DemonstrateSelectMany();
            await DemonstrateClientVsServerEvaluation();
            await DemonstrateUnsupportedCases();
            await DemonstratePerformancePatterns();
        }
        finally
        {
            CleanupDatabase();
        }
    }

    private static async Task DemonstrateDecentDBMetadata()
    {
        PrintSection("DECENTDB METADATA & VERSION");

        var abiVersion = DecentDBConnection.AbiVersion();
        var engineVersion = DecentDBConnection.EngineVersion();

        Console.WriteLine($"  ABI Version:        {abiVersion}");
        Console.WriteLine($"  Engine Version:      {engineVersion}");
        Console.WriteLine($"  Database Path:       {DbPath}");
        Console.WriteLine();
    }

    private static async Task DemonstrateDatabaseOperations()
    {
        PrintSection("DATABASE CREATE & SCHEMA");

        await using var context = new ShowcaseDbContext(DbPath);
        await context.Database.EnsureCreatedAsync();

        Console.WriteLine($"  Database created/verified at: {DbPath}");
        Console.WriteLine();
    }

    private static async Task DemonstrateEFCoreBasicCRUD()
    {
        PrintSection("EF CORE BASIC CRUD OPERATIONS");

        await using var context = new ShowcaseDbContext(DbPath);

        var category = new Category
        {
            Name = "Electronics",
            Description = "Electronic devices and accessories",
            EffectiveFrom = DateOnly.FromDateTime(DateTime.UtcNow),
            BusinessHoursStart = new TimeOnly(9, 0),
            DisplayOrder = 1,
            IsVisible = true
        };

        context.Categories.Add(category);
        await context.SaveChangesAsync();
        Console.WriteLine($"  CREATE: Category '{category.Name}' created with ID {category.Id}");

        category.Description = "Updated: Electronic devices, gadgets, and accessories";
        await context.SaveChangesAsync();
        Console.WriteLine($"  UPDATE: Category description updated");

        var retrieved = await context.Categories.FindAsync(category.Id);
        Console.WriteLine($"  READ:   Retrieved category: {retrieved?.Name}");

        context.Categories.Remove(category);
        await context.SaveChangesAsync();
        Console.WriteLine($"  DELETE: Category '{category.Name}' deleted");
        Console.WriteLine();
    }

    private static async Task DemonstrateNullableComparisons()
    {
        PrintSection("NULLABLE TYPE COMPARISONS");

        await using var context = new ShowcaseDbContext(DbPath);

        var customerWithNulls = new Customer
        {
            FirstName = "NullTest",
            LastName = "User",
            Email = null,
            Phone = null,
            LastPurchaseDate = null,
            TotalSpend = null,
            CreatedAt = DateTime.UtcNow
        };
        context.Customers.Add(customerWithNulls);

        var customerWithValues = new Customer
        {
            FirstName = "ValueTest",
            LastName = "User",
            Email = "value@test.com",
            Phone = "555-1234",
            LastPurchaseDate = DateTime.UtcNow,
            TotalSpend = 150.50m,
            CreatedAt = DateTime.UtcNow
        };
        context.Customers.Add(customerWithValues);
        await context.SaveChangesAsync();

        var nullEmailCustomers = await context.Customers
            .Where(c => c.Email == null)
            .Select(c => c.FirstName)
            .ToListAsync();
        Console.WriteLine($"  String == null: {nullEmailCustomers.Count} customers");

        var notNullEmailCustomers = await context.Customers
            .Where(c => c.Email != null)
            .Select(c => c.FirstName)
            .ToListAsync();
        Console.WriteLine($"  String != null: {notNullEmailCustomers.Count} customers");

        var nullPhoneCustomers = await context.Customers
            .Where(c => c.Phone == null)
            .Select(c => c.FirstName)
            .ToListAsync();
        Console.WriteLine($"  Nullable string == null: {nullPhoneCustomers.Count} customers");

        var notNullPhoneCustomers = await context.Customers
            .Where(c => c.Phone != null)
            .Select(c => c.FirstName)
            .ToListAsync();
        Console.WriteLine($"  Nullable string != null: {notNullPhoneCustomers.Count} customers");

        var nullDateCustomers = await context.Customers
            .Where(c => c.LastPurchaseDate == null)
            .Select(c => c.FirstName)
            .ToListAsync();
        Console.WriteLine($"  Nullable DateTime == null: {nullDateCustomers.Count} customers");

        var notNullDateCustomers = await context.Customers
            .Where(c => c.LastPurchaseDate != null)
            .Select(c => c.FirstName)
            .ToListAsync();
        Console.WriteLine($"  Nullable DateTime != null: {notNullDateCustomers.Count} customers");

        var nullDecimalCustomers = await context.Customers
            .Where(c => c.TotalSpend == null)
            .Select(c => c.FirstName)
            .ToListAsync();
        Console.WriteLine($"  Nullable decimal == null: {nullDecimalCustomers.Count} customers");

        var notNullDecimalCustomers = await context.Customers
            .Where(c => c.TotalSpend != null)
            .Select(c => c.FirstName)
            .ToListAsync();
        Console.WriteLine($"  Nullable decimal != null: {notNullDecimalCustomers.Count} customers");

        Console.WriteLine();
    }

    private static async Task DemonstrateLinqQueries()
    {
        PrintSection("LINQ QUERIES");

        await using var context = new ShowcaseDbContext(DbPath);

        var category = new Category { Name = "Tech", Description = "Technology", DisplayOrder = 1, EffectiveFrom = DateOnly.FromDateTime(DateTime.UtcNow) };
        context.Categories.Add(category);
        await context.SaveChangesAsync();

        var product = new Product
        {
            Name = "Laptop Pro",
            Description = "High-performance laptop",
            Price = 1299.99m,
            StockQuantity = 50,
            CategoryId = category.Id,
            CreatedAt = DateTime.UtcNow,
            Sku = Guid.NewGuid(),
            Weight = 2.5m
        };
        context.Products.Add(product);
        await context.SaveChangesAsync();

        var allProducts = await context.Products.ToListAsync();
        Console.WriteLine($"  COUNT:    Total products: {allProducts.Count}");

        var expensiveProducts = await context.Products
            .Where(p => p.Price > 1000m)
            .ToListAsync();
        Console.WriteLine($"  FILTER:   Products > $1000: {expensiveProducts.Count}");

        var orderedProducts = await context.Products
            .OrderByDescending(p => p.Price)
            .Take(5)
            .Select(p => new { p.Name, p.Price })
            .ToListAsync();
        Console.WriteLine($"  ORDER BY: Top 5 most expensive:");
        foreach (var p in orderedProducts)
            Console.WriteLine($"            - {p.Name}: ${p.Price:N2}");

        var paginatedProducts = await context.Products
            .Skip(0)
            .Take(3)
            .ToListAsync();
        Console.WriteLine($"  PAGINATION: First 3 products (skip 0, take 3): {paginatedProducts.Count} items");

        var groupByCategory = await context.Products
            .GroupBy(p => p.CategoryId)
            .Select(g => new { CategoryId = g.Key, Count = g.Count() })
            .ToListAsync();
        Console.WriteLine($"  GROUP BY: Products per category: {groupByCategory.Count} groups");

        var distinctCategories = await context.Categories
            .Select(c => c.Name)
            .Distinct()
            .ToListAsync();
        Console.WriteLine($"  DISTINCT: {distinctCategories.Count} distinct category names");
        Console.WriteLine();
    }

    private static async Task DemonstrateStringOperations()
    {
        PrintSection("STRING OPERATIONS TRANSLATION");

        await using var context = new ShowcaseDbContext(DbPath);

        var containsProducts = await context.Products
            .Where(p => p.Name.Contains("Laptop"))
            .Select(p => p.Name)
            .ToListAsync();
        Console.WriteLine($"  Contains('Laptop'): {containsProducts.Count} products");

        var startsWithProducts = await context.Products
            .Where(p => p.Name.StartsWith("Laptop"))
            .Select(p => p.Name)
            .ToListAsync();
        Console.WriteLine($"  StartsWith('Laptop'): {startsWithProducts.Count} products");

        var upperNames = await context.Products
            .Take(2)
            .Select(p => p.Name.ToUpper())
            .ToListAsync();
        Console.WriteLine($"  ToUpper(): {string.Join(", ", upperNames)}");

        var lowerNames = await context.Products
            .Take(2)
            .Select(p => p.Name.ToLower())
            .ToListAsync();
        Console.WriteLine($"  ToLower(): {string.Join(", ", lowerNames)}");

        var trimmedNames = await context.Products
            .Take(2)
            .Select(p => p.Name.Trim())
            .ToListAsync();
        Console.WriteLine($"  Trim(): {string.Join(", ", trimmedNames)}");

        var substrings = await context.Products
            .Take(2)
            .Select(p => p.Name.Substring(0, Math.Min(5, p.Name.Length)))
            .ToListAsync();
        Console.WriteLine($"  Substring(0,5): {string.Join(", ", substrings)}");

        var replaced = await context.Products
            .Take(2)
            .Select(p => p.Name.Replace("Laptop", "Notebook"))
            .ToListAsync();
        Console.WriteLine($"  Replace('Laptop','Notebook'): {string.Join(", ", replaced)}");
        Console.WriteLine();
    }

    private static async Task DemonstrateMathOperations()
    {
        PrintSection("MATH OPERATIONS TRANSLATION");

        await using var context = new ShowcaseDbContext(DbPath);

        var absProducts = await context.Products
            .Where(p => p.Price > 500m && p.Price < 2000m)
            .Select(p => p.Name)
            .ToListAsync();
        Console.WriteLine($"  Filtered products (500 < price < 2000): {absProducts.Count} products");

        var prices = await context.Products
            .Select(p => p.Price)
            .ToListAsync();
        Console.WriteLine($"  Product prices loaded: {prices.Count} values");
        foreach (var price in prices)
        {
            Console.WriteLine($"    Price: ${price:N2}");
            Console.WriteLine($"    Ceiling: ${Math.Ceiling((double)price):N0}");
            Console.WriteLine($"    Floor: ${Math.Floor((double)price):N0}");
            Console.WriteLine($"    Rounded: ${Math.Round((double)price, 0):N0}");
        }

        var stockValues = await context.Products
            .Select(p => p.StockQuantity)
            .ToListAsync();
        Console.WriteLine($"  Stock quantities loaded: {stockValues.Count} values");

        var absStock = await context.Products
            .Select(p => Math.Abs(p.StockQuantity))
            .ToListAsync();
        Console.WriteLine($"  Math.Abs on integers: {absStock.Count} values");

        var maxStock = await context.Products
            .Select(p => Math.Max(p.StockQuantity, 10))
            .ToListAsync();
        Console.WriteLine($"  Math.Max(stock, 10): {maxStock.Count} values");

        var minStock = await context.Products
            .Select(p => Math.Min(p.StockQuantity, 100))
            .ToListAsync();
        Console.WriteLine($"  Math.Min(stock, 100): {minStock.Count} values");

        Console.WriteLine();
    }

    private static async Task DemonstrateDateTimeOperations()
    {
        PrintSection("DATETIME OPERATIONS");

        await using var context = new ShowcaseDbContext(DbPath);

        var now = DateTime.UtcNow;

        var customer = new Customer
        {
            FirstName = "Date",
            LastName = "Test",
            Email = "date@test.com",
            CreatedAt = now
        };
        context.Customers.Add(customer);
        await context.SaveChangesAsync();

        Console.WriteLine($"  Customer created with CreatedAt: {customer.CreatedAt:O}");

        var recentCustomers = await context.Customers
            .Where(c => c.Email == "date@test.com")
            .ToListAsync();
        Console.WriteLine($"  Customer lookup by email: {recentCustomers.Count} found");

        var customersWithNullPhone = await context.Customers
            .Where(c => c.Phone == null)
            .Select(c => c.FirstName)
            .ToListAsync();
        Console.WriteLine($"  Nullable string comparison (Phone == null): {customersWithNullPhone.Count} customers");

        var customersWithPhone = await context.Customers
            .Where(c => c.Phone != null)
            .Select(c => c.FirstName)
            .ToListAsync();
        Console.WriteLine($"  Nullable string comparison (Phone != null): {customersWithPhone.Count} customers");

        var customersWithNullEmail = await context.Customers
            .Where(c => c.Email == null)
            .Select(c => c.FirstName)
            .ToListAsync();
        Console.WriteLine($"  Nullable string comparison (Email == null): {customersWithNullEmail.Count} customers");

        var customerWithPurchaseDate = await context.Customers
            .Where(c => c.LastPurchaseDate != null)
            .Select(c => c.FirstName)
            .ToListAsync();
        Console.WriteLine($"  Nullable DateTime comparison (LastPurchaseDate != null): {customerWithPurchaseDate.Count} customers");

        var customersWithNullSpend = await context.Customers
            .Where(c => c.TotalSpend == null)
            .Select(c => c.FirstName)
            .ToListAsync();
        Console.WriteLine($"  Nullable decimal comparison (TotalSpend == null): {customersWithNullSpend.Count} customers");

        var customersWithSpend = await context.Customers
            .Where(c => c.TotalSpend != null)
            .Select(c => c.FirstName)
            .ToListAsync();
        Console.WriteLine($"  Nullable decimal comparison (TotalSpend != null): {customersWithSpend.Count} customers");

        Console.WriteLine();
    }

    private static async Task DemonstrateNodaTimeOperations()
    {
        PrintSection("NODATIME OPERATIONS (Instant, LocalDate, LocalDateTime)");

        await using var context = new ShowcaseDbContext(DbPath);

        var today = new LocalDate(DateTime.UtcNow.Year, DateTime.UtcNow.Month, DateTime.UtcNow.Day);
        var now = SystemClock.Instance.GetCurrentInstant();
        var nowLocal = now.InUtc().LocalDateTime;

        var entry1 = new ScheduleEntry
        {
            Title = "NodaTime Meeting",
            ScheduledInstant = now,
            ScheduledDate = today,
            ScheduledLocalDateTime = nowLocal,
            EffectiveFrom = today,
            EffectiveUntil = today.PlusDays(30),
            Priority = 1,
            CreatedAt = DateTime.UtcNow
        };

        var entry2 = new ScheduleEntry
        {
            Title = "NodaTime Review",
            ScheduledInstant = now.Plus(Duration.FromDays(7)),
            ScheduledDate = today.PlusDays(7),
            ScheduledLocalDateTime = nowLocal.PlusDays(7),
            EffectiveFrom = today,
            EffectiveUntil = today.PlusDays(60),
            Priority = 2,
            CreatedAt = DateTime.UtcNow
        };

        var entry3 = new ScheduleEntry
        {
            Title = "NodaTime Planning",
            ScheduledInstant = now.Plus(Duration.FromDays(14)),
            ScheduledDate = today.PlusDays(14),
            ScheduledLocalDateTime = nowLocal.PlusDays(14),
            EffectiveFrom = today.PlusMonths(1),
            EffectiveUntil = today.PlusMonths(3),
            Priority = 3,
            IsCompleted = true,
            CompletedAt = DateTime.UtcNow,
            CreatedAt = DateTime.UtcNow
        };

        context.ScheduleEntries.AddRange(entry1, entry2, entry3);
        await context.SaveChangesAsync();
        Console.WriteLine($"  CREATE: Created {3} ScheduleEntry records with NodaTime types");

        var allEntries = await context.ScheduleEntries.ToListAsync();
        Console.WriteLine($"  READ: All entries: {allEntries.Count}");
        foreach (var entry in allEntries)
        {
            Console.WriteLine($"    - {entry.Title}: Instant={entry.ScheduledInstant}, Date={entry.ScheduledDate}, LocalDateTime={entry.ScheduledLocalDateTime}");
        }

        var pendingEntries = await context.ScheduleEntries
            .Where(e => !e.IsCompleted)
            .ToListAsync();
        Console.WriteLine($"  FILTER: Pending entries: {pendingEntries.Count}");

        var highPriorityEntries = await context.ScheduleEntries
            .Where(e => e.Priority <= 2)
            .ToListAsync();
        Console.WriteLine($"  FILTER: High priority (Priority <= 2): {highPriorityEntries.Count}");

        var todayEntries = await context.ScheduleEntries
            .Where(e => e.ScheduledDate == today)
            .ToListAsync();
        Console.WriteLine($"  MIN/MAX: Entries for today ({today}): {todayEntries.Count}");

        var upcomingEntries = await context.ScheduleEntries
            .Where(e => e.ScheduledDate >= today && e.ScheduledDate <= today.PlusDays(14))
            .OrderBy(e => e.ScheduledDate)
            .ToListAsync();
        Console.WriteLine($"  BETWEEN: Upcoming entries (next 14 days): {upcomingEntries.Count}");

        var thisMonthEntries = await context.ScheduleEntries
            .Where(e => e.ScheduledDate.Year == today.Year && e.ScheduledDate.Month == today.Month)
            .ToListAsync();
        Console.WriteLine($"  LocalDate.Year/Month: This month's entries: {thisMonthEntries.Count}");

        var earliestEntry = await context.ScheduleEntries
            .OrderBy(e => e.ScheduledInstant)
            .FirstOrDefaultAsync();
        Console.WriteLine($"  MIN: Earliest entry: {earliestEntry?.Title} at {earliestEntry?.ScheduledInstant}");

        var latestEntry = await context.ScheduleEntries
            .OrderByDescending(e => e.ScheduledInstant)
            .FirstOrDefaultAsync();
        Console.WriteLine($"  MAX: Latest entry: {latestEntry?.Title} at {latestEntry?.ScheduledInstant}");

        var groupedByDay = await context.ScheduleEntries
            .GroupBy(e => e.ScheduledDate)
            .Select(g => new { Date = g.Key, Count = g.Count() })
            .ToListAsync();
        Console.WriteLine($"  GROUP BY LocalDate: {groupedByDay.Count} unique dates");

        entry1.Priority = 10;
        entry1.IsCompleted = true;
        entry1.CompletedAt = DateTime.UtcNow;
        await context.SaveChangesAsync();
        Console.WriteLine($"  UPDATE: Entry '{entry1.Title}' marked as completed");

        context.ScheduleEntries.Remove(entry3);
        await context.SaveChangesAsync();
        var remainingCount = await context.ScheduleEntries.CountAsync();
        Console.WriteLine($"  DELETE: Removed 1 entry, remaining: {remainingCount}");

        Console.WriteLine();
    }

    private static async Task DemonstratePrimitiveCollections()
    {
        PrintSection("PRIMITIVE COLLECTIONS (JSON ARRAYS)");

        await using var context = new ShowcaseDbContext(DbPath);

        var eventLog = new AppEventLog
        {
            Level = "Info",
            Message = "Application started",
            Timestamp = DateTime.UtcNow,
            Tags = new[] { "startup", "info", "system" },
            AffectedIds = new[] { 1, 2, 3, 4, 5 }
        };

        context.EventLogs.Add(eventLog);
        await context.SaveChangesAsync();
        Console.WriteLine($"  CREATE: EventLog with {eventLog.Tags.Length} tags");

        var logsWithStartupTag = await context.EventLogs
            .Where(l => l.Tags.Contains("startup"))
            .ToListAsync();
        Console.WriteLine($"  Contains('startup'): {logsWithStartupTag.Count} logs");

        var tagCount = await context.EventLogs
            .Where(l => l.Tags.Length > 0)
            .Select(l => l.Tags.Length)
            .ToListAsync();
        Console.WriteLine($"  Array length queries: {tagCount.Count} logs with tags");

        Console.WriteLine();
    }

    private static async Task DemonstrateTransactions()
    {
        PrintSection("TRANSACTIONS");

        await using var context = new ShowcaseDbContext(DbPath);

        Console.WriteLine($"  Before transaction - CurrentTransaction: {(context.Database.CurrentTransaction == null ? "None" : "Active")}");

        await using var transaction = await context.Database.BeginTransactionAsync();
        Console.WriteLine($"  Transaction started - CurrentTransaction: {(context.Database.CurrentTransaction == null ? "None" : "Active")}");

        try
        {
            var customer = new Customer
            {
                FirstName = "Transaction",
                LastName = "Test",
                Email = "transaction@test.com",
                CreatedAt = DateTime.UtcNow
            };
            context.Customers.Add(customer);
            await context.SaveChangesAsync();

            customer.LoyaltyPoints = 500;
            await context.SaveChangesAsync();

            await transaction.CommitAsync();
            Console.WriteLine($"  Transaction committed - CurrentTransaction: {(context.Database.CurrentTransaction == null ? "None" : "Active")}");
        }
        catch
        {
            await transaction.RollbackAsync();
            Console.WriteLine($"  Transaction rolled back");
        }

        var customersInTx = await context.Customers
            .Where(c => c.Email == "transaction@test.com")
            .ToListAsync();
        Console.WriteLine($"  Verified customer exists: {customersInTx.Count > 0}");
        Console.WriteLine();
    }

    private static async Task DemonstrateConcurrencyControl()
    {
        PrintSection("CONCURRENCY CONTROL");

        await using var context = new ShowcaseDbContext(DbPath);

        var product = await context.Products.FirstOrDefaultAsync();
        if (product != null)
        {
            Console.WriteLine($"  Product Version (ConcurrencyCheck): {product.Version}");

            product.StockQuantity -= 1;
            product.Version++;
            await context.SaveChangesAsync();

            var updated = await context.Products.FindAsync(product.Id);
            Console.WriteLine($"  After update Version: {updated?.Version}");
        }

        Console.WriteLine();
    }

    private static async Task DemonstrateSchemaIntrospection()
    {
        PrintSection("SCHEMA INTROSPECTION");

        await using var context = new ShowcaseDbContext(DbPath);
        var connection = (DecentDBConnection)context.Database.GetDbConnection();
        connection.Open();

        var tablesJson = connection.ListTablesJson();
        Console.WriteLine($"  ListTablesJson(): {tablesJson}");

        var columnsJson = connection.GetTableColumnsJson("Products");
        Console.WriteLine($"  GetTableColumnsJson('Products'): {columnsJson[..Math.Min(200, columnsJson.Length)]}...");

        var indexesJson = connection.ListIndexesJson();
        Console.WriteLine($"  ListIndexesJson(): {indexesJson[..Math.Min(200, indexesJson.Length)]}...");

        var ddl = connection.GetTableDdl("Products");
        Console.WriteLine($"  GetTableDdl('Products'): {ddl[..Math.Min(100, ddl.Length)]}...");

        var viewsJson = connection.ListViewsJson();
        Console.WriteLine($"  ListViewsJson(): {viewsJson}");

        var triggersJson = connection.ListTriggersJson();
        Console.WriteLine($"  ListTriggersJson(): {triggersJson}");

        var ds = connection.GetSchema("Tables");
        Console.WriteLine($"  GetSchema('Tables'): {ds.Rows.Count} tables");

        var columnsDs = connection.GetSchema("Columns", new[] { "Products" });
        Console.WriteLine($"  GetSchema('Columns', ['Products']): {columnsDs.Rows.Count} columns");
        Console.WriteLine();
    }

    private static async Task DemonstrateRawSql()
    {
        PrintSection("RAW SQL EXECUTION");

        await using var context = new ShowcaseDbContext(DbPath);

        var products = await context.Products
            .FromSqlRaw("SELECT * FROM Products")
            .ToListAsync();
        Console.WriteLine($"  FromSqlRaw: {products.Count} products loaded");

        var productNames = products.Select(p => p.Name).ToList();
        Console.WriteLine($"  Products via raw SQL: {string.Join(", ", productNames)}");

        Console.WriteLine();
    }

    private static async Task DemonstrateChangeTracking()
    {
        PrintSection("CHANGE TRACKING");

        await using var context = new ShowcaseDbContext(DbPath);

        var product = await context.Products.FirstOrDefaultAsync();
        if (product != null)
        {
            Console.WriteLine($"  Original price: ${product.Price}");

            context.Entry(product).Property(p => p.Price).CurrentValue = 999.99m;
            context.Entry(product).State = EntityState.Modified;

            var modified = context.ChangeTracker.Entries()
                .Where(e => e.State == EntityState.Modified)
                .ToList();
            Console.WriteLine($"  Tracked modifications: {modified.Count} entities");

            var trackedProduct = context.ChangeTracker.Entries<Product>()
                .First(e => e.Entity.Id == product.Id);
            var originalPrice = trackedProduct.Property(p => p.Price).OriginalValue;
            var currentPrice = trackedProduct.Property(p => p.Price).CurrentValue;
            Console.WriteLine($"  Original value: ${originalPrice}");
            Console.WriteLine($"  Current value:  ${currentPrice}");
        }

        Console.WriteLine();
    }

    private static async Task DemonstrateBulkOperations()
    {
        PrintSection("BULK OPERATIONS");

        await using var context = new ShowcaseDbContext(DbPath);

        var stopwatch = Stopwatch.StartNew();

        var categories = Enumerable.Range(1, 100)
            .Select(i => new Category
            {
                Name = $"BulkCategory{i}",
                Description = $"Bulk category {i}",
                EffectiveFrom = DateOnly.FromDateTime(DateTime.UtcNow),
                DisplayOrder = i
            })
            .ToList();

        context.Categories.AddRange(categories);
        await context.SaveChangesAsync();
        stopwatch.Stop();

        Console.WriteLine($"  Bulk insert 100 categories: {stopwatch.ElapsedMilliseconds}ms");

        var tags = Enumerable.Range(1, 50)
            .Select(i => new Tag
            {
                Name = $"BulkTag{i}",
                IsSystem = false,
                UsageCount = 0,
                CreatedAt = DateTime.UtcNow
            })
            .ToList();

        context.Tags.AddRange(tags);
        await context.SaveChangesAsync();
        Console.WriteLine($"  Bulk insert 50 tags completed");

        var tagIds = tags.Select(t => t.Id).ToList();
        var deleted = await context.Tags
            .Where(t => tagIds.Contains(t.Id))
            .ExecuteDeleteAsync();
        Console.WriteLine($"  Bulk delete: {deleted} tags deleted");
        Console.WriteLine();
    }

    private static async Task DemonstrateLikePatternMatching()
    {
        PrintSection("PATTERN MATCHING (EF.Functions.Like)");

        await using var context = new ShowcaseDbContext(DbPath);

        var products = new[]
        {
            new Product { Name = "Laptop Pro", Description = "High-end laptop", Price = 1299.99m, StockQuantity = 10, CategoryId = 1, CreatedAt = DateTime.UtcNow },
            new Product { Name = "Laptop Air", Description = "Lightweight laptop", Price = 999.99m, StockQuantity = 15, CategoryId = 1, CreatedAt = DateTime.UtcNow },
            new Product { Name = "Desktop Tower", Description = "Powerful desktop", Price = 799.99m, StockQuantity = 5, CategoryId = 1, CreatedAt = DateTime.UtcNow },
            new Product { Name = "Tablet Pro", Description = "Professional tablet", Price = 699.99m, StockQuantity = 20, CategoryId = 1, CreatedAt = DateTime.UtcNow },
            new Product { Name = "Smartphone X", Description = "Latest smartphone", Price = 899.99m, StockQuantity = 25, CategoryId = 1, CreatedAt = DateTime.UtcNow },
        };
        context.Products.AddRange(products);
        await context.SaveChangesAsync();

        var startsWithLaptop = await context.Products
            .Where(p => EF.Functions.Like(p.Name, "Laptop%"))
            .Select(p => p.Name)
            .ToListAsync();
        Console.WriteLine($"  StartsWith 'Laptop%': {string.Join(", ", startsWithLaptop)}");

        var endsWithPro = await context.Products
            .Where(p => EF.Functions.Like(p.Name, "%Pro"))
            .Select(p => p.Name)
            .ToListAsync();
        Console.WriteLine($"  EndsWith '%Pro': {string.Join(", ", endsWithPro)}");

        var containsPad = await context.Products
            .Where(p => EF.Functions.Like(p.Name, "%Pad%"))
            .Select(p => p.Name)
            .ToListAsync();
        Console.WriteLine($"  Contains '%Pad%': {string.Join(", ", containsPad)}");

        var secondCharIs = await context.Products
            .Where(p => EF.Functions.Like(p.Name, "_e%"))
            .Select(p => p.Name)
            .ToListAsync();
        Console.WriteLine($"  Second char 'e' (_e%): {string.Join(", ", secondCharIs)}");

        var notLaptop = await context.Products
            .Where(p => !EF.Functions.Like(p.Name, "Laptop%"))
            .Select(p => p.Name)
            .ToListAsync();
        Console.WriteLine($"  NOT StartsWith 'Laptop%': {string.Join(", ", notLaptop)}");
        Console.WriteLine();
    }

    private static async Task DemonstrateSetOperations()
    {
        PrintSection("SET OPERATIONS (Union, Concat, Intersect, Except)");

        await using var context = new ShowcaseDbContext(DbPath);

        var expensiveProducts = await context.Products
            .Where(p => (double)p.Price > 800.0)
            .Select(p => p.Name)
            .ToListAsync();

        var inStockProducts = await context.Products
            .Where(p => p.StockQuantity > 10)
            .Select(p => p.Name)
            .ToListAsync();

        var union = expensiveProducts.Union(inStockProducts).ToList();
        Console.WriteLine($"  UNION (expensive OR in stock): {union.Count} - {string.Join(", ", union)}");

        var concat = expensiveProducts.Concat(inStockProducts).ToList();
        Console.WriteLine($"  CONCAT (all items, duplicates): {concat.Count} items");

        var intersect = expensiveProducts.Intersect(inStockProducts).ToList();
        Console.WriteLine($"  INTERSECT (expensive AND in stock): {intersect.Count} - {string.Join(", ", intersect)}");

        var except = expensiveProducts.Except(inStockProducts).ToList();
        Console.WriteLine($"  EXCEPT (expensive but NOT in stock): {except.Count} - {string.Join(", ", except)}");

        var distinct = (await context.Products.Select(p => p.Name).ToListAsync()).Distinct().ToList();
        Console.WriteLine($"  DISTINCT names: {distinct.Count} unique");
        Console.WriteLine();
    }

    private static async Task DemonstrateExplicitJoins()
    {
        PrintSection("EXPLICIT JOIN QUERIES");

        await using var context = new ShowcaseDbContext(DbPath);

        var category1 = new Category { Name = "Electronics", Description = "Electronic items", DisplayOrder = 1, EffectiveFrom = DateOnly.FromDateTime(DateTime.UtcNow) };
        var category2 = new Category { Name = "Accessories", Description = "Accessory items", DisplayOrder = 2, EffectiveFrom = DateOnly.FromDateTime(DateTime.UtcNow) };
        context.Categories.AddRange(category1, category2);
        await context.SaveChangesAsync();

        var product1 = new Product { Name = "Mouse", Description = "Wireless mouse", Price = 29.99m, StockQuantity = 100, CategoryId = category2.Id, CreatedAt = DateTime.UtcNow };
        var product2 = new Product { Name = "Keyboard", Description = "Mechanical keyboard", Price = 89.99m, StockQuantity = 50, CategoryId = category2.Id, CreatedAt = DateTime.UtcNow };
        context.Products.AddRange(product1, product2);
        await context.SaveChangesAsync();

        var customer = new Customer { FirstName = "Join", LastName = "Test", Email = "join@test.com", CreatedAt = DateTime.UtcNow };
        context.Customers.Add(customer);
        await context.SaveChangesAsync();

        var innerJoin = await context.Products
            .Join(context.Categories,
                p => p.CategoryId,
                c => c.Id,
                (p, c) => new { ProductName = p.Name, CategoryName = c.Name })
            .ToListAsync();
        Console.WriteLine($"  INNER JOIN (products + categories): {innerJoin.Count} results");
        foreach (var item in innerJoin.Take(3))
            Console.WriteLine($"    - {item.ProductName} -> {item.CategoryName}");

        var multiJoin = await context.Orders
            .Join(context.Customers, o => o.CustomerId, c => c.Id, (o, c) => new { o, c })
            .Join(context.Addresses, x => x.o.ShippingAddressId, a => a.Id, (x, a) => new { Customer = x.c, Order = x.o, Address = a })
            .Select(x => new { CustomerName = x.Customer.FirstName, City = x.Address.City, OrderTotal = x.Order.TotalAmount })
            .ToListAsync();
        Console.WriteLine($"  MULTI-JOIN (orders + customers + addresses): {multiJoin.Count} results");
        Console.WriteLine();
    }

    private static async Task DemonstrateSubqueries()
    {
        PrintSection("SUBQUERIES");

        await using var context = new ShowcaseDbContext(DbPath);

        var avgPrice = context.Products.Average(p => p.Price);
        var aboveAverageProducts = await context.Products
            .Where(p => p.Price > avgPrice)
            .Select(p => new { p.Name, p.Price })
            .ToListAsync();
        Console.WriteLine($"  Scalar subquery (products above avg ${avgPrice:N2}): {aboveAverageProducts.Count}");
        foreach (var p in aboveAverageProducts)
            Console.WriteLine($"    - {p.Name}: ${p.Price:N2}");

        var categoryIds = await context.Categories.Where(c => c.Name == "Electronics").Select(c => c.Id).ToListAsync();
        var productsInElectronics = await context.Products
            .Where(p => categoryIds.Contains(p.CategoryId))
            .Select(p => p.Name)
            .ToListAsync();
        Console.WriteLine($"  Subquery with Contains (Electronics products): {productsInElectronics.Count}");

        var customersWithOrders = await context.Customers
            .Where(c => context.Orders.Any(o => o.CustomerId == c.Id))
            .Select(c => c.FirstName)
            .ToListAsync();
        Console.WriteLine($"  Correlated subquery (customers with orders): {customersWithOrders.Count}");

        var customersWithoutOrders = await context.Customers
            .Where(c => !context.Orders.Any(o => o.CustomerId == c.Id))
            .Select(c => c.FirstName)
            .ToListAsync();
        Console.WriteLine($"  Correlated subquery (customers WITHOUT orders): {customersWithoutOrders.Count}");

        var productsWithHighStock = await context.Products
            .Where(p => p.StockQuantity > context.Products.Where(p2 => p2.CategoryId == p.CategoryId).Average(p2 => p2.StockQuantity))
            .Select(p => new { p.Name, p.StockQuantity, p.CategoryId })
            .ToListAsync();
        Console.WriteLine($"  Correlated subquery (above avg stock per category): {productsWithHighStock.Count}");

        var topCategoryByProducts = await context.Categories
            .OrderByDescending(c => context.Products.Count(p => p.CategoryId == c.Id))
            .Select(c => c.Name)
            .FirstOrDefaultAsync();
        Console.WriteLine($"  Subquery in OrderBy: Top category = {topCategoryByProducts}");
        Console.WriteLine();
    }

    private static async Task DemonstrateIncludeThenInclude()
    {
        PrintSection("INCLUDE / THENINCLUDE (Relationship Loading)");

        await using var context = new ShowcaseDbContext(DbPath);

        var category = new Category { Name = "IncludeTest", Description = "For include demos", DisplayOrder = 99, EffectiveFrom = DateOnly.FromDateTime(DateTime.UtcNow) };
        context.Categories.Add(category);
        await context.SaveChangesAsync();

        var product1 = new Product { Name = "IncProduct1", Description = "Test", Price = 10m, StockQuantity = 5, CategoryId = category.Id, CreatedAt = DateTime.UtcNow };
        var product2 = new Product { Name = "IncProduct2", Description = "Test", Price = 20m, StockQuantity = 10, CategoryId = category.Id, CreatedAt = DateTime.UtcNow };
        context.Products.AddRange(product1, product2);
        await context.SaveChangesAsync();

        var customer = new Customer { FirstName = "Inc", LastName = "Customer", Email = "include@test.com", CreatedAt = DateTime.UtcNow };
        context.Customers.Add(customer);
        await context.SaveChangesAsync();

        var order = new Order { OrderNumber = "INC-001", CustomerId = customer.Id, OrderDate = DateTime.UtcNow, TotalAmount = 50m, Status = OrderStatus.Pending, CreatedAt = DateTime.UtcNow };
        context.Orders.Add(order);
        await context.SaveChangesAsync();

        var orderItem1 = new OrderItem { OrderId = order.Id, ProductId = product1.Id, UnitPrice = 10m, Quantity = 2, Discount = 0, CreatedAt = DateTime.UtcNow };
        var orderItem2 = new OrderItem { OrderId = order.Id, ProductId = product2.Id, UnitPrice = 20m, Quantity = 1, Discount = 0, CreatedAt = DateTime.UtcNow };
        context.OrderItems.AddRange(orderItem1, orderItem2);
        await context.SaveChangesAsync();

        var productsWithCategoryViaJoin = await context.Products
            .Join(context.Categories, p => p.CategoryId, c => c.Id, (p, c) => new { Product = p, Category = c })
            .Where(x => x.Product.Name.StartsWith("Inc"))
            .Select(x => new { ProductName = x.Product.Name, CategoryName = x.Category.Name })
            .ToListAsync();
        Console.WriteLine($"  JOIN (Product -> Category): {productsWithCategoryViaJoin.Count} products");
        foreach (var item in productsWithCategoryViaJoin)
            Console.WriteLine($"    {item.ProductName} -> {item.CategoryName}");

        var ordersWithItemsViaJoin = await context.Orders
            .Join(context.OrderItems, o => o.Id, oi => oi.OrderId, (o, oi) => new { Order = o, OrderItem = oi })
            .Join(context.Products, x => x.OrderItem.ProductId, p => p.Id, (x, p) => new { OrderNumber = x.Order.OrderNumber, ProductName = p.Name, Quantity = x.OrderItem.Quantity })
            .Where(x => x.OrderNumber == "INC-001")
            .ToListAsync();
        Console.WriteLine($"  MULTI-JOIN (Order -> OrderItem -> Product): {ordersWithItemsViaJoin.Count} items");
        foreach (var item in ordersWithItemsViaJoin)
            Console.WriteLine($"    Order {item.OrderNumber}: {item.Quantity}x {item.ProductName}");

        Console.WriteLine("  Note: Navigation properties removed to avoid FK constraints.");
        Console.WriteLine("        Use explicit JOINs for relationship queries instead.");
        Console.WriteLine();
    }

    private static async Task DemonstrateExistenceAndChildFilters()
    {
        PrintSection("EXISTENCE & CHILD FILTERS");

        await using var context = new ShowcaseDbContext(DbPath);

        var parentWithChildren = await context.Categories
            .Where(c => context.Products.Any(p => p.CategoryId == c.Id))
            .Select(c => c.Name)
            .ToListAsync();
        Console.WriteLine($"  Categories WITH products: {parentWithChildren.Count} - {string.Join(", ", parentWithChildren)}");

        var parentWithoutChildren = await context.Categories
            .Where(c => !context.Products.Any(p => p.CategoryId == c.Id))
            .Select(c => c.Name)
            .ToListAsync();
        Console.WriteLine($"  Categories WITHOUT products: {parentWithoutChildren.Count}");

        var productsWithOrders = await context.Products
            .Where(p => context.OrderItems.Any(oi => oi.ProductId == p.Id))
            .Select(p => p.Name)
            .ToListAsync();
        Console.WriteLine($"  Products that have been ordered: {productsWithOrders.Count}");

        var productsNeverOrdered = await context.Products
            .Where(p => !context.OrderItems.Any(oi => oi.ProductId == p.Id))
            .Select(p => p.Name)
            .ToListAsync();
        Console.WriteLine($"  Products NEVER ordered: {productsNeverOrdered.Count}");

        var categoriesWithManyProducts = await context.Categories
            .Where(c => context.Products.Count(p => p.CategoryId == c.Id) > 2)
            .Select(c => new { Category = c.Name, Count = context.Products.Count(p => p.CategoryId == c.Id) })
            .ToListAsync();
        Console.WriteLine($"  Categories with >2 products: {categoriesWithManyProducts.Count}");

        var customersWithOrderCount = await context.Customers
            .Select(c => new { Name = c.FirstName + " " + c.LastName, OrderCount = context.Orders.Count(o => o.CustomerId == c.Id) })
            .ToListAsync();
        Console.WriteLine($"  Customer order counts: {customersWithOrderCount.Count} customers");

        var customersWithHighValueOrders = await context.Customers
            .Where(c => context.Orders.Any(o => o.CustomerId == c.Id && o.TotalAmount > 100))
            .Select(c => c.FirstName)
            .ToListAsync();
        Console.WriteLine($"  Customers with orders > $100: {customersWithHighValueOrders.Count}");
        Console.WriteLine();
    }

    private static async Task DemonstrateConditionalLogic()
    {
        PrintSection("CONDITIONAL LOGIC (Ternary, Coalesce)");

        await using var context = new ShowcaseDbContext(DbPath);

        var priceCategories = await context.Products
            .Select(p => new
            {
                p.Name,
                p.Price,
                Category = p.Price > 500 ? "Premium" : "Standard"
            })
            .ToListAsync();
        Console.WriteLine($"  Ternary operator (price categories): {priceCategories.Count}");
        foreach (var p in priceCategories.Take(3))
            Console.WriteLine($"    {p.Name}: ${p.Price} -> {p.Category}");

        var productsWithDiscount = await context.Products
            .Select(p => new
            {
                p.Name,
                p.Price,
                Description = p.Description ?? "(No description)"
            })
            .ToListAsync();
        Console.WriteLine($"  Null coalescing (default description): {productsWithDiscount.Count}");

        var addressDisplay = await context.Customers
            .Select(c => new
            {
                Name = c.FirstName,
                Phone = c.Phone ?? "N/A"
            })
            .ToListAsync();
        Console.WriteLine($"  Coalesce with string fallback: {addressDisplay.Count}");

        var complexConditional = await context.Products
            .Select(p => new
            {
                p.Name,
                p.Price,
                p.StockQuantity,
                Status = p.StockQuantity == 0 ? "OutOfStock" :
                         p.StockQuantity < 10 ? "LowStock" : "InStock"
            })
            .ToListAsync();
        Console.WriteLine($"  Complex conditional: {complexConditional.Count} products");
        Console.WriteLine();
    }

    private static async Task DemonstrateQueryComposition()
    {
        PrintSection("QUERY COMPOSITION (Reusable IQueryable)");

        await using var context = new ShowcaseDbContext(DbPath);

        IQueryable<Product> ActiveProducts() => context.Products.Where(p => p.IsActive);
        IQueryable<Product> InStock() => context.Products.Where(p => p.StockQuantity > 0);
        IQueryable<Product> AbovePrice(decimal price) => context.Products.Where(p => p.Price > price);

        var active = await ActiveProducts().CountAsync();
        Console.WriteLine($"  Composed: Active products: {active}");

        var inStock = await InStock().CountAsync();
        Console.WriteLine($"  Composed: In-stock products: {inStock}");

        var above500 = await AbovePrice(500m).CountAsync();
        Console.WriteLine($"  Composed: Above $500: {above500}");

        var combined = await ActiveProducts()
            .Intersect(InStock())
            .Intersect(AbovePrice(100m))
            .CountAsync();
        Console.WriteLine($"  Composed: Active + InStock + Above $100: {combined}");

        IQueryable<T> ApplyFilters<T>(IQueryable<T> query, bool filterActive, bool filterInStock) where T : Product
        {
            if (filterActive) query = query.Where(p => p.IsActive);
            if (filterInStock) query = query.Where(p => p.StockQuantity > 0);
            return query;
        }

        var filtered1 = await ApplyFilters(context.Products.AsQueryable(), true, false).CountAsync();
        var filtered2 = await ApplyFilters(context.Products.AsQueryable(), true, true).CountAsync();
        Console.WriteLine($"  Conditional filter (active): {filtered1}");
        Console.WriteLine($"  Conditional filter (active+instock): {filtered2}");
        Console.WriteLine();
    }

    private static async Task DemonstrateSelectMany()
    {
        PrintSection("SELECTMANY (Flatten Collections)");

        await using var context = new ShowcaseDbContext(DbPath);

        var allOrderItems = await context.Orders
            .Join(context.OrderItems, o => o.Id, oi => oi.OrderId, (o, oi) => new { OrderNumber = o.OrderNumber, OrderItem = oi })
            .Take(10)
            .ToListAsync();
        Console.WriteLine($"  SelectMany (flatten order items): {allOrderItems.Count} items");

        var customerOrderItems = await context.Customers
            .Join(context.Customers, c => c.Id, c2 => c2.Id, (c, c2) => c)
            .SelectMany(c => context.Orders.Where(o => o.CustomerId == c.Id), (c, o) => new { Customer = c.FirstName, OrderId = o.Id })
            .Take(10)
            .ToListAsync();
        Console.WriteLine($"  SelectMany (customers -> orders): {customerOrderItems.Count} records");

        var productWithTags = await context.Products
            .Join(context.ProductTags, p => p.Id, pt => pt.ProductId, (p, pt) => new { Product = p.Name, TagId = pt.TagId })
            .Take(10)
            .ToListAsync();
        Console.WriteLine($"  SelectMany (products -> tags): {productWithTags.Count} records");

        var allProductsFlat = await context.Categories
            .SelectMany(c => context.Products.Where(p => p.CategoryId == c.Id), (c, p) => new { Category = c.Name, Product = p.Name })
            .Take(10)
            .ToListAsync();
        Console.WriteLine($"  SelectMany (categories -> products): {allProductsFlat.Count} records");
        Console.WriteLine();
    }

    private static async Task DemonstrateClientVsServerEvaluation()
    {
        PrintSection("CLIENT VS SERVER EVALUATION");

        await using var context = new ShowcaseDbContext(DbPath);

        Console.WriteLine("  Server-evaluated (translates to SQL):");
        var serverWhere = await context.Products.Where(p => p.Price > 100).CountAsync();
        Console.WriteLine($"    Where(p => p.Price > 100): {serverWhere} (SQL WHERE)");

        var serverSelect = await context.Products.Select(p => p.Name.ToUpper()).Take(3).ToListAsync();
        Console.WriteLine($"    Select(p => p.Name.ToUpper()): {string.Join(", ", serverSelect)} (SQL UPPER)");

        Console.WriteLine("  Mixed evaluation examples:");
        var products = await context.Products.ToListAsync();
        var clientFiltered = products.Where(p => p.Price > MethodThatCannotBeTranslated()).ToList();
        Console.WriteLine($"    Complex method in Where triggers client eval: {clientFiltered.Count} (after ToList)");

        Console.WriteLine("  Avoiding client eval:");
        var serverSide = await context.Products
            .Where(p => p.Price > 100 && p.Name.Contains("Laptop"))
            .ToListAsync();
        Console.WriteLine($"    Fully server-evaluated: {serverSide.Count} products");

        Console.WriteLine("  Note: Complex .NET methods like string.Join, DateTime.Now, custom");
        Console.WriteLine("        methods require client evaluation after data is fetched.");
        Console.WriteLine();
    }

    private static decimal MethodThatCannotBeTranslated() => 100m;

    private static async Task DemonstrateUnsupportedCases()
    {
        PrintSection("UNSUPPORTED / PROBLEMATIC CASES");

        await using var context = new ShowcaseDbContext(DbPath);

        Console.WriteLine("  Known limitations in DecentDB EF Core provider:");
        Console.WriteLine();

        Console.WriteLine("  1. Decimal comparisons in ranges:");
        try
        {
            var decimalRange = await context.Products
                .Where(p => p.Price >= 100 && p.Price <= 500)
                .Select(p => p.Name)
                .ToListAsync();
            Console.WriteLine($"     Decimal range works: {decimalRange.Count} products");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"     Decimal range issue: {ex.Message}");
        }

        Console.WriteLine("  2. DateTime.Now (non-deterministic):");
        Console.WriteLine("     Using DateTime.UtcNow instead of DateTime.Now recommended");

        Console.WriteLine("  3. Composite primary keys:");
        Console.WriteLine("     DecentDB doesn't support composite PKs - use single bigint PK");

        Console.WriteLine("  4. Foreign key constraints:");
        Console.WriteLine("     Only column-level FKs supported, not table-level constraints");

        Console.WriteLine("  5. Window functions (limited):");
        try
        {
            var ranked = await context.Products
                .OrderByDescending(p => p.Price)
                .Take(5)
                .Select(p => p.Name)
                .ToListAsync();
            Console.WriteLine($"     Equivalent to ROW_NUMBER: {ranked.Count} products");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"     Window function issue: {ex.Message}");
        }

        Console.WriteLine("  Workarounds demonstrated:");
        Console.WriteLine("    - Use decimal ranges with explicit AND conditions");
        Console.WriteLine("    - Use client-side ordering for complex window functions");
        Console.WriteLine("    - Handle FK enforcement at application level");
        Console.WriteLine();
    }

    private static async Task DemonstratePerformancePatterns()
    {
        PrintSection("PERFORMANCE PATTERNS");

        await using var context = new ShowcaseDbContext(DbPath);

        Console.WriteLine("  1. Projection vs Include (avoid over-fetching):");
        var projectedNames = await context.Products
            .Where(p => p.Price > 100)
            .Select(p => new { p.Name, p.Price })
            .Take(5)
            .ToListAsync();
        Console.WriteLine($"     Projected only: {projectedNames.Count} records (minimal data)");

        var withInclude = await context.Products
            .Take(5)
            .ToListAsync();
        Console.WriteLine($"     Without Include: {withInclude.Count} records (FK nav props removed)");

        Console.WriteLine("  2. AsNoTracking for read-only:");
        var noTracking = await context.Products
            .AsNoTracking()
            .Where(p => p.Price > 100)
            .Take(5)
            .ToListAsync();
        Console.WriteLine($"     AsNoTracking: {noTracking.Count} records (no change tracking)");

        Console.WriteLine("  3. Split queries (reduces cartesian products):");
        var splitQuery = await context.Products
            .AsSplitQuery()
            .Take(3)
            .ToListAsync();
        Console.WriteLine($"     Split query: {splitQuery.Count} products (FK nav props removed)");

        Console.WriteLine("  4. Keyset pagination (efficient than offset):");
        var lastPrice = 0m;
        var keysetPage = await context.Products
            .OrderBy(p => p.Price)
            .Where(p => p.Price > lastPrice)
            .Take(3)
            .Select(p => new { p.Name, p.Price })
            .ToListAsync();
        Console.WriteLine($"     Keyset pagination: {keysetPage.Count} records");

        Console.WriteLine("  5. Batch size optimization:");
        Console.WriteLine("     Use AddRange with reasonable batch sizes (100-1000)");
        Console.WriteLine();
    }

    private static void PrintSection(string title)
    {
        Console.WriteLine();
        Console.WriteLine($"═══════════════════════════════════════════════════════════════════════════════════");
        Console.WriteLine($"  {title}");
        Console.WriteLine($"═══════════════════════════════════════════════════════════════════════════════════");
    }

    private static void CleanupDatabase()
    {
        Console.WriteLine();
        Console.WriteLine("═══════════════════════════════════════════════════════════════════════════════════");
        Console.WriteLine("  CLEANUP");
        Console.WriteLine("═══════════════════════════════════════════════════════════════════════════════════");

        try
        {
            if (File.Exists(DbPath))
            {
                File.Delete(DbPath);
                Console.WriteLine($"  Deleted database file: {DbPath}");
            }

            if (File.Exists($"{DbPath}-wal"))
            {
                File.Delete($"{DbPath}-wal");
                Console.WriteLine($"  Deleted WAL file: {DbPath}-wal");
            }

            if (File.Exists($"{DbPath}-shm"))
            {
                File.Delete($"{DbPath}-shm");
                Console.WriteLine($"  Deleted SHM file: {DbPath}-shm");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  Cleanup warning: {ex.Message}");
        }

        Console.WriteLine();
        Console.WriteLine("═══════════════════════════════════════════════════════════════════════════════════");
        Console.WriteLine("  SHOWCASE COMPLETE");
        Console.WriteLine("═══════════════════════════════════════════════════════════════════════════════════");
    }
}
