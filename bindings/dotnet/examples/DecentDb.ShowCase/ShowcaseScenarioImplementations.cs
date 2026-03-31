using System.Data;
using System.Diagnostics;
using System.Text;
using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using DecentDb.ShowCase.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using NodaTime;

namespace DecentDb.ShowCase;

internal static partial class ShowcaseScenarioCatalog
{
    private static async Task DemonstrateDecentDBMetadata(ShowcaseScenarioContext scenario)
    {

        var abiVersion = DecentDBConnection.AbiVersion();
        var engineVersion = DecentDBConnection.EngineVersion();

        scenario.WriteLine($"  ABI Version:        {abiVersion}");
        scenario.WriteLine($"  Engine Version:      {engineVersion}");
        scenario.WriteLine($"  Database Path:       {scenario.DbPath}");
        scenario.WriteLine();
    }
    private static async Task DemonstrateDatabaseOperations(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();
        await context.Database.EnsureCreatedAsync();

        scenario.WriteLine($"  Database created/verified at: {scenario.DbPath}");
        scenario.WriteLine();
    }

    private static async Task DemonstrateMigrationsAndSchemaLifecycle(ShowcaseScenarioContext scenario)
    {
        var migrationDbPath = scenario.CreateAuxiliaryDatabasePath("migrations");
        MigrationShowcaseSeed.SeedInitialState(migrationDbPath);

        IReadOnlyList<string> appliedMigrations;
        await using (var context = new MigrationShowcaseDbContext(migrationDbPath))
        {
            await context.Database.MigrateAsync();
            appliedMigrations = (await context.Database.GetAppliedMigrationsAsync()).ToList();
        }

        using var conn = scenario.CreateOpenConnection(migrationDbPath);
        var columnsJson = conn.GetTableColumnsJson("migration_contacts");
        var displayName = MigrationShowcaseSql.ExecuteScalar(
            conn,
            """
            SELECT "display_name"
            FROM "migration_contacts"
            WHERE "id" = 1
            """);
        var slug = MigrationShowcaseSql.ExecuteScalar(
            conn,
            """
            SELECT "slug"
            FROM "migration_contacts"
            WHERE "id" = 1
            """);

        scenario.WriteLine(
            $"  Applied migrations: {string.Join(" -> ", appliedMigrations.Select(MigrationShowcaseSql.ShortMigrationName))}");
        scenario.WriteLine(
            $"  Renamed table + evolved columns: {columnsJson.Contains("display_name", StringComparison.Ordinal) && columnsJson.Contains("slug", StringComparison.Ordinal)}");
        scenario.WriteLine($"  Existing contact preserved: {displayName} / slug={slug}");
        scenario.WriteLine(
            $"  FK rows preserved: nodes={MigrationShowcaseSql.ExecuteScalar(conn, "SELECT COUNT(*) FROM \"migration_nodes\"")}, location counts={MigrationShowcaseSql.ExecuteScalar(conn, "SELECT COUNT(*) FROM \"migration_location_counts\"")}");

        MigrationShowcaseSql.ExecuteNonQuery(
            conn,
            """
            INSERT INTO "migration_contacts" ("id", "display_name", "slug")
            VALUES (2, 'Grace Hopper', 'pending')
            """);

        scenario.WriteLine(
            $"  Latest follow-up removed transient uniqueness/indexing: {!conn.ListIndexesJson().Contains("IX_migration_contacts_display_name", StringComparison.Ordinal)}");
        scenario.WriteLine("  Duplicate slug insert now succeeds after the final cleanup migration");
        scenario.WriteLine();
    }

    private static async Task DemonstrateAdvancedModeling(ShowcaseScenarioContext scenario)
    {
        var advancedDbPath = scenario.CreateAuxiliaryDatabasePath("advanced-modeling");

        await using var context = new AdvancedModelingShowcaseDbContext(advancedDbPath);
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var customer = new AdvancedModelingShowcaseCustomer
        {
            Name = "Ada Lovelace",
            Profile = new AdvancedModelingShowcaseProfile
            {
                Email = "ada@example.com",
                City = "London"
            }
        };

        var efTag = new AdvancedModelingShowcaseTag { Name = "efcore" };
        var dbTag = new AdvancedModelingShowcaseTag { Name = "database" };
        var videoTag = new AdvancedModelingShowcaseTag { Name = "video" };

        var article = new AdvancedModelingShowcaseArticle
        {
            Title = "EF Modeling",
            WordCount = 1200,
            Tags = [efTag, dbTag]
        };

        var video = new AdvancedModelingShowcaseVideo
        {
            Title = "DecentDB Deep Dive",
            DurationSeconds = 600,
            Tags = [dbTag, videoTag]
        };

        context.Customers.Add(customer);
        context.Documents.Add(new AdvancedModelingShowcaseDocument { Title = "Release Checklist", Revision = 1 });
        context.ContentItems.AddRange(article, video);
        context.Entry(customer).Property("priority").CurrentValue = 7;
        context.Entry(article).Property("published_order").CurrentValue = 1;
        context.Entry(video).Property("published_order").CurrentValue = 2;
        context.Entry(article).Property("etag").CurrentValue = 1;
        context.Entry(video).Property("etag").CurrentValue = 1;
        await context.SaveChangesAsync();

        var loadedCustomer = await context.Customers
            .Where(x => x.Profile.City == "London" && EF.Property<int>(x, "priority") == 7)
            .SingleAsync();

        var orderedContent = await context.ContentItems
            .OrderBy(x => EF.Property<int>(x, "published_order"))
            .Select(x => new
            {
                x.Title,
                Kind = EF.Property<string>(x, "content_kind")
            })
            .ToListAsync();

        var loadedArticle = await context.Articles
            .Include(x => x.Tags)
            .SingleAsync();

        var projections = await context.ContentProjections
            .FromSqlRaw("""
                        SELECT "title" AS "Title", "content_kind" AS "ContentKind"
                        FROM "adv_content_items"
                        ORDER BY "title"
                        """)
            .ToListAsync();

        var documentEntity = context.Model.FindEntityType(typeof(AdvancedModelingShowcaseDocument));
        var contentEntity = context.Model.FindEntityType(typeof(AdvancedModelingShowcaseContentItem));

        scenario.WriteLine(
            $"  Owned type + shadow property: {loadedCustomer.Name} / {loadedCustomer.Profile.City} / priority={context.Entry(loadedCustomer).Property<int>("priority").CurrentValue}");
        scenario.WriteLine(
            $"  TPH inheritance: {string.Join(", ", orderedContent.Select(x => $"{x.Title}={x.Kind}"))}");
        scenario.WriteLine(
            $"  Skip-navigation many-to-many: {loadedArticle.Title} tags => {string.Join(", ", loadedArticle.Tags.Select(x => x.Name).OrderBy(x => x))}");
        scenario.WriteLine(
            $"  Keyless raw SQL: {string.Join(", ", projections.Select(x => $"{x.Title}={x.ContentKind}"))}");
        scenario.WriteLine(
            $"  Concurrency tokens: [ConcurrencyCheck]={documentEntity!.FindProperty(nameof(AdvancedModelingShowcaseDocument.Revision))!.IsConcurrencyToken}, shadow={contentEntity!.FindProperty("etag")!.IsConcurrencyToken}");
        scenario.WriteLine();
    }
    private static async Task DemonstrateEFCoreBasicCRUD(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

        var category = ShowcaseSeeder.CreateCategory(
            name: "Electronics",
            displayOrder: 1,
            description: "Electronic devices and accessories",
            effectiveFrom: DateOnly.FromDateTime(DateTime.UtcNow),
            businessHoursStart: new TimeOnly(9, 0));

        context.Categories.Add(category);
        await context.SaveChangesAsync();
        scenario.WriteLine($"  CREATE: Category '{category.Name}' created with ID {category.Id}");

        category.Description = "Updated: Electronic devices, gadgets, and accessories";
        await context.SaveChangesAsync();
        scenario.WriteLine($"  UPDATE: Category description updated");

        var retrieved = await context.Categories.FindAsync(category.Id);
        scenario.WriteLine($"  READ:   Retrieved category: {retrieved?.Name}");

        context.Categories.Remove(category);
        await context.SaveChangesAsync();
        scenario.WriteLine($"  DELETE: Category '{category.Name}' deleted");
        scenario.WriteLine();
    }
    private static async Task DemonstrateNullableComparisons(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

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
        scenario.WriteLine($"  String == null: {nullEmailCustomers.Count} customers");

        var notNullEmailCustomers = await context.Customers
            .Where(c => c.Email != null)
            .Select(c => c.FirstName)
            .ToListAsync();
        scenario.WriteLine($"  String != null: {notNullEmailCustomers.Count} customers");

        var nullPhoneCustomers = await context.Customers
            .Where(c => c.Phone == null)
            .Select(c => c.FirstName)
            .ToListAsync();
        scenario.WriteLine($"  Nullable string == null: {nullPhoneCustomers.Count} customers");

        var notNullPhoneCustomers = await context.Customers
            .Where(c => c.Phone != null)
            .Select(c => c.FirstName)
            .ToListAsync();
        scenario.WriteLine($"  Nullable string != null: {notNullPhoneCustomers.Count} customers");

        var nullDateCustomers = await context.Customers
            .Where(c => c.LastPurchaseDate == null)
            .Select(c => c.FirstName)
            .ToListAsync();
        scenario.WriteLine($"  Nullable DateTime == null: {nullDateCustomers.Count} customers");

        var notNullDateCustomers = await context.Customers
            .Where(c => c.LastPurchaseDate != null)
            .Select(c => c.FirstName)
            .ToListAsync();
        scenario.WriteLine($"  Nullable DateTime != null: {notNullDateCustomers.Count} customers");

        var nullDecimalCustomers = await context.Customers
            .Where(c => c.TotalSpend == null)
            .Select(c => c.FirstName)
            .ToListAsync();
        scenario.WriteLine($"  Nullable decimal == null: {nullDecimalCustomers.Count} customers");

        var notNullDecimalCustomers = await context.Customers
            .Where(c => c.TotalSpend != null)
            .Select(c => c.FirstName)
            .ToListAsync();
        scenario.WriteLine($"  Nullable decimal != null: {notNullDecimalCustomers.Count} customers");

        scenario.WriteLine();
    }
    private static async Task DemonstrateLinqQueries(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

        var category = ShowcaseSeeder.CreateCategory(
            name: "Tech",
            displayOrder: 1,
            description: "Technology",
            effectiveFrom: DateOnly.FromDateTime(DateTime.UtcNow));
        context.Categories.Add(category);
        await context.SaveChangesAsync();

        var product = ShowcaseSeeder.CreateProduct(
            name: "Laptop Pro",
            description: "High-performance laptop",
            price: 1299.99m,
            stockQuantity: 50,
            categoryId: category.Id,
            createdAt: DateTime.UtcNow,
            sku: Guid.NewGuid(),
            weight: 2.5m);
        context.Products.Add(product);
        await context.SaveChangesAsync();

        var allProducts = await context.Products.ToListAsync();
        scenario.WriteLine($"  COUNT:    Total products: {allProducts.Count}");

        var expensiveProducts = await context.Products
            .Where(p => p.Price > 1000m)
            .ToListAsync();
        scenario.WriteLine($"  FILTER:   Products > $1000: {expensiveProducts.Count}");

        var orderedProducts = await context.Products
            .OrderByDescending(p => p.Price)
            .Take(5)
            .Select(p => new { p.Name, p.Price })
            .ToListAsync();
        scenario.WriteLine($"  ORDER BY: Top 5 most expensive:");
        foreach (var p in orderedProducts)
            scenario.WriteLine($"            - {p.Name}: ${p.Price:N2}");

        var paginatedProducts = await context.Products
            .Skip(0)
            .Take(3)
            .ToListAsync();
        scenario.WriteLine($"  PAGINATION: First 3 products (skip 0, take 3): {paginatedProducts.Count} items");

        var groupByCategory = await context.Products
            .GroupBy(p => p.CategoryId)
            .Select(g => new { CategoryId = g.Key, Count = g.Count() })
            .ToListAsync();
        scenario.WriteLine($"  GROUP BY: Products per category: {groupByCategory.Count} groups");

        var distinctCategories = await context.Categories
            .Select(c => c.Name)
            .Distinct()
            .ToListAsync();
        scenario.WriteLine($"  DISTINCT: {distinctCategories.Count} distinct category names");
        scenario.WriteLine();
    }
    private static async Task DemonstrateStringOperations(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

        var containsProducts = await context.Products
            .Where(p => p.Name.Contains("Laptop"))
            .Select(p => p.Name)
            .ToListAsync();
        scenario.WriteLine($"  Contains('Laptop'): {containsProducts.Count} products");

        var startsWithProducts = await context.Products
            .Where(p => p.Name.StartsWith("Laptop"))
            .Select(p => p.Name)
            .ToListAsync();
        scenario.WriteLine($"  StartsWith('Laptop'): {startsWithProducts.Count} products");

        var upperNames = await context.Products
            .Take(2)
            .Select(p => p.Name.ToUpper())
            .ToListAsync();
        scenario.WriteLine($"  ToUpper(): {string.Join(", ", upperNames)}");

        var lowerNames = await context.Products
            .Take(2)
            .Select(p => p.Name.ToLower())
            .ToListAsync();
        scenario.WriteLine($"  ToLower(): {string.Join(", ", lowerNames)}");

        var trimmedNames = await context.Products
            .Take(2)
            .Select(p => p.Name.Trim())
            .ToListAsync();
        scenario.WriteLine($"  Trim(): {string.Join(", ", trimmedNames)}");

        var substrings = await context.Products
            .Take(2)
            .Select(p => p.Name.Substring(0, Math.Min(5, p.Name.Length)))
            .ToListAsync();
        scenario.WriteLine($"  Substring(0,5): {string.Join(", ", substrings)}");

        var replaced = await context.Products
            .Take(2)
            .Select(p => p.Name.Replace("Laptop", "Notebook"))
            .ToListAsync();
        scenario.WriteLine($"  Replace('Laptop','Notebook'): {string.Join(", ", replaced)}");
        scenario.WriteLine();
    }
    private static async Task DemonstrateMathOperations(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

        var absProducts = await context.Products
            .Where(p => p.Price > 500m && p.Price < 2000m)
            .Select(p => p.Name)
            .ToListAsync();
        scenario.WriteLine($"  Filtered products (500 < price < 2000): {absProducts.Count} products");

        var prices = await context.Products
            .Select(p => p.Price)
            .ToListAsync();
        scenario.WriteLine($"  Product prices loaded: {prices.Count} values");
        foreach (var price in prices)
        {
            scenario.WriteLine($"    Price: ${price:N2}");
            scenario.WriteLine($"    Ceiling: ${Math.Ceiling(price):N0}");
            scenario.WriteLine($"    Floor: ${Math.Floor(price):N0}");
            scenario.WriteLine($"    Rounded: ${Math.Round(price, 0):N0}");
        }

        var stockValues = await context.Products
            .Select(p => p.StockQuantity)
            .ToListAsync();
        scenario.WriteLine($"  Stock quantities loaded: {stockValues.Count} values");

        var absStock = await context.Products
            .Select(p => Math.Abs(p.StockQuantity))
            .ToListAsync();
        scenario.WriteLine($"  Math.Abs on integers: {absStock.Count} values");

        var maxStock = await context.Products
            .Select(p => Math.Max(p.StockQuantity, 10))
            .ToListAsync();
        scenario.WriteLine($"  Math.Max(stock, 10): {maxStock.Count} values");

        var minStock = await context.Products
            .Select(p => Math.Min(p.StockQuantity, 100))
            .ToListAsync();
        scenario.WriteLine($"  Math.Min(stock, 100): {minStock.Count} values");

        scenario.WriteLine();
    }
    private static async Task DemonstrateDateTimeOperations(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

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

        scenario.WriteLine($"  Customer created with CreatedAt: {customer.CreatedAt:O}");

        var recentCustomers = await context.Customers
            .Where(c => c.Email == "date@test.com")
            .ToListAsync();
        scenario.WriteLine($"  Customer lookup by email: {recentCustomers.Count} found");

        var customersWithNullPhone = await context.Customers
            .Where(c => c.Phone == null)
            .Select(c => c.FirstName)
            .ToListAsync();
        scenario.WriteLine($"  Nullable string comparison (Phone == null): {customersWithNullPhone.Count} customers");

        var customersWithPhone = await context.Customers
            .Where(c => c.Phone != null)
            .Select(c => c.FirstName)
            .ToListAsync();
        scenario.WriteLine($"  Nullable string comparison (Phone != null): {customersWithPhone.Count} customers");

        var customersWithNullEmail = await context.Customers
            .Where(c => c.Email == null)
            .Select(c => c.FirstName)
            .ToListAsync();
        scenario.WriteLine($"  Nullable string comparison (Email == null): {customersWithNullEmail.Count} customers");

        var customerWithPurchaseDate = await context.Customers
            .Where(c => c.LastPurchaseDate != null)
            .Select(c => c.FirstName)
            .ToListAsync();
        scenario.WriteLine($"  Nullable DateTime comparison (LastPurchaseDate != null): {customerWithPurchaseDate.Count} customers");

        var customersWithNullSpend = await context.Customers
            .Where(c => c.TotalSpend == null)
            .Select(c => c.FirstName)
            .ToListAsync();
        scenario.WriteLine($"  Nullable decimal comparison (TotalSpend == null): {customersWithNullSpend.Count} customers");

        var customersWithSpend = await context.Customers
            .Where(c => c.TotalSpend != null)
            .Select(c => c.FirstName)
            .ToListAsync();
        scenario.WriteLine($"  Nullable decimal comparison (TotalSpend != null): {customersWithSpend.Count} customers");

        scenario.WriteLine();
    }
    private static async Task DemonstrateNodaTimeOperations(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

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
        scenario.WriteLine($"  CREATE: Created {3} ScheduleEntry records with NodaTime types");

        var allEntries = await context.ScheduleEntries.ToListAsync();
        scenario.WriteLine($"  READ: All entries: {allEntries.Count}");
        foreach (var entry in allEntries)
        {
            scenario.WriteLine($"    - {entry.Title}: Instant={entry.ScheduledInstant}, Date={entry.ScheduledDate}, LocalDateTime={entry.ScheduledLocalDateTime}");
        }

        var pendingEntries = await context.ScheduleEntries
            .Where(e => !e.IsCompleted)
            .ToListAsync();
        scenario.WriteLine($"  FILTER: Pending entries: {pendingEntries.Count}");

        var highPriorityEntries = await context.ScheduleEntries
            .Where(e => e.Priority <= 2)
            .ToListAsync();
        scenario.WriteLine($"  FILTER: High priority (Priority <= 2): {highPriorityEntries.Count}");

        var todayEntries = await context.ScheduleEntries
            .Where(e => e.ScheduledDate == today)
            .ToListAsync();
        scenario.WriteLine($"  MIN/MAX: Entries for today ({today}): {todayEntries.Count}");

        var upcomingEntries = await context.ScheduleEntries
            .Where(e => e.ScheduledDate >= today && e.ScheduledDate <= today.PlusDays(14))
            .OrderBy(e => e.ScheduledDate)
            .ToListAsync();
        scenario.WriteLine($"  BETWEEN: Upcoming entries (next 14 days): {upcomingEntries.Count}");

        var thisMonthEntries = await context.ScheduleEntries
            .Where(e => e.ScheduledDate.Year == today.Year && e.ScheduledDate.Month == today.Month)
            .ToListAsync();
        scenario.WriteLine($"  LocalDate.Year/Month: This month's entries: {thisMonthEntries.Count}");

        var earliestEntry = await context.ScheduleEntries
            .OrderBy(e => e.ScheduledInstant)
            .FirstOrDefaultAsync();
        scenario.WriteLine($"  MIN: Earliest entry: {earliestEntry?.Title} at {earliestEntry?.ScheduledInstant}");

        var latestEntry = await context.ScheduleEntries
            .OrderByDescending(e => e.ScheduledInstant)
            .FirstOrDefaultAsync();
        scenario.WriteLine($"  MAX: Latest entry: {latestEntry?.Title} at {latestEntry?.ScheduledInstant}");

        var groupedByDay = await context.ScheduleEntries
            .GroupBy(e => e.ScheduledDate)
            .Select(g => new { Date = g.Key, Count = g.Count() })
            .ToListAsync();
        scenario.WriteLine($"  GROUP BY LocalDate: {groupedByDay.Count} unique dates");

        entry1.Priority = 10;
        entry1.IsCompleted = true;
        entry1.CompletedAt = DateTime.UtcNow;
        await context.SaveChangesAsync();
        scenario.WriteLine($"  UPDATE: Entry '{entry1.Title}' marked as completed");

        context.ScheduleEntries.Remove(entry3);
        await context.SaveChangesAsync();
        var remainingCount = await context.ScheduleEntries.CountAsync();
        scenario.WriteLine($"  DELETE: Removed 1 entry, remaining: {remainingCount}");

        scenario.WriteLine();
    }
    private static async Task DemonstratePrimitiveCollections(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

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
        scenario.WriteLine($"  CREATE: EventLog with {eventLog.Tags.Length} tags");

        var logsWithStartupTag = await context.EventLogs
            .Where(l => l.Tags.Contains("startup"))
            .ToListAsync();
        scenario.WriteLine($"  Contains('startup'): {logsWithStartupTag.Count} logs");

        var tagCount = await context.EventLogs
            .Where(l => l.Tags.Length > 0)
            .Select(l => l.Tags.Length)
            .ToListAsync();
        scenario.WriteLine($"  Array length queries: {tagCount.Count} logs with tags");

        scenario.WriteLine();
    }
    private static async Task DemonstrateTransactions(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

        scenario.WriteLine($"  Before transaction - CurrentTransaction: {(context.Database.CurrentTransaction == null ? "None" : "Active")}");

        await using var transaction = await context.Database.BeginTransactionAsync();
        scenario.WriteLine($"  Transaction started - CurrentTransaction: {(context.Database.CurrentTransaction == null ? "None" : "Active")}");

        try
        {
            var customer = ShowcaseSeeder.CreateCustomer(
                firstName: "Transaction",
                lastName: "Test",
                email: "transaction@test.com",
                createdAt: DateTime.UtcNow);
            context.Customers.Add(customer);
            await context.SaveChangesAsync();

            customer.LoyaltyPoints = 500;
            await context.SaveChangesAsync();

            await transaction.CommitAsync();
            scenario.WriteLine($"  Transaction committed - CurrentTransaction: {(context.Database.CurrentTransaction == null ? "None" : "Active")}");
        }
        catch
        {
            await transaction.RollbackAsync();
            scenario.WriteLine($"  Transaction rolled back");
        }

        var customersInTx = await context.Customers
            .Where(c => c.Email == "transaction@test.com")
            .ToListAsync();
        scenario.WriteLine($"  Verified customer exists: {customersInTx.Count > 0}");
        scenario.WriteLine();
    }

    private static async Task DemonstrateOperationalBehaviors(ShowcaseScenarioContext scenario)
    {
        await using (var context = scenario.CreateContext())
        {
            await context.Database.EnsureCreatedAsync();
            await using var efTransaction = await context.Database.BeginTransactionAsync(IsolationLevel.ReadCommitted);
            scenario.WriteLine($"  EF isolation level: {efTransaction.GetDbTransaction().IsolationLevel}");
            scenario.WriteLine($"  EF savepoints supported: {efTransaction.SupportsSavepoints}");
            await efTransaction.RollbackAsync();
        }

        using (var connection = scenario.CreateOpenConnection())
        {
            using var readUncommitted = connection.BeginTransaction(IsolationLevel.ReadUncommitted);
            scenario.WriteLine($"  ADO isolation level: {readUncommitted.IsolationLevel}");
            readUncommitted.Rollback();

            using var command = connection.CreateCommand();
            command.CommandTimeout = 5;
            scenario.WriteLine(
                $"  CommandTimeout API: {command.CommandTimeout}s configured (native execution remains synchronous)");
        }

        var maintenanceDbPath = scenario.CreateAuxiliaryDatabasePath("maintenance");
        using (var inMemory = new DecentDBConnection("Data Source=:memory:"))
        {
            inMemory.Open();
            using var command = inMemory.CreateCommand();
            command.CommandText = """
                                  CREATE TABLE maintenance_demo (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
                                  INSERT INTO maintenance_demo (id, value) VALUES (1, 'hello');
                                  """;
            command.ExecuteNonQuery();
            inMemory.SaveAs(maintenanceDbPath);
        }

        var vacuumed = await DecentDBMaintenance.VacuumAtomicAsync(maintenanceDbPath);
        using var verify = scenario.CreateOpenConnection(maintenanceDbPath);
        using var verifyCommand = verify.CreateCommand();
        verifyCommand.CommandText = "SELECT COUNT(*) FROM maintenance_demo";
        var copiedRows = Convert.ToInt64(verifyCommand.ExecuteScalar());

        scenario.WriteLine($"  SaveAs copied rows: {copiedRows}");
        scenario.WriteLine($"  VacuumAtomicAsync: {vacuumed}");
        scenario.WriteLine();
    }
    private static async Task DemonstrateConcurrencyControl(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

        var product = await context.Products.FirstOrDefaultAsync();
        if (product != null)
        {
            scenario.WriteLine($"  Product Version (ConcurrencyCheck): {product.Version}");

            product.StockQuantity -= 1;
            product.Version++;
            await context.SaveChangesAsync();

            var updated = await context.Products.FindAsync(product.Id);
            scenario.WriteLine($"  After update Version: {updated?.Version}");
        }

        scenario.WriteLine();
    }
    private static async Task DemonstrateFailurePathsAndRecovery(ShowcaseScenarioContext scenario)
    {
        var failureDbPath = scenario.CreateAuxiliaryDatabasePath("failures");

        await using (var setup = new FailureShowcaseDbContext(failureDbPath))
        {
            await setup.Database.EnsureDeletedAsync();
            await setup.Database.EnsureCreatedAsync();

            setup.Users.Add(new FailureShowcaseUser
            {
                Email = "ada@example.com",
                DisplayName = "Ada Lovelace"
            });
            setup.Documents.Add(new FailureShowcaseDocument
            {
                Title = "Design Notes",
                Version = 1
            });
            await setup.SaveChangesAsync();
        }

        await using (var uniqueContext = new FailureShowcaseDbContext(failureDbPath))
        {
            uniqueContext.Users.Add(new FailureShowcaseUser
            {
                Email = "ada@example.com",
                DisplayName = "Duplicate Ada"
            });

            try
            {
                await uniqueContext.SaveChangesAsync();
            }
            catch (DbUpdateException ex)
            {
                var inner = ex.InnerException as DecentDB.Native.DecentDBException;
                scenario.WriteLine(
                    $"  Unique violation: {ex.GetType().Name} / inner={ex.InnerException?.GetType().Name ?? "none"} / code={inner?.ErrorCode}");
                uniqueContext.ChangeTracker.Clear();
            }
        }

        await using (var foreignKeyContext = new FailureShowcaseDbContext(failureDbPath))
        {
            foreignKeyContext.Children.Add(new FailureShowcaseChild
            {
                ParentId = 999,
                Name = "Orphaned child"
            });

            try
            {
                await foreignKeyContext.SaveChangesAsync();
            }
            catch (DbUpdateException ex)
            {
                var inner = ex.InnerException as DecentDB.Native.DecentDBException;
                scenario.WriteLine(
                    $"  FK violation: {ex.GetType().Name} / inner={ex.InnerException?.GetType().Name ?? "none"} / code={inner?.ErrorCode}");
                foreignKeyContext.ChangeTracker.Clear();
            }
        }

        await using var writer1 = new FailureShowcaseDbContext(failureDbPath);
        await using var writer2 = new FailureShowcaseDbContext(failureDbPath);
        var doc1 = await writer1.Documents.SingleAsync();
        var doc2 = await writer2.Documents.SingleAsync();

        doc1.Title = "Writer one update";
        doc1.Version = 2;
        await writer1.SaveChangesAsync();

        doc2.Title = "Writer two update";
        doc2.Version = 2;

        try
        {
            await writer2.SaveChangesAsync();
        }
        catch (DbUpdateConcurrencyException)
        {
            await writer2.Entry(doc2).ReloadAsync();
            doc2.Title = "Writer two retried";
            doc2.Version = 3;
            await writer2.SaveChangesAsync();
            scenario.WriteLine("  Concurrency retry: reload current values, update, save again");
        }

        scenario.WriteLine();
    }
    private static async Task DemonstrateSchemaIntrospection(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();
        var connection = (DecentDBConnection)context.Database.GetDbConnection();
        connection.Open();

        var tablesJson = connection.ListTablesJson();
        scenario.WriteLine($"  ListTablesJson(): {tablesJson}");

        var columnsJson = connection.GetTableColumnsJson("Products");
        scenario.WriteLine($"  GetTableColumnsJson('Products'): {columnsJson[..Math.Min(200, columnsJson.Length)]}...");

        var indexesJson = connection.ListIndexesJson();
        scenario.WriteLine($"  ListIndexesJson(): {indexesJson[..Math.Min(200, indexesJson.Length)]}...");

        var ddl = connection.GetTableDdl("Products");
        scenario.WriteLine($"  GetTableDdl('Products'): {ddl[..Math.Min(100, ddl.Length)]}...");

        var viewsJson = connection.ListViewsJson();
        scenario.WriteLine($"  ListViewsJson(): {viewsJson}");

        var triggersJson = connection.ListTriggersJson();
        scenario.WriteLine($"  ListTriggersJson(): {triggersJson}");

        var ds = connection.GetSchema("Tables");
        scenario.WriteLine($"  GetSchema('Tables'): {ds.Rows.Count} tables");

        var columnsDs = connection.GetSchema("Columns", new[] { "Products" });
        scenario.WriteLine($"  GetSchema('Columns', ['Products']): {columnsDs.Rows.Count} columns");
        scenario.WriteLine();
    }
    private static async Task DemonstrateRawSql(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

        var products = await context.Products
            .FromSqlRaw("SELECT * FROM Products")
            .ToListAsync();
        scenario.WriteLine($"  FromSqlRaw: {products.Count} products loaded");

        var productNames = products.Select(p => p.Name).ToList();
        scenario.WriteLine($"  Products via raw SQL: {string.Join(", ", productNames)}");

        using var connection = scenario.CreateOpenConnection();
        using (var setup = connection.CreateCommand())
        {
            setup.CommandText = """
                                DROP TABLE IF EXISTS raw_sql_demo;
                                CREATE TABLE raw_sql_demo (
                                  id INTEGER PRIMARY KEY,
                                  name TEXT NOT NULL
                                )
                                """;
            setup.ExecuteNonQuery();
        }

        using var insert = connection.CreateCommand();
        insert.CommandText = """
                             INSERT INTO raw_sql_demo (id, name)
                             VALUES (@id, @name)
                             """;
        insert.Parameters.Add(new DecentDBParameter("@id", 1));
        insert.Parameters.Add(new DecentDBParameter("@name", "RawSqlInsertedRow"));
        var inserted = await insert.ExecuteNonQueryAsync();

        using var update = connection.CreateCommand();
        update.CommandText = """
                             UPDATE raw_sql_demo
                             SET name = @updatedName
                             WHERE id = @id
                             """;
        update.Parameters.Add(new DecentDBParameter("@updatedName", "RawSqlUpdatedTag"));
        update.Parameters.Add(new DecentDBParameter("@id", 1));
        var updated = await update.ExecuteNonQueryAsync();

        using var verify = connection.CreateCommand();
        verify.CommandText = "SELECT name FROM raw_sql_demo WHERE id = 1";
        var rawSqlTag = (string)verify.ExecuteScalar()!;

        scenario.WriteLine($"  Parameterized INSERT rows: {inserted}");
        scenario.WriteLine($"  Parameterized UPDATE rows: {updated} -> {rawSqlTag}");

        scenario.WriteLine();
    }
    private static async Task DemonstrateChangeTracking(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

        var product = await context.Products.FirstOrDefaultAsync();
        if (product != null)
        {
            scenario.WriteLine($"  Original price: ${product.Price}");

            context.Entry(product).Property(p => p.Price).CurrentValue = 999.99m;
            context.Entry(product).State = EntityState.Modified;

            var modified = context.ChangeTracker.Entries()
                .Where(e => e.State == EntityState.Modified)
                .ToList();
            scenario.WriteLine($"  Tracked modifications: {modified.Count} entities");

            var trackedProduct = context.ChangeTracker.Entries<Product>()
                .First(e => e.Entity.Id == product.Id);
            var originalPrice = trackedProduct.Property(p => p.Price).OriginalValue;
            var currentPrice = trackedProduct.Property(p => p.Price).CurrentValue;
            scenario.WriteLine($"  Original value: ${originalPrice}");
            scenario.WriteLine($"  Current value:  ${currentPrice}");
        }

        scenario.WriteLine();
    }
    private static async Task DemonstrateBulkOperations(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

        var stopwatch = Stopwatch.StartNew();

        var categories = Enumerable.Range(1, 100)
            .Select(i => ShowcaseSeeder.CreateCategory(
                name: $"BulkCategory{i}",
                displayOrder: i,
                description: $"Bulk category {i}",
                effectiveFrom: DateOnly.FromDateTime(DateTime.UtcNow)))
            .ToList();

        context.Categories.AddRange(categories);
        await context.SaveChangesAsync();
        stopwatch.Stop();

        scenario.WriteLine($"  Bulk insert 100 categories: {stopwatch.ElapsedMilliseconds}ms");

        var tags = Enumerable.Range(1, 50)
            .Select(i => ShowcaseSeeder.CreateTag(
                name: $"BulkTag{i}",
                createdAt: DateTime.UtcNow))
            .ToList();

        context.Tags.AddRange(tags);
        await context.SaveChangesAsync();
        scenario.WriteLine($"  Bulk insert 50 tags completed");

        var tagIds = tags.Select(t => t.Id).ToList();
        var updated = await context.Tags
            .Where(t => tagIds.Take(10).Contains(t.Id))
            .ExecuteUpdateAsync(setters => setters
                .SetProperty(t => t.Name, "BulkTagUpdated"));
        scenario.WriteLine($"  Bulk update: {updated} tags renamed");

        var deleted = await context.Tags
            .Where(t => tagIds.Contains(t.Id))
            .ExecuteDeleteAsync();
        scenario.WriteLine($"  Bulk delete: {deleted} tags deleted");
        scenario.WriteLine();
    }
    private static async Task DemonstrateLikePatternMatching(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

        var category = ShowcaseSeeder.CreateCategory(
            name: "LikeTest",
            displayOrder: 999,
            effectiveFrom: DateOnly.FromDateTime(DateTime.UtcNow));
        context.Categories.Add(category);
        await context.SaveChangesAsync();

        var products = new[]
        {
            ShowcaseSeeder.CreateProduct("Laptop Pro", 1299.99m, 10, category.Id, "High-end laptop", DateTime.UtcNow),
            ShowcaseSeeder.CreateProduct("Laptop Air", 999.99m, 15, category.Id, "Lightweight laptop", DateTime.UtcNow),
            ShowcaseSeeder.CreateProduct("Desktop Tower", 799.99m, 5, category.Id, "Powerful desktop", DateTime.UtcNow),
            ShowcaseSeeder.CreateProduct("Tablet Pro", 699.99m, 20, category.Id, "Professional tablet", DateTime.UtcNow),
            ShowcaseSeeder.CreateProduct("Smartphone X", 899.99m, 25, category.Id, "Latest smartphone", DateTime.UtcNow),
        };
        context.Products.AddRange(products);
        await context.SaveChangesAsync();

        var startsWithLaptop = await context.Products
            .Where(p => EF.Functions.Like(p.Name, "Laptop%"))
            .Select(p => p.Name)
            .ToListAsync();
        scenario.WriteLine($"  StartsWith 'Laptop%': {string.Join(", ", startsWithLaptop)}");

        var endsWithPro = await context.Products
            .Where(p => EF.Functions.Like(p.Name, "%Pro"))
            .Select(p => p.Name)
            .ToListAsync();
        scenario.WriteLine($"  EndsWith '%Pro': {string.Join(", ", endsWithPro)}");

        var containsPad = await context.Products
            .Where(p => EF.Functions.Like(p.Name, "%Pad%"))
            .Select(p => p.Name)
            .ToListAsync();
        scenario.WriteLine($"  Contains '%Pad%': {string.Join(", ", containsPad)}");

        var secondCharIs = await context.Products
            .Where(p => EF.Functions.Like(p.Name, "_e%"))
            .Select(p => p.Name)
            .ToListAsync();
        scenario.WriteLine($"  Second char 'e' (_e%): {string.Join(", ", secondCharIs)}");

        var notLaptop = await context.Products
            .Where(p => !EF.Functions.Like(p.Name, "Laptop%"))
            .Select(p => p.Name)
            .ToListAsync();
        scenario.WriteLine($"  NOT StartsWith 'Laptop%': {string.Join(", ", notLaptop)}");
        scenario.WriteLine();
    }
    private static async Task DemonstrateSetOperations(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

        var expensiveProducts = await context.Products
            .Where(p => p.Price > 800.0m)
            .Select(p => p.Name)
            .ToListAsync();

        var inStockProducts = await context.Products
            .Where(p => p.StockQuantity > 10)
            .Select(p => p.Name)
            .ToListAsync();

        var union = expensiveProducts.Union(inStockProducts).ToList();
        scenario.WriteLine($"  UNION (expensive OR in stock): {union.Count} - {string.Join(", ", union)}");

        var concat = expensiveProducts.Concat(inStockProducts).ToList();
        scenario.WriteLine($"  CONCAT (all items, duplicates): {concat.Count} items");

        var intersect = expensiveProducts.Intersect(inStockProducts).ToList();
        scenario.WriteLine($"  INTERSECT (expensive AND in stock): {intersect.Count} - {string.Join(", ", intersect)}");

        var except = expensiveProducts.Except(inStockProducts).ToList();
        scenario.WriteLine($"  EXCEPT (expensive but NOT in stock): {except.Count} - {string.Join(", ", except)}");

        var distinct = (await context.Products.Select(p => p.Name).ToListAsync()).Distinct().ToList();
        scenario.WriteLine($"  DISTINCT names: {distinct.Count} unique");
        scenario.WriteLine();
    }
    private static async Task DemonstrateExplicitJoins(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

        var category1 = ShowcaseSeeder.CreateCategory(
            name: "Electronics",
            displayOrder: 1,
            description: "Electronic items",
            effectiveFrom: DateOnly.FromDateTime(DateTime.UtcNow));
        var category2 = ShowcaseSeeder.CreateCategory(
            name: "Accessories",
            displayOrder: 2,
            description: "Accessory items",
            effectiveFrom: DateOnly.FromDateTime(DateTime.UtcNow));
        context.Categories.AddRange(category1, category2);
        await context.SaveChangesAsync();

        var product1 = ShowcaseSeeder.CreateProduct(
            name: "Mouse",
            description: "Wireless mouse",
            price: 29.99m,
            stockQuantity: 100,
            categoryId: category2.Id,
            createdAt: DateTime.UtcNow);
        var product2 = ShowcaseSeeder.CreateProduct(
            name: "Keyboard",
            description: "Mechanical keyboard",
            price: 89.99m,
            stockQuantity: 50,
            categoryId: category2.Id,
            createdAt: DateTime.UtcNow);
        context.Products.AddRange(product1, product2);
        await context.SaveChangesAsync();

        var customer = ShowcaseSeeder.CreateCustomer(
            firstName: "Join",
            lastName: "Test",
            email: "join@test.com",
            createdAt: DateTime.UtcNow);
        context.Customers.Add(customer);
        await context.SaveChangesAsync();

        var innerJoin = await context.Products
            .Join(context.Categories,
                p => p.CategoryId,
                c => c.Id,
                (p, c) => new { ProductName = p.Name, CategoryName = c.Name })
            .ToListAsync();
        scenario.WriteLine($"  INNER JOIN (products + categories): {innerJoin.Count} results");
        foreach (var item in innerJoin.Take(3))
            scenario.WriteLine($"    - {item.ProductName} -> {item.CategoryName}");

        var multiJoin = await context.Orders
            .Join(context.Customers, o => o.CustomerId, c => c.Id, (o, c) => new { o, c })
            .Join(context.Addresses, x => x.o.ShippingAddressId, a => a.Id, (x, a) => new { Customer = x.c, Order = x.o, Address = a })
            .Select(x => new { CustomerName = x.Customer.FirstName, City = x.Address.City, OrderTotal = x.Order.TotalAmount })
            .ToListAsync();
        scenario.WriteLine($"  MULTI-JOIN (orders + customers + addresses): {multiJoin.Count} results");
        scenario.WriteLine();
    }
    private static async Task DemonstrateSubqueries(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

        var avgPrice = context.Products.Average(p => p.Price);
        var aboveAverageProducts = await context.Products
            .Where(p => p.Price > avgPrice)
            .Select(p => new { p.Name, p.Price })
            .ToListAsync();
        scenario.WriteLine($"  Scalar subquery (products above avg ${avgPrice:N2}): {aboveAverageProducts.Count}");
        foreach (var p in aboveAverageProducts)
            scenario.WriteLine($"    - {p.Name}: ${p.Price:N2}");

        var categoryIds = await context.Categories.Where(c => c.Name == "Electronics").Select(c => c.Id).ToListAsync();
        var productsInElectronics = await context.Products
            .Where(p => categoryIds.Contains(p.CategoryId))
            .Select(p => p.Name)
            .ToListAsync();
        scenario.WriteLine($"  Subquery with Contains (Electronics products): {productsInElectronics.Count}");

        var customersWithOrders = await context.Customers
            .Where(c => context.Orders.Any(o => o.CustomerId == c.Id))
            .Select(c => c.FirstName)
            .ToListAsync();
        scenario.WriteLine($"  Correlated subquery (customers with orders): {customersWithOrders.Count}");

        var customersWithoutOrders = await context.Customers
            .Where(c => !context.Orders.Any(o => o.CustomerId == c.Id))
            .Select(c => c.FirstName)
            .ToListAsync();
        scenario.WriteLine($"  Correlated subquery (customers WITHOUT orders): {customersWithoutOrders.Count}");

        var productsWithHighStock = await context.Products
            .Where(p => p.StockQuantity > context.Products.Where(p2 => p2.CategoryId == p.CategoryId).Average(p2 => p2.StockQuantity))
            .Select(p => new { p.Name, p.StockQuantity, p.CategoryId })
            .ToListAsync();
        scenario.WriteLine($"  Correlated subquery (above avg stock per category): {productsWithHighStock.Count}");

        var topCategoryByProducts = await context.Categories
            .OrderByDescending(c => context.Products.Count(p => p.CategoryId == c.Id))
            .Select(c => c.Name)
            .FirstOrDefaultAsync();
        scenario.WriteLine($"  Subquery in OrderBy: Top category = {topCategoryByProducts}");
        scenario.WriteLine();
    }
    private static async Task DemonstrateIncludeThenInclude(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

        var category = ShowcaseSeeder.CreateCategory(
            name: "IncludeTest",
            displayOrder: 99,
            description: "For include demos",
            effectiveFrom: DateOnly.FromDateTime(DateTime.UtcNow));
        context.Categories.Add(category);
        await context.SaveChangesAsync();

        var product1 = ShowcaseSeeder.CreateProduct(
            name: "IncProduct1",
            description: "Test",
            price: 10m,
            stockQuantity: 5,
            categoryId: category.Id,
            createdAt: DateTime.UtcNow);
        var product2 = ShowcaseSeeder.CreateProduct(
            name: "IncProduct2",
            description: "Test",
            price: 20m,
            stockQuantity: 10,
            categoryId: category.Id,
            createdAt: DateTime.UtcNow);
        context.Products.AddRange(product1, product2);
        await context.SaveChangesAsync();

        var customer = ShowcaseSeeder.CreateCustomer(
            firstName: "Inc",
            lastName: "Customer",
            email: "include@test.com",
            createdAt: DateTime.UtcNow);
        context.Customers.Add(customer);
        await context.SaveChangesAsync();

        var order = ShowcaseSeeder.CreateOrder(
            orderNumber: "INC-001",
            customerId: customer.Id,
            totalAmount: 50m,
            orderDate: DateTime.UtcNow,
            createdAt: DateTime.UtcNow);
        context.Orders.Add(order);
        await context.SaveChangesAsync();

        var orderItem1 = ShowcaseSeeder.CreateOrderItem(
            orderId: order.Id,
            productId: product1.Id,
            unitPrice: 10m,
            quantity: 2,
            createdAt: DateTime.UtcNow);
        var orderItem2 = ShowcaseSeeder.CreateOrderItem(
            orderId: order.Id,
            productId: product2.Id,
            unitPrice: 20m,
            quantity: 1,
            createdAt: DateTime.UtcNow);
        context.OrderItems.AddRange(orderItem1, orderItem2);
        await context.SaveChangesAsync();

        var productsWithCategoryViaJoin = await context.Products
            .Include(p => p.Category)
            .Where(p => p.Name.StartsWith("Inc"))
            .Select(p => new { ProductName = p.Name, CategoryName = p.Category!.Name })
            .ToListAsync();
        scenario.WriteLine($"  INCLUDE (Product -> Category): {productsWithCategoryViaJoin.Count} products");
        foreach (var item in productsWithCategoryViaJoin)
            scenario.WriteLine($"    {item.ProductName} -> {item.CategoryName}");

        var ordersWithItemsViaJoin = await context.Orders
            .Include(o => o.OrderItems)
            .ThenInclude(oi => oi.Product)
            .Where(o => o.OrderNumber == "INC-001")
            .SelectMany(o => o.OrderItems, (o, oi) => new { OrderNumber = o.OrderNumber, ProductName = oi.Product!.Name, Quantity = oi.Quantity })
            .ToListAsync();
        scenario.WriteLine($"  INCLUDE / THENINCLUDE (Order -> OrderItem -> Product): {ordersWithItemsViaJoin.Count} items");
        foreach (var item in ordersWithItemsViaJoin)
            scenario.WriteLine($"    Order {item.OrderNumber}: {item.Quantity}x {item.ProductName}");

        scenario.WriteLine("  Note: Navigation properties and Include() successfully mapped and executed!");
        scenario.WriteLine();
    }
    private static async Task DemonstrateExistenceAndChildFilters(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

        var parentWithChildren = await context.Categories
            .Where(c => context.Products.Any(p => p.CategoryId == c.Id))
            .Select(c => c.Name)
            .ToListAsync();
        scenario.WriteLine($"  Categories WITH products: {parentWithChildren.Count} - {string.Join(", ", parentWithChildren)}");

        var parentWithoutChildren = await context.Categories
            .Where(c => !context.Products.Any(p => p.CategoryId == c.Id))
            .Select(c => c.Name)
            .ToListAsync();
        scenario.WriteLine($"  Categories WITHOUT products: {parentWithoutChildren.Count}");

        var productsWithOrders = await context.Products
            .Where(p => context.OrderItems.Any(oi => oi.ProductId == p.Id))
            .Select(p => p.Name)
            .ToListAsync();
        scenario.WriteLine($"  Products that have been ordered: {productsWithOrders.Count}");

        var productsNeverOrdered = await context.Products
            .Where(p => !context.OrderItems.Any(oi => oi.ProductId == p.Id))
            .Select(p => p.Name)
            .ToListAsync();
        scenario.WriteLine($"  Products NEVER ordered: {productsNeverOrdered.Count}");

        var categoriesWithManyProducts = await context.Categories
            .Where(c => context.Products.Count(p => p.CategoryId == c.Id) > 2)
            .Select(c => new { Category = c.Name, Count = context.Products.Count(p => p.CategoryId == c.Id) })
            .ToListAsync();
        scenario.WriteLine($"  Categories with >2 products: {categoriesWithManyProducts.Count}");

        var customersWithOrderCount = await context.Customers
            .Select(c => new { Name = c.FirstName + " " + c.LastName, OrderCount = context.Orders.Count(o => o.CustomerId == c.Id) })
            .ToListAsync();
        scenario.WriteLine($"  Customer order counts: {customersWithOrderCount.Count} customers");

        var customersWithHighValueOrders = await context.Customers
            .Where(c => context.Orders.Any(o => o.CustomerId == c.Id && o.TotalAmount > 100))
            .Select(c => c.FirstName)
            .ToListAsync();
        scenario.WriteLine($"  Customers with orders > $100: {customersWithHighValueOrders.Count}");
        scenario.WriteLine();
    }
    private static async Task DemonstrateCompositeForeignKeys(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

        var location = new WarehouseLocation
        {
            WarehouseCode = "WH-EAST",
            BinCode = "A-01",
            Zone = "Electronics",
            TemperatureControlled = true
        };

        context.WarehouseLocations.Add(location);
        await context.SaveChangesAsync();

        var count = new InventoryCount
        {
            ProductName = "Laptop Pro",
            QuantityOnHand = 25,
            CountedAt = DateTime.UtcNow,
            Location = location
        };

        context.InventoryCounts.Add(count);
        await context.SaveChangesAsync();

        var matchingCounts = await context.InventoryCounts
            .Include(ic => ic.Location)
            .Where(ic => ic.Location != null
                && ic.Location.WarehouseCode == "WH-EAST"
                && ic.Location.BinCode == "A-01")
            .ToListAsync();

        scenario.WriteLine($"  Parent composite key: ({location.WarehouseCode}, {location.BinCode})");
        scenario.WriteLine($"  Child saved through EF nav: InventoryCount #{count.Id} -> ({count.WarehouseCode}, {count.BinCode})");
        scenario.WriteLine($"  Include/query over composite FK: {matchingCounts.Count} matching inventory counts");
        scenario.WriteLine();
    }
    private static async Task DemonstrateConditionalLogic(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

        var priceCategories = await context.Products
            .Select(p => new
            {
                p.Name,
                p.Price,
                Category = p.Price > 500 ? "Premium" : "Standard"
            })
            .ToListAsync();
        scenario.WriteLine($"  Ternary operator (price categories): {priceCategories.Count}");
        foreach (var p in priceCategories.Take(3))
            scenario.WriteLine($"    {p.Name}: ${p.Price} -> {p.Category}");

        var productsWithDiscount = await context.Products
            .Select(p => new
            {
                p.Name,
                p.Price,
                Description = p.Description ?? "(No description)"
            })
            .ToListAsync();
        scenario.WriteLine($"  Null coalescing (default description): {productsWithDiscount.Count}");

        var addressDisplay = await context.Customers
            .Select(c => new
            {
                Name = c.FirstName,
                Phone = c.Phone ?? "N/A"
            })
            .ToListAsync();
        scenario.WriteLine($"  Coalesce with string fallback: {addressDisplay.Count}");

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
        scenario.WriteLine($"  Complex conditional: {complexConditional.Count} products");
        scenario.WriteLine();
    }
    private static async Task DemonstrateQueryComposition(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

        IQueryable<Product> ActiveProducts() => context.Products.Where(p => p.IsActive);
        IQueryable<Product> InStock() => context.Products.Where(p => p.StockQuantity > 0);
        IQueryable<Product> AbovePrice(decimal price) => context.Products.Where(p => p.Price > price);

        var active = await ActiveProducts().CountAsync();
        scenario.WriteLine($"  Composed: Active products: {active}");

        var inStock = await InStock().CountAsync();
        scenario.WriteLine($"  Composed: In-stock products: {inStock}");

        var above500 = await AbovePrice(500m).CountAsync();
        scenario.WriteLine($"  Composed: Above $500: {above500}");

        var combined = await ActiveProducts()
            .Intersect(InStock())
            .Intersect(AbovePrice(100m))
            .CountAsync();
        scenario.WriteLine($"  Composed: Active + InStock + Above $100: {combined}");

        IQueryable<T> ApplyFilters<T>(IQueryable<T> query, bool filterActive, bool filterInStock) where T : Product
        {
            if (filterActive) query = query.Where(p => p.IsActive);
            if (filterInStock) query = query.Where(p => p.StockQuantity > 0);
            return query;
        }

        var filtered1 = await ApplyFilters(context.Products.AsQueryable(), true, false).CountAsync();
        var filtered2 = await ApplyFilters(context.Products.AsQueryable(), true, true).CountAsync();
        scenario.WriteLine($"  Conditional filter (active): {filtered1}");
        scenario.WriteLine($"  Conditional filter (active+instock): {filtered2}");
        scenario.WriteLine();
    }
    private static async Task DemonstrateSelectMany(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

        var allOrderItems = await context.Orders
            .Join(context.OrderItems, o => o.Id, oi => oi.OrderId, (o, oi) => new { OrderNumber = o.OrderNumber, OrderItem = oi })
            .Take(10)
            .ToListAsync();
        scenario.WriteLine($"  SelectMany (flatten order items): {allOrderItems.Count} items");

        var customerOrderItems = await context.Customers
            .Join(context.Customers, c => c.Id, c2 => c2.Id, (c, c2) => c)
            .SelectMany(c => context.Orders.Where(o => o.CustomerId == c.Id), (c, o) => new { Customer = c.FirstName, OrderId = o.Id })
            .Take(10)
            .ToListAsync();
        scenario.WriteLine($"  SelectMany (customers -> orders): {customerOrderItems.Count} records");

        var productWithTags = await context.Products
            .Join(context.ProductTags, p => p.Id, pt => pt.ProductId, (p, pt) => new { Product = p.Name, TagId = pt.TagId })
            .Take(10)
            .ToListAsync();
        scenario.WriteLine($"  SelectMany (products -> tags): {productWithTags.Count} records");

        var allProductsFlat = await context.Categories
            .SelectMany(c => context.Products.Where(p => p.CategoryId == c.Id), (c, p) => new { Category = c.Name, Product = p.Name })
            .Take(10)
            .ToListAsync();
        scenario.WriteLine($"  SelectMany (categories -> products): {allProductsFlat.Count} records");
        scenario.WriteLine();
    }
    private static async Task DemonstrateClientVsServerEvaluation(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

        scenario.WriteLine("  Server-evaluated (translates to SQL):");
        var serverWhere = await context.Products.Where(p => p.Price > 100).CountAsync();
        scenario.WriteLine($"    Where(p => p.Price > 100): {serverWhere} (SQL WHERE)");

        var serverSelect = await context.Products.Select(p => p.Name.ToUpper()).Take(3).ToListAsync();
        scenario.WriteLine($"    Select(p => p.Name.ToUpper()): {string.Join(", ", serverSelect)} (SQL UPPER)");

        scenario.WriteLine("  Mixed evaluation examples:");
        var products = await context.Products.ToListAsync();
        var clientFiltered = products.Where(p => p.Price > MethodThatCannotBeTranslated()).ToList();
        scenario.WriteLine($"    Complex method in Where triggers client eval: {clientFiltered.Count} (after ToList)");

        scenario.WriteLine("  Avoiding client eval:");
        var serverSide = await context.Products
            .Where(p => p.Price > 100 && p.Name.Contains("Laptop"))
            .ToListAsync();
        scenario.WriteLine($"    Fully server-evaluated: {serverSide.Count} products");

        scenario.WriteLine("  Note: Complex .NET methods like string.Join, DateTime.Now, custom");
        scenario.WriteLine("        methods require client evaluation after data is fetched.");
        scenario.WriteLine();
    }
    private static async Task DemonstrateWindowFunctions(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

        if (!await context.Categories.AnyAsync(c => c.Name == "Window Electronics"))
        {
            var electronics = new Category
            {
                Name = "Window Electronics",
                Description = "Window function demo category",
                EffectiveFrom = new DateOnly(2026, 1, 1),
                BusinessHoursStart = new TimeOnly(9, 0),
                DisplayOrder = 100,
                IsVisible = true
            };
            var accessories = new Category
            {
                Name = "Window Accessories",
                Description = "Window function demo category",
                EffectiveFrom = new DateOnly(2026, 1, 1),
                BusinessHoursStart = new TimeOnly(9, 0),
                DisplayOrder = 101,
                IsVisible = true
            };
            context.Categories.AddRange(electronics, accessories);
            await context.SaveChangesAsync();

            context.Products.AddRange(
                new Product
                {
                    Name = "Window Laptop",
                    Description = "Window demo product",
                    Price = 1200m,
                    StockQuantity = 10,
                    CategoryId = electronics.Id,
                    CreatedAt = new DateTime(2021, 1, 10, 0, 0, 0, DateTimeKind.Utc),
                    IsActive = true
                },
                new Product
                {
                    Name = "Window Tablet",
                    Description = "Window demo product",
                    Price = 1200m,
                    StockQuantity = 12,
                    CategoryId = electronics.Id,
                    CreatedAt = new DateTime(2022, 6, 15, 0, 0, 0, DateTimeKind.Utc),
                    IsActive = true
                },
                new Product
                {
                    Name = "Window Mouse",
                    Description = "Window demo product",
                    Price = 90m,
                    StockQuantity = 30,
                    CategoryId = electronics.Id,
                    CreatedAt = new DateTime(2023, 3, 20, 0, 0, 0, DateTimeKind.Utc),
                    IsActive = true
                },
                new Product
                {
                    Name = "Window Cable",
                    Description = "Window demo product",
                    Price = 25m,
                    StockQuantity = 50,
                    CategoryId = accessories.Id,
                    CreatedAt = new DateTime(2021, 2, 1, 0, 0, 0, DateTimeKind.Utc),
                    IsActive = true
                },
                new Product
                {
                    Name = "Window Charger",
                    Description = "Window demo product",
                    Price = 25m,
                    StockQuantity = 40,
                    CategoryId = accessories.Id,
                    CreatedAt = new DateTime(2022, 7, 1, 0, 0, 0, DateTimeKind.Utc),
                    IsActive = true
                });
            await context.SaveChangesAsync();
        }

        var rankingRows = await context.Products
            .Where(p => p.Name.StartsWith("Window "))
            .OrderBy(p => p.CategoryId)
            .ThenBy(p => p.CreatedAt)
            .Select(p => new
            {
                p.Name,
                p.CategoryId,
                RowNumber = EF.Functions.RowNumber(p.CategoryId, p.CreatedAt),
                PriceRank = EF.Functions.Rank(p.CategoryId, p.Price, descending: true),
                DensePriceRank = EF.Functions.DenseRank(p.CategoryId, p.Price, descending: true),
                PercentPriceRank = EF.Functions.PercentRank(p.CategoryId, p.Price, descending: true)
            })
            .ToListAsync();

        var offsetRows = await context.Products
            .Where(p => p.Name.StartsWith("Window "))
            .OrderBy(p => p.CategoryId)
            .ThenBy(p => p.CreatedAt)
            .Select(p => new
            {
                p.Name,
                p.CategoryId,
                PreviousProductId = EF.Functions.Lag(p.CategoryId, p.Id, p.CreatedAt, defaultValue: -1L),
                NextProductId = EF.Functions.Lead(p.CategoryId, p.Id, p.CreatedAt, defaultValue: -1L)
            })
            .ToListAsync();

        var valueRows = await context.Products
            .Where(p => p.Name.StartsWith("Window "))
            .OrderBy(p => p.CategoryId)
            .ThenBy(p => p.CreatedAt)
            .Select(p => new
            {
                p.Name,
                p.CategoryId,
                FirstProduct = EF.Functions.FirstValue(p.CategoryId, p.Name, p.CreatedAt),
                LastProduct = EF.Functions.LastValue(p.CategoryId, p.Name, p.CreatedAt),
                SecondProduct = EF.Functions.NthValue(p.CategoryId, p.Name, 2, p.CreatedAt)
            })
            .ToListAsync();

        scenario.WriteLine($"  Ranking rows translated via EF.Functions: {rankingRows.Count}");
        foreach (var row in rankingRows.Take(3))
        {
            scenario.WriteLine(
                $"    category {row.CategoryId}: {row.Name} -> row #{row.RowNumber}, rank {row.PriceRank}, dense rank {row.DensePriceRank}, percent rank {row.PercentPriceRank:F2}");
        }

        scenario.WriteLine($"  Offset rows translated via EF.Functions: {offsetRows.Count}");
        foreach (var row in offsetRows.Take(3))
        {
            scenario.WriteLine(
                $"    category {row.CategoryId}: {row.Name} -> prev id {row.PreviousProductId}, next id {row.NextProductId}");
        }

        scenario.WriteLine($"  Value window rows translated via EF.Functions: {valueRows.Count}");
        foreach (var row in valueRows.Take(3))
        {
            scenario.WriteLine(
                $"    category {row.CategoryId}: {row.Name} -> first {row.FirstProduct}, last {row.LastProduct}, second {row.SecondProduct ?? "NULL"}");
        }

        scenario.WriteLine();
    }
    private static async Task DemonstrateAllBuiltInTypes(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

        scenario.WriteLine("  Seeding 1000 records with all built-in C# types...");

        var records = new List<AllTypesDemo>();
        var random = new Random(42);
        var baseDate = new DateTime(2024, 1, 1);

        for (int i = 0; i < 1000; i++)
        {
            records.Add(new AllTypesDemo
            {
                SignedByte = (sbyte)(random.Next(sbyte.MinValue, sbyte.MaxValue)),
                UnsignedByte = (byte)(random.Next(byte.MinValue, byte.MaxValue)),
                Int16 = (short)(random.Next(short.MinValue, short.MaxValue)),
                UInt16 = (ushort)(random.Next(ushort.MinValue, ushort.MaxValue)),
                Int32 = random.Next(int.MinValue, int.MaxValue),
                UInt32 = (uint)(random.Next(1, int.MaxValue) * 2),
                Int64 = random.NextInt64(),
                UInt64 = (ulong)(random.NextInt64() % long.MaxValue),
                Single = (float)(random.NextDouble() * 1000),
                Double = random.NextDouble() * 1000,
                Decimal = (decimal)(random.NextDouble() * 1000),
                Boolean = random.Next(2) == 1,
                Character = (char)('A' + random.Next(26)),
                Text = $"Text_{i}_{Guid.NewGuid().ToString()[..8]}",
                DateTime = baseDate.AddDays(random.Next(0, 365 * 5)),
                DateOnly = DateOnly.FromDateTime(baseDate.AddDays(random.Next(0, 365 * 5))),
                TimeOnly = new TimeOnly(random.Next(0, 24), random.Next(0, 60), random.Next(0, 60)),
                Guid = Guid.NewGuid()
            });
        }

        context.AllTypesDemos.AddRange(records);
        await context.SaveChangesAsync();
        scenario.WriteLine($"  Seeded {records.Count} records");

        scenario.WriteLine();
        scenario.WriteLine("  Testing aggregations on each type:");
        scenario.WriteLine();

        var testType = async (string name, Func<Task> test) =>
        {
            try { await test(); }
            catch (Exception ex) { scenario.WriteLine($"  {name}: FAILED - {ex.Message[..Math.Min(60, ex.Message.Length)]}"); }
        };

        await testType("sbyte", async () =>
        {
            var min = await context.AllTypesDemos.MinAsync(x => x.SignedByte);
            var max = await context.AllTypesDemos.MaxAsync(x => x.SignedByte);
            var avg = await context.AllTypesDemos.AverageAsync(x => (double)x.SignedByte);
            var sum = await context.AllTypesDemos.SumAsync(x => (double)x.SignedByte);
            var count = await context.AllTypesDemos.CountAsync(x => x.SignedByte > 0);
            scenario.WriteLine($"  sbyte: Min={min}, Max={max}, Avg={avg:F2}, Sum={sum:F2}, Count={count}");
        });

        await testType("byte", async () =>
        {
            var min = await context.AllTypesDemos.MinAsync(x => x.UnsignedByte);
            var max = await context.AllTypesDemos.MaxAsync(x => x.UnsignedByte);
            var avg = await context.AllTypesDemos.AverageAsync(x => (double)x.UnsignedByte);
            var sum = await context.AllTypesDemos.SumAsync(x => (double)x.UnsignedByte);
            var count = await context.AllTypesDemos.CountAsync(x => x.UnsignedByte > 100);
            scenario.WriteLine($"  byte: Min={min}, Max={max}, Avg={avg:F2}, Sum={sum:F2}, Count={count}");
        });

        await testType("short", async () =>
        {
            var min = await context.AllTypesDemos.MinAsync(x => x.Int16);
            var max = await context.AllTypesDemos.MaxAsync(x => x.Int16);
            var avg = await context.AllTypesDemos.AverageAsync(x => (double)x.Int16);
            var sum = await context.AllTypesDemos.SumAsync(x => (double)x.Int16);
            var count = await context.AllTypesDemos.CountAsync(x => x.Int16 > 0);
            scenario.WriteLine($"  short: Min={min}, Max={max}, Avg={avg:F2}, Sum={sum:F2}, Count={count}");
        });

        await testType("ushort", async () =>
        {
            var min = await context.AllTypesDemos.MinAsync(x => x.UInt16);
            var max = await context.AllTypesDemos.MaxAsync(x => x.UInt16);
            var avg = await context.AllTypesDemos.AverageAsync(x => (double)x.UInt16);
            var sum = await context.AllTypesDemos.SumAsync(x => (double)x.UInt16);
            var count = await context.AllTypesDemos.CountAsync(x => x.UInt16 > 10000);
            scenario.WriteLine($"  ushort: Min={min}, Max={max}, Avg={avg:F2}, Sum={sum:F2}, Count={count}");
        });

        await testType("int", async () =>
        {
            var min = await context.AllTypesDemos.MinAsync(x => x.Int32);
            var max = await context.AllTypesDemos.MaxAsync(x => x.Int32);
            var avg = await context.AllTypesDemos.AverageAsync(x => (double)x.Int32);
            var sum = await context.AllTypesDemos.SumAsync(x => (double)x.Int32);
            var count = await context.AllTypesDemos.CountAsync(x => x.Int32 > 0);
            scenario.WriteLine($"  int: Min={min}, Max={max}, Avg={avg:F2}, Sum={sum:F2}, Count={count}");
        });

        await testType("uint", async () =>
        {
            var min = await context.AllTypesDemos.MinAsync(x => x.UInt32);
            var max = await context.AllTypesDemos.MaxAsync(x => x.UInt32);
            var avg = await context.AllTypesDemos.AverageAsync(x => (double)x.UInt32);
            var sum = await context.AllTypesDemos.SumAsync(x => (double)x.UInt32);
            var count = await context.AllTypesDemos.CountAsync(x => x.UInt32 > 1000000);
            scenario.WriteLine($"  uint: Min={min}, Max={max}, Avg={avg:F2}, Sum={sum:F2}, Count={count}");
        });

        await testType("long", async () =>
        {
            var min = await context.AllTypesDemos.MinAsync(x => x.Int64);
            var max = await context.AllTypesDemos.MaxAsync(x => x.Int64);
            var avg = await context.AllTypesDemos.AverageAsync(x => (double)x.Int64);
            var sum = await context.AllTypesDemos.SumAsync(x => (double)x.Int64);
            var count = await context.AllTypesDemos.CountAsync(x => x.Int64 > 0);
            scenario.WriteLine($"  long: Min={min}, Max={max}, Avg={avg:F2}, Sum={sum:F2}, Count={count}");
        });

        await testType("ulong", async () =>
        {
            var min = await context.AllTypesDemos.MinAsync(x => x.UInt64);
            var max = await context.AllTypesDemos.MaxAsync(x => x.UInt64);
            var count = await context.AllTypesDemos.CountAsync();
            var first = await context.AllTypesDemos.OrderBy(x => x.UInt64).FirstAsync();
            scenario.WriteLine($"  ulong: Min={min}, Max={max}, Count={count}, First={first.UInt64}");
        });

        await testType("float", async () =>
        {
            var min = await context.AllTypesDemos.MinAsync(x => x.Single);
            var max = await context.AllTypesDemos.MaxAsync(x => x.Single);
            var avg = await context.AllTypesDemos.AverageAsync(x => x.Single);
            var sum = await context.AllTypesDemos.SumAsync(x => x.Single);
            var count = await context.AllTypesDemos.CountAsync(x => x.Single > 500);
            scenario.WriteLine($"  float: Min={min:F2}, Max={max:F2}, Avg={avg:F2}, Sum={sum:F2}, Count={count}");
        });

        await testType("double", async () =>
        {
            var min = await context.AllTypesDemos.MinAsync(x => x.Double);
            var max = await context.AllTypesDemos.MaxAsync(x => x.Double);
            var avg = await context.AllTypesDemos.AverageAsync(x => x.Double);
            var sum = await context.AllTypesDemos.SumAsync(x => x.Double);
            var count = await context.AllTypesDemos.CountAsync(x => x.Double > 500);
            scenario.WriteLine($"  double: Min={min:F2}, Max={max:F2}, Avg={avg:F2}, Sum={sum:F2}, Count={count}");
        });

        await testType("decimal", async () =>
        {
            var min = await context.AllTypesDemos.MinAsync(x => x.Decimal);
            var max = await context.AllTypesDemos.MaxAsync(x => x.Decimal);
            var avg = await context.AllTypesDemos.AverageAsync(x => (decimal?)x.Decimal);
            var count = await context.AllTypesDemos.CountAsync(x => x.Decimal > 500);
            scenario.WriteLine($"  decimal: Min={min:F2}, Max={max:F2}, Avg={avg:F2}, Count={count}");
        });

        await testType("bool", async () =>
        {
            var trueCount = await context.AllTypesDemos.CountAsync(x => x.Boolean);
            var falseCount = await context.AllTypesDemos.CountAsync(x => !x.Boolean);
            var count = await context.AllTypesDemos.CountAsync();
            scenario.WriteLine($"  bool: True={trueCount}, False={falseCount}, Total={count}");
        });

        await testType("char", async () =>
        {
            var min = await context.AllTypesDemos.MinAsync(x => x.Character);
            var max = await context.AllTypesDemos.MaxAsync(x => x.Character);
            var distinct = await context.AllTypesDemos.Select(x => x.Character).Distinct().CountAsync();
            var count = await context.AllTypesDemos.CountAsync(x => x.Character > 'M');
            scenario.WriteLine($"  char: Min={min}, Max={max}, Distinct={distinct}, Count={count}");
        });

        await testType("string", async () =>
        {
            var distinct = await context.AllTypesDemos.Select(x => x.Text).Distinct().CountAsync();
            var count = await context.AllTypesDemos.CountAsync(x => x.Text.Length > 20);
            var maxLen = await context.AllTypesDemos.MaxAsync(x => x.Text.Length);
            var minLen = await context.AllTypesDemos.MinAsync(x => x.Text.Length);
            scenario.WriteLine($"  string: Distinct={distinct}, MinLen={minLen}, MaxLen={maxLen}, Len>20={count}");
        });

        await testType("DateTime", async () =>
        {
            var min = await context.AllTypesDemos.MinAsync(x => x.DateTime);
            var max = await context.AllTypesDemos.MaxAsync(x => x.DateTime);
            var count = await context.AllTypesDemos.CountAsync();
            scenario.WriteLine($"  DateTime: Min={min:yyyy-MM-dd}, Max={max:yyyy-MM-dd}, Count={count}");
        });

        await testType("DateOnly", async () =>
        {
            var min = await context.AllTypesDemos.MinAsync(x => x.DateOnly);
            var max = await context.AllTypesDemos.MaxAsync(x => x.DateOnly);
            var count = await context.AllTypesDemos.CountAsync();
            scenario.WriteLine($"  DateOnly: Min={min}, Max={max}, Count={count}");
        });

        await testType("TimeOnly", async () =>
        {
            var min = await context.AllTypesDemos.MinAsync(x => x.TimeOnly);
            var max = await context.AllTypesDemos.MaxAsync(x => x.TimeOnly);
            var count = await context.AllTypesDemos.CountAsync();
            scenario.WriteLine($"  TimeOnly: Min={min}, Max={max}, Count={count}");
        });

        await testType("Guid", async () =>
        {
            var distinct = await context.AllTypesDemos.Select(x => x.Guid).Distinct().CountAsync();
            var count = await context.AllTypesDemos.CountAsync();
            var first = await context.AllTypesDemos.OrderBy(x => x.Guid).FirstAsync();
            scenario.WriteLine($"  Guid: Distinct={distinct}, Total={count}, First={first.Guid:N}");
        });

        scenario.WriteLine();
    }
    private static async Task DemonstrateAllNullableBuiltInTypes(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

        scenario.WriteLine("  Seeding 500 records with nullable built-in C# types...");

        var records = new List<AllTypesNullableDemo>();
        var random = new Random(42);
        var baseDate = new DateTime(2024, 1, 1);

        for (int i = 0; i < 500; i++)
        {
            records.Add(new AllTypesNullableDemo
            {
                SignedByte = i % 3 == 0 ? null : (sbyte)(random.Next(sbyte.MinValue, sbyte.MaxValue)),
                UnsignedByte = i % 3 == 0 ? null : (byte)(random.Next(byte.MinValue, byte.MaxValue)),
                Int16 = i % 3 == 0 ? null : (short)(random.Next(short.MinValue, short.MaxValue)),
                UInt16 = i % 3 == 0 ? null : (ushort)(random.Next(ushort.MinValue, ushort.MaxValue)),
                Int32 = i % 3 == 0 ? null : random.Next(int.MinValue, int.MaxValue),
                UInt32 = i % 3 == 0 ? null : (uint)(random.Next(1, int.MaxValue) * 2),
                Int64 = i % 3 == 0 ? null : random.NextInt64(),
                UInt64 = i % 3 == 0 ? null : (ulong)(random.NextInt64() % long.MaxValue),
                Single = i % 3 == 0 ? null : (float)(random.NextDouble() * 1000),
                Double = i % 3 == 0 ? null : random.NextDouble() * 1000,
                Decimal = i % 3 == 0 ? null : (decimal)(random.NextDouble() * 1000),
                Boolean = i % 3 == 0 ? null : (random.Next(2) == 1),
                Character = i % 3 == 0 ? null : (char)('A' + random.Next(26)),
                Text = i % 3 == 0 ? null : $"Text_{i}_{Guid.NewGuid().ToString()[..8]}",
                DateTime = i % 3 == 0 ? null : baseDate.AddDays(random.Next(0, 365 * 5)),
                DateOnly = i % 3 == 0 ? null : DateOnly.FromDateTime(baseDate.AddDays(random.Next(0, 365 * 5))),
                TimeOnly = i % 3 == 0 ? null : new TimeOnly(random.Next(0, 24), random.Next(0, 60), random.Next(0, 60)),
                Guid = i % 3 == 0 ? null : Guid.NewGuid()
            });
        }

        context.AllTypesNullableDemos.AddRange(records);
        await context.SaveChangesAsync();
        scenario.WriteLine($"  Seeded {records.Count} records (1/3 are null for each nullable type)");

        scenario.WriteLine();
        scenario.WriteLine("  Testing nullable aggregations (ignoring nulls):");
        scenario.WriteLine();

        var testNullableType = async (string name, Func<Task> test) =>
        {
            try { await test(); }
            catch (Exception ex) { scenario.WriteLine($"  {name}: FAILED - {ex.Message[..Math.Min(60, ex.Message.Length)]}"); }
        };

        await testNullableType("sbyte?", async () =>
        {
            var min = await context.AllTypesNullableDemos.MinAsync(x => x.SignedByte);
            var max = await context.AllTypesNullableDemos.MaxAsync(x => x.SignedByte);
            var avg = await context.AllTypesNullableDemos.AverageAsync(x => (double)x.SignedByte!.Value);
            var sum = await context.AllTypesNullableDemos.SumAsync(x => (double)x.SignedByte!.Value);
            var nonNull = await context.AllTypesNullableDemos.CountAsync(x => x.SignedByte != null);
            var nullCount = await context.AllTypesNullableDemos.CountAsync(x => x.SignedByte == null);
            scenario.WriteLine($"  sbyte?: Min={min}, Max={max}, Avg={avg:F2}, Sum={sum:F2}, NonNull={nonNull}, Null={nullCount}");
        });

        await testNullableType("byte?", async () =>
        {
            var min = await context.AllTypesNullableDemos.MinAsync(x => x.UnsignedByte);
            var max = await context.AllTypesNullableDemos.MaxAsync(x => x.UnsignedByte);
            var avg = await context.AllTypesNullableDemos.AverageAsync(x => (double)x.UnsignedByte!.Value);
            var sum = await context.AllTypesNullableDemos.SumAsync(x => (double)x.UnsignedByte!.Value);
            var nonNull = await context.AllTypesNullableDemos.CountAsync(x => x.UnsignedByte != null);
            var nullCount = await context.AllTypesNullableDemos.CountAsync(x => x.UnsignedByte == null);
            scenario.WriteLine($"  byte?: Min={min}, Max={max}, Avg={avg:F2}, Sum={sum:F2}, NonNull={nonNull}, Null={nullCount}");
        });

        await testNullableType("short?", async () =>
        {
            var min = await context.AllTypesNullableDemos.MinAsync(x => x.Int16);
            var max = await context.AllTypesNullableDemos.MaxAsync(x => x.Int16);
            var avg = await context.AllTypesNullableDemos.AverageAsync(x => (double)x.Int16!.Value);
            var sum = await context.AllTypesNullableDemos.SumAsync(x => (double)x.Int16!.Value);
            var nonNull = await context.AllTypesNullableDemos.CountAsync(x => x.Int16 != null);
            var nullCount = await context.AllTypesNullableDemos.CountAsync(x => x.Int16 == null);
            scenario.WriteLine($"  short?: Min={min}, Max={max}, Avg={avg:F2}, Sum={sum:F2}, NonNull={nonNull}, Null={nullCount}");
        });

        await testNullableType("ushort?", async () =>
        {
            var min = await context.AllTypesNullableDemos.MinAsync(x => x.UInt16);
            var max = await context.AllTypesNullableDemos.MaxAsync(x => x.UInt16);
            var avg = await context.AllTypesNullableDemos.AverageAsync(x => (double)x.UInt16!.Value);
            var sum = await context.AllTypesNullableDemos.SumAsync(x => (double)x.UInt16!.Value);
            var nonNull = await context.AllTypesNullableDemos.CountAsync(x => x.UInt16 != null);
            var nullCount = await context.AllTypesNullableDemos.CountAsync(x => x.UInt16 == null);
            scenario.WriteLine($"  ushort?: Min={min}, Max={max}, Avg={avg:F2}, Sum={sum:F2}, NonNull={nonNull}, Null={nullCount}");
        });

        await testNullableType("int?", async () =>
        {
            var min = await context.AllTypesNullableDemos.MinAsync(x => x.Int32);
            var max = await context.AllTypesNullableDemos.MaxAsync(x => x.Int32);
            var avg = await context.AllTypesNullableDemos.AverageAsync(x => (double)x.Int32!.Value);
            var sum = await context.AllTypesNullableDemos.SumAsync(x => (double)x.Int32!.Value);
            var nonNull = await context.AllTypesNullableDemos.CountAsync(x => x.Int32 != null);
            var nullCount = await context.AllTypesNullableDemos.CountAsync(x => x.Int32 == null);
            scenario.WriteLine($"  int?: Min={min}, Max={max}, Avg={avg:F2}, Sum={sum:F2}, NonNull={nonNull}, Null={nullCount}");
        });

        await testNullableType("uint?", async () =>
        {
            var min = await context.AllTypesNullableDemos.MinAsync(x => x.UInt32);
            var max = await context.AllTypesNullableDemos.MaxAsync(x => x.UInt32);
            var avg = await context.AllTypesNullableDemos.AverageAsync(x => (double)x.UInt32!.Value);
            var sum = await context.AllTypesNullableDemos.SumAsync(x => (double)x.UInt32!.Value);
            var nonNull = await context.AllTypesNullableDemos.CountAsync(x => x.UInt32 != null);
            var nullCount = await context.AllTypesNullableDemos.CountAsync(x => x.UInt32 == null);
            scenario.WriteLine($"  uint?: Min={min}, Max={max}, Avg={avg:F2}, Sum={sum:F2}, NonNull={nonNull}, Null={nullCount}");
        });

        await testNullableType("long?", async () =>
        {
            var min = await context.AllTypesNullableDemos.MinAsync(x => x.Int64);
            var max = await context.AllTypesNullableDemos.MaxAsync(x => x.Int64);
            var avg = await context.AllTypesNullableDemos.AverageAsync(x => (double)x.Int64!.Value);
            var sum = await context.AllTypesNullableDemos.SumAsync(x => (double)x.Int64!.Value);
            var nonNull = await context.AllTypesNullableDemos.CountAsync(x => x.Int64 != null);
            var nullCount = await context.AllTypesNullableDemos.CountAsync(x => x.Int64 == null);
            scenario.WriteLine($"  long?: Min={min}, Max={max}, Avg={avg:F2}, Sum={sum:F2}, NonNull={nonNull}, Null={nullCount}");
        });

        await testNullableType("ulong?", async () =>
        {
            var min = await context.AllTypesNullableDemos.MinAsync(x => x.UInt64);
            var max = await context.AllTypesNullableDemos.MaxAsync(x => x.UInt64);
            var nonNull = await context.AllTypesNullableDemos.CountAsync(x => x.UInt64 != null);
            var nullCount = await context.AllTypesNullableDemos.CountAsync(x => x.UInt64 == null);
            scenario.WriteLine($"  ulong?: Min={min}, Max={max}, NonNull={nonNull}, Null={nullCount}");
        });

        await testNullableType("float?", async () =>
        {
            var min = await context.AllTypesNullableDemos.MinAsync(x => x.Single);
            var max = await context.AllTypesNullableDemos.MaxAsync(x => x.Single);
            var avg = await context.AllTypesNullableDemos.AverageAsync(x => x.Single!.Value);
            var sum = await context.AllTypesNullableDemos.SumAsync(x => x.Single!.Value);
            var nonNull = await context.AllTypesNullableDemos.CountAsync(x => x.Single != null);
            var nullCount = await context.AllTypesNullableDemos.CountAsync(x => x.Single == null);
            scenario.WriteLine($"  float?: Min={min:F2}, Max={max:F2}, Avg={avg:F2}, Sum={sum:F2}, NonNull={nonNull}, Null={nullCount}");
        });

        await testNullableType("double?", async () =>
        {
            var min = await context.AllTypesNullableDemos.MinAsync(x => x.Double);
            var max = await context.AllTypesNullableDemos.MaxAsync(x => x.Double);
            var avg = await context.AllTypesNullableDemos.AverageAsync(x => x.Double!.Value);
            var sum = await context.AllTypesNullableDemos.SumAsync(x => x.Double!.Value);
            var nonNull = await context.AllTypesNullableDemos.CountAsync(x => x.Double != null);
            var nullCount = await context.AllTypesNullableDemos.CountAsync(x => x.Double == null);
            scenario.WriteLine($"  double?: Min={min:F2}, Max={max:F2}, Avg={avg:F2}, Sum={sum:F2}, NonNull={nonNull}, Null={nullCount}");
        });

        await testNullableType("decimal?", async () =>
        {
            var min = await context.AllTypesNullableDemos.MinAsync(x => x.Decimal);
            var max = await context.AllTypesNullableDemos.MaxAsync(x => x.Decimal);
            var avg = await context.AllTypesNullableDemos.AverageAsync(x => (decimal?)x.Decimal);
            var nonNull = await context.AllTypesNullableDemos.CountAsync(x => x.Decimal != null);
            var nullCount = await context.AllTypesNullableDemos.CountAsync(x => x.Decimal == null);
            scenario.WriteLine($"  decimal?: Min={min:F2}, Max={max:F2}, Avg={avg:F2}, NonNull={nonNull}, Null={nullCount}");
        });

        await testNullableType("bool?", async () =>
        {
            var trueCount = await context.AllTypesNullableDemos.CountAsync(x => x.Boolean == true);
            var falseCount = await context.AllTypesNullableDemos.CountAsync(x => x.Boolean == false);
            var nullCount = await context.AllTypesNullableDemos.CountAsync(x => x.Boolean == null);
            var nonNull = trueCount + falseCount;
            scenario.WriteLine($"  bool?: True={trueCount}, False={falseCount}, Null={nullCount}, NonNull={nonNull}");
        });

        await testNullableType("char?", async () =>
        {
            var min = await context.AllTypesNullableDemos.MinAsync(x => x.Character);
            var max = await context.AllTypesNullableDemos.MaxAsync(x => x.Character);
            var distinct = await context.AllTypesNullableDemos.Where(x => x.Character != null).Select(x => x.Character).Distinct().CountAsync();
            var nonNull = await context.AllTypesNullableDemos.CountAsync(x => x.Character != null);
            var nullCount = await context.AllTypesNullableDemos.CountAsync(x => x.Character == null);
            scenario.WriteLine($"  char?: Min={min}, Max={max}, Distinct={distinct}, NonNull={nonNull}, Null={nullCount}");
        });

        await testNullableType("string?", async () =>
        {
            var distinct = await context.AllTypesNullableDemos.Where(x => x.Text != null).Select(x => x.Text).Distinct().CountAsync();
            var nonNull = await context.AllTypesNullableDemos.CountAsync(x => x.Text != null);
            var nullCount = await context.AllTypesNullableDemos.CountAsync(x => x.Text == null);
            var maxLen = await context.AllTypesNullableDemos.Where(x => x.Text != null).MaxAsync(x => x.Text!.Length);
            scenario.WriteLine($"  string?: Distinct={distinct}, NonNull={nonNull}, Null={nullCount}, MaxLen={maxLen}");
        });

        await testNullableType("DateTime?", async () =>
        {
            var min = await context.AllTypesNullableDemos.MinAsync(x => x.DateTime);
            var max = await context.AllTypesNullableDemos.MaxAsync(x => x.DateTime);
            var nonNull = await context.AllTypesNullableDemos.CountAsync(x => x.DateTime != null);
            var nullCount = await context.AllTypesNullableDemos.CountAsync(x => x.DateTime == null);
            var sample = await context.AllTypesNullableDemos
                .Where(x => x.DateTime != null)
                .Select(x => x.DateTime!.Value)
                .OrderBy(x => x)
                .FirstAsync();
            var yearMatches = await context.AllTypesNullableDemos.CountAsync(x => x.DateTime != null && x.DateTime.Value.Year == sample.Year);
            var monthMatches = await context.AllTypesNullableDemos.CountAsync(x => x.DateTime != null && x.DateTime.Value.Month == sample.Month);
            var dayMatches = await context.AllTypesNullableDemos.CountAsync(x => x.DateTime != null && x.DateTime.Value.Day == sample.Day);
            var dayOfYearMatches = await context.AllTypesNullableDemos.CountAsync(x => x.DateTime != null && x.DateTime.Value.DayOfYear == sample.DayOfYear);
            var hourMatches = await context.AllTypesNullableDemos.CountAsync(x => x.DateTime != null && x.DateTime.Value.Hour == sample.Hour);
            var minuteMatches = await context.AllTypesNullableDemos.CountAsync(x => x.DateTime != null && x.DateTime.Value.Minute == sample.Minute);
            var secondMatches = await context.AllTypesNullableDemos.CountAsync(x => x.DateTime != null && x.DateTime.Value.Second == sample.Second);
            scenario.WriteLine($"  DateTime?: Min={min:yyyy-MM-dd}, Max={max:yyyy-MM-dd}, NonNull={nonNull}, Null={nullCount}");
            scenario.WriteLine($"     Members: Year={yearMatches}, Month={monthMatches}, Day={dayMatches}, DayOfYear={dayOfYearMatches}, Hour={hourMatches}, Minute={minuteMatches}, Second={secondMatches}");
        });

        await testNullableType("DateOnly?", async () =>
        {
            var min = await context.AllTypesNullableDemos.MinAsync(x => x.DateOnly);
            var max = await context.AllTypesNullableDemos.MaxAsync(x => x.DateOnly);
            var nonNull = await context.AllTypesNullableDemos.CountAsync(x => x.DateOnly != null);
            var nullCount = await context.AllTypesNullableDemos.CountAsync(x => x.DateOnly == null);
            var sample = await context.AllTypesNullableDemos
                .Where(x => x.DateOnly != null)
                .Select(x => x.DateOnly!.Value)
                .OrderBy(x => x)
                .FirstAsync();
            var yearMatches = await context.AllTypesNullableDemos.CountAsync(x => x.DateOnly != null && x.DateOnly.Value.Year == sample.Year);
            var monthMatches = await context.AllTypesNullableDemos.CountAsync(x => x.DateOnly != null && x.DateOnly.Value.Month == sample.Month);
            var dayMatches = await context.AllTypesNullableDemos.CountAsync(x => x.DateOnly != null && x.DateOnly.Value.Day == sample.Day);
            var dayOfYearMatches = await context.AllTypesNullableDemos.CountAsync(x => x.DateOnly != null && x.DateOnly.Value.DayOfYear == sample.DayOfYear);
            scenario.WriteLine($"  DateOnly?: Min={min}, Max={max}, NonNull={nonNull}, Null={nullCount}");
            scenario.WriteLine($"     Members: Year={yearMatches}, Month={monthMatches}, Day={dayMatches}, DayOfYear={dayOfYearMatches}");
        });

        await testNullableType("TimeOnly?", async () =>
        {
            var min = await context.AllTypesNullableDemos.MinAsync(x => x.TimeOnly);
            var max = await context.AllTypesNullableDemos.MaxAsync(x => x.TimeOnly);
            var nonNull = await context.AllTypesNullableDemos.CountAsync(x => x.TimeOnly != null);
            var nullCount = await context.AllTypesNullableDemos.CountAsync(x => x.TimeOnly == null);
            var sample = await context.AllTypesNullableDemos
                .Where(x => x.TimeOnly != null)
                .Select(x => x.TimeOnly!.Value)
                .OrderBy(x => x)
                .FirstAsync();
            var hourMatches = await context.AllTypesNullableDemos.CountAsync(x => x.TimeOnly != null && x.TimeOnly.Value.Hour == sample.Hour);
            var minuteMatches = await context.AllTypesNullableDemos.CountAsync(x => x.TimeOnly != null && x.TimeOnly.Value.Minute == sample.Minute);
            var secondMatches = await context.AllTypesNullableDemos.CountAsync(x => x.TimeOnly != null && x.TimeOnly.Value.Second == sample.Second);
            var millisecondMatches = await context.AllTypesNullableDemos.CountAsync(x => x.TimeOnly != null && x.TimeOnly.Value.Millisecond == sample.Millisecond);
            scenario.WriteLine($"  TimeOnly?: Min={min}, Max={max}, NonNull={nonNull}, Null={nullCount}");
            scenario.WriteLine($"     Members: Hour={hourMatches}, Minute={minuteMatches}, Second={secondMatches}, Millisecond={millisecondMatches}");
        });

        await testNullableType("Guid?", async () =>
        {
            var distinct = await context.AllTypesNullableDemos.Where(x => x.Guid != null).Select(x => x.Guid).Distinct().CountAsync();
            var nonNull = await context.AllTypesNullableDemos.CountAsync(x => x.Guid != null);
            var nullCount = await context.AllTypesNullableDemos.CountAsync(x => x.Guid == null);
            scenario.WriteLine($"  Guid?: Distinct={distinct}, NonNull={nonNull}, Null={nullCount}");
        });

        scenario.WriteLine();
    }
    private static async Task DemonstratePerformancePatterns(ShowcaseScenarioContext scenario)
    {
        await using var context = scenario.CreateContext();

        scenario.WriteLine("  These are sanity checks for common EF Core usage patterns, not benchmark claims.");

        scenario.WriteLine("  1. Projection vs Include (avoid over-fetching):");
        List<string> projectedNames = [];
        var projectedElapsed = await scenario.MeasureAsync(async () =>
        {
            projectedNames = await context.Products
                .Where(p => p.Price > 100)
                .Select(p => p.Name)
                .Take(5)
                .ToListAsync();
        });
        scenario.WriteLine($"     Projected only: {projectedNames.Count} records (minimal data)");
        scenario.WriteLine($"     Projection timing: {projectedElapsed.TotalMilliseconds:F2} ms");

        List<Product> withInclude = [];
        var trackedElapsed = await scenario.MeasureAsync(async () =>
        {
            withInclude = await context.Products
                .Take(5)
                .ToListAsync();
        });
        scenario.WriteLine($"     Without Include: {withInclude.Count} records (FK nav props removed)");
        scenario.WriteLine($"     Tracked query timing: {trackedElapsed.TotalMilliseconds:F2} ms");

        scenario.WriteLine("  2. AsNoTracking for read-only:");
        List<Product> noTracking = [];
        var noTrackingElapsed = await scenario.MeasureAsync(async () =>
        {
            noTracking = await context.Products
                .AsNoTracking()
                .Where(p => p.Price > 100)
                .Take(5)
                .ToListAsync();
        });
        scenario.WriteLine($"     AsNoTracking: {noTracking.Count} records (no change tracking)");
        scenario.WriteLine($"     AsNoTracking timing: {noTrackingElapsed.TotalMilliseconds:F2} ms");

        scenario.WriteLine("  3. Split queries (reduces cartesian products):");
        List<Order> splitQuery = [];
        var splitElapsed = await scenario.MeasureAsync(async () =>
        {
            splitQuery = await context.Orders
                .Include(o => o.OrderItems)
                .ThenInclude(oi => oi.Product)
                .AsSplitQuery()
                .Take(1)
                .ToListAsync();
        });
        var splitOrderItems = splitQuery.SelectMany(o => o.OrderItems).Count();
        scenario.WriteLine($"     Split query: {splitQuery.Count} orders / {splitOrderItems} items");
        scenario.WriteLine($"     Split query timing: {splitElapsed.TotalMilliseconds:F2} ms");

        scenario.WriteLine("  4. Keyset pagination (efficient than offset):");
        var lastPrice = 0m;
        List<string> keysetPage = [];
        var keysetElapsed = await scenario.MeasureAsync(async () =>
        {
            keysetPage = await context.Products
                .OrderBy(p => p.Price)
                .Where(p => p.Price > lastPrice)
                .Take(3)
                .Select(p => p.Name)
                .ToListAsync();
        });
        scenario.WriteLine($"     Keyset pagination: {keysetPage.Count} records");
        scenario.WriteLine($"     Keyset timing: {keysetElapsed.TotalMilliseconds:F2} ms");

        scenario.WriteLine("  5. Batch size optimization:");
        scenario.WriteLine("     Use AddRange with reasonable batch sizes (100-1000)");
        scenario.WriteLine("     Prefer set-based updates/deletes and statement reuse where practical");

        scenario.WriteLine("  6. Async streaming over query results:");
        var streamedNames = new List<string>();
        var asyncStreamElapsed = await scenario.MeasureAsync(async () =>
        {
            await foreach (var name in context.Products
                               .OrderBy(p => p.Id)
                               .Select(p => p.Name)
                               .Take(3)
                               .AsAsyncEnumerable())
            {
                streamedNames.Add(name);
            }
        });
        scenario.WriteLine($"     AsAsyncEnumerable: {string.Join(", ", streamedNames)}");
        scenario.WriteLine($"     Async streaming timing: {asyncStreamElapsed.TotalMilliseconds:F2} ms");
        scenario.WriteLine();
    }

    private static decimal MethodThatCannotBeTranslated() => 100m;

    private static async Task DemonstrateUnsupportedCases(ShowcaseScenarioContext scenario)
    {

        await using var context = scenario.CreateContext();

        scenario.WriteLine("  Edge cases and notes for DecentDB EF Core provider:");
        scenario.WriteLine();

        scenario.WriteLine("  1. Decimal comparisons in ranges:");
        var decimalRange = await context.Products
            .Where(p => p.Price >= 100 && p.Price <= 500)
            .Select(p => p.Name)
            .ToListAsync();
        scenario.WriteLine($"     Decimal range query: {decimalRange.Count} products (query executed successfully)");

        scenario.WriteLine("  2. DateTime.Now (non-deterministic):");
        scenario.WriteLine("     Using DateTime.UtcNow instead of DateTime.Now recommended");

        scenario.WriteLine("  3. Composite primary keys:");
        scenario.WriteLine("     Supported via EF Core model configuration (see ProductTag and WarehouseLocation)");

        scenario.WriteLine("  4. Foreign key constraints:");
        scenario.WriteLine("     Self-referencing and composite FKs are supported, including ON DELETE/UPDATE actions");

        scenario.WriteLine("  5. Window functions:");
        scenario.WriteLine("     LINQ translation is available through EF.Functions window helpers");

        scenario.WriteLine();
    }
}
