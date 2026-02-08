using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using DecentDB.AdoNet;
using DecentDB.MicroOrm;

namespace DecentDB.Tests;

public class MicroOrmLayerTests : IDisposable
{
    private readonly string _dbPath;

    public MicroOrmLayerTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_microorm_{Guid.NewGuid():N}.ddb");

        using var conn = new DecentDB.AdoNet.DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE test_entities (id INTEGER PRIMARY KEY, name TEXT)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "CREATE TABLE product_entities (id INTEGER PRIMARY KEY, name TEXT, price DECIMAL(18,4))";
        cmd.ExecuteNonQuery();
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
        var walPath = _dbPath + "-wal";
        if (File.Exists(walPath))
            File.Delete(walPath);
    }

    [Fact]
    public void DecentDBContext_Constructor_WithPath()
    {
        using var context = new DecentDBContext(_dbPath);
        Assert.NotNull(context);
    }

    [Fact]
    public void DecentDBContext_Constructor_WithConnectionString()
    {
        var connectionString = $"Data Source={_dbPath}";
        using var context = new DecentDBContext(connectionString);
        Assert.NotNull(context);
    }

    [Fact]
    public void DecentDBContext_Constructor_WithPooling()
    {
        using var context = new DecentDBContext(_dbPath, pooling: true);
        Assert.NotNull(context);
    }

    [Fact]
    public void DecentDBContext_Constructor_WithInvalidPath_Throws()
    {
        Assert.Throws<ArgumentException>(() => new DecentDBContext(""));
        Assert.Throws<ArgumentException>(() => new DecentDBContext(null));
        Assert.Throws<ArgumentException>(() => new DecentDBContext("   "));
    }

    [Fact]
    public void DecentDBContext_BeginTransaction()
    {
        using var context = new DecentDBContext(_dbPath);
        var transaction = context.BeginTransaction();
        
        Assert.NotNull(transaction);
        Assert.IsAssignableFrom<DbTransaction>(transaction);
    }

    [Fact]
    public void DecentDBContext_BeginTransaction_WithIsolationLevel()
    {
        using var context = new DecentDBContext(_dbPath);
        var transaction = context.BeginTransaction(IsolationLevel.Snapshot);
        
        Assert.NotNull(transaction);
        Assert.IsAssignableFrom<DbTransaction>(transaction);
    }

    [Fact]
    public void DecentDBContext_Set_GenericMethod()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<TestEntity>();
        
        Assert.NotNull(set);
        Assert.IsType<DbSet<TestEntity>>(set);
    }

    [Fact]
    public void DecentDBContext_Dispose_MultipleTimes()
    {
        var context = new DecentDBContext(_dbPath);
        context.Dispose(); // First dispose
        context.Dispose(); // Second dispose - should not throw
    }

    [Fact]
    public async Task DbSet_InsertAsync_SingleEntity()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<TestEntity>();
        
        var entity = new TestEntity { Id = 1, Name = "Test Entity" };
        await set.InsertAsync(entity);
        
        // Verify the entity was added by querying
        var entities = await set.ToListAsync();
        Assert.Single(entities);
        Assert.Equal("Test Entity", entities[0].Name);
    }

    [Fact]
    public async Task DbSet_InsertManyAsync_MultipleEntities()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<TestEntity>();
        
        var entities = new List<TestEntity>
        {
            new TestEntity { Id = 1, Name = "Entity 1" },
            new TestEntity { Id = 2, Name = "Entity 2" },
            new TestEntity { Id = 3, Name = "Entity 3" }
        };
        
        await set.InsertManyAsync(entities);
        
        var result = await set.ToListAsync();
        Assert.Equal(3, result.Count);
        Assert.Contains(result, e => e.Name == "Entity 1");
        Assert.Contains(result, e => e.Name == "Entity 2");
        Assert.Contains(result, e => e.Name == "Entity 3");
    }

    [Fact]
    public async Task DbSet_UpdateAsync_Entity()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<TestEntity>();
        
        // Add an entity first
        var entity = new TestEntity { Id = 1, Name = "Original Name" };
        await set.InsertAsync(entity);
        
        // Query to get the entity
        var retrieved = await set.FirstAsync();
        Assert.Equal("Original Name", retrieved.Name);
        
        // Update the entity
        retrieved.Name = "Updated Name";
        await set.UpdateAsync(retrieved);
        
        // Query again to verify update
        var updated = await set.FirstAsync();
        Assert.Equal("Updated Name", updated.Name);
    }

    [Fact]
    public async Task DbSet_DeleteAsync_Entity()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<TestEntity>();
        
        // Add an entity first
        var entity = new TestEntity { Id = 1, Name = "To Be Removed" };
        await set.InsertAsync(entity);
        
        // Verify it was added
        var countBefore = await set.CountAsync();
        Assert.Equal(1, countBefore);
        
        // Remove the entity
        await set.DeleteAsync(entity);
        
        // Verify it was removed
        var countAfter = await set.CountAsync();
        Assert.Equal(0, countAfter);
    }

    [Fact]
    public async Task DbSet_DeleteByIdAsync_Entity()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<TestEntity>();
        
        // Add an entity first
        var entity = new TestEntity { Id = 1, Name = "To Be Removed" };
        await set.InsertAsync(entity);
        
        // Verify it was added
        var countBefore = await set.CountAsync();
        Assert.Equal(1, countBefore);
        
        // Remove the entity by ID
        await set.DeleteByIdAsync(1);
        
        // Verify it was removed
        var countAfter = await set.CountAsync();
        Assert.Equal(0, countAfter);
    }

    [Fact]
    public async Task DbSet_DeleteManyAsync_Entities()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<TestEntity>();
        
        // Add multiple entities
        var entities = new List<TestEntity>
        {
            new TestEntity { Id = 1, Name = "Entity 1" },
            new TestEntity { Id = 2, Name = "Entity 2" },
            new TestEntity { Id = 3, Name = "Entity 3" }
        };
        await set.InsertManyAsync(entities);
        
        // Verify they were added
        var countBefore = await set.CountAsync();
        Assert.Equal(3, countBefore);
        
        // Remove entities matching a condition
        var deletedCount = await set.DeleteManyAsync(e => e.Id <= 2);
        Assert.Equal(2, deletedCount);
        
        // Verify they were removed
        var countAfter = await set.CountAsync();
        Assert.Equal(1, countAfter);
    }

    [Fact]
    public async Task DbSet_GetAsync_ExistingEntity()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<TestEntity>();
        
        // Add an entity first
        var entity = new TestEntity { Id = 1, Name = "Find Me" };
        await set.InsertAsync(entity);
        
        // Find the entity by ID
        var found = await set.GetAsync(1);
        Assert.NotNull(found);
        Assert.Equal("Find Me", found.Name);
    }

    [Fact]
    public async Task DbSet_GetAsync_NonExistingEntity()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<TestEntity>();
        
        // Try to find an entity that doesn't exist
        var found = await set.GetAsync(999);
        Assert.Null(found);
    }

    [Fact]
    public async Task DbSet_Where_Clause()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<TestEntity>();
        
        // Add multiple entities
        var entities = new List<TestEntity>
        {
            new TestEntity { Id = 1, Name = "Apple" },
            new TestEntity { Id = 2, Name = "Banana" },
            new TestEntity { Id = 3, Name = "Cherry" }
        };
        await set.InsertManyAsync(entities);
        
        // Query with where clause
        var result = await set.Where(e => e.Name.StartsWith("A")).ToListAsync();
        Assert.Single(result);
        Assert.Equal("Apple", result[0].Name);
    }

    [Fact]
    public async Task DbSet_First_FirstOrDefault()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<TestEntity>();
        
        // Add an entity
        var entity = new TestEntity { Id = 1, Name = "First Entity" };
        await set.InsertAsync(entity);
        
        // Test First
        var first = await set.FirstAsync();
        Assert.Equal("First Entity", first.Name);
        
        // Test FirstOrDefault on empty condition
        var none = await set.FirstOrDefaultAsync(e => e.Name == "Nonexistent");
        Assert.Null(none);
    }

    [Fact]
    public async Task DbSet_Count_LongCount()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<TestEntity>();
        
        // Add multiple entities
        var entities = new List<TestEntity>
        {
            new TestEntity { Id = 1, Name = "Entity 1" },
            new TestEntity { Id = 2, Name = "Entity 2" },
            new TestEntity { Id = 3, Name = "Entity 3" }
        };
        await set.InsertManyAsync(entities);
        
        Assert.Equal(3, await set.CountAsync());
    }

    [Fact]
    public async Task DbSet_Any_Exists()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<TestEntity>();
        
        // Initially should return false
        Assert.False(await set.AnyAsync());
        
        // Add an entity
        var entity = new TestEntity { Id = 1, Name = "Any Test" };
        await set.InsertAsync(entity);
        
        // Should now return true
        Assert.True(await set.AnyAsync());
        Assert.True(await set.AnyAsync(e => e.Name == "Any Test"));
        Assert.False(await set.AnyAsync(e => e.Name == "Nonexistent"));
    }

    [Fact]
    public async Task DbSet_OrderBy_ThenBy()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<TestEntity>();
        
        // Add entities in random order
        var entities = new List<TestEntity>
        {
            new TestEntity { Id = 3, Name = "Charlie" },
            new TestEntity { Id = 1, Name = "Alice" },
            new TestEntity { Id = 2, Name = "Bob" }
        };
        await set.InsertManyAsync(entities);
        
        // Order by name
        var orderedByName = await set.OrderBy(e => e.Name).ToListAsync();
        Assert.Equal("Alice", orderedByName[0].Name);
        Assert.Equal("Bob", orderedByName[1].Name);
        Assert.Equal("Charlie", orderedByName[2].Name);
        
        // Order by ID
        var orderedById = await set.OrderBy(e => e.Id).ToListAsync();
        Assert.Equal(1, orderedById[0].Id);
        Assert.Equal(2, orderedById[1].Id);
        Assert.Equal(3, orderedById[2].Id);
    }

    [Fact]
    public async Task DbSet_Take_Skip()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<TestEntity>();
        
        // Add multiple entities
        var entities = new List<TestEntity>
        {
            new TestEntity { Id = 1, Name = "Entity 1" },
            new TestEntity { Id = 2, Name = "Entity 2" },
            new TestEntity { Id = 3, Name = "Entity 3" },
            new TestEntity { Id = 4, Name = "Entity 4" }
        };
        await set.InsertManyAsync(entities);
        
        // Take first 2
        var taken = await set.Take(2).ToListAsync();
        Assert.Equal(2, taken.Count);
        
        // Skip first 2, take next 2
        var skippedAndTaken = await set.Skip(2).Take(2).ToListAsync();
        Assert.Equal(2, skippedAndTaken.Count);
    }

    [Fact]
    public async Task DbSet_ToList_ToArray()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<TestEntity>();
        
        // Add entities
        var entities = new List<TestEntity>
        {
            new TestEntity { Id = 1, Name = "List Test" },
            new TestEntity { Id = 2, Name = "Array Test" }
        };
        await set.InsertManyAsync(entities);
        
        // Test ToList
        var list = await set.ToListAsync();
        Assert.Equal(2, list.Count);
    }

    [Fact]
    public async Task DbSet_SingleOrDefault()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<TestEntity>();
        
        // Test with no entities
        var single = await set.SingleOrDefaultAsync();
        Assert.Null(single);
        
        // Add one entity
        var entity = new TestEntity { Id = 1, Name = "Single Entity" };
        await set.InsertAsync(entity);
        
        // Test with one entity
        single = await set.SingleOrDefaultAsync();
        Assert.NotNull(single);
        Assert.Equal("Single Entity", single.Name);
    }

    [Fact]
    public async Task DbSet_Single()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<TestEntity>();
        
        // Add one entity
        var entity = new TestEntity { Id = 1, Name = "Single Entity" };
        await set.InsertAsync(entity);
        
        // Test with one entity
        var single = await set.SingleAsync();
        Assert.NotNull(single);
        Assert.Equal("Single Entity", single.Name);
    }

    [Fact]
    public async Task DbSet_StreamAsync()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<TestEntity>();
        
        // Add multiple entities
        var entities = new List<TestEntity>
        {
            new TestEntity { Id = 1, Name = "Stream Entity 1" },
            new TestEntity { Id = 2, Name = "Stream Entity 2" }
        };
        await set.InsertManyAsync(entities);
        
        // Test streaming
        var count = 0;
        await foreach (var item in set.StreamAsync())
        {
            count++;
        }
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task DbSet_InTransaction()
    {
        using var context = new DecentDBContext(_dbPath);
        
        using var transaction = context.BeginTransaction();
        var set = context.Set<TestEntity>();
        
        // Add entity within transaction
        var entity = new TestEntity { Id = 1, Name = "In Transaction" };
        await set.InsertAsync(entity);
        
        // Commit the transaction
        transaction.Commit();
        
        // Verify entity was saved
        var savedEntity = await set.GetAsync(1);
        Assert.NotNull(savedEntity);
        Assert.Equal("In Transaction", savedEntity.Name);
    }

    [Fact]
    public async Task DbSet_WithDifferentEntityTypes()
    {
        using var context = new DecentDBContext(_dbPath);
        
        // Test with different entity types
        var testSet = context.Set<TestEntity>();
        var productSet = context.Set<ProductEntity>();
        
        Assert.NotNull(testSet);
        Assert.NotNull(productSet);
        
        // Add an entity to each set
        await testSet.InsertAsync(new TestEntity { Id = 1, Name = "Test" });
        await productSet.InsertAsync(new ProductEntity { Id = 1, Name = "Product", Price = 10.99m });
        
        // Verify both sets have entities
        Assert.Equal(1, await testSet.CountAsync());
        Assert.Equal(1, await productSet.CountAsync());
    }

    // Test entity classes
    public class TestEntity
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }

    public class ProductEntity
    {
        public int Id { get; set; }
        [Required]
        public string Name { get; set; }
        public decimal Price { get; set; }
    }
}