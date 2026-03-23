using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DecentDB.AdoNet;
using DecentDB.MicroOrm;
using Xunit;

namespace DecentDB.Tests;

/// <summary>
/// Comprehensive tests covering MicroOrm features not exercised by existing test classes.
/// </summary>
public sealed class MicroOrmComprehensiveTests : IDisposable
{
    private readonly string _dbPath;

    public MicroOrmComprehensiveTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_microorm_comp_{Guid.NewGuid():N}.ddb");

        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, department TEXT, salary INTEGER, active BOOL)";
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

    [Table("employees")]
    private sealed class Employee
    {
        public long Id { get; set; }
        public string Name { get; set; } = "";
        public string? Department { get; set; }
        public int Salary { get; set; }
        public bool Active { get; set; }
    }

    private async Task SeedEmployees(DbSet<Employee> set)
    {
        await set.InsertManyAsync(new[]
        {
            new Employee { Id = 1, Name = "Alice", Department = "Engineering", Salary = 120000, Active = true },
            new Employee { Id = 2, Name = "Bob", Department = "Marketing", Salary = 80000, Active = true },
            new Employee { Id = 3, Name = "Carol", Department = "Engineering", Salary = 140000, Active = false },
            new Employee { Id = 4, Name = "Dave", Department = null, Salary = 90000, Active = true },
            new Employee { Id = 5, Name = "Eve", Department = "Marketing", Salary = 95000, Active = true },
        });
    }

    // ───────── OrderByDescending ─────────

    [Fact]
    public async Task OrderByDescending_SortsByColumnDesc()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        var result = await set.OrderByDescending(e => e.Salary).ToListAsync();

        Assert.Equal(5, result.Count);
        Assert.Equal(3L, result[0].Id); // Carol: 140k
        Assert.Equal(1L, result[1].Id); // Alice: 120k
        Assert.Equal(5L, result[2].Id); // Eve:   95k
    }

    // ───────── ThenBy / ThenByDescending ─────────

    [Fact]
    public async Task ThenBy_MultiColumnSort()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        // Sort by department ASC, then salary ASC within each department
        var result = await set
            .Where(e => e.Department != null)
            .OrderBy(e => e.Department)
            .ThenBy(e => e.Salary)
            .ToListAsync();

        Assert.Equal(4, result.Count);
        // Engineering: Alice (120k) before Carol (140k)
        Assert.Equal("Alice", result[0].Name);
        Assert.Equal("Carol", result[1].Name);
        // Marketing: Bob (80k) before Eve (95k)
        Assert.Equal("Bob", result[2].Name);
        Assert.Equal("Eve", result[3].Name);
    }

    [Fact]
    public async Task ThenByDescending_MultiColumnSort()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        // Sort by department ASC, then salary DESC within each department
        var result = await set
            .Where(e => e.Department != null)
            .OrderBy(e => e.Department)
            .ThenByDescending(e => e.Salary)
            .ToListAsync();

        Assert.Equal(4, result.Count);
        // Engineering: Carol (140k) before Alice (120k)
        Assert.Equal("Carol", result[0].Name);
        Assert.Equal("Alice", result[1].Name);
        // Marketing: Eve (95k) before Bob (80k)
        Assert.Equal("Eve", result[2].Name);
        Assert.Equal("Bob", result[3].Name);
    }

    // ───────── Where: OR conditions ─────────

    [Fact]
    public async Task Where_OrCondition()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        var result = await set
            .Where(e => e.Name == "Alice" || e.Name == "Eve")
            .OrderBy(e => e.Id)
            .ToListAsync();

        Assert.Equal(2, result.Count);
        Assert.Equal("Alice", result[0].Name);
        Assert.Equal("Eve", result[1].Name);
    }

    // ───────── Where: null comparisons (IS NULL / IS NOT NULL) ─────────

    [Fact]
    public async Task Where_IsNull()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        var result = await set.Where(e => e.Department == null).ToListAsync();

        Assert.Single(result);
        Assert.Equal("Dave", result[0].Name);
    }

    [Fact]
    public async Task Where_IsNotNull()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        var result = await set.Where(e => e.Department != null).ToListAsync();

        Assert.Equal(4, result.Count);
        Assert.DoesNotContain(result, e => e.Name == "Dave");
    }

    // ───────── Where: NotEqual ─────────

    [Fact]
    public async Task Where_NotEqual()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        var result = await set
            .Where(e => e.Department != "Engineering")
            .OrderBy(e => e.Id)
            .ToListAsync();

        // Marketing employees (Bob, Eve) — Dave has null department so excluded by SQL != semantics
        Assert.All(result, e => Assert.NotEqual("Engineering", e.Department));
    }

    // ───────── Where: LessThan / LessThanOrEqual ─────────

    [Fact]
    public async Task Where_LessThan()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        var result = await set.Where(e => e.Salary < 90000).ToListAsync();

        Assert.Single(result);
        Assert.Equal("Bob", result[0].Name);
    }

    [Fact]
    public async Task Where_LessThanOrEqual()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        var result = await set
            .Where(e => e.Salary <= 90000)
            .OrderBy(e => e.Salary)
            .ToListAsync();

        Assert.Equal(2, result.Count);
        Assert.Equal("Bob", result[0].Name);   // 80k
        Assert.Equal("Dave", result[1].Name);  // 90k
    }

    // ───────── Where: string EndsWith ─────────

    [Fact]
    public async Task Where_EndsWith()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        var result = await set
            .Where(e => e.Name.EndsWith("e"))
            .OrderBy(e => e.Id)
            .ToListAsync();

        Assert.Equal(3, result.Count);
        Assert.Equal("Alice", result[0].Name);
        Assert.Equal("Dave", result[1].Name);
        Assert.Equal("Eve", result[2].Name);
    }

    [Fact]
    public async Task Where_Contains()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        var result = await set
            .Where(e => e.Name.Contains("o"))
            .OrderBy(e => e.Id)
            .ToListAsync();

        Assert.Equal(2, result.Count);
        Assert.Equal("Bob", result[0].Name);
        Assert.Equal("Carol", result[1].Name);
    }

    // ───────── FirstAsync with predicate ─────────

    [Fact]
    public async Task FirstAsync_WithPredicate()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        var eng = await set.FirstAsync(e => e.Department == "Engineering");
        Assert.NotNull(eng);
        Assert.Equal("Engineering", eng.Department);
    }

    [Fact]
    public async Task FirstAsync_WithPredicate_NoMatch_Throws()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => set.FirstAsync(e => e.Name == "NonExistent"));
    }

    // ───────── CountAsync with predicate ─────────

    [Fact]
    public async Task CountAsync_WithPredicate()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        var engCount = await set.CountAsync(e => e.Department == "Engineering");
        Assert.Equal(2, engCount);

        var activeCount = await set.CountAsync(e => e.Active == true);
        Assert.Equal(4, activeCount);
    }

    // ───────── Multiple chained Where clauses (AND) ─────────

    [Fact]
    public async Task Where_ChainedMultiple_ImpliesAnd()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        var result = await set
            .Where(e => e.Department == "Engineering")
            .Where(e => e.Active == true)
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal("Alice", result[0].Name);
    }

    // ───────── Where with boolean property ─────────

    [Fact]
    public async Task Where_BooleanEquality()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        var inactive = await set.Where(e => e.Active == false).ToListAsync();

        Assert.Single(inactive);
        Assert.Equal("Carol", inactive[0].Name);
    }

    // ───────── Where with captured variable ─────────

    [Fact]
    public async Task Where_CapturedVariable()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        var minSalary = 100000;
        var result = await set.Where(e => e.Salary >= minSalary).OrderBy(e => e.Id).ToListAsync();

        Assert.Equal(2, result.Count);
        Assert.Equal("Alice", result[0].Name);
        Assert.Equal("Carol", result[1].Name);
    }

    // ───────── OrderBy + Skip + Take combined ─────────

    [Fact]
    public async Task OrderBy_Skip_Take_Pagination()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        // Page 1: first 2
        var page1 = await set.OrderBy(e => e.Id).Take(2).ToListAsync();
        Assert.Equal(2, page1.Count);
        Assert.Equal(1L, page1[0].Id);
        Assert.Equal(2L, page1[1].Id);

        // Page 2: skip 2, take 2
        var page2 = await set.OrderBy(e => e.Id).Skip(2).Take(2).ToListAsync();
        Assert.Equal(2, page2.Count);
        Assert.Equal(3L, page2[0].Id);
        Assert.Equal(4L, page2[1].Id);

        // Page 3: skip 4, take 2 — only 1 remaining
        var page3 = await set.OrderBy(e => e.Id).Skip(4).Take(2).ToListAsync();
        Assert.Single(page3);
        Assert.Equal(5L, page3[0].Id);
    }

    // ───────── Skip beyond data returns empty ─────────

    [Fact]
    public async Task Skip_BeyondData_ReturnsEmpty()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        var result = await set.OrderBy(e => e.Id).Skip(100).ToListAsync();
        Assert.Empty(result);
    }

    // ───────── Take(0) returns empty ─────────

    [Fact]
    public async Task Take_Zero_ReturnsEmpty()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        var result = await set.Take(0).ToListAsync();
        Assert.Empty(result);
    }

    // ───────── AnyAsync with predicate ─────────

    [Fact]
    public async Task AnyAsync_WithPredicate_NoMatch()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        Assert.False(await set.AnyAsync(e => e.Name == "NonExistent"));
    }

    // ───────── UpdateAsync on non-existent entity (no-op) ─────────

    [Fact]
    public async Task UpdateAsync_NonExistentEntity_DoesNotThrow()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();

        // Update an entity that doesn't exist — should be a no-op (0 rows affected)
        var ghost = new Employee { Id = 999, Name = "Ghost", Salary = 0, Active = false };
        await set.UpdateAsync(ghost);

        // Verify it wasn't inserted
        var found = await set.GetAsync(999);
        Assert.Null(found);
    }

    // ───────── DeleteByIdAsync on non-existent entity (no-op) ─────────

    [Fact]
    public async Task DeleteByIdAsync_NonExistent_DoesNotThrow()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        await set.DeleteByIdAsync(999);

        // Verify nothing was deleted
        Assert.Equal(5, await set.CountAsync());
    }

    // ───────── StreamAsync with Where filter ─────────

    [Fact]
    public async Task StreamAsync_WithFilter()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        var names = new List<string>();
        await foreach (var e in set.Where(e => e.Active == true).OrderBy(e => e.Name).StreamAsync())
        {
            names.Add(e.Name);
        }

        Assert.Equal(4, names.Count);
        Assert.Equal("Alice", names[0]);
        Assert.Equal("Bob", names[1]);
        Assert.Equal("Dave", names[2]);
        Assert.Equal("Eve", names[3]);
    }

    // ───────── Convention: snake_case table name + pluralization ─────────

    [Fact]
    public async Task Convention_SnakeCasePluralTableName()
    {
        // OrderItem → order_items (snake_case + pluralize)
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE order_items (id INTEGER PRIMARY KEY, product_name TEXT, quantity INTEGER)";
        cmd.ExecuteNonQuery();

        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<OrderItem>();

        await set.InsertAsync(new OrderItem { Id = 1, ProductName = "Widget", Quantity = 5 });

        var item = await set.GetAsync(1);
        Assert.NotNull(item);
        Assert.Equal("Widget", item!.ProductName);
        Assert.Equal(5, item.Quantity);
    }

    private sealed class OrderItem
    {
        public long Id { get; set; }
        public string ProductName { get; set; } = "";
        public int Quantity { get; set; }
    }

    // ───────── SQL events capture SQL text ─────────

    [Fact]
    public async Task SqlEvents_CaptureCommandText()
    {
        var executingSql = "";
        var executedSql = "";

        using var ctx = new DecentDBContext(_dbPath);
        ctx.SqlExecuting += (_, args) => executingSql = args.Sql;
        ctx.SqlExecuted += (_, args) => executedSql = args.Sql;

        var set = ctx.Set<Employee>();
        await set.InsertAsync(new Employee { Id = 1, Name = "Test", Salary = 50000, Active = true });

        Assert.Contains("INSERT INTO", executingSql);
        Assert.Contains("employees", executingSql);
        Assert.Contains("INSERT INTO", executedSql);
    }

    // ───────── Derived context: CRUD through DbSet property ─────────

    [Fact]
    public async Task DerivedContext_CrudThroughProperty()
    {
        using var derivedCtx = new EmployeeContext(_dbPath);

        await derivedCtx.Employees.InsertAsync(new Employee { Id = 1, Name = "FromDerived", Salary = 70000, Active = true });

        var emp = await derivedCtx.Employees.GetAsync(1);
        Assert.NotNull(emp);
        Assert.Equal("FromDerived", emp!.Name);

        emp.Salary = 75000;
        await derivedCtx.Employees.UpdateAsync(emp);

        var updated = await derivedCtx.Employees.GetAsync(1);
        Assert.Equal(75000, updated!.Salary);

        await derivedCtx.Employees.DeleteByIdAsync(1);
        Assert.Null(await derivedCtx.Employees.GetAsync(1));
    }

    private sealed class EmployeeContext : DecentDBContext
    {
        public DbSet<Employee> Employees { get; set; } = null!;

        public EmployeeContext(string path) : base(path) { }
    }

    // ───────── Where: complex predicate (AND + OR + comparison) ─────────

    [Fact]
    public async Task Where_ComplexPredicate()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        // Engineering with salary > 130k OR Marketing with salary < 90k
        var result = await set
            .Where(e =>
                (e.Department == "Engineering" && e.Salary > 130000) ||
                (e.Department == "Marketing" && e.Salary < 90000))
            .OrderBy(e => e.Id)
            .ToListAsync();

        Assert.Equal(2, result.Count);
        Assert.Equal("Bob", result[0].Name);    // Marketing, 80k
        Assert.Equal("Carol", result[1].Name);  // Engineering, 140k
    }

    // ───────── Where: GreaterThan on negative boundary ─────────

    [Fact]
    public async Task Where_GreaterThan()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        var result = await set.Where(e => e.Salary > 130000).ToListAsync();

        Assert.Single(result);
        Assert.Equal("Carol", result[0].Name);
    }

    // ───────── InsertMany within explicit transaction + commit ─────────

    [Fact]
    public async Task InsertMany_InExplicitTransaction_Commit()
    {
        using var ctx = new DecentDBContext(_dbPath);
        using var tx = ctx.BeginTransaction();
        var set = ctx.Set<Employee>();

        await set.InsertManyAsync(new[]
        {
            new Employee { Id = 1, Name = "Tx1", Salary = 50000, Active = true },
            new Employee { Id = 2, Name = "Tx2", Salary = 60000, Active = true },
        });

        tx.Commit();

        Assert.Equal(2, await set.CountAsync());
    }

    // ───────── DeleteMany returns 0 on empty table ─────────

    [Fact]
    public async Task DeleteMany_EmptyTable_ReturnsZero()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();

        var deleted = await set.DeleteManyAsync(e => e.Active == true);
        Assert.Equal(0, deleted);
    }

    // ───────── FirstOrDefault on empty table ─────────

    [Fact]
    public async Task FirstOrDefault_EmptyTable_ReturnsNull()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();

        var result = await set.FirstOrDefaultAsync();
        Assert.Null(result);
    }

    // ───────── CountAsync on empty table ─────────

    [Fact]
    public async Task CountAsync_EmptyTable_ReturnsZero()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();

        Assert.Equal(0, await set.CountAsync());
    }

    // ───────── Multiple Set<T> calls return same-shaped sets ─────────

    [Fact]
    public async Task Set_MultipleCallsReturnWorkingSets()
    {
        using var ctx = new DecentDBContext(_dbPath);

        var set1 = ctx.Set<Employee>();
        var set2 = ctx.Set<Employee>();

        await set1.InsertAsync(new Employee { Id = 1, Name = "ViaSet1", Salary = 50000, Active = true });

        var fromSet2 = await set2.GetAsync(1);
        Assert.NotNull(fromSet2);
        Assert.Equal("ViaSet1", fromSet2!.Name);
    }

    // ───────── Where with StartsWith (already tested elsewhere, but verify with multi-char) ─────────

    [Fact]
    public async Task Where_StartsWith_MultiChar()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<Employee>();
        await SeedEmployees(set);

        var result = await set.Where(e => e.Name.StartsWith("Al")).ToListAsync();

        Assert.Single(result);
        Assert.Equal("Alice", result[0].Name);
    }

    // ───────── Enum parameter binding round-trip via AdoNet ─────────

    private enum Status { Inactive = 0, Active = 1, Suspended = 2 }

    [Fact]
    public void EnumParameter_RoundTrip_ViaAdoNet()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmdCreate = conn.CreateCommand();
        cmdCreate.CommandText = "CREATE TABLE enum_test (id INTEGER PRIMARY KEY, status INTEGER)";
        cmdCreate.ExecuteNonQuery();

        using var cmdInsert = conn.CreateCommand();
        cmdInsert.CommandText = "INSERT INTO enum_test (id, status) VALUES ($1, $2)";
        cmdInsert.Parameters.Add(new DecentDBParameter { Value = 1L });
        cmdInsert.Parameters.Add(new DecentDBParameter { Value = Status.Suspended });
        cmdInsert.ExecuteNonQuery();

        using var cmdSelect = conn.CreateCommand();
        cmdSelect.CommandText = "SELECT status FROM enum_test WHERE id = $1";
        cmdSelect.Parameters.Add(new DecentDBParameter { Value = 1L });
        using var reader = cmdSelect.ExecuteReader();
        Assert.True(reader.Read());
        var value = reader.GetFieldValue<Status>(0);
        Assert.Equal(Status.Suspended, value);
    }

    // ───────── Enum round-trip via MicroOrm ─────────

    private enum Priority { Low = 0, Medium = 1, High = 2, Critical = 3 }

    [Table("tasks")]
    private sealed class TaskItem
    {
        public long Id { get; set; }
        public string Title { get; set; } = "";
        public Priority Priority { get; set; }
    }

    [Fact]
    public async Task EnumProperty_RoundTrip_ViaMicroOrm()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT, priority INTEGER)";
        cmd.ExecuteNonQuery();
        conn.Close();

        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<TaskItem>();
        await set.InsertAsync(new TaskItem { Id = 1, Title = "Fix bug", Priority = Priority.Critical });
        await set.InsertAsync(new TaskItem { Id = 2, Title = "Write docs", Priority = Priority.Low });

        var task = await set.GetAsync(1);
        Assert.NotNull(task);
        Assert.Equal(Priority.Critical, task!.Priority);

        var lowPriority = await set.GetAsync(2);
        Assert.NotNull(lowPriority);
        Assert.Equal(Priority.Low, lowPriority!.Priority);
    }

    // ───────── Schema introspection: ListTablesJson ─────────

    [Fact]
    public void ListTablesJson_ReturnsCreatedTable()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        var json = conn.ListTablesJson();
        Assert.Contains("employees", json);
    }

    // ───────── Schema introspection: GetTableColumnsJson ─────────

    [Fact]
    public void GetTableColumnsJson_ReturnsColumnMetadata()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        var json = conn.GetTableColumnsJson("employees");
        Assert.Contains("\"name\"", json);
        Assert.Contains("\"id\"", json);
        Assert.Contains("\"salary\"", json);
        Assert.Contains("\"primary_key\"", json);
    }
}
