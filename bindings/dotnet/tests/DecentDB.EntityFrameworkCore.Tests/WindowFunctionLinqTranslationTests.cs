using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class WindowFunctionLinqTranslationTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_window_linq_{Guid.NewGuid():N}.ddb");

    [Fact]
    public void RankingWindowFunctions_TranslateAndExecute()
    {
        SeedData();

        using var context = CreateContext();
        var query = context.Employees
            .OrderBy(e => e.Department)
            .ThenBy(e => e.Id)
            .Select(e => new
            {
                e.Name,
                e.Department,
                RowNumber = EF.Functions.RowNumber(e.Department, e.Id),
                Rank = EF.Functions.Rank(e.Department, e.Salary, descending: true),
                DenseRank = EF.Functions.DenseRank(e.Department, e.Salary, descending: true),
                PercentRank = EF.Functions.PercentRank(e.Department, e.Salary, descending: true)
            });

        var sql = query.ToQueryString();
        Assert.Contains("ROW_NUMBER() OVER", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("RANK() OVER", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("DENSE_RANK() OVER", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("PERCENT_RANK() OVER", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("PARTITION BY", sql, StringComparison.OrdinalIgnoreCase);

        var rows = query.ToList();
        var eng = rows.Where(row => row.Department == "eng").ToList();
        Assert.Equal([1L, 2L, 3L], eng.Select(row => row.RowNumber).ToArray());
        Assert.Equal([1L, 1L, 3L], eng.Select(row => row.Rank).ToArray());
        Assert.Equal([1L, 1L, 2L], eng.Select(row => row.DenseRank).ToArray());
        Assert.Equal([0.0, 0.0, 1.0], eng.Select(row => row.PercentRank).ToArray());
    }

    [Fact]
    public void LagAndLeadWindowFunctions_TranslateAndExecute()
    {
        SeedData();

        using var context = CreateContext();
        var query = context.Employees
            .OrderBy(e => e.Department)
            .ThenBy(e => e.Id)
            .Select(e => new
            {
                e.Name,
                e.Department,
                PreviousSalary = EF.Functions.Lag(e.Department, e.Salary, e.Id, defaultValue: -1L),
                NextSalary = EF.Functions.Lead(e.Department, e.Salary, e.Id, defaultValue: -1L)
            });

        var sql = query.ToQueryString();
        Assert.Contains("LAG(", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("LEAD(", sql, StringComparison.OrdinalIgnoreCase);

        var rows = query.ToList();
        var eng = rows.Where(row => row.Department == "eng").ToList();
        Assert.Equal([-1L, 120L, 120L], eng.Select(row => row.PreviousSalary).ToArray());
        Assert.Equal([120L, 100L, -1L], eng.Select(row => row.NextSalary).ToArray());
    }

    [Fact]
    public void FirstLastAndNthValue_TranslateToSql()
    {
        SeedData();

        using var context = CreateContext();
        var query = context.Employees
            .Select(e => new
            {
                FirstName = EF.Functions.FirstValue(e.Department, e.Name, e.Id),
                LastName = EF.Functions.LastValue(e.Department, e.Name, e.Id, descending: true),
                SecondName = EF.Functions.NthValue(e.Department, e.Name, 2, e.Id)
            });

        var sql = query.ToQueryString();
        Assert.Contains("FIRST_VALUE(", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("LAST_VALUE(", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("NTH_VALUE(", sql, StringComparison.OrdinalIgnoreCase);
    }

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    private WindowDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<WindowDbContext>()
            .UseDecentDB($"Data Source={_dbPath}")
            .Options;

        return new WindowDbContext(options);
    }

    private void SeedData()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");

        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT NOT NULL, department TEXT NOT NULL, salary INTEGER NOT NULL)";
        command.ExecuteNonQuery();
        command.CommandText = """
            INSERT INTO employees (id, name, department, salary) VALUES
                (1, 'Alice', 'eng', 120),
                (2, 'Bob', 'eng', 120),
                (3, 'Carol', 'eng', 100),
                (4, 'Dave', 'sales', 110),
                (5, 'Eve', 'sales', 105),
                (6, 'Frank', 'sales', 105),
                (7, 'Grace', 'hr', 95)
            """;
        command.ExecuteNonQuery();
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class WindowDbContext : DbContext
    {
        public WindowDbContext(DbContextOptions<WindowDbContext> options)
            : base(options)
        {
        }

        public DbSet<EmployeeRow> Employees => Set<EmployeeRow>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<EmployeeRow>(entity =>
            {
                entity.ToTable("employees");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).HasColumnName("id");
                entity.Property(e => e.Name).HasColumnName("name");
                entity.Property(e => e.Department).HasColumnName("department");
                entity.Property(e => e.Salary).HasColumnName("salary");
            });
        }
    }

    private sealed class EmployeeRow
    {
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Department { get; set; } = string.Empty;
        public long Salary { get; set; }
    }
}
