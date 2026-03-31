using DecentDB.AdoNet;
using System.Diagnostics;

namespace DecentDb.ShowCase;

internal sealed record ShowcaseScenario(string Title, Func<ShowcaseScenarioContext, Task> RunAsync);

internal sealed class ShowcaseScenarioContext
{
    private readonly HashSet<string> _databasePaths = new(StringComparer.Ordinal);

    public ShowcaseScenarioContext(string dbPath, TextWriter output)
    {
        DbPath = dbPath;
        Output = output;
        _databasePaths.Add(dbPath);
    }

    public string DbPath { get; }

    public TextWriter Output { get; }

    public IReadOnlyCollection<string> DatabasePaths => _databasePaths;

    public ShowcaseDbContext CreateContext() => new(DbPath);

    public string CreateAuxiliaryDatabasePath(string suffix)
    {
        var directory = Path.GetDirectoryName(DbPath) ?? Path.GetTempPath();
        var extension = Path.GetExtension(DbPath);
        var stem = Path.GetFileNameWithoutExtension(DbPath);
        var auxiliaryPath = Path.Combine(directory, $"{stem}_{suffix}{extension}");
        _databasePaths.Add(auxiliaryPath);
        return auxiliaryPath;
    }

    public DecentDBConnection CreateOpenConnection(string? dbPath = null)
    {
        var connection = new DecentDBConnection($"Data Source={dbPath ?? DbPath}");
        connection.Open();
        return connection;
    }

    public async Task<TimeSpan> MeasureAsync(Func<Task> operation)
    {
        ArgumentNullException.ThrowIfNull(operation);

        var stopwatch = Stopwatch.StartNew();
        await operation();
        stopwatch.Stop();
        return stopwatch.Elapsed;
    }

    public void WriteLine() => Output.WriteLine();

    public void WriteLine(string? value) => Output.WriteLine(value);
}

internal static class ShowcaseScenarioRunner
{
    public static async Task RunAsync(
        IReadOnlyList<ShowcaseScenario> scenarios,
        ShowcaseScenarioContext context)
    {
        foreach (var scenario in scenarios)
        {
            ShowcaseOutput.WriteSection(context.Output, scenario.Title);
            await scenario.RunAsync(context);
        }
    }
}

internal static class ShowcaseOutput
{
    private const string Divider = "═══════════════════════════════════════════════════════════════════════════════════";

    public static void WriteHeader(TextWriter output, string title)
    {
        output.WriteLine(Divider);
        output.WriteLine($"              {title}");
        output.WriteLine(Divider);
        output.WriteLine();
    }

    public static void WriteSection(TextWriter output, string title)
    {
        output.WriteLine();
        output.WriteLine(Divider);
        output.WriteLine($"  {title}");
        output.WriteLine(Divider);
    }

    public static void WriteCompletion(TextWriter output)
    {
        output.WriteLine();
        output.WriteLine(Divider);
        output.WriteLine("  SHOWCASE COMPLETE");
        output.WriteLine(Divider);
    }
}

internal static class ShowcaseDatabaseCleaner
{
    public static void Cleanup(ShowcaseScenarioContext context)
    {
        context.WriteLine();
        ShowcaseOutput.WriteSection(context.Output, "CLEANUP");

        foreach (var databasePath in context.DatabasePaths)
        {
            TryDelete(context, databasePath, "database file");
            TryDelete(context, $"{databasePath}-wal", "WAL file");
            TryDelete(context, $"{databasePath}-shm", "SHM file");
        }

        ShowcaseOutput.WriteCompletion(context.Output);
    }

    private static void TryDelete(ShowcaseScenarioContext context, string path, string label)
    {
        try
        {
            if (!File.Exists(path))
            {
                return;
            }

            File.Delete(path);
            context.WriteLine($"  Deleted {label}: {path}");
        }
        catch (IOException ex)
        {
            context.WriteLine($"  Cleanup warning: {ex.Message}");
        }
        catch (UnauthorizedAccessException ex)
        {
            context.WriteLine($"  Cleanup warning: {ex.Message}");
        }
    }
}
