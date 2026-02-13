using System.Diagnostics;

namespace EntityFrameworkDemo.Services;

public class PerformanceMetrics
{
    private readonly List<Measurement> _measurements = new();

    public class Measurement
    {
        public string Operation { get; set; } = string.Empty;
        public long ElapsedMilliseconds { get; set; }
        public int? RowCount { get; set; }
        public string? Details { get; set; }
    }

    public async Task<T> MeasureAsync<T>(string operation, Func<Task<T>> action, string? details = null)
    {
        var stopwatch = Stopwatch.StartNew();
        var result = await action();
        stopwatch.Stop();

        _measurements.Add(new Measurement
        {
            Operation = operation,
            ElapsedMilliseconds = stopwatch.ElapsedMilliseconds,
            Details = details
        });

        return result;
    }

    public async Task MeasureAsync(string operation, Func<Task> action, string? details = null)
    {
        var stopwatch = Stopwatch.StartNew();
        await action();
        stopwatch.Stop();

        _measurements.Add(new Measurement
        {
            Operation = operation,
            ElapsedMilliseconds = stopwatch.ElapsedMilliseconds,
            Details = details
        });
    }

    public void PrintReport()
    {
        Console.WriteLine();
        Console.WriteLine("╔══════════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║                    PERFORMANCE METRICS REPORT                            ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        var grouped = _measurements.GroupBy(m => m.Operation.Split(':')[0]);

        foreach (var group in grouped)
        {
            Console.WriteLine($"\n{group.Key}:");
            Console.WriteLine(new string('-', 70));

            foreach (var measurement in group)
            {
                var details = measurement.Details != null ? $" ({measurement.Details})" : "";
                Console.WriteLine($"  {measurement.Operation,-50} {measurement.ElapsedMilliseconds,6}ms{details}");
            }

            var avgTime = group.Average(m => m.ElapsedMilliseconds);
            var totalTime = group.Sum(m => m.ElapsedMilliseconds);
            Console.WriteLine($"  {' ',50} {' ',6}  Avg: {avgTime:F1}ms | Total: {totalTime}ms");
        }

        Console.WriteLine();
        Console.WriteLine($"Total Operations: {_measurements.Count}");
        Console.WriteLine($"Total Time: {_measurements.Sum(m => m.ElapsedMilliseconds)}ms");
        Console.WriteLine();
    }

    public void PrintComparison(string operation1, string operation2)
    {
        var m1 = _measurements.LastOrDefault(m => m.Operation == operation1);
        var m2 = _measurements.LastOrDefault(m => m.Operation == operation2);

        if (m1 != null && m2 != null)
        {
            var diff = m2.ElapsedMilliseconds - m1.ElapsedMilliseconds;
            var pct = m1.ElapsedMilliseconds > 0
                ? (diff / (double)m1.ElapsedMilliseconds) * 100
                : 0;

            Console.WriteLine($"\n{operation1} vs {operation2}:");
            Console.WriteLine($"  {operation1}: {m1.ElapsedMilliseconds}ms");
            Console.WriteLine($"  {operation2}: {m2.ElapsedMilliseconds}ms");
            Console.WriteLine($"  Difference: {diff:+#;-#;0}ms ({pct:+#;-#;0}%)");
        }
    }
}
