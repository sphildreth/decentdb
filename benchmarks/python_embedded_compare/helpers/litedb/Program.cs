using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using LiteDB;

class LiteDbBenchmark
{
    static void Main(string[] args)
    {
        if (args.Length == 0)
        {
            Console.WriteLine("Usage: LiteDbBenchmark <command> [args...]");
            Console.WriteLine("Commands:");
            Console.WriteLine("  create-schema <db_path>");
            Console.WriteLine("  insert-bulk <db_path> <count>");
            Console.WriteLine("  point-lookup <db_path> <count> <warmup>");
            Console.WriteLine("  range-scan <db_path> <count> <warmup>");
            Console.WriteLine("  version");
            return;
        }

        string command = args[0];

        try
        {
            switch (command)
            {
                case "create-schema":
                    CreateSchema(args[1]);
                    break;
                case "insert-bulk":
                    InsertBulk(args[1], int.Parse(args[2]));
                    break;
                case "point-lookup":
                    PointLookup(args[1], int.Parse(args[2]), int.Parse(args[3]));
                    break;
                case "range-scan":
                    RangeScan(args[1], int.Parse(args[2]), int.Parse(args[3]));
                    break;
                case "version":
                    Console.WriteLine("LiteDB 5.0.21");
                    break;
                default:
                    Console.WriteLine($"Unknown command: {command}");
                    Environment.Exit(1);
                    break;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
            Environment.Exit(1);
        }
    }

    static void CreateSchema(string dbPath)
    {
        using var db = new LiteDatabase(dbPath);
        
        // Create indexes on collections
        db.GetCollection("customers").EnsureIndex("CustomerId", true);
        db.GetCollection("orders").EnsureIndex("OrderId", true);
        db.GetCollection("orders").EnsureIndex("CustomerId");
        db.GetCollection("events").EnsureIndex("EventId", true);
        db.GetCollection("events").EnsureIndex("UserId");
        
        Console.WriteLine("OK");
    }

    static void InsertBulk(string dbPath, int count)
    {
        using var db = new LiteDatabase(dbPath);
        var customers = db.GetCollection("customers");
        var orders = db.GetCollection("orders");
        var events = db.GetCollection("events");

        var rand = new Random(42);
        
        // Insert customers
        for (int i = 0; i < count / 10; i++)
        {
            customers.Insert(new BsonDocument
            {
                ["CustomerId"] = i,
                ["Email"] = $"user{i}@example.com",
                ["CreatedAt"] = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - rand.Next(86400 * 30)
            });
        }
        
        // Insert orders
        for (int i = 0; i < count; i++)
        {
            orders.Insert(new BsonDocument
            {
                ["OrderId"] = i,
                ["CustomerId"] = rand.Next(count / 10),
                ["CreatedAt"] = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - rand.Next(86400 * 7),
                ["Status"] = new[] { "paid", "shipped", "cancelled", "refunded" }[rand.Next(4)],
                ["TotalCents"] = rand.Next(100, 50000)
            });
        }
        
        // Insert events
        for (int i = 0; i < count; i++)
        {
            events.Insert(new BsonDocument
            {
                ["EventId"] = i,
                ["UserId"] = rand.Next(count / 10),
                ["Ts"] = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - rand.Next(86400 * 30),
                ["Path"] = $"/page{rand.Next(100)}",
                ["Bytes"] = rand.Next(100, 100000)
            });
        }
        
        Console.WriteLine($"OK:{count}");
    }

    static void PointLookup(string dbPath, int count, int warmup)
    {
        using var db = new LiteDatabase(dbPath);
        var customers = db.GetCollection("customers");
        
        var rand = new Random(42);
        int maxId = count / 10;
        
        // Warmup
        for (int i = 0; i < warmup; i++)
        {
            int id = rand.Next(maxId);
            customers.FindOne(Query.EQ("CustomerId", id));
        }
        
        // Benchmark
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < count; i++)
        {
            int id = rand.Next(maxId);
            customers.FindOne(Query.EQ("CustomerId", id));
        }
        sw.Stop();
        
        double msPerOp = (double)sw.ElapsedTicks / Stopwatch.Frequency * 1000.0 / count;
        Console.WriteLine($"OK:{count}:{msPerOp:F4}");
    }

    static void RangeScan(string dbPath, int count, int warmup)
    {
        using var db = new LiteDatabase(dbPath);
        var orders = db.GetCollection("orders");
        
        var rand = new Random(42);
        
        // Warmup
        for (int i = 0; i < warmup; i++)
        {
            int custId = rand.Next(10);
            long startTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 86400 * 7;
            orders.Find(Query.And(Query.EQ("CustomerId", custId), Query.GTE("CreatedAt", startTime))).Take(10).ToList();
        }
        
        // Benchmark
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < count; i++)
        {
            int custId = rand.Next(10);
            long startTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 86400 * 7;
            orders.Find(Query.And(Query.EQ("CustomerId", custId), Query.GTE("CreatedAt", startTime))).Take(10).ToList();
        }
        sw.Stop();
        
        double msPerOp = (double)sw.ElapsedTicks / Stopwatch.Frequency * 1000.0 / count;
        Console.WriteLine($"OK:{count}:{msPerOp:F4}");
    }
}
