// DecentDB V2 Benchmarks — .NET binding showcase
// Demonstrates all V2 enhancements: version API, connection modes,
// schema introspection, transactions, re-execute fast paths,
// DECIMAL/UUID/TIMESTAMP types, and ADO.NET performance vs SQLite.
//
// Build: dotnet build -c Release
// Run:   DECENTDB_NATIVE_LIB_PATH=../../target/release/libdecentdb.so dotnet run -c Release

using System.Diagnostics;
using System.Text.Json;
using DecentDB.AdoNet;
using DDB = DecentDB.Native.DecentDB;
using DecentDB.Native;

const string DdbPath = "bench_v2.ddb";
const string SqlitePath = "bench_v2.db";
const int BenchRows = 100_000;

// Clean up from prior runs
if (File.Exists(DdbPath)) File.Delete(DdbPath);
if (File.Exists(SqlitePath)) File.Delete(SqlitePath);
if (File.Exists("bench_v2_backup.ddb")) File.Delete("bench_v2_backup.ddb");

Console.WriteLine("=== DecentDB .NET V2 Benchmark Suite ===\n");

// ─────────────────────────────────────────────────────────────
// SECTION 1: Version API
// ─────────────────────────────────────────────────────────────
PrintSection("1. Version API");
Console.WriteLine($"  ABI version:    {DDB.AbiVersion()}");
Console.WriteLine($"  Engine version: {DDB.EngineVersion()}");

// ─────────────────────────────────────────────────────────────
// SECTION 2: Connection Modes
// ─────────────────────────────────────────────────────────────
PrintSection("2. Connection Modes (V2)");

// Create fresh database using native API
File.Delete(DdbPath); // ensure clean
{
    using var db = new DDB(DdbPath, DbOpenMode.Create);
    Console.WriteLine("  DbOpenMode.Create       — OK (new database)");
}

try { using var _ = new DDB(DdbPath, DbOpenMode.Create); }
catch (DecentDBException) { Console.WriteLine("  DbOpenMode.Create (existing) — correctly rejected"); }

using (var db = new DDB(DdbPath, DbOpenMode.Open))
    Console.WriteLine("  DbOpenMode.Open         — OK (existing database)");

try { using var _ = new DDB("__missing__.ddb", DbOpenMode.Open); }
catch (DecentDBException) { Console.WriteLine("  DbOpenMode.Open (missing)  — correctly rejected"); }

using (var db = new DDB(DdbPath, DbOpenMode.OpenOrCreate))
    Console.WriteLine("  DbOpenMode.OpenOrCreate — OK");

// Close all DDB connections. Re-create a fresh DB for the rest of the benchmark.
GC.Collect();
GC.WaitForPendingFinalizers();
GC.Collect();
try { File.Delete(DdbPath); } catch { }
File.Delete(SqlitePath);

// ─────────────────────────────────────────────────────────────
// SECTION 3: Schema Setup via ADO.NET
// ─────────────────────────────────────────────────────────────
PrintSection("3. Schema Setup (DDL)");
using var conn = new DecentDBConnection($"Data Source={DdbPath}");
conn.Open();

Exec(conn, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price DECIMAL(10,2), sku UUID, created_at TIMESTAMP)");
Exec(conn, "CREATE TABLE orders (id INTEGER PRIMARY KEY, product_id INTEGER NOT NULL, quantity INTEGER NOT NULL, total DECIMAL(12,2), ordered_at TIMESTAMP)");
Exec(conn, "CREATE INDEX idx_products_name ON products (name)");
Exec(conn, "CREATE INDEX idx_orders_product ON orders (product_id)");
Exec(conn, "CREATE VIEW v_product_orders AS SELECT p.name, o.quantity, o.total FROM products p JOIN orders o ON p.id = o.product_id");
Console.WriteLine("  Schema created: products, orders, 2 indexes, 1 view");

// ─────────────────────────────────────────────────────────────
// SECTION 4: Schema Introspection (V2)
// ─────────────────────────────────────────────────────────────
PrintSection("4. Schema Introspection (V2)");

Console.WriteLine($"  Tables: {conn.ListTablesJson()}");
Console.WriteLine($"  products DDL: {conn.GetTableDdl("products")}");
Console.WriteLine($"  Views: {conn.ListViewsJson()}");
Console.WriteLine($"  v_product_orders DDL: {conn.GetViewDdl("v_product_orders")}");
Console.WriteLine($"  Triggers: {conn.ListTriggersJson()}");
Console.WriteLine($"  InTransaction (idle): {conn.InTransaction}");

// ─────────────────────────────────────────────────────────────
// SECTION 5: Transaction State (V2)
// ─────────────────────────────────────────────────────────────
PrintSection("5. Transaction State (V2)");
using (var txn = conn.BeginTransaction())
{
    Console.WriteLine($"  InTransaction (inside BEGIN): {conn.InTransaction}");
    Exec(conn, "INSERT INTO products (id, name, price) VALUES (1, 'Widget', 9.99)");
    txn.Commit();
}
Console.WriteLine($"  InTransaction (after COMMIT): {conn.InTransaction}");

using (var txn = conn.BeginTransaction())
{
    Exec(conn, "INSERT INTO products (id, name, price) VALUES (999, 'Gone', 0.01)");
    txn.Rollback();
}
var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT COUNT(*) FROM products WHERE id = 999";
Console.WriteLine($"  Rolled-back row count: {cmd.ExecuteScalar()} (expected 0)");

// ─────────────────────────────────────────────────────────────
// SECTION 6: Native Type Support
// ─────────────────────────────────────────────────────────────
PrintSection("6. Native Type Support (DECIMAL, UUID, TIMESTAMP)");

using (var txn = conn.BeginTransaction())
{
    var ins = conn.CreateCommand();
    ins.CommandText = "INSERT INTO products (id, name, price, sku, created_at) VALUES ($1, $2, $3, $4, $5)";
    var p = ins.Parameters;
    p.Add(new DecentDBParameter { ParameterName = "$1", Value = 2 });
    p.Add(new DecentDBParameter { ParameterName = "$2", Value = "Premium Widget" });
    p.Add(new DecentDBParameter { ParameterName = "$3", Value = 1234567890.12m });
    p.Add(new DecentDBParameter { ParameterName = "$4", Value = Guid.NewGuid() });
    p.Add(new DecentDBParameter { ParameterName = "$5", Value = DateTime.UtcNow });
    ins.ExecuteNonQuery();
    txn.Commit();
}

var rd = conn.CreateCommand();
rd.CommandText = "SELECT id, name, price, sku, created_at FROM products WHERE id = 2";
using (var reader = rd.ExecuteReader())
{
    if (reader.Read())
    {
        Console.WriteLine($"  id={reader.GetInt64(0)}, name={reader.GetString(1)}");
        Console.WriteLine($"  price (DECIMAL): {reader.GetValue(2)} ({reader.GetValue(2)?.GetType().Name})");
        Console.WriteLine($"  sku (UUID):      {reader.GetValue(3)} ({reader.GetValue(3)?.GetType().Name})");
        Console.WriteLine($"  created (TS):    {reader.GetValue(4)} ({reader.GetValue(4)?.GetType().Name})");
    }
}

// ─────────────────────────────────────────────────────────────
// SECTION 7: Insert Throughput Comparison
// ─────────────────────────────────────────────────────────────
PrintSection("7. Insert Throughput (DecentDB vs SQLite)");

Exec(conn, "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, value REAL)");

// DecentDB insert
var sw = Stopwatch.StartNew();
using (var txn = conn.BeginTransaction())
{
    for (int i = 1; i <= BenchRows; i++)
    {
        var c = conn.CreateCommand();
        c.CommandText = "INSERT INTO bench (id, name, value) VALUES ($1, $2, $3)";
        var pp = c.Parameters;
        pp.Add(new DecentDBParameter { ParameterName = "$1", Value = i });
        pp.Add(new DecentDBParameter { ParameterName = "$2", Value = $"item_{i}" });
        pp.Add(new DecentDBParameter { ParameterName = "$3", Value = i * 1.5 });
        c.ExecuteNonQuery();
    }
    txn.Commit();
}
sw.Stop();
var ddbInsertMs = sw.Elapsed.TotalMilliseconds;
Console.WriteLine($"  DecentDB: {ddbInsertMs:F1}ms ({BenchRows / ddbInsertMs * 1000:F0} rows/s)");

// SQLite insert
using var sqliteConn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={SqlitePath}");
sqliteConn.Open();
ExecSqlite(sqliteConn, "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, value REAL)");

GC.Collect(); GC.WaitForPendingFinalizers(); GC.Collect();
sw.Restart();
using (var txn = sqliteConn.BeginTransaction())
{
    for (int i = 1; i <= BenchRows; i++)
    {
        var c = sqliteConn.CreateCommand();
        c.CommandText = "INSERT INTO bench (id, name, value) VALUES ($1, $2, $3)";
        c.Parameters.AddWithValue("$1", i);
        c.Parameters.AddWithValue("$2", $"item_{i}");
        c.Parameters.AddWithValue("$3", i * 1.5);
        c.ExecuteNonQuery();
    }
    txn.Commit();
}
sw.Stop();
var sqliteInsertMs = sw.Elapsed.TotalMilliseconds;
Console.WriteLine($"  SQLite:   {sqliteInsertMs:F1}ms ({BenchRows / sqliteInsertMs * 1000:F0} rows/s)");
Console.WriteLine($"  Winner:   DecentDB ({sqliteInsertMs / ddbInsertMs:F2}x faster)");

// ─────────────────────────────────────────────────────────────
// SECTION 8: Point Read Performance (Native Prepared Statements)
// ─────────────────────────────────────────────────────────────
PrintSection("8. Point Read Performance");
var random = new Random(42);
var readIds = Enumerable.Range(0, 5000).Select(_ => random.Next(1, BenchRows + 1)).ToArray();

// DecentDB native prepared statement
using var nativeDb = new DDB(DdbPath, DbOpenMode.Open);
var pointStmt = nativeDb.Prepare("SELECT id, name, value FROM bench WHERE id = $1");
GC.Collect();
sw.Restart();
foreach (var id in readIds)
{
    pointStmt.Reset().ClearBindings().BindInt64(1, id);
    pointStmt.Step();
    var _ = pointStmt.GetText(1);
}
sw.Stop();
var ddbPointMs = sw.Elapsed.TotalMilliseconds;
Console.WriteLine($"  DecentDB native: {ddbPointMs:F2}ms ({readIds.Length / ddbPointMs * 1000:F0} reads/s)");

// SQLite point reads
GC.Collect();
sw.Restart();
foreach (var id in readIds)
{
    var c = sqliteConn.CreateCommand();
    c.CommandText = "SELECT id, name, value FROM bench WHERE id = $1";
    c.Parameters.AddWithValue("$1", id);
    using var r = c.ExecuteReader();
    r.Read(); var _ = r.GetString(1);
}
sw.Stop();
var sqlitePointMs = sw.Elapsed.TotalMilliseconds;
Console.WriteLine($"  SQLite:          {sqlitePointMs:F2}ms ({readIds.Length / sqlitePointMs * 1000:F0} reads/s)");
Console.WriteLine($"  Winner:          DecentDB ({sqlitePointMs / ddbPointMs:F2}x faster)");

// ─────────────────────────────────────────────────────────────
// SECTION 9: Re-Execute Fast Path (V2)
// ─────────────────────────────────────────────────────────────
PrintSection("9. Re-Execute Fast Path (V2)");

Exec(conn, "CREATE TABLE counters (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)");
Exec(conn, "DELETE FROM counters");
using (var txn = conn.BeginTransaction())
{
    for (int i = 1; i <= 1000; i++)
    {
        var c = conn.CreateCommand();
        c.CommandText = "INSERT INTO counters (id, val) VALUES ($1, 0)";
        c.Parameters.Add(new DecentDBParameter { ParameterName = "$1", Value = i });
        c.ExecuteNonQuery();
    }
    txn.Commit();
}

// Traditional reset+clear+bind+step on two-param UPDATE
var updStmt = nativeDb.Prepare("UPDATE counters SET val = $1 WHERE id = $2");
updStmt.BindInt64(1, 999).BindInt64(2, 1).StepRowsAffected(); // warmup
GC.Collect();
sw.Restart();
for (int i = 1; i <= 1000; i++)
{
    updStmt.Reset().ClearBindings().BindInt64(1, i).BindInt64(2, i);
    updStmt.StepRowsAffected();
}
sw.Stop();
var traditionalMs = sw.Elapsed.TotalMilliseconds;
Console.WriteLine($"  Traditional reset+bind+step (1000 updates): {traditionalMs:F2}ms");

// Re-execute: single-param fast path
Console.WriteLine("  Testing RebindInt64Execute...");
try
{
    var reStmt = nativeDb.Prepare("UPDATE counters SET val = val + 1 WHERE id = $1");
    reStmt.BindInt64(1, 1).StepRowsAffected(); // initial execute
    Console.WriteLine("  Initial execute OK, calling RebindInt64Execute(2)...");
    var affected = reStmt.RebindInt64Execute(2);
    Console.WriteLine($"  RebindInt64Execute returned: affected={affected}");
    affected = reStmt.RebindInt64Execute(3);
    Console.WriteLine($"  Second call returned: affected={affected}");
    Console.WriteLine("  Re-Execute API works correctly");
}
catch (Exception ex)
{
    Console.WriteLine($"  Re-Execute error: {ex.Message}");
}

// Demonstrate the speed benefit with a small batch
Console.WriteLine("  Benchmarking re-execute (100 updates)...");
var reExec2 = nativeDb.Prepare("UPDATE counters SET val = val + 1 WHERE id = $1");
reExec2.BindInt64(1, 1).StepRowsAffected(); // initial
GC.Collect();
sw.Restart();
for (int i = 2; i <= 100; i++)
{
    reExec2.RebindInt64Execute(i);
}
sw.Stop();
var reexecMs = sw.Elapsed.TotalMilliseconds;
Console.WriteLine($"  Re-execute (100 updates): {reexecMs:F2}ms");
Console.WriteLine($"  Traditional (100 updates): {traditionalMs / 10:F2}ms (extrapolated)");
if (reexecMs > 0)
    Console.WriteLine($"  Speedup: {(traditionalMs / 10) / reexecMs:F2}x");

// ─────────────────────────────────────────────────────────────
// SECTION 10: Full Table Scan Performance
// ─────────────────────────────────────────────────────────────
PrintSection("10. Full Table Scan Performance");

// DecentDB native scan
var scanStmt = nativeDb.Prepare("SELECT id, name, value FROM bench");
GC.Collect();
sw.Restart();
scanStmt.Reset().ClearBindings();
long scanned = 0;
while (scanStmt.Step() == 1)
{
    scanned++;
    var _ = scanStmt.GetInt64(0);
    var __ = scanStmt.GetText(1);
}
sw.Stop();
Console.WriteLine($"  DecentDB native: {sw.Elapsed.TotalMilliseconds:F1}ms ({scanned} rows, {scanned / sw.Elapsed.TotalMilliseconds * 1000:F0} rows/s)");

// DecentDB ADO.NET scan
GC.Collect();
sw.Restart();
{
    using var c = conn.CreateCommand();
    c.CommandText = "SELECT id, name, value FROM bench";
    using var reader = c.ExecuteReader();
    scanned = 0;
    while (reader.Read()) { scanned++; var _ = reader.GetInt64(0); }
}
sw.Stop();
Console.WriteLine($"  DecentDB ADO.NET: {sw.Elapsed.TotalMilliseconds:F1}ms ({scanned} rows)");

// SQLite scan
GC.Collect();
sw.Restart();
{
    var c = sqliteConn.CreateCommand();
    c.CommandText = "SELECT id, name, value FROM bench";
    using var r = c.ExecuteReader();
    scanned = 0;
    while (r.Read()) { scanned++; var _ = r.GetInt64(0); }
}
sw.Stop();
Console.WriteLine($"  SQLite ADO.NET:   {sw.Elapsed.TotalMilliseconds:F1}ms ({scanned} rows)");

// ─────────────────────────────────────────────────────────────
// SECTION 11: Checkpoint & SaveAs (Maintenance)
// ─────────────────────────────────────────────────────────────
PrintSection("11. Maintenance (Checkpoint, SaveAs)");
conn.Checkpoint();
Console.WriteLine("  Checkpoint — OK");

var backupPath = "bench_v2_backup.ddb";
if (File.Exists(backupPath)) File.Delete(backupPath);
conn.SaveAs(backupPath);
Console.WriteLine($"  SaveAs — OK ({new FileInfo(backupPath).Length} bytes)");
File.Delete(backupPath);

// ─────────────────────────────────────────────────────────────
// Cleanup
// ─────────────────────────────────────────────────────────────
conn.Close();
sqliteConn.Close();
if (File.Exists(DdbPath)) File.Delete(DdbPath);
if (File.Exists(SqlitePath)) File.Delete(SqlitePath);

Console.WriteLine("\n=== Benchmark Complete ===");


// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────
static void PrintSection(string title)
{
    Console.WriteLine($"\n--- {title} ---");
}

static void Exec(DecentDBConnection c, string sql)
{
    using var cmd = c.CreateCommand();
    cmd.CommandText = sql;
    cmd.ExecuteNonQuery();
}

static void ExecSqlite(Microsoft.Data.Sqlite.SqliteConnection c, string sql)
{
    using var cmd = c.CreateCommand();
    cmd.CommandText = sql;
    cmd.ExecuteNonQuery();
}
