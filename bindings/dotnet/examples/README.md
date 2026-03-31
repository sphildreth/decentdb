# DecentDB.EntityFrameworkCore Showcase

This directory contains a comprehensive showcase demonstrating the capabilities of the DecentDB.EntityFrameworkCore provider and its integration with both standard .NET types and NodaTime.

## Overview

The ShowCase project is a console application that exercises all major features of:
1. **Entity Framework Core** - ORM capabilities, LINQ queries, change tracking
2. **DecentDB Engine** - Database operations, transactions, schema introspection
3. **NodaTime Integration** - Precise date/time handling with EF Core

## Prerequisites

- .NET 10.0 SDK or later
- DecentDB native library (`libdecentdb.so` on Linux, `decentdb.dll` on Windows, `libdecentdb.dylib` on macOS)

### Environment Setup

Set the path to the DecentDB native library before running:

```bash
# Linux
export DECENTDB_NATIVE_LIB_PATH=/path/to/libdecentdb.so

# macOS
export DECENTDB_NATIVE_LIB_PATH=/path/to/libdecentdb.dylib

# Windows (PowerShell)
$env:DECENTDB_NATIVE_LIB_PATH="C:\path\to\decentdb.dll"
```

## Building and Running

```bash
cd bindings/dotnet
dotnet restore
dotnet run --project examples/DecentDb.ShowCase/DecentDb.ShowCase.csproj
```

## Entity Framework Core Features Demonstrated

### Basic CRUD Operations
- **Create** - Inserting entities with auto-generated keys
- **Read** - Querying entities using FindAsync and LINQ
- **Update** - Modifying entity properties and saving changes
- **Delete** - Removing entities from the database

### LINQ Query Operations
| Feature | Description |
|---------|-------------|
| `Count()` | Count total entities in a table |
| `Where()` | Filter entities with predicates |
| `OrderBy()` / `OrderByDescending()` | Sort results |
| `Take()` / `Skip()` | Pagination support |
| `GroupBy()` | Group and aggregate data |
| `Distinct()` | Return unique values |
| `Select()` | Project to anonymous or specific types |

### String Operations (SQL Translation)
| C# Method | SQL Generated |
|-----------|---------------|
| `string.Contains()` | `LIKE '%value%'` |
| `string.StartsWith()` | `LIKE 'value%'` |
| `string.ToUpper()` | `UPPER()` |
| `string.ToLower()` | `LOWER()` |
| `string.Trim()` | `TRIM()` |
| `string.Substring()` | `SUBSTRING()` |
| `string.Replace()` | `REPLACE()` |

### Math Operations (SQL Translation)
| C# Method | SQL Generated |
|-----------|---------------|
| `Math.Abs()` | `ABS()` |
| `Math.Ceiling()` | `CEIL()` |
| `Math.Floor()` | `FLOOR()` |
| `Math.Round()` | `ROUND()` |
| `Math.Max()` | `CASE WHEN ... THEN ...` |
| `Math.Min()` | `CASE WHEN ... THEN ...` |

### DateTime Operations
- Storage and retrieval of `DateTime` values
- Microsecond precision storage
- Query by specific date values
- Date arithmetic comparisons

### Primitive Collections (JSON Arrays)
- Storing arrays as JSON in TEXT columns
- Querying array contents with `.Contains()`
- Array length operations
- Array element access

### Change Tracking
- Monitoring entity modifications
- Accessing original vs current values
- Entity state tracking (Added, Modified, Deleted)
- Property-level change detection

### Transactions
- Explicit transaction management
- Begin/Commit/Rollback lifecycle
- Transaction verification via `CurrentTransaction`
- Multiple operations in single transaction

### Concurrency Control
- `[ConcurrencyCheck]` attribute support
- Row versioning
- Optimistic concurrency detection

### Raw SQL Execution
- `FromSqlRaw()` - Execute raw SQL and materialize entities
- `ExecuteSqlRawAsync()` - Execute non-query SQL
- Parameterized queries for security

### Bulk Operations
- `AddRange()` for batch inserts
- `ExecuteUpdateAsync()` for set-based bulk updates
- `ExecuteDeleteAsync()` for bulk deletes
- Performance measurement of bulk operations

### Advanced Query Operations (per EF Core spec)
| Feature | Description |
|---------|-------------|
| **EF.Functions.Like** | Pattern matching with wildcards (`%`, `_`) |
| **Set Operations** | Union, Concat, Intersect, Except translated server-side for representative shapes |
| **Explicit Joins** | LINQ Join/GroupJoin for multi-table queries |
| **Subqueries** | Correlated subqueries, scalar subqueries |
| **Existence Filters** | Any/All over child collections |
| **Conditional Logic** | Ternary operator, null coalescing |
| **Query Composition** | Reusable IQueryable with conditional filters |
| **SelectMany** | Collection flattening |
| **AsAsyncEnumerable** | Async streaming-friendly enumeration over composed queries |
| **Client vs Server Evaluation** | Understanding translation boundaries |
| **AsNoTracking** | Read-only query optimization |

## DecentDB-Specific Features

### Metadata & Versioning
```csharp
var abiVersion = DecentDBConnection.AbiVersion();
var engineVersion = DecentDBConnection.EngineVersion();
```

### Schema Introspection
| Method | Returns |
|--------|---------|
| `ListTablesJson()` | JSON array of table names |
| `GetTableColumnsJson(table)` | Column metadata (name, type, constraints) |
| `ListIndexesJson()` | Index definitions |
| `GetTableDdl(table)` | CREATE TABLE statement |
| `ListViewsJson()` | View names |
| `GetViewDdl(view)` | CREATE VIEW statement |
| `ListTriggersJson()` | Trigger definitions |

### ADO.NET GetSchema
```csharp
connection.GetSchema("Tables")      // List all tables
connection.GetSchema("Columns")    // Column metadata
connection.GetSchema("Indexes")     // Index information
```

### Database Maintenance
- **Checkpoint** - Flush WAL to main database file
- **SaveAs** - Export database to new file
- **VacuumAtomicAsync** - Compact a file-backed database with the maintenance helper

### Provider Ergonomics
- `DecentDBConnectionStringBuilder` can be passed directly to `UseDecentDB(...)`
- The showcase uses builder-driven EF Core configuration
- Typed setup keeps ADO.NET and EF Core configuration aligned

### Performance Sanity Patterns
- Projection vs tracked reads to illustrate over-fetching tradeoffs
- `AsNoTracking()` for read-heavy paths
- `AsSplitQuery()` over included relationship graphs
- Keyset-style pagination patterns for stable paging
- `AsAsyncEnumerable()` timing-oriented smoke coverage for streaming-friendly paths
- Explicit note that these are sanity checks, **not** benchmark claims

### Transaction State
```csharp
var inTransaction = connection.InTransaction;  // Engine-verified state
```

## NodaTime Integration

The `DecentDB.EntityFrameworkCore.NodaTime` package provides first-class support for NodaTime types in Entity Framework Core.

### Supported NodaTime Types

| NodaTime Type | Storage | Description |
|---------------|--------|-------------|
| `Instant` | INT64 (Unix ticks) | Point in UTC time |
| `LocalDate` | INT64 (epoch days) | Calendar date without time |
| `LocalDateTime` | INT64 (Unix ticks) | Date and time without zone |

### Type Mappings

NodaTime types are automatically mapped to their integer storage representations:
- **Instant** stored as Unix ticks (100-nanosecond intervals since 1970-01-01 UTC)
- **LocalDate** stored as days since 1970-01-01 (Hinnant civil calendar algorithm)
- **LocalDateTime** stored as Unix ticks in UTC

### Member Translations

The NodaTime provider translates these LocalDate member accesses to SQL:

| NodaTime Member | SQL Expression |
|-----------------|----------------|
| `LocalDate.Year` | Extracted from epoch days |
| `LocalDate.Month` | Extracted from epoch days |
| `LocalDate.Day` | Extracted from epoch days |
| `LocalDate.DayOfYear` | Extracted from epoch days |

### NodaTime Query Examples

```csharp
// Filter by LocalDate equality
var todayEntries = await context.ScheduleEntries
    .Where(e => e.ScheduledDate == today)
    .ToListAsync();

// Filter by LocalDate range (BETWEEN)
var upcomingEntries = await context.ScheduleEntries
    .Where(e => e.ScheduledDate >= startDate && e.ScheduledDate <= endDate)
    .OrderBy(e => e.ScheduledDate)
    .ToListAsync();

// Extract year/month from LocalDate
var thisYearEntries = await context.ScheduleEntries
    .Where(e => e.ScheduledDate.Year == 2026)
    .ToListAsync();

var thisMonthEntries = await context.ScheduleEntries
    .Where(e => e.ScheduledDate.Month == 3)
    .ToListAsync();

// Query by Instant
var recentEntries = await context.ScheduleEntries
    .Where(e => e.ScheduledInstant >= Instant.FromDateTimeUtc(DateTime.UtcNow))
    .ToListAsync();

// MIN/MAX with Instant
var earliest = await context.ScheduleEntries
    .OrderBy(e => e.ScheduledInstant)
    .FirstOrDefaultAsync();

var latest = await context.ScheduleEntries
    .OrderByDescending(e => e.ScheduledInstant)
    .FirstOrDefaultAsync();

// GROUP BY LocalDate
var entriesByDate = await context.ScheduleEntries
    .GroupBy(e => e.ScheduledDate)
    .Select(g => new { Date = g.Key, Count = g.Count() })
    .ToListAsync();
```

### Using NodaTime in Your Application

1. **Add the NuGet package reference:**
```xml
<PackageReference Include="NodaTime" Version="3.3.0" />
<ProjectReference Include="DecentDB.EntityFrameworkCore.NodaTime" />
```

2. **Configure NodaTime in DbContext:**
```csharp
protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
{
    optionsBuilder.UseDecentDB(connectionString, builder =>
    {
        builder.UseNodaTime();
    });
}
```

3. **Add NodaTime properties to entities:**
```csharp
using NodaTime;

public class Event
{
    public Instant ScheduledAt { get; set; }
    public LocalDate EventDate { get; set; }
    public LocalDateTime CreatedAt { get; set; }
}
```

## Entity Models

The showcase defines these entity types to demonstrate various features:

| Entity | Purpose |
|--------|---------|
| `Product` | Standard entity with decimal, bool, datetime, guid, bytes |
| `Category` | Unique constraints, DateOnly, TimeOnly |
| `Customer` | Email uniqueness, nullable fields |
| `Order` | Enum support, decimal totals |
| `OrderItem` | Order line items, decimal pricing |
| `Address` | Double for coordinates, bool flags |
| `Tag` | Simple entity for many-to-many |
| `ProductTag` | Composite primary key join table |
| `Employee` | Self-referencing, row versioning |
| `AppEventLog` | Primitive collections (JSON arrays) |
| `ScheduleEntry` | NodaTime types (Instant, LocalDate, LocalDateTime) |
| `WarehouseLocation` | Composite primary key parent entity |
| `InventoryCount` | Composite foreign key child relationship |

## Architecture

The showcase exercises the complete stack:

```
┌─────────────────────────────────────────────────────────────────┐
│                         Showcase Program                          │
├─────────────────────────────────────────────────────────────────┤
│  Entity Framework Core                                           │
│  ├── LINQ Queries (Where, Select, GroupBy, etc.)               │
│  ├── Change Tracking                                            │
│  ├── Migrations Support                                        │
│  └── Type Mappings                                            │
├─────────────────────────────────────────────────────────────────┤
│  DecentDB.EntityFrameworkCore                                   │
│  ├── String Method Translators (Contains, ToUpper, etc.)       │
│  ├── Math Method Translators (Abs, Ceiling, Floor, etc.)       │
│  ├── Primitive Collection Translators (JSON arrays)              │
│  └── SQL Generation                                            │
├─────────────────────────────────────────────────────────────────┤
│  DecentDB.EntityFrameworkCore.NodaTime                          │
│  ├── NodaTime Type Mappings (Instant, LocalDate, LocalDateTime)│
│  └── Member Translators (Year, Month, Day, DayOfYear)          │
├─────────────────────────────────────────────────────────────────┤
│  DecentDB.AdoNet                                                │
│  ├── ADO.NET Provider Implementation                           │
│  ├── Prepared Statement Caching                                 │
│  └── Parameter Binding                                          │
├─────────────────────────────────────────────────────────────────┤
│  DecentDB.Native (P/Invoke)                                    │
│  ├── C ABI Function Bindings                                    │
│  └── Safe Handle Management                                     │
├─────────────────────────────────────────────────────────────────┤
│  DecentDB Engine (Rust)                                        │
│  ├── B+Tree Storage                                            │
│  ├── WAL (Write-Ahead Logging)                                 │
│  └── ACID Transactions                                         │
└─────────────────────────────────────────────────────────────────┘
```

## Output

When you run the showcase, you'll see output demonstrating:

```
═══════════════════════════════════════════════════════════════════════════════════
  DECENTDB METADATA & VERSION
═══════════════════════════════════════════════════════════════════════════════════
  ABI Version:        1
  Engine Version:      2.0.2

═══════════════════════════════════════════════════════════════════════════════════
  EF CORE BASIC CRUD OPERATIONS
═══════════════════════════════════════════════════════════════════════════════════
  CREATE: Category 'Electronics' created with ID 1
  UPDATE: Category description updated
  READ:   Retrieved category: Electronics
  DELETE: Category 'Electronics' deleted

═══════════════════════════════════════════════════════════════════════════════════
  LINQ QUERIES
═══════════════════════════════════════════════════════════════════════════════════
  COUNT:    Total products: 1
  FILTER:   Products > $1000: 0
  ORDER BY: Top 5 most expensive: ...

═══════════════════════════════════════════════════════════════════════════════════
  NODATIME OPERATIONS (Instant, LocalDate, LocalDateTime)
═══════════════════════════════════════════════════════════════════════════════════
  CREATE: Created 3 ScheduleEntry records with NodaTime types
  READ: All entries: 3
    - NodaTime Meeting: Instant=2026-03-30T18:30:27Z, Date=Monday, March 30, 2026
  FILTER: Pending entries: 2
  BETWEEN: Upcoming entries (next 14 days): 3
  LocalDate.Year/Month: This month's entries: 1
  MIN/MAX: Earliest/Latest entries...
  GROUP BY LocalDate: 3 unique dates

═══════════════════════════════════════════════════════════════════════════════════
  COMPOSITE FOREIGN KEYS
═══════════════════════════════════════════════════════════════════════════════════
  Parent composite key: (WH-EAST, A-01)
  Child saved through EF nav: InventoryCount #1 -> (WH-EAST, A-01)
  Include/query over composite FK: 1 matching inventory counts
```

## Testing

The showcase serves as both a demonstration and validation of:

1. **A broad set of EF Core features** work correctly with DecentDB
2. **DecentDB-specific features** are properly exposed
3. **NodaTime integration** provides accurate date/time handling
4. **Type conversions** preserve precision across all data types
5. **Schema generation** creates correct DDL statements

### Running the Showcase

The showcase is intended to run to completion successfully on a healthy build. When you run it:

```bash
dotnet run --project examples/DecentDb.ShowCase/DecentDb.ShowCase.csproj
```

If it throws, treat that as a regression in the engine, EF Core provider, or showcase sample rather than expected behavior.

## Limitations

There are no showcase-specific EF Core limitations currently tracked here.

If a scenario demonstrated below stops working, treat that as a regression in the
engine, provider, or sample rather than expected behavior.

## EF Core Feature Coverage Matrix

This showcase validates the following EF Core features against DecentDB:

| Feature | Status | Notes |
|---------|--------|-------|
| Basic CRUD (Create, Read, Update, Delete) | ✅ Working | |
| LINQ Where/Filter | ✅ Working | |
| LINQ Select/Projection | ✅ Working | |
| LINQ OrderBy/ThenBy | ✅ Working | |
| LINQ Skip/Take (Pagination) | ✅ Working | |
| LINQ GroupBy | ✅ Working | |
| LINQ Distinct | ✅ Working | |
| LINQ Count/LongCount | ✅ Working | |
| LINQ Min/Max/Sum/Average | ✅ Working | |
| String operations (Contains, StartsWith, etc.) | ✅ Working | |
| String transformations (ToUpper, ToLower, etc.) | ✅ Working | |
| Math operations (Abs, Ceiling, Floor, Round) | ✅ Working | |
| Math operations (Max, Min) | ✅ Working | |
| DateTime queries | ✅ Working | |
| NodaTime (Instant, LocalDate, LocalDateTime) | ✅ Working | |
| NodaTime member access (Year, Month, Day) | ✅ Working | |
| Primitive collections (JSON arrays) | ✅ Working | |
| Transactions | ✅ Working | Explicit isolation-level selection is covered in tests |
| Savepoints | 🚫 Not supported | EF transaction savepoint APIs intentionally throw `NotSupportedException` |
| Concurrency control | ✅ Working | |
| Raw SQL (FromSqlRaw) | ✅ Working | |
| Change tracking | ✅ Working | |
| Bulk operations (AddRange, ExecuteUpdateAsync, ExecuteDeleteAsync) | ✅ Working | Rowcount and persistence are covered in provider tests |
| EF.Functions.Like pattern matching | ✅ Working | |
| Set operations (Union, Concat, Intersect, Except) | ✅ Working | Server-side translation is covered in provider tests |
| Window functions via `EF.Functions` | ✅ Working | Provider-specific LINQ helpers translate to `OVER (...)` SQL |
| Include / ThenInclude | ✅ Working | Navigation properties over foreign keys are supported, including composite FKs in the showcase |
| Explicit JOINs | ✅ Working | |
| Subqueries (correlated) | ✅ Working | |
| Existence queries (Any/All) | ✅ Working | |
| Query composition (reusable IQueryable) | ✅ Working | |
| SelectMany | ✅ Working | |
| AsAsyncEnumerable() | ✅ Working | Async streaming-friendly query enumeration is covered in provider tests |
| Composite primary keys / foreign keys | ✅ Working | `ProductTag` and `WarehouseLocation` / `InventoryCount` demonstrate composite key mapping |
| AsNoTracking | ✅ Working | |
| Keyset pagination | ✅ Working | |
| Provider ergonomics (`UseDecentDB(DecentDBConnectionStringBuilder)`) | ✅ Working | Typed builder configuration is covered in provider tests and used by the showcase |
| Performance sanity patterns | ✅ Working | Showcase timings + dedicated EF sanity tests cover tracked vs no-tracking, split queries, keyset paging, async streaming, and bulk mutation rowcounts |

The matrix above reflects the current supported showcase surface for DecentDB + EF Core.
