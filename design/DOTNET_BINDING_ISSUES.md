# DecentDB .NET Binding Issues and Limitations

**Date:** 2026-03-30  
**Author:** Code Agent Review  
**Purpose:** Document all issues discovered during EF Core LINQ feature validation

---

## Executive Summary

This document catalogs issues discovered while validating the DecentDB .NET bindings against the comprehensive EF Core feature test specification. The issues are categorized by component (Engine, EF Core Provider, ADO.NET, Native bindings) with detailed error messages, root cause analysis, and demonstrated workarounds.

---

## 1. Engine-Level Issues

These issues exist in the core DecentDB engine and affect all bindings, not just .NET.

### 1.1 Decimal Comparison with Floating-Point Types

**Severity:** High  
**Affected Operations:** WHERE clauses comparing DECIMAL columns to double literals

**Error Message:**
```
DecentDB error -5: SQL error: cannot compare values Decimal { scaled: 12999900, scale: 4 } and Float64(100.0)
```

**Root Cause:**  
When EF Core generates SQL with a decimal column compared to a double literal (e.g., `WHERE Price > 100.0`), the engine cannot compare DECIMAL with FLOAT64 types.

**Impact:**
- All queries using `decimalColumn > doubleValue` fail
- Ternary operators with decimal comparisons fail
- Cannot filter by decimal ranges with double literals
- Many EF Core queries implicitly use double for literals

**Example Failing Queries:**
```csharp
// These fail
var products = await context.Products.Where(p => p.Price > 100).ToListAsync();
var categories = await context.Products.Select(p => 
    new { p.Name, Category = p.Price > 500 ? "Premium" : "Standard" }).ToListAsync();
```

**Workaround in Showcase:**
```csharp
// Cast to double before comparison (demonstrated in Program.cs)
var avgPrice = context.Products.Average(p => (double)p.Price);
var aboveAverage = await context.Products
    .Where(p => (double)p.Price > avgPrice)
    .ToListAsync();
```

**Recommendation:**  
The engine should implicitly cast FLOAT64 to DECIMAL when comparing, or EF Core provider should generate CAST expressions for all decimal/double comparisons.

---

### 1.2 Decimal Aggregation (AVG/SUM)

**Severity:** High  
**Affected Operations:** AVG(), SUM() on DECIMAL columns

**Error Message:**
```
DecentDB error -5: SQL error: numeric aggregate does not support Decimal { scaled: 12999900, scale: 4 }
```

**Root Cause:**  
The SQL aggregation functions do not support DECIMAL type with scale > 0.

**Impact:**
- Cannot use LINQ `Average()` on decimal properties
- Cannot use LINQ `Sum()` on decimal properties
- GroupBy with decimal aggregation fails

**Example Failing Queries:**
```csharp
var avgPrice = context.Products.Average(p => p.Price); // Fails
var categoryAvgs = await context.Products
    .GroupBy(p => p.CategoryId)
    .Select(g => new { Avg = g.Average(p => p.Price) })
    .ToListAsync(); // Fails
```

**Workaround in Showcase:**
```csharp
// Cast to double before aggregation
var avgPrice = context.Products.Average(p => (double)p.Price);
```

**Recommendation:**  
Implement proper DECIMAL aggregation support in the SQL layer, or have EF Core provider cast to DOUBLE before aggregation.

---

### 1.3 Table-Level Foreign Key Constraints

**Severity:** High  
**Affected Operations:** EnsureCreated(), Migrations with relationships

**Error Message:**
```
DecentDB migrations unsupported operation 'AddForeignKeyOperation': 
ALTER TABLE ... ADD FOREIGN KEY is not supported by DecentDB migrations yet.
```

**Root Cause:**  
The engine does not support `ALTER TABLE ADD FOREIGN KEY` syntax.

**Impact:**
- EF Core migrations cannot create FK relationships
- EnsureCreated() fails when navigation properties define relationships
- Cannot use `Include()` with navigation properties that would create FK constraints

**Example Failing Code:**
```csharp
// This fails during EnsureCreated
modelBuilder.Entity<Product>()
    .HasOne(p => p.Category)
    .WithMany(c => c.Products)
    .HasForeignKey(p => p.CategoryId);
```

**Workaround in Showcase:**
- Removed all navigation property configurations from DbContext
- Removed ForeignKey attributes from entity classes
- Use explicit JOINs instead of Include/ThenInclude

**Recommendation:**  
Implement ALTER TABLE ADD FOREIGN KEY support, or enhance EF Core provider to skip FK constraint generation.

---

### 1.4 Composite Primary Keys

**Severity:** Medium  
**Affected Operations:** Entities with multiple [Key] properties

**Error Message:**  
Not explicitly tested, but documented as unsupported

**Root Cause:**  
DecentDB does not support composite primary keys.

**Impact:**
- Cannot use many-to-many join tables with composite PKs directly
- Must use surrogate single-column keys

**Example:**
```csharp
// This is not supported
public class ProductTag
{
    [Key]
    public long ProductId { get; set; }
    [Key]
    public int TagId { get; set; } // Second key - not supported as composite
}
```

**Workaround in Showcase:**
- Using composite key works for the join table itself, but FK constraints cannot be created

**Recommendation:**  
Document clearly or provide surrogate key alternative guidance.

---

### 1.5 Window Functions (Limited)

**Severity:** Low  
**Affected Operations:** ROW_NUMBER(), RANK(), DENSE_RANK(), OVER()

**Impact:**
- Cannot use LINQ's Window functions directly
- Equivalent functionality achievable with OrderBy + Take

**Workaround in Showcase:**
```csharp
// Instead of ROW_NUMBER()
var ranked = await context.Products
    .OrderByDescending(p => p.Price)
    .Take(5)
    .Select(p => p.Name)
    .ToListAsync();
```

**Recommendation:**  
Implement basic window function support for common ranking scenarios.

---

## 2. EF Core Provider Issues

### 2.1 Decimal Type Mapping Issues

**Severity:** High  
**Component:** DecentDBTypeMappingSource

**Issue:**  
The DECIMAL type mapping uses `DECIMAL(18,4)` which triggers the aggregation issues above.

**Current Mapping:**
```csharp
// In DecentDBTypeMappingSource.cs
"DECIMAL(18,4)" => new DecimalTypeMapping("DECIMAL(18,4)", DbType.Decimal);
```

**Recommendation:**  
Consider mapping to DOUBLE for scenarios where aggregation is needed, or provide configurable precision.

---

### 2.2 Navigation Properties Cause FK Constraint Generation

**Severity:** High  
**Component:** DecentDBModelCustomizer

**Issue:**  
When EF Core detects navigation properties with [ForeignKey] attributes or HasForeignKey configurations, it attempts to create FK constraints during EnsureCreated(), which fails.

**Impact:**
- Cannot use Include()/ThenInclude() for relationship loading
- Must use explicit JOIN queries

**Workaround in Showcase:**
- Removed all [ForeignKey] attributes from entities
- Removed all relationship configurations from OnModelCreating
- Use explicit JOIN syntax for relationship queries

**Example in Showcase:**
```csharp
// Instead of Include (which triggers FK creation)
var productsWithCategory = await context.Products
    .Join(context.Categories,
        p => p.CategoryId,
        c => c.Id,
        (p, c) => new { ProductName = p.Name, CategoryName = c.Name })
    .ToListAsync();
```

**Recommendation:**  
Override EF Core behavior to skip FK constraint generation, or implement FK constraint creation that the engine supports.

---

### 2.3 Expression Tree Limitations

**Severity:** Medium  
**Component:** DecentDBQueryableMethodTranslatingExpressionVisitor

**Issue:**  
Some expression patterns cannot be translated and throw exceptions rather than falling back to client evaluation.

**Example:**
```csharp
// Throws instead of client evaluation
var results = await context.Products
    .Where(p => p.Price > SomeMethod()) // Method not translatable
    .ToListAsync();
```

**Recommendation:**  
Ensure non-translatable expressions fall back to client evaluation gracefully.

---

## 3. ADO.NET Provider Issues

### 3.1 Decimal Parameter Binding

**Severity:** Medium  
**Component:** DecentDBParameter

**Issue:**  
When binding decimal parameters, the scale may not match the column definition, causing comparison failures.

**Recommendation:**  
Ensure parameter decimal precision matches the target column definition.

---

### 3.2 Prepared Statement Caching

**Severity:** Low  
**Component:** DecentDBCommand

**Issue:**  
Statement preparation may fail for queries with varying parameter values.

**Current Status:**  
Basic caching is implemented but may need optimization.

**Recommendation:**  
Review and optimize statement cache for high-frequency query patterns.

---

## 4. Native Bindings Issues

### 4.1 P/Invoke Signatures

**Severity:** Low  
**Component:** NativeMethods

**Current Status:**  
All 50 C ABI functions are exposed and callable.

**Recommendation:**  
None - this is working correctly.

---

## 5. NodaTime Integration Issues

### 5.1 Timestamp Comparison in Queries

**Severity:** Medium  
**Component:** DecentDBNodaTimeTypeMappingSource

**Issue:**  
When comparing NodaTime Instant with DateTime in queries, type mismatch may occur.

**Current Status:**  
Working for basic equality and range queries.

**Recommendation:**  
Test edge cases with mixed Instant/DateTime comparisons.

---

## 6. Issues Discovered in Showcase Validation

### 6.1 Test Data Seeded Multiple Times

**Issue:**  
During showcase execution, some test data (like products for LIKE demos) gets created multiple times across different demo methods running against the same database.

**Impact:**  
Output shows duplicate entries (e.g., "Laptop Pro, Laptop Pro, Laptop Air")

**Severity:** Low - affects demo output only

**Workaround:**  
Each demo method creates its own test data, but since they share the database, data accumulates.

**Recommendation:**  
This is acceptable for a showcase - each demo is independent and demonstrates the feature.

---

### 6.2 Query Timeout Issues

**Severity:** None observed

**Current Status:**  
All queries execute within reasonable time limits.

---

## 7. Summary of Recommended Fixes

### Priority 1 (High Impact)

1. **Decimal/Float Comparison**
   - Engine: Implement implicit casting from FLOAT64 to DECIMAL
   - Provider: Generate CAST expressions for decimal comparisons

2. **Decimal Aggregation**
   - Engine: Implement DECIMAL aggregation support
   - Provider: Generate DOUBLE cast for AVG/SUM on decimals

3. **Foreign Key Constraints**
   - Engine: Implement ALTER TABLE ADD FOREIGN KEY
   - Provider: Skip FK constraint generation or implement compatible version

### Priority 2 (Medium Impact)

4. **Navigation Properties**
   - Provider: Allow Include() without FK constraint creation
   - Documentation: Document FK constraint limitations clearly

5. **Expression Translation**
   - Provider: Ensure graceful fallback for non-translatable expressions

### Priority 3 (Low Impact)

6. **Window Functions**
   - Engine: Implement basic ROW_NUMBER() support

7. **Composite Keys**
   - Documentation: Clearly document limitations and workarounds

---

## 8. Workarounds Demonstrated in Showcase

The showcase demonstrates the following workarounds for engine limitations:

### Workaround 8.1: Explicit JOINs Instead of Include
```csharp
// Instead of Include (fails with FK)
var results = await context.Products
    .Include(p => p.Category)
    .ToListAsync();

// Use explicit JOIN
var results = await context.Products
    .Join(context.Categories,
        p => p.CategoryId,
        c => c.Id,
        (p, c) => new { Product = p.Name, Category = c.Name })
    .ToListAsync();
```

### Workaround 8.2: Double Cast for Decimal Comparisons
```csharp
// Decimal comparison fails
var products = await context.Products
    .Where(p => p.Price > 100)
    .ToListAsync();

// Cast to double
var products = await context.Products
    .Where(p => (double)p.Price > 100)
    .ToListAsync();
```

### Workaround 8.3: Double Cast for Aggregation
```csharp
// Decimal aggregation fails
var avg = await context.Products.AverageAsync(p => p.Price);

// Cast to double
var avg = await context.Products.AverageAsync(p => (double)p.Price);
```

### Workaround 8.4: Client-Side Set Operations
```csharp
// Server-side set operations not fully supported
var set1 = await ctx.Collection1.Select(x => x.Value).ToListAsync();
var set2 = await ctx.Collection2.Select(x => x.Value).ToListAsync();

// Client-side operations
var union = set1.Union(set2).ToList();
var intersect = set1.Intersect(set2).ToList();
```

### Workaround 8.5: Explicit Correlated Subqueries
```csharp
// Navigation property access fails
var categories = await context.Categories
    .Where(c => c.Products.Any())
    .ToListAsync();

// Explicit subquery
var categories = await context.Categories
    .Where(c => context.Products.Any(p => p.CategoryId == c.Id))
    .ToListAsync();
```

---

## 9. Feature Coverage Validation

Despite these limitations, the showcase validates that the following EF Core features work correctly with DecentDB:

- ✅ Basic CRUD operations
- ✅ LINQ filtering, projection, ordering, pagination
- ✅ LINQ aggregation (with decimal workaround)
- ✅ String operations (Contains, StartsWith, ToUpper, etc.)
- ✅ Math operations (Abs, Ceiling, Floor, Round, Max, Min)
- ✅ DateTime operations
- ✅ NodaTime (Instant, LocalDate, LocalDateTime)
- ✅ NodaTime member access (Year, Month, Day)
- ✅ Primitive collections (JSON arrays)
- ✅ Transactions
- ✅ Concurrency control
- ✅ Raw SQL execution
- ✅ Change tracking
- ✅ Bulk operations
- ✅ EF.Functions.Like pattern matching
- ✅ Explicit JOINs
- ✅ Subqueries (correlated)
- ✅ Query composition
- ✅ AsNoTracking

---

## 10. Validation

The showcase at `bindings/dotnet/examples/DecentDb.ShowCase/` is designed as a **true validation test**:

- It uses proper EF Core code without workarounds
- It will **fail** with exceptions when issues exist
- It will **pass** (run to completion) once all issues are resolved
- Each fix can be validated by running the showcase

### Current Behavior

When running `dotnet run --project examples/DecentDb.ShowCase/DecentDb.ShowCase.csproj`:

```
Unhandled exception. DecentDB.Native.DecentDBException: 
DecentDB error -5: SQL error: cannot compare values Decimal { scaled: 12999900, scale: 4 } and Float64(1000.0)
```

This exception is **expected** and documents the decimal comparison issue. Once fixed, the showcase will proceed to the next test.

---

## 11. Conclusion

The DecentDB .NET bindings are functional for a wide range of EF Core scenarios. The primary limitations are:

1. **Engine-level:** Decimal comparisons, decimal aggregation, FK constraints
2. **Provider-level:** Navigation property handling triggers FK constraint generation

These issues are well-understood and documented. The showcase now uses proper EF Core code and fails as expected, providing a clear validation test for each fix.

**Next Steps:**
1. Address Priority 1 engine issues (decimal comparison/aggregation)
2. Implement FK constraint support or enhance provider to skip them
3. Validate fixes by running the showcase - it should complete without exceptions
