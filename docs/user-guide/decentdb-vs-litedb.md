# DecentDB vs LiteDB: When to Choose Which

This document helps developers decide between **DecentDB** and **LiteDB** for
embedded application data. They are both serverless, file-backed databases, but
they use different data models and target different developer experiences.

> **Versions compared:** DecentDB 2.15.0 workspace behavior vs public LiteDB
> documentation as of 2026-06-28.
>
> **Scope note:** LiteDB is a .NET embedded document database. It supports a
> SQL-like query language, LINQ, BSON documents, indexes, ACID transactions, and
> a single-file deployment model. It is not a relational SQL engine in the same
> sense as DecentDB, SQLite, H2, or Firebird.
>
> **See also:** [DecentDB vs SQLite](decentdb-vs-sqlite.md),
> [DecentDB vs RocksDB](decentdb-vs-rocksdb.md), [SQL Feature
> Matrix](sql-feature-matrix.md), and [SQL Reference](sql-reference.md).

## They Compete for Local App Data, Not for the Same Data Model

LiteDB and DecentDB are both reasonable choices when an application needs a
local database file without running a separate server. The key distinction is
data model:

- **LiteDB** is a .NET document database. It stores BSON documents in
  collections, maps naturally to POCO classes, supports LINQ and SQL-like
  commands, and is shaped around document-oriented application state.
- **DecentDB** is a relational SQL database. It stores typed rows in tables,
  supports relational constraints, joins, SQL query planning, indexes,
  transactions, branch/diff workflows, local-first sync inspection, and
  engine-enforced policies/masks.

The short version:

- Choose **LiteDB** when the application is .NET-first and the data is naturally
  document-shaped.
- Choose **DecentDB** when the data is relational, cross-language, query-heavy,
  policy-sensitive, or needs DecentDB-native branch/sync workflows.

## At a Glance

| Dimension | DecentDB | LiteDB |
|-----------|----------|--------|
| **Core identity** | Rust-native embedded relational database | .NET embedded NoSQL document database |
| **Data model** | Tables, rows, columns, relational constraints | Collections of BSON documents |
| **Primary ecosystem** | Rust plus C ABI bindings for many languages, including .NET | .NET/C# |
| **Query style** | SQL with joins, constraints, aggregates, windows, CTEs | LINQ/fluent API plus SQL-like document query syntax |
| **Schema** | Explicit relational schema | Schema-less documents with POCO mapping |
| **Relationships** | Foreign keys, joins, cascades, constraints | Embedded documents or references; no relational JOIN model |
| **Transactions** | ACID SQL transactions | ACID transactions, cross-collection support documented |
| **Durability posture** | WAL + fsync-on-commit by default for native files | WAL/recovery behavior documented by LiteDB; connection/configuration dependent |
| **Concurrency** | One writer, many readers; local native cross-process WAL coordination when supported | No reader locks and per-collection writer locks in LiteDB v5 docs |
| **Search/indexes** | B-tree, full-text, trigram, expression/partial/covering subsets | Document-field indexes, partial document loading, index-only queries |
| **Security** | TDE, row policies, projection masks, audit context | Datafile encryption support |
| **Deployment** | Native library/CLI plus bindings | Small .NET DLL/NuGet package |

## When DecentDB Is the Better Fit

### 1. Your data is relational

Use DecentDB when your data has strong relationships, constraints, and joins:

```sql
CREATE TABLE customers (
  id INT PRIMARY KEY,
  email TEXT NOT NULL UNIQUE
);

CREATE TABLE orders (
  id INT PRIMARY KEY,
  customer_id INT NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
  total DECIMAL(10, 2) NOT NULL CHECK (total >= 0)
);

SELECT c.email, COUNT(o.id) AS order_count, SUM(o.total) AS total_spend
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.id
GROUP BY c.email
ORDER BY total_spend DESC;
```

LiteDB can represent related data through embedded documents or references, and
that can be exactly right for document-shaped applications. If the natural
model is normalized relational data, DecentDB will be simpler and safer.

### 2. You need engine-enforced constraints

DecentDB enforces relational invariants:

- primary keys;
- unique constraints;
- not-null constraints;
- check constraints;
- foreign keys;
- cascade and set-null actions.

```sql
INSERT INTO orders (customer_id, total) VALUES (999, 12.00);
-- Fails if customer 999 does not exist.
```

LiteDB applications can validate document shape and references in application
code. DecentDB is the better fit when the database itself must reject invalid
relationships.

### 3. You need SQL joins, aggregates, windows, and CTEs

DecentDB supports application SQL that is difficult to replace with a document
query API:

```sql
WITH monthly AS (
  SELECT customer_id,
         DATE_TRUNC('month', created_at) AS month,
         SUM(total) AS revenue
  FROM orders
  GROUP BY customer_id, DATE_TRUNC('month', created_at)
)
SELECT customer_id,
       month,
       revenue,
       LAG(revenue) OVER (PARTITION BY customer_id ORDER BY month) AS previous_revenue
FROM monthly;
```

LiteDB has a SQL-like document query language and LINQ integration, but it is
not a general relational engine with the same join/constraint model.

### 4. You need one database engine across languages

DecentDB exposes one engine through a stable C ABI and maintained bindings:

- Rust;
- C/C++;
- Python;
- .NET;
- Go;
- Java;
- Node.js;
- Dart.

LiteDB is intentionally .NET-centered. That is a major strength for C#
applications, but it is not the same cross-language engine strategy.

### 5. You need branch, diff, restore, and time-travel workflows

DecentDB supports local database branch workflows:

```bash
decentdb snapshot create --db app.ddb --name before-repair
decentdb branch create --db app.ddb --name repair-test --from before-repair
decentdb branch diff --db app.ddb --left main --right repair-test --format table
decentdb branch restore --db app.ddb --branch main --snapshot before-repair --dry-run
```

LiteDB users can copy files, rebuild datafiles, and use application-level
versioning. DecentDB is better when branch/diff/restore are expected database
features.

### 6. You need local-first sync inspection and conflict workflows

DecentDB exposes sync operations through SQL and CLI:

```sql
SELECT * FROM sys_sync_status;
SELECT * FROM sys_sync_conflicts;
SELECT * FROM sys_sync_doctor;
```

LiteDB can be used in applications that implement sync, but sync journals,
scopes, conflicts, retention, and doctor output are not LiteDB's core product
identity. Choose DecentDB when the local database should own those concepts.

### 7. You need local row policies, projection masks, and audit context

DecentDB has engine-level controls for local application data:

```sql
SET AUDIT CONTEXT tenant_id = 'tenant-a';

CREATE POLICY tenant_filter ON invoices
USING tenant_id = current_tenant();

CREATE MASK card_mask ON payments(card_number)
USING '**** **** **** ' || right(card_number, 4);
```

LiteDB supports datafile encryption, but row-level policy enforcement and
projection masking belong in application code. DecentDB is the better fit when
these controls need to be database behavior.

## When LiteDB Is the Better Fit

### 1. Your application is .NET-first and document-shaped

LiteDB is a natural fit for C# applications with POCO objects:

```csharp
public class Customer
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public string[] Phones { get; set; } = Array.Empty<string>();
}

using var db = new LiteDatabase("Filename=app.db");
var customers = db.GetCollection<Customer>("customers");
customers.Insert(new Customer { Name = "Alice", Phones = new[] { "555-0100" } });
customers.EnsureIndex(x => x.Name);
```

If the app already treats data as object documents and does not need relational
joins or constraints, LiteDB can be much more direct than SQL.

### 2. You want schema-less local data

Document databases are useful when records vary by shape:

```json
{
  "_id": 1,
  "name": { "first": "Alice", "last": "Nguyen" },
  "phones": ["555-0100", "555-0101"],
  "preferences": {
    "theme": "dark",
    "notifications": true
  }
}
```

DecentDB can store JSON and query JSON fields, but its primary model is
schema-first relational tables. If the application is mostly nested,
heterogeneous documents, LiteDB is the more idiomatic fit.

### 3. You want a tiny .NET-only dependency

LiteDB is distributed as a small .NET library and installed through NuGet. For
some desktop, local, or small web applications, that simplicity is the point:
no native library, no database server, no cross-language concern.

DecentDB is broader, but that also means native artifacts and binding surfaces.

### 4. You want LINQ and MongoDB-like document APIs

LiteDB's developer experience is built around .NET object mapping, LINQ, and
document collection APIs. If the team prefers that style over SQL, LiteDB is
likely the better fit.

DecentDB is SQL-first. It has a .NET binding, but it does not try to be a
document-object database.

### 5. You need embedded file storage inside the database

LiteDB includes FileStorage for storing files and streams inside the database,
similar in spirit to GridFS. That can be convenient for single-file desktop
applications.

DecentDB can store BLOBs, but it does not present the same document-database
file-storage API.

### 6. You need LiteDB Studio

LiteDB Studio provides a UI for exploring and managing LiteDB databases. If
that is part of your team or support workflow, it is a practical advantage.

DecentDB has CLI and documentation surfaces, but it is not LiteDB Studio.

## Side-by-Side Examples

### Document-shaped local settings

```csharp
// LiteDB: nested document state maps naturally to a C# object.
var profiles = db.GetCollection<UserProfile>("profiles");
profiles.Upsert(new UserProfile {
    Id = userId,
    Preferences = new Preferences { Theme = "dark", FontSize = 14 }
});
```

Prefer **LiteDB** when the data is naturally object/document state and the
application is .NET-centered.

### Relational invariants

```sql
-- DecentDB: relationships and invariants are database-enforced.
CREATE TABLE accounts (
  id INT PRIMARY KEY,
  email TEXT NOT NULL UNIQUE
);

CREATE TABLE sessions (
  id UUID PRIMARY KEY DEFAULT GEN_RANDOM_UUID(),
  account_id INT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE
);
```

Prefer **DecentDB** when invalid relationships must be impossible to commit.

### Local data governance

```sql
-- DecentDB: local database policy/mask behavior.
CREATE POLICY tenant_only ON orders
USING tenant_id = current_tenant();

CREATE MASK email_mask ON users(email)
USING left(email, 2) || '***';
```

Prefer **DecentDB** when local enforcement belongs in the database rather than
only in application code.

## Summary Decision Matrix

| Your situation | Recommendation |
|----------------|----------------|
| .NET-only application with POCO/document data | **LiteDB** |
| Schema-less nested documents are the natural model | **LiteDB** |
| LINQ/document API is preferred over SQL | **LiteDB** |
| Need a tiny NuGet-only embedded database | **LiteDB** |
| Need LiteDB Studio or FileStorage | **LiteDB** |
| Need relational constraints and joins | **DecentDB** |
| Need SQL aggregates, windows, CTEs, and typed tables | **DecentDB** |
| Need the same engine across many languages | **DecentDB** |
| Need branch/diff/restore/time-travel workflows | **DecentDB** |
| Need sync journal/conflict/retention inspection | **DecentDB** |
| Need row policies, projection masks, and audit context | **DecentDB** |

## Bottom Line

Pick **LiteDB** for .NET document storage: POCOs, BSON, LINQ, small NuGet
deployment, and schema-flexible local app data.

Pick **DecentDB** for relational embedded application data: SQL, constraints,
joins, multi-language bindings, branch workflows, sync inspection, and
engine-enforced local governance.

The fair question is not whether documents or tables are universally better.
The right model is the one that matches the application's data and operational
requirements.

## External References

- [LiteDB home page](https://www.litedb.org/)
- [LiteDB documentation](https://www.litedb.org/docs/)
- [LiteDB SQL-like SELECT documentation](https://www.litedb.org/api/query/)
- [LiteDB data structure documentation](https://www.litedb.org/docs/data-structure/)
- [LiteDB encryption documentation](https://www.litedb.org/docs/encryption/)
- [LiteDB repository](https://github.com/litedb-org/LiteDB)
