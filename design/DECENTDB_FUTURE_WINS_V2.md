# DecentDB: Future Wins & Real Differentiators

This document reframes DecentDB’s future roadmap around a blunt market truth:

> **DecentDB should not try to win by becoming “SQLite, but with more features.”**
>
> It should win by becoming the embedded SQL engine that makes **local-first sync, branchable data workflows, and agent-friendly developer experience** feel native and obvious.

DecentDB already has meaningful foundations in place. The next phase should focus less on accumulating checklist items and more on building a clear product identity that developers can remember in one sentence.

## Core Positioning

### The one-line story
**DecentDB is the embedded SQL database built for modern apps, offline sync, and AI-assisted development.**

### The strategic insight
Several capabilities that once felt like obvious “wow” features are no longer unique enough to carry the positioning story by themselves:

- Browser/WASM support matters, but it is no longer unique.
- JSONB matters, but it is no longer unique.
- Full-text search matters, but it is increasingly expected.
- Vector search matters, but it is rapidly becoming a baseline checkbox.

Those are still good features. They are just not the strongest identity anchors anymore.

The true opportunity is to own the space where other embedded engines still feel incomplete:

- **native sync**
- **branch / diff / restore workflows**
- **developer ergonomics across languages**
- **agent-friendly introspection and repair**
- **portable application database bundles**

---

## What DecentDB Already Has Going For It

This roadmap should treat existing DecentDB capabilities as foundations, not as future aspirations.

### Already-shipped or materially-advanced strengths
- Native rich types: `TIMESTAMP`, `UUID`, `DECIMAL`
- JSON scalar and table functions
- Trigram indexes, expression indexes, partial indexes
- Recursive CTEs, savepoints, generated columns, temp tables
- `INSERT ... ON CONFLICT`
- `RETURNING`
- EF Core integration
- Cost-based optimizer and `ANALYZE`
- In-memory VFS for testing
- Bulk load API foundation
- Same-process shared WAL visibility

That means the future roadmap should emphasize **identity-level wins**, not just “fill remaining parity gaps.”

---

## The New Strategic Pillars

## 1. Local-First Sync as a First-Class Capability
### Why this matters
This is the strongest opportunity for DecentDB to become memorable.

Embedded databases are often fantastic as local stores, but the moment an application needs:

- offline writes
- background sync
- conflict handling
- selective replication
- device-to-device movement
- browser/desktop/mobile/server continuity

the burden usually shifts into custom middleware, background jobs, triggers, queues, or third-party sync systems.

### The DecentDB win
DecentDB should aim for:

- built-in changesets
- built-in push/pull replication
- selective table or row-scope sync
- conflict resolution rules
- resumable replication streams
- deterministic merge behavior

### The desired developer experience
```sql
CREATE PUBLICATION field_sync
  FOR TABLE customers, jobs, invoices
  WHERE tenant_id = $tenant;

PUSH CHANGES TO 'https://sync.example.com' USING field_sync;
PULL CHANGES FROM 'https://sync.example.com' USING field_sync;
SHOW SYNC STATUS;
SHOW CONFLICTS;
RESOLVE CONFLICTS USING LAST_WRITE_WINS;
```

### Why this is a separator
SQLite can be part of local-first systems, but the sync story is generally external or low-level. DecentDB can win by making sync feel like a native database feature, not a stack of adjacent products.

### Priority
**Top-tier strategic bet.**

---

## 2. Branchable Databases: Branch, Diff, Restore, Merge
### Why this matters
Branching is already one of the most distinctive ideas in the current roadmap. It should be expanded into a full workflow, not treated as a niche optimization.

A copied database file is useful.
A **branchable data workflow** is memorable.

### The DecentDB win
Move from “copy-on-write branching” to a broader story:

- cheap local branches
- schema-safe migration rehearsal
- data diffs
- point-in-time restore
- time-travel reads
- guarded branch merge flows where feasible

### Desired workflow
```sql
CREATE BRANCH feature_pricing FROM main;
SHOW DIFF main..feature_pricing;
SELECT * FROM orders AS OF '2026-03-01T12:00:00Z';
RESTORE DATABASE TO TXID 1844221;
MERGE BRANCH feature_pricing INTO main;
```

### Important framing
A full arbitrary Git-like merge engine for relational data is dangerous and probably out of scope in early phases.

But these are absolutely realistic and high-value:

- branch for test environments
- branch for migration rehearsal
- restore to transaction / timestamp
- compare branch state
- narrow, rule-bound merge pathways

### Why this is a separator
Very few embedded engines feel natively designed for:

- ephemeral environments
- AI agent sandboxes
- migration rehearsal
- support/debug workflows
- “what if” simulations

DecentDB can.

### Priority
**Top-tier strategic bet.**

---

## 3. Schema-First, Strongly-Typed SDK Generation
### Why this matters
The engine can be excellent and still lose adoption if the integration story feels hand-built.

Developers remember ergonomics.

### The DecentDB win
Given a DecentDB schema, generate strongly typed bindings and helpers for:

- .NET
- Python
- Go
- Node/TypeScript
- Java
- Rust

### Output should include
- generated types/models
- query result contracts
- migration compatibility checks
- parameter binding helpers
- schema drift detection
- optional repository/query wrappers

### Desired workflow
```bash
decentdb generate --lang csharp --schema db.ddb --out ./Generated
decentdb generate --lang typescript --schema db.ddb --out ./src/generated
decentdb generate --lang python --schema db.ddb --out ./client
```

### Why this is a separator
This turns DecentDB from “just an embedded database” into a cross-language application platform with a much sharper onboarding story.

### Priority
**Top-tier strategic bet.**

---

## 4. Built-in Doctor, Advisor, and Self-Inspection
### Why this matters
Embedded databases often fail silently from the developer’s perspective. There is no DBA watching them. There is just an application team trying to figure out why things feel weird.

This is especially important in a world where coding agents are generating significant portions of application code.

### The DecentDB win
Expose a first-class diagnostics surface:

- `PRAGMA doctor;`
- `decentdb doctor app.ddb`
- `sys.*` virtual tables
- structured JSON output for agents and CI
- safe fix suggestions

### Example outputs
- missing or unused indexes
- slow query snapshots
- WAL growth / checkpoint pressure
- fragmentation and oversized rows
- hot JSON path access
- branch ancestry and branch locks
- sync lag / replication errors
- suspicious schema patterns
- unsafe pragmas or deployment settings

### Desired workflow
```bash
decentdb doctor ./app.ddb --format markdown
decentdb doctor ./app.ddb --format json
```

### Why this is a separator
This aligns perfectly with modern developer expectations:
- fast local diagnosis
- CI gate integration
- agent-readable output
- fewer “mystery slow” incidents

### Priority
**Top-tier strategic bet.**

---

## 5. Application Database Bundle Format
### Why this matters
SQLite is often used as a portable application file format, but that story is mostly accidental. DecentDB can make it explicit.

### The DecentDB win
Treat a DecentDB file as a portable app bundle that may contain:

- relational data
- blobs/assets
- metadata manifest
- optional FTS/vector indexes
- optional encryption
- optional signatures
- optional sync metadata
- export/import tooling

### Use cases
- desktop apps
- field/offline data packs
- reproducible bug reports
- AI workspace snapshots
- sharable demo datasets
- import/export across products

### Possible commands
```bash
decentdb export-bundle ./customer.ddb ./customer.ddbx
decentdb verify-bundle ./customer.ddbx
decentdb import-bundle ./customer.ddbx ./restored.ddb
```

### Why this is a separator
It gives DecentDB a concrete “product object” developers can reason about and move around, rather than just “a file the library happens to use.”

### Priority
**High-value platform bet.**

---

## 6. Policy-Aware Embedded SQL
### Why this matters
Encryption-at-rest is good, but in many applications it is not enough.

A meaningful embedded database differentiator would be policy built into the engine for applications that need local protection and auditable behavior.

### The DecentDB win
Support combinations of:

- page encryption
- column encryption
- masked projections
- row filters
- append-only audit trails
- signed change history

### Example direction
```sql
CREATE POLICY tenant_filter
  ON invoices
  USING tenant_id = current_tenant();

CREATE MASK ssn_mask
  ON employees(ssn)
  USING '***-**-' || right(ssn, 4);
```

### Why this is a separator
This creates a much stronger story for regulated, enterprise, offline, and field-device scenarios than TDE alone.

### Priority
**Selective but powerful differentiator.**

---

## 7. Native Geospatial Data Support
### Why this matters
Location-aware applications are pervasive: mobile apps, IoT, logistics, field service, geospatial analytics. SQLite's geo story is limited to R-Tree indexes for bounding boxes and Geopoly for simple polygons. DecentDB can do better.

The geo opportunity is not about being SpatiaLite — it's about making location data first-class without requiring an extension.

### The DecentDB win
Expose native `GEOGRAPHY` and `GEOMETRY` types with:

- **Native types**: `GEOGRAPHY(POINT)`, `GEOGRAPHY(POLYGON)`, `GEOMETRY` with SRID support
- **Spatial indexing**: R*-Tree index (better than SQLite's R-Tree for dynamic data)
- **Core functions**: `ST_Distance`, `ST_Contains`, `ST_Within`, `ST_Intersects`, `ST_Point`, `ST_AsGeoJSON`
- **Spherical calculations**: Haversine and Vincenty formulas for Earth geometry
- **Input/output**: WKB, WKT, and GeoJSON formats

### Desired developer experience
```sql
CREATE TABLE locations (
    id INTEGER PRIMARY KEY,
    name TEXT,
    coordinates GEOGRAPHY(POINT, 4326),  -- WGS84
    boundary GEOGRAPHY(POLYGON, 4326)    -- optional polygon
);

-- Nearest-neighbor query with spatial index
SELECT name, ST_Distance(coordinates, ST_Point(-122.4194, 37.7749))
FROM locations
WHERE ST_DWithin(coordinates, ST_Point(-122.4194, 37.7749), 5000)  -- within 5km
ORDER BY coordinates <-> ST_Point(-122.4194, 37.7749)
LIMIT 10;

-- Point-in-polygon
SELECT id, name FROM zones WHERE ST_Contains(boundary, ST_Point(-122.4194, 37.7749));
```

### Why this is a differentiator
| Approach | SQLite | DecentDB |
|---|---|---|
| Point storage | Two REAL columns | Native POINT type |
| Distance calculation | Manual Haversine SQL | `ST_Distance` built-in |
| Spatial index | R-Tree (static) | R*-Tree (dynamic) |
| Polygon operations | Geopoly (convex only) | Full polygon support |
| Type safety | None | Static geography type |

### Implementation considerations
- **Rust crate**: Leverage `geo` or `geosine` for geometry math
- **SRID support**: WGS84 (4326) default; planar vs spherical mode
- **WASM compatibility**: Required for browser/OPFS use cases
- **Performance**: Spatial index must be zero-copy with page cache

### Priority
**Tier 3 — Feature-completeness win. Useful for specific verticals (mobile, IoT, logistics) but not a primary separator.**

---

## Market Reality Check: Which Features Help But Do Not Define the Story

The following features are still worthwhile. They simply should not be the center of the DecentDB identity because they are increasingly table stakes or already available elsewhere in some form.

### Valuable but not primary separators
- First-class WASM / browser support
- JSONB
- Full-text search
- Vector / HNSW indexes
- Native geospatial types & R*-Tree index
- Built-in HTTP serve mode
- Group commit
- Compression
- TDE
- Bulk-load ergonomics
- Replication transport
- Object storage VFS

These are good roadmap items. They are just better framed as **enablers** than as the main “why DecentDB exists” story.

---

## Updated Prioritization

## Tier 1 — Identity-Defining Roadmap
These are the capabilities most likely to give DecentDB a memorable market position.

| Priority | Theme | Why it matters |
|---|---|---|
| 1 | Native local-first sync & merge | Strongest product identity and real painkiller |
| 2 | Branch / diff / restore / time-travel | Memorable workflow; ideal for agents, tests, support |
| 3 | Schema-first typed SDK/codegen | Adoption accelerator across languages |
| 4 | Doctor / advisor / introspection | High leverage for humans and coding agents |

## Tier 2 — High-Value Platform Multipliers
These improve the product substantially and reinforce the core story.

| Priority | Theme | Why it matters |
|---|---|---|
| 5 | Application database bundle format | Portable, shareable, product-friendly artifact |
| 6 | Policy-aware embedded SQL | Enterprise and regulated-use strength |
| 7 | Built-in observability / `sys.*` | Strong complement to doctor/advisor workflows |
| 8 | WASM + OPFS | Essential for local-first browser adoption |
| 9 | Built-in HTTP / remote serve mode | Makes DecentDB usable in edge/server helper scenarios |

## Tier 3 — Performance and Feature-Completeness Wins
Important, but these should support the larger story rather than define it.

| Priority | Theme | Why it matters |
|---|---|---|
| 10 | Transparent write queuing | Makes single-writer reality feel modern and painless |
| 11 | Group commit / WAL batching | Major write throughput improvement |
| 12 | Cross-process WAL coordination | Better real-world app architecture support |
| 13 | JSONB | Better JSON performance and indexing |
| 14 | Compression | Smaller files, better cache behavior |
| 15 | Bulk-load follow-ons | Better ETL and migration workflows |
| 16 | Non-blocking schema migration | Valuable operational differentiator |
| 17 | Native geospatial types & R*-Tree | First-class location data, spatial indexes, ST_* functions |

## Tier 4 — Important Capability Checks
Strong features, but less likely to be the decisive reason someone chooses DecentDB.

| Priority | Theme | Why it matters |
|---|---|---|
| 18 | Vector / HNSW index | AI-era expectation; useful but less unique |
| 19 | Full-text search | Search expectation; good for completeness |
| 20 | TDE | Valuable and practical, but not identity-defining alone |
| 21 | WAL streaming replication | Helpful for HA/read-scale scenarios |
| 22 | Object storage VFS | Interesting deployment story, especially edge/serverless |

---

## Revised Roadmap Themes

## A. DecentDB should own “offline + sync”
This is the category where DecentDB can become hard to ignore.

Build toward:
- native changesets
- sync publications/subscriptions
- push/pull replication
- resumable streams
- merge rules
- conflict inspection tooling
- browser/desktop/mobile/server parity

## B. DecentDB should own “branchable relational workflows”
This is the category where DecentDB can feel futuristic.

Build toward:
- branch
- diff
- restore
- time-travel reads
- migration rehearsal
- ephemeral test copies
- narrow merge workflows

## C. DecentDB should own “developer experience for serious apps”
This is the category where adoption accelerates.

Build toward:
- schema-first code generation
- typed bindings
- migration drift checks
- doctor/advisor
- agent-readable diagnostics
- reproducible support bundles

## D. DecentDB should remain excellent at core engine mechanics
These features are still important, but they should serve the bigger product story:

- write queuing
- group commit
- JSONB
- FTS
- vector
- geo / spatial types and R*-Tree
- compression
- TDE
- cross-process coordination
- non-blocking DDL
- observability
- object storage VFS

---

## Recommended Near-Term Sequence

## Phase 1 — Tighten the identity
Focus on features that create the clearest product story fast.

1. Expand observability into a real **doctor/advisor** experience
2. Deliver **branch + restore + diff** before chasing more parity features
3. Start designing **changesets and sync protocol primitives**
4. Formalize a **schema-first code generation** toolchain

## Phase 2 — Make local-first real
1. WASM + OPFS
2. selective sync publications
3. pull/push replication
4. conflict visibility and deterministic merge policies

## Phase 3 — Make DecentDB operationally inevitable
1. serve mode
2. application bundle format
3. policy-aware data controls
4. richer background maintenance / migration operations

## Phase 4 — Continue performance and capability depth
1. JSONB
2. write queuing + group commit refinements
3. compression
4. vector
5. FTS
6. geo / spatial types and R*-Tree index
7. object storage VFS
8. replication and HA enhancements

---

## Messaging Guidance

### Good positioning
- **The embedded SQL database for modern local-first apps**
- **Branchable relational data for apps, agents, and edge**
- **Embedded SQL with native sync**
- **A serious application database, not just a file format**

### Weak positioning
- “SQLite but faster”
- “SQLite but with more features”
- “SQLite alternative”
- “Embedded Postgres-lite”

Those may be useful comparison points, but they should not be the core identity.

---

## Conclusion

DecentDB does not need to beat every embedded database at every existing checkbox.

It needs to become the obvious answer for developers who want all of the following at once:

- embedded relational storage
- strong SQL ergonomics
- native local-first sync
- branchable workflows
- agent-friendly diagnostics
- portable app-grade database artifacts

That is a real lane.

If DecentDB executes on that lane, the story changes from:

> “Here is another embedded database.”

to:

> **“Here is the embedded SQL engine designed for how modern applications are actually built.”**

---

## Appendix: Current Market Notes Informing This Rewrite

These notes are included so this roadmap reflects the current market more honestly.

### Observations
- Official SQLite now has a WASM/OPFS story, which reduces browser support as a unique differentiator.
- SQLite now has JSONB support, which reduces JSONB as a flagship separator by itself.
- SQLite R-Tree and Geopoly provide basic geo capabilities but lack type safety and full geometry support.
- PostGIS is the reference standard for geo in serious RDBMS; SpatiaLite extends SQLite with similar capabilities.
- SQLite already has strong FTS support.
- DuckDB has broadened via extensions in areas like FTS and vector search.
- The modern gap is less about raw feature presence and more about integrated workflows, sync, and developer experience.

### Reference URLs
- SQLite WASM / OPFS:
  - https://sqlite.org/wasm/doc/trunk/persistence.md
- SQLite JSONB:
  - https://sqlite.org/jsonb.html
- SQLite session / changesets:
  - https://sqlite.org/sessionintro.html
- SQLite R-Tree:
  - https://sqlite.org/rtree.html
- SQLite Geopoly:
  - https://www3.sqlite.org/geopoly/
- SpatiaLite (geo extension for SQLite):
  - https://www.gaia-gis.it/fossil/libspatialite/index
- PostGIS:
  - https://postgis.net/
- DuckDB full-text search:
  - https://duckdb.org/docs/stable/core_extensions/full_text_search.html
- DuckDB vector similarity search:
  - https://duckdb.org/docs/stable/core_extensions/vss.html
- LiteDB docs:
  - https://www.litedb.org/docs/
- H2 features:
  - https://www.h2database.com/html/features.html
