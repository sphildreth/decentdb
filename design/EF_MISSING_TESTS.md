# DecentDB EF Core & ADO.NET Test Coverage Gaps — Remediation Plan

**Status:** Draft — ready for implementation
**Scope:** `bindings/dotnet/` (DecentDB.AdoNet, DecentDB.EntityFrameworkCore, DecentDB.EntityFrameworkCore.NodaTime, their test projects)
**Author trigger:** April 2026 regression where `HasData` seeds for `bool` and `Guid` emitted SQL that DecentDB's strict type system rejected (`0`/`1` for BOOLEAN columns; `X'...'` blob literals for UUID columns). Neither failure was caught by any existing test because the `HasData` literal-emission path was essentially uncovered.

---

## 1. Background

### 1.1 The three value paths through the stack

Every CLR value that reaches DecentDB travels through one of three independent code paths. A bug in any one of them can hide in production until the right workload hits:

| Path | Where | How value becomes SQL | What it exercises |
|---|---|---|---|
| **A. ADO.NET parameter binding** | `DecentDBParameter` → `DecentDBCommand.BindValue` (`bindings/dotnet/src/DecentDB.AdoNet/DecentDBCommand.cs:500-618`) | Value is bound to a prepared statement slot via `sqlite3_bind_*`-style native calls (`BindInt64`, `BindDouble`, `BindText`, `BindBlob`) | Raw `IDbCommand.Parameters.Add(...)` + `ExecuteReader`/`ExecuteNonQuery` |
| **B. EF Core parameterized SaveChanges** | EF's `RelationalTypeMapping.Converter` runs `ConvertToProvider(clrValue)` → result handed to path A | Same path A underneath, but the *provider value* (post-converter) is what gets bound | `DbContext.Add(...); SaveChanges();` |
| **C. EF Core literal SQL emission** | `RelationalTypeMapping.GenerateSqlLiteral(value)` — returns a SQL fragment that is inlined directly into the generated SQL. Used by `HasData`, migrations, and some query translation constants | No native binding; pure string formatting | `modelBuilder.Entity<T>().HasData(...)` + `Database.EnsureCreated()` / `Database.Migrate()` |

**Key insight:** Path C is where the April 2026 regression lived. It has no parameter serialization — every supported CLR type must produce a string that DecentDB's parser accepts *as text*. DecentDB is deliberately stricter than SQLite:

- `INSERT INTO t (flag) VALUES (0)` fails with `cannot cast Int64(0) to BOOL` — SQLite would silently coerce.
- `INSERT INTO t (id) VALUES (X'AA...')` fails with `cannot cast Text("xAA...") to UUID` — SQLite treats `X'...'` as a blob literal, DecentDB parses it as text.
- Only `UUID_PARSE('...')` produces a UUID literal; only `TRUE`/`FALSE` produce a boolean literal.

Every CLR type whose EF mapping uses a `ValueConverter` is a latent Path C bug candidate, because EF's default literal generator operates on the *CLR* value (and emits e.g. `'2024-01-02 03:04:05'` for `DateTime`) while our storage expects the *converted* value (`1704164645000000` as `INTEGER`).

### 1.2 Types currently mapped

`bindings/dotnet/src/DecentDB.EntityFrameworkCore/Storage/Internal/DecentDBTypeMappingSource.cs` (`_clrMappings`, lines 86–104) declares:

| CLR | Store | Converter | Current literal behavior |
|---|---|---|---|
| `bool` | `BOOLEAN` | none | **Fixed** — `DecentDBBoolTypeMapping` emits `TRUE`/`FALSE` |
| `byte` | `INTEGER` | none | Default integer literal |
| `short` | `INTEGER` | none | Default integer literal |
| `int` | `INTEGER` | none | Default integer literal |
| `long` | `INTEGER` | none | Default integer literal |
| `float` | `REAL` | none | Default float literal |
| `double` | `REAL` | none | Default float literal |
| `decimal` | `DECIMAL(18,4)` | none (scale via `DecimalScaleNormalizer` at bind time only) | Default decimal literal |
| `string` | `TEXT` | none | Default string literal |
| `byte[]` | `BLOB` | none | **Suspect** — EF default is `X'...'` which DecentDB parses as TEXT |
| `DateTime` | `TIMESTAMP` (µs since epoch, INTEGER) | `DateTime ↔ long` | **Suspect** — EF default literal for `DateTime` is ISO string |
| `DateTimeOffset` | `TIMESTAMP` (µs since epoch, INTEGER) | `DateTimeOffset ↔ long` | **Suspect** — same |
| `DateOnly` | `INTEGER` (days since epoch) | `DateOnly ↔ long` | **Suspect** — same |
| `TimeOnly` | `INTEGER` (ticks) | `TimeOnly ↔ long` | **Suspect** — same |
| `TimeSpan` | `INTEGER` (ticks) | `TimeSpan ↔ long` | **Suspect** — same |
| `Guid` | `UUID` | `Guid ↔ byte[]` | **Fixed** — `DecentDBGuidTypeMapping` emits `UUID_PARSE('...')` |

`DecentDBNodaTimeTypeMappingSource` adds:

| CLR | Store | Converter | Current literal behavior |
|---|---|---|---|
| `NodaTime.Instant` | `INTEGER` (Unix ticks) | `Instant ↔ long` | **Suspect** |
| `NodaTime.LocalDate` | `INTEGER` (days since epoch) | `LocalDate ↔ long` | **Suspect** |
| `NodaTime.LocalDateTime` | `INTEGER` (ticks) | `LocalDateTime ↔ long` | **Suspect** |

Types accepted by `DecentDBCommand.BindValue` but **not** in `_clrMappings`:
- `ushort`, `uint`, `ulong` — bound via `unchecked((long)value)` (`DecentDBCommand.cs:508-524`). EF falls back to a default mapping whose behavior for DecentDB is untested.
- `Enum` — bound via `Convert.ToInt64(value)` (`DecentDBCommand.cs:612-616`). Works for `int`-backed enums at bind time; EF literal emission untested.

Types with **no support at all**:
- `sbyte` — throws `NotSupportedException: Unsupported parameter type: System.SByte`.
- `char` — same.

### 1.3 Current test inventory (relevant files)

- `bindings/dotnet/tests/DecentDB.EntityFrameworkCore.Tests/HasDataLiteralRegressionTests.cs` — only Path C test, covers `bool` + `Guid` (3 tests).
- `bindings/dotnet/tests/DecentDB.EntityFrameworkCore.Tests/TypeMappingTests.cs` — Path A/B for `DateTime`, `DateTimeOffset`, `DateOnly`, `TimeOnly`, `TimeSpan`, `Guid`, `decimal` via converter round-trip.
- `bindings/dotnet/tests/DecentDB.EntityFrameworkCore.Tests/ComprehensiveCrudTests.cs` — Path B for most primitives.
- `bindings/dotnet/tests/DecentDB.EntityFrameworkCore.Tests/DecimalPrecisionTests.cs` — Path A/B for decimal precision; no Path C.
- `bindings/dotnet/tests/DecentDB.EntityFrameworkCore.Tests/NullableAggregateShapeTests.cs` — nullable primitives in queries, Path B only.
- `bindings/dotnet/tests/DecentDB.EntityFrameworkCore.Tests/NodaTimeIntegrationTests.cs` — Path A/B for NodaTime, no Path C.
- `bindings/dotnet/tests/DecentDB.Tests/AllDataTypesTests.cs` — Path A for raw primitives and nullables.

**No file exercises**:
- Path C for any type other than `bool`/`Guid`.
- A direct "does `GenerateSqlLiteral` produce DecentDB-parseable SQL?" contract test.
- Round-trip behavior for `DateTimeKind.Local`/`Unspecified`, `DateTimeOffset` with non-zero offset, `ushort`/`uint`/`ulong`, `sbyte`, `char`, long-backed enums, or nullable NodaTime.

---

## 2. Coverage matrix (the audit output)

Legend: ✅ covered · ⚠️ partial/implicit · ❌ missing

| CLR type | A (ADO.NET) | B (EF SaveChanges) | C (HasData literal) | Risk if C wrong |
|---|---|---|---|---|
| `bool` | ✅ | ✅ | ✅ (`HasDataLiteralRegressionTests`) | — resolved |
| `bool?` | ✅ | ✅ | ❌ | Low (`NULL` literal is trivial, but non-null path re-enters `DecentDBBoolTypeMapping`, so same risk) |
| `byte` / `short` / `int` / `long` | ✅ | ✅ | ❌ | Low — integer literals are basic |
| nullable variants of above | ✅ | ✅ | ❌ | Low |
| `float` / `double` | ✅ | ✅ | ❌ | **Medium** — `NaN`, `±Infinity`, `-0.0`, subnormals have provider-specific literal syntax |
| nullable `float`/`double` | ✅ | ✅ | ❌ | Medium |
| `decimal` | ✅ | ✅ | ❌ | **High** — EF's default literal is `1234.56`; with custom `HasPrecision` + strict DecentDB scale check, `1.005m` into `DECIMAL(10,2)` could reject |
| `decimal?` | ✅ | ✅ | ❌ | High |
| `string` | ✅ | ✅ | ❌ | **High** — embedded `'`, `\`, newlines, CJK/emoji Unicode never tested in literal path |
| `byte[]` | ✅ | ✅ | ❌ | **Very high** — DecentDB does NOT accept `X'...'` as a BLOB literal (our probe confirmed it is parsed as TEXT). EF's default literal is `X'...'`. Almost certainly broken today. |
| `Guid` | ✅ | ✅ | ✅ (`HasDataLiteralRegressionTests`) | — resolved |
| `Guid?` | ✅ | ✅ | ❌ | Same resolution path, but nullable path untested |
| `Guid.Empty` | ⚠️ | ⚠️ | ❌ | Low |
| `DateTime (Utc)` | ✅ | ✅ | ❌ | **Very high** — converter expects `long` µs-since-epoch; EF default literal is `'2024-01-02 03:04:05.000'`. Guaranteed parse failure against INTEGER column. |
| `DateTime (Local)` | ❌ | ❌ | ❌ | **High** — converter calls `ToUniversalTime()` at bind time; literal path has no such conversion |
| `DateTime (Unspecified)` | ❌ | ❌ | ❌ | High — ambiguous semantics |
| `DateTime?` | ✅ | ✅ | ❌ | Very high |
| `DateTimeOffset (UTC)` | ✅ | ✅ | ❌ | Very high — same class of bug as `DateTime` |
| `DateTimeOffset (non-zero offset)` | ❌ | ❌ | ❌ | High — silent drift if converter sometimes skipped |
| `DateTimeOffset?` | ✅ | ✅ | ❌ | Very high |
| `DateOnly` / `DateOnly?` | ✅ | ✅ | ❌ | Very high — EF default literal is `'2024-02-04'`, storage expects `int` days |
| `TimeOnly` / `TimeOnly?` | ✅ | ✅ | ❌ | Very high — EF default literal is `'09:30:15'`, storage expects `long` ticks |
| `TimeSpan` / `TimeSpan?` | ✅ | ✅ | ❌ | Very high — EF default literal is `'12:30:00'`, storage expects `long` ticks |
| `enum` (int-backed) | ⚠️ | ✅ | ❌ | Medium |
| `enum` (byte/short/long-backed) | ❌ | ❌ | ❌ | Unknown |
| `enum?` | ❌ | ⚠️ | ❌ | Unknown |
| `char` / `char?` | ❌ (throws) | ❌ | ❌ | **Hard failure** — `NotSupportedException` at bind time, no test asserts either support or a clear error |
| `sbyte` / `sbyte?` | ❌ (throws) | ❌ | ❌ | Hard failure |
| `ushort` / `uint` | ⚠️ (binds via unchecked cast) | ⚠️ (no explicit EF mapping) | ❌ | Medium |
| `ulong` | ⚠️ | ⚠️ | ❌ | **High** — `ulong.MaxValue` overflows `long` silently in bind |
| unsigned nullables | ❌ | ❌ | ❌ | Same |
| `NodaTime.Instant` | ✅ | ✅ | ❌ | Very high |
| `NodaTime.LocalDate` | ✅ | ✅ | ❌ | Very high |
| `NodaTime.LocalDateTime` | ✅ | ✅ | ❌ | Very high |
| NodaTime nullables | ❌ | ❌ | ❌ | Very high |
| Large `byte[]` (>1 MB) | ❌ | ❌ | — (n/a) | Memory / chunking untested |

**Rough totals:** ~56 type/path cells, ~40 missing. Single highest-leverage area: Path C for every type with a converter.

---

## 3. Cross-cutting findings

### 3.1 Finding F1 — Path C is untested for every converted type

Every type whose mapping uses `WithComposedConverter` or a built-in converter (`DateTime`, `DateTimeOffset`, `DateOnly`, `TimeOnly`, `TimeSpan`, `Guid` before the fix, all three NodaTime types) is a Path C bug candidate. EF Core's default `GenerateSqlLiteral` runs on the CLR value, not the provider value, so the emitted literal does not match the INTEGER storage column. **This is the single largest risk.**

### 3.2 Finding F2 — BLOB literal is almost certainly broken

Our probe (recorded during the April 2026 fix) showed:
```
ERR: INSERT INTO g VALUES (3, X'AAAAAAAABBBBCCCCDDDDEEEEEEEEEEEE') -> cannot cast Text("xAAAAAAAA...") to UUID
```
DecentDB parses `X'...'` as `Text("x...")`. EF Core's `ByteArrayTypeMapping.GenerateNonNullSqlLiteral` emits exactly this form. Any `HasData` seed with a `byte[]` property will fail today. No test catches it.

### 3.3 Finding F3 — Three CLR types are advertised by the ADO.NET binder but absent from `_clrMappings`

`ushort`, `uint`, `ulong` are bound via `unchecked((long)value)` in `DecentDBCommand.cs`. EF Core's mapping source falls back to its default behavior for these, which may or may not pick them up. `ulong.MaxValue` in particular overflows `long` silently.

### 3.4 Finding F4 — Two CLR types throw `NotSupportedException` with no actionable diagnostic

`sbyte` and `char` are not in any mapping table and have no branch in `DecentDBCommand.BindValue`. A user who writes `public char Initial { get; set; }` in an entity will get:
```
NotSupportedException: Unsupported parameter type: System.Char
```
at the first `SaveChanges`. Either add support or reject at model-building time with a clear message.

### 3.5 Finding F5 — DateTime kind semantics are untested

`DecentDBTypeMappingSource.cs:34` converter normalizes via `value.ToUniversalTime()`. For `DateTimeKind.Unspecified` this treats the value as Local, which on a non-UTC CI runner silently shifts the stored instant. No test pins this.

### 3.6 Finding F6 — `DateTimeOffset` with non-zero offset is untested

The converter uses `UtcTicks`, which is correct, but no test exercises values like `new DateTimeOffset(2024, 1, 1, 12, 0, 0, TimeSpan.FromHours(5.5))`. A future refactor could easily regress this without breaking any test.

### 3.7 Finding F7 — Decimal precision/scale has no Path C test

`DecimalPrecisionTests` covers Path A/B only. A `HasData` seed with `HasPrecision(10,2)` and a value like `1.005m` will hit DecentDB's strict scale enforcement because EF's literal does not run through `DecimalScaleNormalizer`.

### 3.8 Finding F8 — String literal escaping untested in Path C

Embedded `'`, `\`, newlines, CJK, emoji, zero-width joiners, RTL marks — all pass through EF's `StringTypeMapping.GenerateNonNullSqlLiteral` which uses ANSI-style single-quote doubling. We do not assert DecentDB's parser agrees.

### 3.9 Finding F9 — No contract test for `GenerateSqlLiteral` executability

The most general version of the April 2026 regression would have been caught by a single parameterized test: enumerate every `RelationalTypeMapping` in `_clrMappings`, pick a representative value, call `GenerateSqlLiteral`, and execute `SELECT <literal>` against DecentDB. If the SELECT fails or the value does not round-trip, the mapping is broken. This test does not exist.

### 3.10 Finding F10 — NodaTime Path C is completely unprotected

`NodaTimeIntegrationTests` covers Path A/B. `HasDataLiteralRegressionTests` uses the NodaTime provider but only seeds `bool` and `Guid` — it never seeds an `Instant`, `LocalDate`, or `LocalDateTime`.

---

## 4. Implementation slices

Slices are designed to be independent, self-contained, and sequenced so each one's output is usable even if later slices are deferred. Each slice:
- Is a single PR's worth of work (~1–4 hours).
- Lists the exact files to add/modify.
- Lists the exact assertions required.
- Calls out which findings (F1–F10) it closes.

Slices are numbered S1–S10. **Recommended execution order is numeric.** Slices S1 and S2 are prerequisites for the rest because they establish the testing scaffold and the universal contract test.

### 4.1 S1 — Universal literal-executability contract test

**Closes:** F1, F2 (partial), F9.
**Priority:** **P0 — do this first.** Would have caught the April 2026 regression on its own.

**Goal:** A single parameterized test that, for every CLR type registered in `DecentDBTypeMappingSource._clrMappings` (and the NodaTime equivalents), calls `GenerateSqlLiteral` with a known value and executes the resulting SQL against a real DecentDB connection, asserting the value round-trips.

**Files to create:**
- `bindings/dotnet/tests/DecentDB.EntityFrameworkCore.Tests/LiteralExecutabilityContractTests.cs`

**Test shape:**

```csharp
public sealed class LiteralExecutabilityContractTests : IDisposable
{
    public static IEnumerable<object[]> Cases => new[]
    {
        new object[] { typeof(bool),           false },
        new object[] { typeof(bool),           true },
        new object[] { typeof(byte),           (byte)42 },
        new object[] { typeof(short),          (short)-12345 },
        new object[] { typeof(int),            int.MinValue },
        new object[] { typeof(int),            int.MaxValue },
        new object[] { typeof(long),           long.MinValue },
        new object[] { typeof(long),           long.MaxValue },
        new object[] { typeof(float),          3.14f },
        new object[] { typeof(double),         2.718281828 },
        new object[] { typeof(decimal),        1234.5678m },
        new object[] { typeof(string),         "hello" },
        new object[] { typeof(string),         "with 'single' quotes" },
        new object[] { typeof(string),         "line1\nline2" },
        new object[] { typeof(string),         "日本語 🎵 Straße" },
        new object[] { typeof(byte[]),         new byte[] { 0x01, 0x02, 0xFF, 0x00, 0xAB } },
        new object[] { typeof(Guid),           Guid.Parse("11111111-2222-3333-4444-555555555555") },
        new object[] { typeof(Guid),           Guid.Empty },
        new object[] { typeof(DateTime),       new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc) },
        new object[] { typeof(DateTimeOffset), new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero) },
        new object[] { typeof(DateOnly),       new DateOnly(2024, 2, 4) },
        new object[] { typeof(TimeOnly),       new TimeOnly(9, 30, 15) },
        new object[] { typeof(TimeSpan),       TimeSpan.FromHours(12.5) },
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public void GenerateSqlLiteral_ProducesExecutableRoundTrippableSql(Type clrType, object value)
    {
        using var ctx = CreateContext();
        var mapping = ctx.GetService<IRelationalTypeMappingSource>().FindMapping(clrType)!;
        var literal = mapping.GenerateSqlLiteral(value);

        using var conn = (DecentDBConnection)ctx.Database.GetDbConnection();
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT {literal}";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var actual = reader.GetValue(0);

        // Normalize through the same converter round-trip as the mapping
        var expectedProvider = mapping.Converter?.ConvertToProvider(value) ?? value;
        var actualProvider   = mapping.Converter?.ConvertToProvider(actual)  ?? actual;
        Assert.Equal(NormalizeForCompare(expectedProvider), NormalizeForCompare(actualProvider));
    }
}
```

**Acceptance:**
- All rows pass, OR
- Failing rows produce a clear fix-up ticket for S3–S9.

Expected initial failures (based on audit): `byte[]`, `DateTime`, `DateTimeOffset`, `DateOnly`, `TimeOnly`, `TimeSpan`. Each failure enters its corresponding slice.

---

### 4.2 S2 — Comprehensive `HasData` literal regression suite

**Closes:** F1, F2, F7, F8, F10 (partial).
**Priority:** P0. Would also have caught the April 2026 regression.

**Goal:** For each CLR type in the provider, define a minimal entity, seed a representative value via `HasData`, call `EnsureCreated`, and assert the seed row is persisted and queryable with the expected value.

**Files to create:**
- `bindings/dotnet/tests/DecentDB.EntityFrameworkCore.Tests/HasDataLiteralMatrixTests.cs`

**Structure:** one test method per type, or one parameterized `[Theory]` per primitive family. Suggested layout:

```
HasDataLiteralMatrixTests
├── EnsureCreated_ByteSeed_PersistsSeedRow
├── EnsureCreated_ShortSeed_PersistsSeedRow
├── EnsureCreated_IntSeed_PersistsSeedRow
├── EnsureCreated_LongSeed_PersistsSeedRow
├── EnsureCreated_FloatSeed_PersistsSeedRow                ← include NaN + Infinity cases
├── EnsureCreated_DoubleSeed_PersistsSeedRow               ← include NaN + Infinity cases
├── EnsureCreated_DecimalSeed_DefaultPrecision
├── EnsureCreated_DecimalSeed_CustomPrecision_10_2         ← closes F7
├── EnsureCreated_StringSeed_SimpleAscii
├── EnsureCreated_StringSeed_WithSingleQuotes              ← closes F8
├── EnsureCreated_StringSeed_WithBackslashAndNewline       ← closes F8
├── EnsureCreated_StringSeed_WithCjkAndEmoji               ← closes F8
├── EnsureCreated_ByteArraySeed_PersistsSeedRow            ← closes F2 (expected to fail until S4)
├── EnsureCreated_ByteArraySeed_EmptyArray
├── EnsureCreated_DateTimeSeed_UtcKind                     ← expected to fail until S5
├── EnsureCreated_DateTimeSeed_LocalKind                   ← closes F5
├── EnsureCreated_DateTimeOffsetSeed_UtcOffset             ← expected to fail until S5
├── EnsureCreated_DateTimeOffsetSeed_PositiveOffset        ← closes F6
├── EnsureCreated_DateTimeOffsetSeed_NegativeOffset        ← closes F6
├── EnsureCreated_DateOnlySeed_PersistsSeedRow             ← expected to fail until S5
├── EnsureCreated_TimeOnlySeed_PersistsSeedRow             ← expected to fail until S5
├── EnsureCreated_TimeSpanSeed_PersistsSeedRow             ← expected to fail until S5
├── EnsureCreated_GuidSeed_GuidEmpty
├── EnsureCreated_EnumSeed_IntBacked
├── EnsureCreated_EnumSeed_LongBacked
├── EnsureCreated_NullableBoolSeed_WithAndWithoutValue
├── EnsureCreated_NullableGuidSeed_WithAndWithoutValue
├── EnsureCreated_NullableDateTimeSeed_WithAndWithoutValue
├── [NodaTime scope] EnsureCreated_InstantSeed_PersistsSeedRow
├── [NodaTime scope] EnsureCreated_LocalDateSeed_PersistsSeedRow
└── [NodaTime scope] EnsureCreated_LocalDateTimeSeed_PersistsSeedRow
```

**Pattern:** mirror `HasDataLiteralRegressionTests`'s existing shape — inline DbContext + entity per test, `UseDecentDB($"Data Source={tempPath}")`, `EnsureCreated`, single-row assert, cleanup on `Dispose`.

**Acceptance:** Every test either passes, or is marked `[Fact(Skip="tracked in slice S#")]` with a link to the slice that fixes the mapping. No test may silently fail.

---

### 4.3 S3 — Fix `DateTime` / `DateTimeOffset` literal emission

**Closes:** F1 (for DateTime family), F5, F6.
**Depends on:** S2.
**Priority:** P0.

**Goal:** Introduce custom `DecentDBDateTimeTypeMapping` and `DecentDBDateTimeOffsetTypeMapping` that generate INTEGER literals matching the existing converter output. Replace the `WithComposedConverter` calls in both mapping sources.

**Files to add:**
- `bindings/dotnet/src/DecentDB.EntityFrameworkCore/Storage/DecentDBDateTimeTypeMapping.cs`
- `bindings/dotnet/src/DecentDB.EntityFrameworkCore/Storage/DecentDBDateTimeOffsetTypeMapping.cs`

**Files to modify:**
- `bindings/dotnet/src/DecentDB.EntityFrameworkCore/Storage/Internal/DecentDBTypeMappingSource.cs` — replace the `dateTimeMapping` / `dateTimeOffsetMapping` construction.
- `bindings/dotnet/src/DecentDB.EntityFrameworkCore.NodaTime/Storage/Internal/DecentDBNodaTimeTypeMappingSource.cs` — same substitution (the NodaTime source also declares CLR `DateTime`/`DateTimeOffset`).

**Implementation pattern** (follow `DecentDBGuidTypeMapping`):

```csharp
public sealed class DecentDBDateTimeTypeMapping : RelationalTypeMapping
{
    private static readonly ValueConverter<DateTime, long> Converter = new(
        v => (v.Kind == DateTimeKind.Utc ? v : v.ToUniversalTime()).Ticks / 10L - DateTime.UnixEpoch.Ticks / 10L,
        v => new DateTime(v * 10L + DateTime.UnixEpoch.Ticks, DateTimeKind.Utc));

    public DecentDBDateTimeTypeMapping()
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(DateTime), Converter),
            storeType: "TIMESTAMP",
            storeTypePostfix: StoreTypePostfix.None,
            dbType: System.Data.DbType.Int64)) { }

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var micros = (long)(Converter.ConvertToProvider(value) ?? 0L);
        return micros.ToString(CultureInfo.InvariantCulture);
    }
    // Clone override as in DecentDBGuidTypeMapping
}
```

**Acceptance:**
- `DateTime` / `DateTimeOffset` rows in S1 pass.
- S2 tests `EnsureCreated_DateTimeSeed_UtcKind`, `EnsureCreated_DateTimeSeed_LocalKind`, all `DateTimeOffset*` variants pass.
- `TypeMappingTests.TypeMappings_RoundTripDateTimeGuidAndDecimal` still passes (Converter signature preserved).
- Explicit test for `DateTimeKind.Unspecified`: document chosen semantics (recommend: treat as `Utc`; throw at bind if ambiguity is undesirable) and assert it.

---

### 4.4 S4 — Fix `DateOnly` / `TimeOnly` / `TimeSpan` literal emission

**Closes:** F1 (for time family).
**Depends on:** S2.
**Priority:** P0.

**Goal:** Same pattern as S3 for the three date/time primitives stored as INTEGER.

**Files to add:**
- `bindings/dotnet/src/DecentDB.EntityFrameworkCore/Storage/DecentDBDateOnlyTypeMapping.cs`
- `bindings/dotnet/src/DecentDB.EntityFrameworkCore/Storage/DecentDBTimeOnlyTypeMapping.cs`
- `bindings/dotnet/src/DecentDB.EntityFrameworkCore/Storage/DecentDBTimeSpanTypeMapping.cs`

**Files to modify:** both type-mapping sources.

**Acceptance:** S1 and S2 rows for these three types pass.

---

### 4.5 S5 — Fix `byte[]` (BLOB) literal emission

**Closes:** F2.
**Depends on:** S1.
**Priority:** P0 — almost certainly a latent production bug today.

**Goal:** Provide a BLOB literal form that DecentDB accepts. Options (to be confirmed by a probe):
1. Find or add a DecentDB built-in function like `BLOB_PARSE('hex')` / `FROM_HEX('...')` — preferred.
2. If no such function exists, file an engine ticket and use a string literal through an explicit cast.

**Investigation step (sub-slice S5a):**
- Run `SELECT X'00FF'`, `SELECT CAST('00FF' AS BLOB)`, `SELECT FROM_HEX('00FF')` against a scratch DecentDB connection.
- Document which, if any, works.

**Implementation (sub-slice S5b):**
- `bindings/dotnet/src/DecentDB.EntityFrameworkCore/Storage/DecentDBByteArrayTypeMapping.cs` — override `GenerateNonNullSqlLiteral` to emit whatever form S5a identifies.
- If no server-side syntax exists, raise an ADR requesting one, and in the interim make the mapping throw a clear `NotSupportedException` from the literal path so seed-data bugs surface at compile-test time not in production.

**Acceptance:** `EnsureCreated_ByteArraySeed_PersistsSeedRow` and the empty-array variant pass, or they are skipped with a link to the engine ticket.

---

### 4.6 S6 — NodaTime literal emission

**Closes:** F10.
**Depends on:** S3 pattern.
**Priority:** P1.

**Goal:** Apply the S3/S4 pattern to `Instant`, `LocalDate`, `LocalDateTime`.

**Files to add:**
- `bindings/dotnet/src/DecentDB.EntityFrameworkCore.NodaTime/Storage/DecentDBInstantTypeMapping.cs`
- `bindings/dotnet/src/DecentDB.EntityFrameworkCore.NodaTime/Storage/DecentDBLocalDateTypeMapping.cs`
- `bindings/dotnet/src/DecentDB.EntityFrameworkCore.NodaTime/Storage/DecentDBLocalDateTimeTypeMapping.cs`

**Files to modify:**
- `bindings/dotnet/src/DecentDB.EntityFrameworkCore.NodaTime/Storage/Internal/DecentDBNodaTimeTypeMappingSource.cs`.

**Tests to add:**
- Extend `LiteralExecutabilityContractTests` with NodaTime cases (guarded by a fixture that enables `UseNodaTime()`).
- The three `NodaTime scope` rows in the S2 matrix.

**Acceptance:** All three NodaTime types round-trip through Path C.

---

### 4.7 S7 — Fill Path-A/B gaps for unsigned integers, enums, nullables

**Closes:** F3 (ushort/uint/ulong), enum-backed variants.
**Priority:** P1.

**Goal:** Make ADO.NET, EF SaveChanges, and EF literal paths explicit (and tested) for:
- `ushort`, `uint`, `ulong` and their nullables.
- `enum` with non-int underlying types (`byte`, `short`, `long`).
- Nullable enums.

**Files to modify:**
- `bindings/dotnet/src/DecentDB.EntityFrameworkCore/Storage/Internal/DecentDBTypeMappingSource.cs` — add entries to `_clrMappings` for `ushort`, `uint`, `ulong` using `LongTypeMapping` with an unchecked converter; add `ulong` overflow guard (throw in converter if `value > long.MaxValue`).
- `bindings/dotnet/src/DecentDB.AdoNet/DecentDBCommand.cs` — no change expected but verify `ulong` overflow behavior is safe.

**Tests to add:**
- Extend `AllDataTypesTests.cs` with rows for `ushort`/`uint`/`ulong`.
- Extend `LiteralExecutabilityContractTests` and `HasDataLiteralMatrixTests` with the same types and with `byte`-, `short`-, `long`-backed enums.

**Acceptance:** `ulong.MaxValue` either round-trips correctly OR throws a clear, typed exception — never silently truncates.

---

### 4.8 S8 — Handle `sbyte` and `char`

**Closes:** F4.
**Priority:** P2.

**Goal:** Either add first-class support or refuse at model-building time with a clear error.

**Recommended approach:** add support — `sbyte → INTEGER`, `char → TEXT(1)`. They are standard C# primitives and the cost is trivial.

**Files to modify:**
- `DecentDBCommand.cs` — add binding branches (`sbyte` → `BindInt64`, `char` → `BindText(value.ToString())`).
- `DecentDBTypeMappingSource.cs` — add `_clrMappings` entries.
- `DecentDBDataReader.cs` — add `GetChar` / `GetSByte` support if not already present.

**Tests to add:**
- `AllDataTypesTests.cs` rows for `sbyte` and `char`.
- `LiteralExecutabilityContractTests` rows.
- `HasDataLiteralMatrixTests` rows, including `char` with a Unicode code point > U+FFFF (surrogate pair) — document whether that is supported or rejected.

**Acceptance:** No entity with `char` or `sbyte` can throw a generic `NotSupportedException` in the binding path.

---

### 4.9 S9 — Decimal `HasData` with custom precision / scale

**Closes:** F7.
**Priority:** P1.

**Goal:** Ensure literal emission for `decimal` honors the column's `HasPrecision(p, s)` and either emits a value that passes DecentDB's scale check or rejects the seed at model build.

**Files to add/modify:**
- `bindings/dotnet/src/DecentDB.EntityFrameworkCore/Storage/DecentDBDecimalTypeMapping.cs` — subclass `DecimalTypeMapping`, override `GenerateNonNullSqlLiteral` to pre-round the value through `DecimalScaleNormalizer` using the mapping's declared scale.
- Wire into `CreateDecimalMapping` in `DecentDBTypeMappingSource.cs`.

**Tests to add:**
- `HasDataLiteralMatrixTests`:
  - `EnsureCreated_DecimalSeed_CustomPrecision_10_2` with `1.005m` (expected normalized to `1.01`, or rejected with a clear error — document the chosen behavior).
  - Negative and max-scale cases.

**Acceptance:** `HasPrecision(p, s)` + `HasData` behaves identically to `HasPrecision(p, s)` + `SaveChanges`.

---

### 4.10 S10 — Large BLOB and streaming

**Closes:** residual gap (no specific finding; observed during audit).
**Priority:** P3.

**Goal:** Add a Path A test that round-trips a 4 MB `byte[]` via parameter and a DataReader, asserting content equality and documenting the upper bound.

**Files to add:**
- `bindings/dotnet/tests/DecentDB.AdoNet.Tests/LargeBlobRoundTripTests.cs` (or in the EF tests project if no separate ADO.NET project exists).

**Acceptance:** One passing test; failure mode for oversized blobs is documented.

---

## 5. Sequencing and dependencies

```
S1 ──────────────┐
                 ├──► S3 ──► S4 ──► S6
S2 ──────────────┤          │
                 ├──► S5    └──► S9
                 │
                 └──► S7 ──► S8 ──► S10
```

- **S1 and S2 are independent and should land first.** They create the safety net; every subsequent slice is graded by how many S1/S2 rows it turns green.
- **S3, S4, S5, S6** are the literal-emission fixes; each depends only on the matching row in S1/S2.
- **S7, S8, S9** are type-surface expansions.
- **S10** is the large-payload edge case.

**Minimum viable regression-proof:** S1 + S2 + S3 + S4 + S5. After those five land, every data type our provider advertises will have both a generator-level contract test and a seed-path regression test.

---

## 6. Non-goals

- **Query-translation constant folding.** Path C also fires for constants embedded by the query translator (e.g. `Where(x => x.Flag == true)`). Existing query tests largely cover this via round-trip assertions, and the April 2026 regression was not a query-translation issue. Revisit only if S1 surfaces failures in query contexts.
- **Schema DDL literals.** `CREATE TABLE` defaults via `HasDefaultValue(...)` also use `GenerateSqlLiteral`, but that path is out of scope for this plan.
- **Provider-specific functions** (`GEN_RANDOM_UUID`, `UUID_PARSE`, geospatial) — covered by other specs.

---

## 7. Definition of done (for the whole epic)

The epic is complete when:
1. `LiteralExecutabilityContractTests` is green for every registered CLR mapping in both mapping sources.
2. `HasDataLiteralMatrixTests` is green for every row listed in §4.2 (or explicitly skipped with a linked engine ticket).
3. No CLR primitive documented as "supported" throws `NotSupportedException` anywhere in the binding path.
4. A single-PR regression of the kind fixed in April 2026 — reverting the bool or Guid literal change — fails at least one test in CI.

---

## 8. Appendix — probe commands used during audit

These commands were executed against a scratch DecentDB connection during the April 2026 fix and are reproduced here so implementers can replay them against a current build:

```
CREATE TABLE b (id INT64, flag BOOL)
INSERT INTO b VALUES (1, FALSE)                                         -- OK
INSERT INTO b VALUES (2, TRUE)                                          -- OK
INSERT INTO b VALUES (3, 0)                                             -- ERR: cannot cast Int64(0) to BOOL
INSERT INTO b VALUES (4, CAST(0 AS BOOL))                               -- ERR: cannot cast Int64(0) to BOOL

CREATE TABLE g (id INT64, u UUID)
INSERT INTO g VALUES (1, CAST('aaaaaaaa-...' AS UUID))                  -- ERR: cannot cast Text(...) to UUID
INSERT INTO g VALUES (2, 'aaaaaaaa-...')                                -- ERR
INSERT INTO g VALUES (3, X'AAAAAAAA...')                                -- ERR: parsed as Text("xAAA...")
INSERT INTO g VALUES (4, UUID_PARSE('aaaaaaaa-...'))                    -- OK
```

Key takeaways for implementers:
- Boolean literals: only `TRUE` / `FALSE`.
- UUID literals: only `UUID_PARSE('...')`.
- `X'...'` is **TEXT**, not BLOB. Confirm BLOB literal syntax before implementing S5.
- DecentDB does not implicitly coerce between integer and boolean, or between text and UUID. Every converter-backed type must emit its storage-level form.
