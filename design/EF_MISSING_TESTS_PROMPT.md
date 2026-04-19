# DecentDB EF Core / ADO.NET Type-Coverage Remediation Prompt

You are an AI coding agent working in the DecentDB repository
(the repo root). Your task is to execute the slices defined
in `design/EF_MISSING_TESTS.md` — a remediation plan that closes gaps in test
coverage for CLR/EF Core data types in the DecentDB .NET bindings. These gaps
were exposed by an April 2026 regression where `HasData` seeds for `bool` and
`Guid` emitted SQL that DecentDB's strict type system rejected. The fix
landed, but the surrounding audit showed that most converter-backed types
(`DateTime`, `DateTimeOffset`, `DateOnly`, `TimeOnly`, `TimeSpan`, `byte[]`,
and all NodaTime types) likely carry the same class of bug.

**Read `design/EF_MISSING_TESTS.md` end-to-end before writing any code.** It is
the single source of truth for what to build, in what order, and to what
acceptance bar. This prompt tells you *how* to execute it; the design document
tells you *what* to execute.

---

## 0. Ground rules

1. **Do not skip slices.** Execute S1 through S10 in the numeric order listed
   in §4 of the design document. Each slice is sized to be a single, reviewable
   PR. Never batch two slices into one commit.
2. **Do not modify unrelated files.** Your blast radius is confined to:
   - `bindings/dotnet/src/DecentDB.AdoNet/**`
   - `bindings/dotnet/src/DecentDB.EntityFrameworkCore/**`
   - `bindings/dotnet/src/DecentDB.EntityFrameworkCore.NodaTime/**`
   - `bindings/dotnet/tests/**`
   - `design/EF_MISSING_TESTS.md` (to tick off slice status only)
   - Any `design/adr/*.md` you create as a direct consequence of a slice.
3. **Do not suppress warnings.** `#pragma warning disable` and
   `<NoWarn>` are banned as a means of silencing `EF1001` or any other
   analyzer warning. If a warning appears, fix the underlying design (move
   types out of `.Internal`, make them public, etc. — the same pattern used
   when `DecentDBBoolTypeMapping` / `DecentDBGuidTypeMapping` were lifted out
   of the internal namespace).
4. **Never add `InternalsVisibleTo` to work around visibility.** If a type
   needs to be shared across assemblies, it is part of the provider's public
   surface; mark it `public` and put it in a non-`Internal` namespace.
5. **Do not create documentation or README files** unless the slice
   explicitly calls for an ADR. Inline XML doc comments on public types are
   welcome and encouraged.
6. **Preserve formatting.** Match the existing file's indentation, brace
   style, and `using` ordering. Do not reformat untouched code.
7. **No emojis** in code, comments, commit messages, or file contents unless
   the design document explicitly asks for them.
8. **One slice, one PR, one commit (or a small, coherent series).** Use a
   commit message of the form `S#: <short description>` — e.g.
   `S1: add LiteralExecutabilityContractTests`.

---

## 1. Required reading

Before starting each slice, re-read:

1. `design/EF_MISSING_TESTS.md` — specifically the slice you are about to
   execute (§4.x), plus §3 (findings) so you understand *why* you are
   writing the test.
2. The prior-art fix for `bool` and `Guid`:
   - `bindings/dotnet/src/DecentDB.EntityFrameworkCore/Storage/DecentDBBoolTypeMapping.cs`
   - `bindings/dotnet/src/DecentDB.EntityFrameworkCore/Storage/DecentDBGuidTypeMapping.cs`
   - `bindings/dotnet/tests/DecentDB.EntityFrameworkCore.Tests/HasDataLiteralRegressionTests.cs`
   Every new type mapping must follow the same pattern (public class,
   non-`Internal` namespace, `RelationalTypeMappingParameters`-based
   constructor, `Clone` override, override of `GenerateNonNullSqlLiteral`
   and — when a converter is present — `GenerateSqlLiteral`).
3. The DecentDB SQL reference for supported literal syntaxes:
   `docs/user-guide/sql-reference.md`.
4. Appendix §8 of the design document for the probe commands that show which
   literal forms DecentDB accepts (notably: `X'...'` is parsed as `TEXT`, not
   `BLOB`; `UUID_PARSE(...)` is the only UUID literal; boolean literals are
   only `TRUE`/`FALSE`).

---

## 2. Per-slice workflow

For every slice S1…S10, follow exactly these steps.

### 2.1 Plan

1. Re-read the slice's §4.x entry.
2. Write/update a todo list for the slice's sub-steps.
3. If the slice is marked "expected to fail until S#", confirm you have the
   right dependencies in place first.

### 2.2 If the slice needs a probe

Some slices (notably S5 — BLOB literal format) require a probe against a real
DecentDB connection before implementation. To run a probe:

1. Create a throwaway project under `/tmp/probeapp/` with a `ProjectReference`
   to `bindings/dotnet/src/DecentDB.AdoNet/DecentDB.AdoNet.csproj`.
2. Write a short `Program.cs` that opens a connection to a
   `/tmp/probe_*.ddb` file and runs each candidate literal form via
   `ExecuteNonQuery`, printing OK / ERR per row.
3. Record the probe results in the slice's PR description (or as a comment
   at the top of any new type-mapping file you add).
4. Do **not** commit the probe program. `/tmp` is fine.

### 2.3 Implement

1. Add new files under the paths listed in the slice. Public type-mapping
   classes live in `…/Storage/` (not `…/Storage/Internal/`). Test files go
   in `bindings/dotnet/tests/DecentDB.EntityFrameworkCore.Tests/` unless the
   slice specifies otherwise.
2. Modify the mapping sources
   (`DecentDBTypeMappingSource.cs`,
   `DecentDBNodaTimeTypeMappingSource.cs`) to wire the new mapping in.
3. Never downgrade an existing test. If an existing test (e.g.
   `TypeMappingTests.TypeMappings_RoundTripDateTimeGuidAndDecimal`) depended
   on the previous mapping shape (`Converter != null`, provider type =
   `byte[]`, etc.), your new mapping must preserve that contract. See
   `DecentDBGuidTypeMapping` for a worked example where a `Guid → byte[]`
   converter is kept *and* literal emission is overridden.

### 2.4 Verify

Run, from `bindings/dotnet/`:

```bash
dotnet build DecentDB.NET.sln --nologo 2>&1 | tail -40
dotnet test tests/DecentDB.EntityFrameworkCore.Tests/DecentDB.EntityFrameworkCore.Tests.csproj --nologo 2>&1 | tail -10
```

Your slice passes only if **all** of the following hold:

1. `dotnet build` reports `0 Warning(s), 0 Error(s)`.
   - No `EF1001`. No `CS####` warnings. No `NU####` warnings introduced by
     your change. If your change legitimately exposes a pre-existing
     warning that is not yours to fix, stop and escalate.
2. The full EF Core test project (currently 163 tests, plus whatever the
   slice adds) passes: `Failed: 0, Passed: N, Skipped: M`.
3. The tests added by this slice cover every acceptance bullet in the
   slice's §4.x entry.
4. Any test that is intentionally skipped must be `[Fact(Skip="tracked in
   slice S#")]` with a link in the skip reason to the slice that will
   unblock it. No silent skips, no `[Theory]` rows commented out.
5. No test uses `Assert.True(true)` or a placeholder assertion — every
   test method must make a meaningful assertion or not exist.

If any of the above fails, **do not mark the slice complete**. Iterate until
green or, if you have identified a true engine-side blocker, open an ADR
under `design/adr/` documenting the blocker and linking it from the slice's
skip reason.

### 2.5 Update the design doc

After a slice is fully green, edit `design/EF_MISSING_TESTS.md` and change
the slice's header from `### 4.x S# — <title>` to
`### 4.x S# — <title> — DONE` and add a one-line summary under the header of
what landed (e.g. `Landed in <commit-sha>: <short description>`). Do not
otherwise rewrite the slice.

### 2.6 Commit

Commit the slice with:

```
S#: <short description>

<one-paragraph summary of what the slice did and what acceptance bars it hit>

Closes findings: F#, F#.

Co-Authored-By: Abacus.AI CLI <agent@abacus.ai>
```

Only commit when the user explicitly asks. If the user has not asked, stop
after §2.5 and report status.

---

## 3. Slice-specific guidance

These notes augment — they do not replace — §4.x of the design document.

### 3.1 S1 — Universal literal-executability contract test

- Use xUnit's `[Theory]` + `[MemberData]` rather than hand-rolling a loop.
- The test must consume the same `IRelationalTypeMappingSource` the provider
  registers (retrieve via `context.GetService<IRelationalTypeMappingSource>()`)
  — do not instantiate `DecentDBTypeMappingSource` directly.
- For types with a `Converter`, compare both sides through the converter so
  you do not get false negatives from CLR-level equality quirks (e.g.
  `DateTime` with differing `Kind`).
- Expect this slice to produce a list of failing rows. **That is the
  point.** Record those failures and they become the work list for
  S3/S4/S5/S6. Do not "fix" failures in this slice — S1 only writes the
  test.

### 3.2 S2 — Comprehensive `HasData` matrix

- Mirror `HasDataLiteralRegressionTests` exactly: inline `DbContext` per
  test, temp `.ddb` path with GUID suffix, `IDisposable.Dispose()` cleaning
  up the file and `-wal` sidecar.
- For float/double NaN/Infinity cases, use
  `Assert.Equal(float.NaN, seeded.Value)` with awareness that
  `Assert.Equal` handles NaN correctly — do not use `==`.
- Tests for types not yet fixed must be marked with
  `[Fact(Skip="blocked on S#")]`. They are part of this slice's deliverable
  so that the next slice can unskip them one-by-one.

### 3.3 S3 / S4 / S6 — Custom mapping pattern

Use this template (adapt store type, converter, literal format):

```csharp
using System.Globalization;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace DecentDB.EntityFrameworkCore.Storage;

public sealed class DecentDB<Foo>TypeMapping : RelationalTypeMapping
{
    private static readonly ValueConverter<<Clr>, long> FooConverter = new(
        clr => /* clr -> long */,
        provider => /* long -> clr */);

    public DecentDB<Foo>TypeMapping()
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(<Clr>), FooConverter),
            storeType: "INTEGER",
            storeTypePostfix: StoreTypePostfix.None,
            dbType: System.Data.DbType.Int64)) { }

    private DecentDB<Foo>TypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters) { }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new DecentDB<Foo>TypeMapping(parameters);

    public override string GenerateSqlLiteral(object? value)
        => value is null ? "NULL" : GenerateFromProvider(ToProvider(value));

    protected override string GenerateNonNullSqlLiteral(object value)
        => GenerateFromProvider(ToProvider(value));

    private static long ToProvider(object value)
        => value switch
        {
            <Clr> clr => (long)(FooConverter.ConvertToProvider(clr) ?? 0L),
            long l    => l,
            _ => throw new InvalidCastException(
                $"Cannot convert {value.GetType()} to <Clr> literal.")
        };

    private static string GenerateFromProvider(long provider)
        => provider.ToString(CultureInfo.InvariantCulture);
}
```

Key points:
- `GenerateSqlLiteral` must be overridden (not just `GenerateNonNullSqlLiteral`)
  so EF does not bypass you for the null case or mis-route through the
  converter.
- Accept both CLR and provider values in the `value` parameter — EF can hand
  you either depending on call site (observed in practice).
- Always round `InvariantCulture`.

### 3.4 S5 — BLOB literal

- **Probe first** (see §2.2). Candidate forms to try:
  - `SELECT X'00FF'`
  - `SELECT CAST('00FF' AS BLOB)`
  - `SELECT FROM_HEX('00FF')`
  - `SELECT BLOB_PARSE('00FF')`
  - Any other built-in listed in `docs/user-guide/sql-reference.md`.
- If none of these work, the outcome of this slice is an ADR under
  `design/adr/` proposing a new DecentDB built-in (e.g. `BLOB_PARSE(hex)`),
  plus a test mapping that throws a clear `NotSupportedException` from
  `GenerateNonNullSqlLiteral` with a message pointing at the ADR. Do not
  silently emit broken SQL.

### 3.5 S7 — Unsigned integers and non-int enums

- `ulong` must not silently truncate. Add an explicit guard in the converter:
  `if (value > long.MaxValue) throw new OverflowException(...)`.
- For enums, prefer not enumerating every underlying type; instead, add a
  generic fallback in `DecentDBTypeMappingSource.FindMapping` that detects
  `clrType.IsEnum` and returns a long-backed mapping with an appropriate
  converter.

### 3.6 S8 — `sbyte` and `char`

- `char` should map to `TEXT(1)`. Decide up-front whether surrogate pairs
  (code points > U+FFFF, which don't fit in a single `char`) are supported
  and write a test that pins the decision.
- Update `DecentDBDataReader.GetChar` if it does not already return the
  single-char string.

### 3.7 S9 — Decimal precision in literals

- Do not re-implement scale rounding; reuse
  `DecentDB.AdoNet.DecimalScaleNormalizer` (the same helper `DecentDBCommand`
  uses). This keeps bind-time and literal-time semantics identical.

---

## 4. Global verification (before opening any PR)

After every slice, and especially before the final PR of the epic, run from
the repo root:

```bash
# Native build (tests need the cdylib).
cargo build -p decentdb

# .NET — full solution, warnings-as-errors mindset.
cd bindings/dotnet
dotnet build DecentDB.NET.sln --nologo
dotnet test  DecentDB.NET.sln --nologo -v minimal
```

Required outcomes:

- `dotnet build`: `0 Warning(s), 0 Error(s)`.
- `dotnet test`: all projects green, no skips except the ones you explicitly
  introduced with a slice reference.
- `get_diagnostics` (VS Code) on every file you touched: clean.

If you added an ADR, also run `cargo doc --workspace --no-deps` to ensure
it did not break the docs build.

---

## 5. Definition of done (epic)

The whole epic is complete when:

1. Every slice S1–S10 is marked `DONE` in `design/EF_MISSING_TESTS.md`.
2. `LiteralExecutabilityContractTests` (S1) is green for every registered
   CLR mapping in both `DecentDBTypeMappingSource` and
   `DecentDBNodaTimeTypeMappingSource`.
3. `HasDataLiteralMatrixTests` (S2) is green for every non-skipped row, and
   every skipped row has a linked engine ticket.
4. No CLR primitive that the documentation lists as "supported" throws
   `NotSupportedException` anywhere in the binding path.
5. Reverting the April 2026 `bool`/`Guid` literal fix (i.e. deleting
   `DecentDBBoolTypeMapping` and restoring the old `WithComposedConverter`
   form) causes at least one S1 and at least one S2 test to fail in CI.
   This is the regression-proof exit condition spelled out in §7 of the
   design document.
6. `dotnet build DecentDB.NET.sln` reports zero warnings.
7. `dotnet test DecentDB.NET.sln` reports zero failures.

---

## 6. If you get stuck

- **A test fails in a way you can't explain.** Re-run the probe commands
  from Appendix §8 of the design document against the current build — DecentDB's
  accepted literal syntax may have evolved and the design document may need
  an update.
- **A slice can't be done without an engine change.** Stop. Open an ADR
  under `design/adr/NNNN-<slug>.md` (follow the format in existing ADRs
  such as `design/adr/0072-new-data-types-decimal-uuid.md`), link it from
  the slice, and move on to the next independent slice.
- **You are tempted to add `InternalsVisibleTo` or `#pragma warning disable`.**
  Stop. That means a type is in the wrong namespace or has the wrong
  visibility. Re-read §0 ground rules and refactor instead.
- **A slice seems to require rewriting an existing test.** Stop. The
  existing test encodes a contract. If you believe the contract is wrong,
  raise it with the user before changing the test.

---

## 7. Reporting

At the end of each slice, report in a single message:

```
Slice: S# — <title>
Status: DONE | BLOCKED | WIP
Files added:    <list>
Files modified: <list>
Tests added:    <count> (<names>)
Tests skipped:  <count> (<names> + reason)
Build:   0 Warning(s), 0 Error(s)
Test:    Passed <n>, Failed 0, Skipped <m>
Findings closed: F#, F#
Next slice: S#
```

If `BLOCKED`, include the blocker and the link to the ADR or engine ticket.

---

*End of prompt. Begin with slice S1.*
