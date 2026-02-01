# Copilot Instructions (DecentDB)

These instructions guide GitHub Copilot Chat when editing this repository.

## North Star

- **Priority #1: Durable ACID writes**
- **Priority #2: Fast reads**
- Current concurrency model: **single process**, **one writer**, **multiple concurrent reader threads**.
- Correctness is enforced via **tests from day one** (unit + property + crash-injection + differential testing).

## Non-negotiable constraints

- Follow the repository workflow and Definition of Done in `AGENTS.md`.
- If a change could affect **persistent formats** (db header, page layout, WAL frame format, postings formats), **checkpoint/truncation strategy**, **locking/concurrency semantics**, **isolation guarantees**, or **SQL dialect behavior**:
  - **STOP** and propose an **ADR** in `design/adr/` (see `design/adr/README.md`) before implementing.
- Avoid new dependencies. If a new dependency is truly necessary, request approval and justify it.

## Nim Skill (how to write Nim in this repo)

### Style and structure

- Make the **smallest diff** that solves the problem; do not perform drive-by refactors or formatting changes.
- Match existing module boundaries and naming conventions.
- Prefer explicit, boring code over clever tricks (especially macro-heavy code).
- Avoid leaking internal details across modules unless the repo already follows that pattern.

### Correctness and safety

- Prefer total/checked behavior over undefined behavior.
- Keep invariants close to the code that relies on them; validate inputs at module boundaries.
- Preserve Snapshot Isolation semantics (see ADR-0023) and the one-writer / many-readers model.

### Error handling

- Follow the repo’s error handling strategy (see `design/adr/0010-error-handling-strategy.md`).
- Produce errors that are actionable: include the minimal context (operation + key identifiers) while avoiding expensive string building on hot paths.

### Performance discipline

- Do not introduce avoidable allocations or quadratic work.
  - Be careful with string concatenation inside loops.
  - Be careful with `seq` growth; pre-size or reuse when possible.
- Avoid logging on hot paths unless it is gated and **zero-cost when disabled**.

## FFI / C ABI Skill (when exporting native APIs)

- Use stable exports: `{.exportc, cdecl, dynlib.}` and keep signatures C-friendly.
- Do **not** return pointers to Nim-managed memory unless lifetime rules are explicit and enforced.
- Prefer `(cstring, length)` pairs for TEXT/BLOB-like data.
- Define ownership for every pointer:
  - who allocates
  - who frees
  - when it becomes invalid
- Avoid cross-thread use of a single statement handle; keep thread-safety consistent with the repo’s concurrency model.

## Testing and validation

- Add tests for new behavior and key edge cases in the same change.
- Run the **narrowest relevant tests** first, then broaden.
- If you touch durability/format-sensitive code, add or update crash/differential tests as appropriate.

## Output expectations

When responding, include:
- What changed (files/symbols)
- Which tests were run (commands)
- Any remaining risks or follow-ups (especially if an ADR is needed)
