# ADR-0047: Engine-Enforced String Length Constraints (Deferred)

**Status**: Partially Implemented / Deferred
**Date**: 2026-01-31

## Context
Dapper support requires predictable string handling and optional length guardrails (`[MaxLength(n)]`). Implementing engine-enforced `VARCHAR(n)` would:
- change SQL grammar and binder behavior
- likely add catalog persistence for declared lengths
- introduce backward-compatibility considerations

## Decision
- MVP does **not** implement engine-enforced `VARCHAR(n)` / max length constraints with actual length validation.
- However, `VARCHAR` and `CHARACTER VARYING` are now supported as aliases for `TEXT` type (case-insensitive, with optional length specifications like `VARCHAR(255)`).
- Length guardrails are enforced in the .NET layer on write/bind paths using UTF-8 byte length.
- Revisit engine-enforced lengths post-MVP behind a dedicated SQL-dialect ADR.

## Consequences
- **Pros**:
  - Maintains SQL dialect compatibility with existing applications expecting VARCHAR support
  - Avoids persistence changes for MVP
  - Keeps SELECT hot paths untouched
  - Provides syntactic compatibility with SQL standards
- **Cons**:
  - Actual length constraints are not enforced at the engine level
  - Guardrails are advisory unless clients use the .NET provider (other clients may not enforce)

## References
- design/DAPPER_SUPPORT.md (Phase 4: String Length Constraints)
- design/adr/0043-dotnet-string-encoding.md
