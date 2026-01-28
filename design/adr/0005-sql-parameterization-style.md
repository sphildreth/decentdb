# SQL Parameterization Style
**Date:** 2026-01-28
**Status:** Accepted

### Decision
Use Postgres-style positional parameters (`$1, $2, ...`) for MVP.

### Rationale
- Consistent with libpg_query parser choice
- Familiar to developers with PostgreSQL experience
- Simple to implement and test
- Well-defined semantics

### Alternatives Considered
- Named parameters (`:name`): More flexible but more complex
- Question marks (`?`): Familiar from other databases but less explicit

### Trade-offs
- **Pros**: Simple, consistent with parser choice, well-understood
- **Cons**: Less flexible than named parameters

### References
- SPEC.md §6.3 (Parameterization)
- PRD.md §2.1 (Functional goals)
