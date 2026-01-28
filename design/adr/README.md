# ADRs (Architecture Decision Records)
**Date:** 2026-01-28

This folder contains Architecture Decision Records (ADRs) that document important technical decisions.

## When to create an ADR
Agents must create an ADR when a decision:
- Changes persistent formats (db header, pages, WAL, indexes, search postings)
- Changes durability or recovery semantics
- Changes concurrency/locking behavior
- Introduces/removes a significant dependency
- Commits the project to a protocol or compatibility surface (SQL dialect quirks, parameter style)
- Has meaningful trade-offs that future contributors will need to understand

If you are unsure, **create an ADR**.

## How to create an ADR
1. Copy the template:
   - `docs/adr/0000-template.md` → `docs/adr/NNNN-short-title.md`
2. Choose the next sequential number `NNNN` (4 digits).
3. Use a short, kebab-case title:
   - Example: `0003-wal-frame-format.md`
4. Fill out every section. Keep it concise and specific.
5. In your PR, link the ADR in the description and mention the decision impact.

## ADR numbering rules
- 4 digits, zero-padded, sequential.
- Do not reuse numbers.
- If two PRs race, the later PR should renumber to the next available number.

## ADR lifecycle
- **Accepted:** implemented or actively being implemented.
- **Superseded:** replaced by a newer ADR (link to the newer one in References).
- **Rejected:** decision was considered and explicitly not chosen (still valuable).
