# Architecture Decision Records (ADRs)

DecentDB uses Architecture Decision Records (ADRs) to capture **important technical decisions** (especially those affecting durability, recovery, persistent formats, and SQL semantics) along with the reasoning and trade-offs.

The canonical ADRs live in the repository at `design/adr/`.

## What is an ADR?

An ADR is a short document describing:

- **Decision**: what we chose to do
- **Rationale**: why this choice
- **Alternatives considered**: what we didn’t choose
- **Trade-offs**: what gets better/worse
- **References**: relevant code/PRs/issues/spec sections

DecentDB ADRs follow the template in `design/adr/0000-template.md`.

## When an ADR is required

Per the project workflow, you must write an ADR *before* implementing changes that could affect:

- Persistent formats (DB header, page layout, WAL format, index/postings formats)
- Durability, recovery, checkpointing, or truncation strategy
- Locking/concurrency semantics that affect correctness
- SQL dialect behavior (including edge-case semantics)
- Adding/removing significant dependencies

If you’re unsure, write the ADR—small ADRs are cheaper than silent, implicit decisions.

## How to create an ADR

1. Copy `design/adr/0000-template.md` to `design/adr/NNNN-short-title.md`
2. Pick the next sequential `NNNN` (4 digits, zero-padded)
3. Use a short kebab-case title
4. Fill in every section (keep it concise and specific)
5. Link the ADR from the PR description and call out any compatibility / durability impact

## Numbering rules

- Numbers are sequential and must not be reused.
- If two PRs race, the later PR renumbers to the next available `NNNN`.

## Status lifecycle

- **Proposed**: under review
- **Accepted**: approved (and implemented or actively being implemented)
- **Superseded**: replaced by a newer ADR (link it in References)
- **Rejected**: considered and explicitly not chosen

## Finding ADRs

This docs page intentionally does **not** maintain a manual ADR index (it quickly becomes stale). To browse decisions:

- Open the `design/adr/` folder in the repo and sort by filename/number.
- Start with the ADRs referenced by `design/SPEC.md` and the relevant subsystem docs.

For the authoritative ADR workflow details, see `design/adr/README.md`.
