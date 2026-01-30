# Architecture Decision Records (ADRs)

This directory contains Architecture Decision Records (ADRs) documenting significant architectural decisions in DecentDb.

## What is an ADR?

An Architecture Decision Record (ADR) captures an important architectural decision made along with its context and consequences.

Format:
- **Title**: Decision name
- **Status**: Proposed, Accepted, Deprecated, Superseded
- **Context**: Why we needed to decide
- **Decision**: What we decided
- **Consequences**: What happens because of this decision

## ADR Index

### Storage & File Format

| ADR | Title | Status |
|-----|-------|--------|
| ADR-0016 | Database Header Checksum | ✅ Accepted |
| ADR-0020 | Overflow Pages for BLOBs | ✅ Accepted |
| ADR-0029 | Freelist Page Format | ✅ Accepted |
| ADR-0030 | Record Format | ✅ Accepted |
| ADR-0031 | Overflow Page Format | ✅ Accepted |
| ADR-0032 | BTree Page Layout | ✅ Accepted |

### WAL & Durability

| ADR | Title | Status |
|-----|-------|--------|
| ADR-0002 | WAL Commit Record Format | ✅ Accepted |
| ADR-0003 | Snapshot LSN Atomicity | ✅ Accepted |
| ADR-0004 | WAL Checkpoint Strategy | ✅ Accepted |
| ADR-0017 | Bulk Load API Design | ✅ Accepted |
| ADR-0018 | Checkpointing Reader Count | ✅ Accepted |
| ADR-0019 | WAL Retention for Active Readers | ✅ Accepted |
| ADR-0023 | Isolation Level Specification | ✅ Accepted |
| ADR-0024 | WAL Growth Prevention | ✅ Accepted |
| ADR-0033 | WAL Frame Format | ✅ Accepted |
| ADR-0037 | Group Commit / WAL Batching | ⏳ Proposed (Post-1.0) |

### SQL & Query Processing

| ADR | Title | Status |
|-----|-------|--------|
| ADR-0005 | SQL Parameterization | ✅ Accepted |
| ADR-0035 | SQL Parser libpg_query | ✅ Accepted |
| ADR-0038 | Cost-Based Optimization | ⏳ Proposed (Post-1.0) |

### Data Model

| ADR | Title | Status |
|-----|-------|--------|
| ADR-0006 | Foreign Key Index Creation | ✅ Accepted |
| ADR-0009 | Foreign Key Enforcement Timing | ✅ Accepted |
| ADR-0036 | Catalog Constraints & Index Metadata | ✅ Accepted |

### Search

| ADR | Title | Status |
|-----|-------|--------|
| ADR-0007 | Trigram Postings Storage | ✅ Accepted |
| ADR-0008 | Trigram Pattern Length Guardrails | ✅ Accepted |

### Infrastructure

| ADR | Title | Status |
|-----|-------|--------|
| ADR-0001 | Page Size (4096 default) | ✅ Accepted |
| ADR-0010 | Error Handling Strategy | ✅ Accepted |
| ADR-0011 | Memory Management Strategy | ✅ Accepted |
| ADR-0012 | B+Tree Space Management | ✅ Accepted |
| ADR-0013 | Index Statistics Strategy | ✅ Accepted |
| ADR-0014 | Performance Targets | ✅ Accepted |
| ADR-0015 | Testing Strategy Enhancements | ✅ Accepted |
| ADR-0021 | Sort Buffer Memory Limits | ✅ Accepted |
| ADR-0022 | External Merge Sort | ✅ Accepted |
| ADR-0025 | Memory Leak Prevention | ✅ Accepted |
| ADR-0026 | Race Condition Testing | ✅ Accepted |
| ADR-0027 | Bulk Load API Specification | ✅ Accepted |

### Documentation

| ADR | Title | Status |
|-----|-------|--------|
| ADR-0028 | Summary of Design Document Updates | ✅ Accepted |

### CLI & API

| ADR | Title | Status |
|-----|-------|--------|
| ADR-003 | CLI Engine Enhancements | ✅ Accepted |

## How to Create an ADR

1. Copy `design/adr/template.md` to `design/adr/adr-XXXX-title.md`
2. Fill in all sections
3. Set status to "Proposed"
4. Open PR for review
5. Update to "Accepted" when merged

## ADR Template

```markdown
# ADR-XXXX: Title

**Status:** Proposed / Accepted / Deprecated / Superseded by ADR-YYYY

**Date:** YYYY-MM-DD

## Context

What is the issue that we're seeing that is motivating this decision or change?

## Decision

What is the change that we're proposing or have agreed to implement?

## Consequences

What becomes easier or more difficult to do because of this change?

### Positive

- Benefit 1
- Benefit 2

### Negative

- Drawback 1
- Drawback 2

### Neutral

- Observation 1

## Alternatives Considered

### Alternative A

Why it was rejected.

### Alternative B

Why it was rejected.

## References

- Link to related issues
- Link to related PRs
- External resources
```

## Reading ADRs

ADRs are located in `design/adr/` directory.

To read about a specific decision:
1. Find it in the index above
2. Open the corresponding file
3. Read context, decision, and consequences

## ADR Lifecycle

```
Proposed → Accepted → (Deprecated | Superseded)
   ↓
Rejected
```

- **Proposed**: Under review
- **Accepted**: Merged and implemented
- **Deprecated**: No longer relevant, but kept for history
- **Superseded**: Replaced by newer ADR
- **Rejected**: Not accepted

## Why ADRs?

1. **Documentation**: Capture why decisions were made
2. **Onboarding**: New team members understand history
3. **Review**: Force thinking through consequences
4. **Transparency**: Visible decision-making process

## Questions?

See individual ADR files for specific decisions.
For ADR process questions, open a discussion.
