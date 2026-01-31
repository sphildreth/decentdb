# ADR-0041: .NET Connection Pooling and Single-Writer Strategy

**Status**: Accepted
**Date**: 2026-01-30

## Context
DecentDB MVP concurrency is single process, one writer, many readers. ADO.NET consumers (including Dapper) often open/close connections frequently, and micro-ORM query composition benefits from reusing an open connection.

## Decision
- The ADO.NET provider does **not** implement a global cross-connection pool in MVP.
- Single-writer enforcement is provided by the engine’s locking semantics; the provider must not claim stronger guarantees.
- The Micro-ORM (`DecentDbContext`) implements **context-scoped pooling**:
  - pooled mode: a single open `DecentDbConnection` is reused within the context
  - non-pooled mode: open/close per operation
  - when a transaction is active, all operations use the transaction’s connection

## Consequences
- **Pros**: Simple and predictable; avoids complex lifetime bugs; aligns with one-writer/many-readers semantics.
- **Cons**: High churn workloads may benefit from a future global pool; write contention behavior is engine-driven.

## References
- design/adr/0023-isolation-level-specification.md
- design/DAPPER_SUPPORT.md (Phase 5: pooling semantics)
- bindings/dotnet/src/DecentDb.MicroOrm/DecentDbContext.cs
- bindings/dotnet/src/DecentDb.AdoNet/DecentDbConnection.cs
