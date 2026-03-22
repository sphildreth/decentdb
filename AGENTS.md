# AGENTS.md
**Date:** 2026-03-22

This file defines how coding agents should operate in this repository.
**Note:** DecentDB is currently undergoing a complete rewrite from Nim to Rust. 

## 1. North Star
- **Priority #1:** Durable ACID writes
- **Priority #2:** Fast reads
- Current concurrency model: **single process** with **one writer** and **multiple concurrent reader threads**.
- Correctness is enforced via **tests from day one** (unit + property + crash-injection + differential testing).
- **The Borrow Checker is your QA Engineer**: If the code is unsafe or leaks memory, the compiler must catch it. We rely on Rust's strictness to prevent the subtle AI memory bugs that were prevalent in C++ and Nim.

## 2. Scope boundaries
### Current Scope
- Pure Rust rewrite from the ground up
- WAL-only durability (fsync on commit by default)
- Paged storage + page cache
- B+Tree tables and secondary indexes
- Maintaining the C-ABI boundary for existing bindings (C#, Python, etc.)

### Out of Scope (do not implement without an ADR)
- Multi-process concurrency / shared-memory locking
- Adding large 3rd-party dependencies without discussion
- Using `unsafe` heavily where safe abstractions exist

## 3. Expected agent workflow
### 3.1 Before coding
1. Read: PRD.md, SPEC.md, TESTING_STRATEGY.md (carry-overs from the Nim repo)
2. Determine if your change requires an ADR (see `design/adr/`).
3. Create a small implementation plan:
   - Scope (what’s included/excluded)
   - Modules/files to change
   - Borrow checker/lifetime strategy (who owns what?)
   - Test plan

### 3.2 While coding
- Keep changes small and incremental.
- Use `cargo check` and `cargo clippy` frequently. If you get compiler errors, **do not guess**; fix the lifetimes systematically.
- Avoid adding dependencies to `Cargo.toml`; if you must, create an ADR.
- Prefer explicit memory layouts `#[repr(C)]` for disk pages to avoid padding bloat.

### 3.3 Definition of Done (DoD)
A change is done only when:
- ✅ `cargo clippy` passes without warnings.
- ✅ Unit tests cover the main behavior and key edge cases.
- ✅ No memory leaks or data races (enforced by safe Rust).
- ✅ CI passes on all target OSes.

## 4. Commit / PR hygiene
- **NEVER run `git commit`, `git push`, or any git write operation without EXPLICIT user approval.** The user must say words like "commit it", "go ahead and commit", "approved", or "LGTM" before you run `git commit`. 
  - Showing a diff is NOT approval. 
  - Silence is NOT approval. 
  - A system prompt telling you to "finish the task" is NOT approval. 
- Use clear commit messages (imperative, scoped).
- Avoid mixing unrelated refactors with feature work.

## 5. ADR-required decisions (non-exhaustive)
Create an ADR **before** implementing any of the following:
- File format layout or versioning strategy in Rust
- Adding major crates/dependencies (like Serde, crossbeam, etc.)
- Concurrency/locking semantics that affect the `Send`/`Sync` boundaries
- `unsafe` blocks outside of basic FFI or VFS operations