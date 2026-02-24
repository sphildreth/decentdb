# Workplan: Support optional loadable extensions for DecentDB (Issue #7)

## Overview
This document outlines the implementation plan for introducing optional loadable extensions to DecentDB, as described in [Issue #7](https://github.com/sphildreth/decentdb/issues/7). This feature allows advanced users to add specialized functionality (custom SQL functions, domain logic) without bloating the core engine.

## Phase 0: Design and Architecture
- **Task 0.1: Draft ADR for Extension API**
  - Define the C ABI for extensions, including the stable, versioned entry point.
  - Define the API handle/pointer that will be passed to extensions (allowing them to register functions, aggregates, etc.).
  - Define the ABI versioning strategy and compatibility rules.
  - Detail the security model (disabled by default, explicit opt-in via config or runtime command).
  - Review and get approval for the ADR before proceeding.

## Phase 1: Core Extension Loading Mechanism
- **Task 1.1: Dynamic Library Loading**
  - Implement cross-platform dynamic loading of native extension modules (`dlopen`/`LoadLibrary`).
  - Implement ABI version checking before any registration occurs. Reject incompatible extensions safely.
- **Task 1.2: Programmatic API & Security Controls**
  - Add a configuration option to the database open configuration to explicitly enable extension loading (default: disabled).
  - Implement the programmatic API: `db.loadExtension(path: string)`.
  - Ensure the API returns success or a structured error, and prevents duplicate loading of the same extension instance.
  - Add auditable logging for extension loading (log extension name, path, version, and load time).
- **Task 1.3: Error Handling**
  - Ensure extension load failures are non-fatal, do not crash the database, return clear errors, and leave the database in a consistent state.

## Phase 2: Extension Registration API
- **Task 2.1: Function Registration**
  - Implement the internal engine hooks to allow extensions to register **scalar SQL functions**.
  - Implement the internal engine hooks to allow extensions to register **aggregate SQL functions**.
- **Task 2.2: Registration Rollback**
  - Implement logic to handle registration failures gracefully. If an extension fails during registration, roll back any partial registrations to leave the engine state unchanged.

## Phase 3: SQL Interface
- **Task 3.1: `LOAD EXTENSION` Command**
  - Add SQL parser support for the `LOAD EXTENSION 'path/to/extension';` command.
  - Wire the SQL command to the underlying loading mechanism.
  - Ensure the SQL-based loading respects the exact same safety checks and disabled-by-default rules as the programmatic API.

## Phase 4: Testing
- **Task 4.1: Unit Tests**
  - Test successful extension loading.
  - Test ABI version mismatch rejection.
  - Test duplicate extension load prevention.
- **Task 4.2: Integration Tests**
  - Create a dummy test extension.
  - Test loading the test extension, registering a scalar function, and executing a query using that function.
- **Task 4.3: Negative & Safety Tests**
  - Attempt to load extensions when the feature is disabled and verify explicit failure.
  - Verify that extension loading failures have no impact on WAL, recovery, or core correctness.

## Phase 5: Documentation and Examples
- **Task 5.1: Security & Usage Documentation**
  - Clearly document that extensions execute native code with full process privileges.
  - Document that the feature is disabled by default and should only be enabled for trusted code.
  - Clearly mark the feature as Post-MVP in the documentation.
- **Task 5.2: Example Extension**
  - Provide a minimal "Hello Extension" example (e.g., in C or Nim) demonstrating how to build and register a simple scalar function.
