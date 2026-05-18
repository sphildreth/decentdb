# ADR 0160: Binding-Native Semantic Data Types Format Governance

**Status:** Accepted
**Date:** 2026-05-18

## Context

DecentDB is adding semantic data types that must preserve durable on-disk
compatibility while allowing staged adoption across the Rust engine, C ABI, and
language bindings.

This change set needs explicit governance for:

- database file format versioning
- row value-tag namespace allocation
- C ABI value-tag surface evolution
- migration continuity from the immediately previous format

Per ADR 0131, every format bump must ship with a migration path in
`decentdb-migrate` from the previous format version.

## Decision

### 1. Database format version

- Bump `DB_FORMAT_VERSION` from `10` to `11`.
- Version 11 is the baseline for semantic-type rollout governance.

### 2. Row tag namespace governance

- Row value tags remain a compact `u8` namespace shared by persisted row
  encoding.
- Existing tags `0..=12` remain stable.
- Semantic-type tags are allocated from the next contiguous range beginning at
  `13`, through `20` for the initial semantic set.
- `TIMESTAMPTZ` receives its own row tag even though its binary payload matches
  `TIMESTAMP`, because DecentDB row decoding is schema-agnostic today and must
  preserve semantic value identity without catalog context.
- `MACADDR` uses row tag `20` and stores a one-byte length plus 6 or 8 address
  bytes.

### 3. C ABI value tags (v2 semantic tag band)

- C ABI semantic compatibility uses a distinct tag band for new semantic
  values: `ddb_value_tag_t` values `11..=18`.
- These tags represent semantic identity for bindings while preserving
  compatibility with existing scalar and blob/string paths.
- `MACADDR` is active as `DDB_VALUE_MACADDR = 18`, reusing the existing
  IP/CIDR address byte array with `ip_family` carrying the 6- or 8-byte length.

### 4. Canonical semantic storage rules

- `ENUM`: store stable label identifiers (integer ids) rather than declaration
  position or label text. Label-id stability is a schema contract.
- `IPADDR`/`CIDR`: use compact binary payloads instead of canonical text.
- `MACADDR`: use compact 6- or 8-byte binary payloads instead of canonical text.
- `TIMESTAMPTZ`: preserve UTC microseconds as the canonical persisted value
  representation.

### 5. Migration policy

- `decentdb-migrate` adds `v10 -> current` migration support by copying the file
  and patching the header format version/checksum to `DB_FORMAT_VERSION`.
- No page rewrite is required for v10 because v10 files do not carry semantic
  row payloads introduced under v11 governance.

## Consequences

Benefits:

- Explicit, auditable contract for semantic-type format evolution.
- Continuous upgrade path from v10 per ADR 0131.
- Low-risk migration for v10 databases (header-only patch after copy).

Trade-offs:

- v11 cannot be opened by older engines.
- C ABI consumers must treat `DDB_VALUE_MACADDR` as active, not reserved.

## Validation

- `decentdb-migrate` must pass v10 migration tests that verify:
  - destination header upgraded to current `DB_FORMAT_VERSION`
  - existing table data remains readable after migration

## References

- ADR 0131: Legacy Format Migrations
- `design/decentdb-semantic-data-types-market-differentiation.md`
