# Rust FFI And Layout Safety Reference

Use this reference when generating or changing Rust code that touches ABI, layout, or on-disk compatibility.

## Defaults

- Avoid `unsafe` unless it is strictly required.
- Minimize the scope of every `unsafe` block.
- Document the safety invariant for unsafe operations.
- Use explicit layout control where compatibility depends on representation.

## Repository-Specific Guidance

- Treat the C ABI in `include/decentdb.h` and exported `ddb_*` functions as stable integration boundaries.
- Treat on-disk format and WAL semantics as product-level compatibility contracts.
- Do not change layout, ABI shape, or exported behavior casually.
- If a change has broad ABI, format, or binding impact, surface that before implementation and check whether an ADR is required.

## Prefer

- `#[repr(C)]` for FFI-facing structs and enums when representation matters
- `#[repr(transparent)]` for wrapper types where ABI compatibility depends on a single field
- explicit conversion boundaries between safe Rust types and raw FFI representations
- tests or smoke coverage that exercise the affected boundary

## Avoid

- relying on default Rust layout where external compatibility matters
- expanding unsafe scope for convenience
- changing exported field order, width, or semantics without reviewing downstream effects
- introducing parallel native contracts when the stable C ABI should remain authoritative

## Quick Checks

- Does this type cross the C ABI or a binding boundary?
- Does the memory layout need to be stable across versions?
- Is every unsafe block justified by a specific safety invariant?
- Do bindings, docs, or smoke tests need updates?