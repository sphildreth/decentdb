# Rust Error Handling Reference

Use this reference when generating or refactoring fallible Rust code.

## Defaults

- Prefer `Result<T, E>` for expected failures.
- Prefer typed errors for library and reusable engine code.
- Keep error messages actionable, specific, and consistent.
- Use `?` for propagation when it keeps the code linear and readable.
- Avoid `unwrap()` and `expect()` in library paths.

## Repository-Specific Guidance

- Do not add `thiserror`, `anyhow`, or similar crates unless the repository already uses them in that area or the addition is explicitly justified.
- Engine and ABI-facing code should favor explicit, stable error types over opaque error containers.
- If an error crosses public or binding boundaries, consider compatibility and mapping requirements before changing it.

## Prefer

- domain-specific error enums or structs
- `From` conversions where they reduce boilerplate without hiding meaning
- `# Errors` rustdoc sections for public fallible APIs
- preserving source context where it materially helps debugging

## Avoid

- `Result<T, String>` for reusable library surfaces
- `Box<dyn Error>` in stable public APIs
- silent fallbacks that discard the original failure
- panic-based control flow for expected errors

## Quick Checks

- Can callers distinguish important failure modes?
- Does the error type expose only intended semantics?
- Did the change preserve compatibility for bindings or public APIs?
- Are tests covering both the success path and key error paths?