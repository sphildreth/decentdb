# Error Handling Strategy
**Date:** 2026-01-28
**Status:** Accepted

### Decision
Use categorized error codes with Result types for propagation, and define rollback behavior per error type.

### Rationale
- Categorized errors make it easier to handle different failure modes
- Result types provide compile-time safety for error handling
- Defined rollback behavior ensures predictable transaction semantics
- Detailed error messages aid debugging

### Alternatives Considered
- Exceptions only: Less structured, harder to categorize
- Error codes only: Less type-safe

### Trade-offs
- **Pros**: Structured, type-safe, predictable
- **Cons**: More boilerplate than exceptions

### References
- SPEC.md §13 (Error handling)
- TESTING_STRATEGY.md §8 (Error handling tests)
