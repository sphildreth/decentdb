# Trigram Pattern Length Guardrails
**Date:** 2026-01-28
**Status:** Accepted

### Decision
Combine pattern length checks with posting-list frequency thresholds:
- Patterns < 3 chars: never use trigram index
- Patterns 3-5 chars: use only if combined with other filters or rarest trigram < threshold
- Patterns > 5 chars: use trigram index, but cap results if rarest trigram exceeds threshold

### Rationale
- Short patterns match too many rows (poor selectivity)
- Frequency thresholds prevent broad patterns from overwhelming the system
- Combining length and frequency provides better guardrails than either alone

### Alternatives Considered
- Length check only: Too coarse, doesn't account for data distribution
- Frequency check only: Doesn't prevent very short patterns from being used

### Trade-offs
- **Pros**: Better query performance, prevents runaway queries
- **Cons**: More complex logic, may reject some valid use cases

### References
- SPEC.md §8.3 (Query evaluation)
- SPEC.md §8.4 (Broad-pattern guardrails)
