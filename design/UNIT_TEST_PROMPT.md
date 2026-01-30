You are a coding agent working in the Nim repo in this workspace. 

Goal: raise **unit test line coverage to > 90%** (as reported by the repo’s coverage tooling) while keeping production behavior unchanged.

**Constraints**
- Do not change persistent formats, WAL formats, or concurrency semantics; if you think you must, stop and ask (ADR required per AGENTS.md).
- Prefer adding tests over refactors. Keep production code changes minimal and only for testability (e.g., making helpers public, dependency injection hooks), and justify each one.
- Don’t add new dependencies unless absolutely necessary; if needed, propose an ADR first (don’t implement the dependency yet).
- Do not “game” coverage with meaningless tests—assert real invariants and edge cases.

**Read first**
- AGENTS.md
- TESTING_STRATEGY.md
- SPEC.md (for correctness expectations)

**Workflow**
1. Establish baseline:
   - Run the existing test suite and the repo’s coverage script(s) (see coverage_nim.sh and outputs under coverage).
   - Record current coverage % and list the worst-covered modules/files from the coverage report.
2. Pick high-leverage targets:
   - Prioritize core logic that’s safe to test in-process: parsing/binding/planning, record encoding/decoding, pager invariants, WAL frame encode/decode, B+Tree operations, error paths.
   - Avoid long-running benchmarks and crash-injection harness unless necessary to reach 90%.
3. Add focused unit tests:
   - Create/extend tests under nim using Nim’s standard `unittest` style already used in the repo.
   - For each low-covered module, add tests that hit:
     - boundary conditions (empty/singleton/large inputs, min/max values)
     - invalid inputs and error handling (verify specific error types/messages where stable)
     - invariants (round-trips, ordering properties, idempotence)
     - tricky branches (early returns, “impossible” states, defensive checks)
   - Prefer table-driven tests and helper builders to keep test code readable.
4. Minimal testability tweaks (only if required):
   - If internal functions are unreachable from tests, make the smallest visibility/API adjustment needed (or add narrow test hooks guarded so they don’t affect release behavior).
   - Do not change algorithms “to make testing easier”.
5. Iterate to >90%:
   - Re-run coverage after each cluster of tests and keep a short running log of what moved the needle (module → added tests → coverage delta).
6. Validate and finish:
   - Ensure `nimble test` (or the repo’s equivalent) is green.
   - Ensure coverage report shows **>90%** overall.
   - Summarize what you changed, with links to the new/modified test files and the final coverage numbers.

**Deliverables**
- New/updated unit tests under nim (and only minimal prod changes if justified).
- A brief final report: baseline %, final %, and the top modules improved.