You are a coding agent working in the Nim repo in this workspace.

Goal: **increase unit test coverage and confidence** by adding meaningful unit tests (correctness + edge cases) while keeping production behavior unchanged. Coverage % is a useful metric, but it is not the only goal.

**Critical requirement: keep going until I stop you**
- You must continuously add valuable unit tests in an iterative loop.
- You may only stop when I explicitly tell you to stop/pause (e.g., I say “stop”, “pause”, “hold”, or “that’s enough”).
- Do not “wrap up”, “finish”, or “final report” on your own.
- If you run out of obvious targets, broaden scope to other low-covered core modules and continue.

**Constraints**
- Do not change persistent formats, WAL formats, or concurrency semantics; if you think you must, stop and ask (ADR required per AGENTS.md).
- Prefer adding tests over refactors. Keep production code changes minimal and only for testability (e.g., making helpers public, dependency injection hooks), and justify each one.
- Don’t add new dependencies unless absolutely necessary; if needed, propose an ADR first (don’t implement the dependency yet).
- Do not “game” coverage with meaningless tests—assert real invariants and edge cases.

**Autonomy (work independently; no self-termination)**
- Do not pause to ask whether to continue. Continue iterating until I explicitly tell you to stop.
- Use a tight loop: add/adjust tests → run the relevant test target(s) → run coverage → inspect the worst gaps → repeat.
- Never stop because you think coverage is “good enough”, you “might be done”, or you’ve reached diminishing returns.
- If coverage stalls, change tactics rather than stopping: pick a different low-covered module, target missed branches, or improve test inputs to hit error paths.
- Only stop and ask for guidance if you are genuinely blocked (e.g., unclear intended behavior, an ADR-required change, non-deterministic tests you can’t stabilize, or the repo’s tooling cannot be executed in this environment).
- Provide progress as you go: after each iteration (or every ~20–30 minutes of work), report what changed (tests added), which tests were run, and the coverage delta if available.

**Read first**
- AGENTS.md
- TESTING_STRATEGY.md
- SPEC.md (for correctness expectations)

**Workflow**
1. Establish baseline:
   - Run the existing test suite and the repo’s coverage script(s) (see coverage_nim.sh and outputs under build/coverage).
   - Record current overall coverage % and identify the worst-covered modules/files from the coverage report.
2. Pick high-leverage targets:
   - Prioritize core logic that’s safe to test in-process: parsing/binding/planning, record encoding/decoding, pager invariants, WAL frame encode/decode, B+Tree operations, error paths.
   - Avoid long-running benchmarks and crash-injection harness unless specifically requested; focus on fast unit tests.
3. Add focused unit tests:
   - Create/extend tests under tests/nim using Nim’s standard `unittest` style already used in the repo.
   - For each low-covered module, add tests that hit:
     - boundary conditions (empty/singleton/large inputs, min/max values)
     - invalid inputs and error handling (verify specific error types/messages where stable)
     - invariants (round-trips, ordering properties, idempotence)
     - tricky branches (early returns, “impossible” states, defensive checks)
   - Prefer table-driven tests and helper builders to keep test code readable.
4. Minimal testability tweaks (only if required):
   - If internal functions are unreachable from tests, make the smallest visibility/API adjustment needed (or add narrow test hooks guarded so they don’t affect release behavior).
   - Do not change algorithms “to make testing easier”.
5. Iterate and measure:
   - Re-run coverage after each cluster of tests and keep a short running log of what moved the needle (module → added tests → coverage delta).
   - Prefer per-file improvements in the most important modules over chasing an overall percentage.
6. Validate continuously (no “finish” unless I ask for it):
   - Keep `nimble test` (or the repo’s equivalent) green as you go.
   - Re-check coverage regularly, record deltas, and keep moving to the next highest-leverage gaps.
   - Only write a final summary when I explicitly request one.

**Deliverables (continuous)**
- New/updated unit tests under tests/nim (and only minimal prod changes if justified).
- Ongoing progress reports as you work (baseline + deltas), and a final report only when I request it.