These action items are in regards to the DecentDB repository and work done in the current branch:

  - If there are new features added to DecentDB or the DecentDB bindings, then this is a minor version bump not just a patch.
  - Ensure that the `/docs/about/changelog.md` is updated.
  - Ensure you have created ADRs where applicable.
  - Ensure that none of your changes have significantly impacted DecentDB engine benchmarks. Run `nimble bench_embedded_pipeline` and compare changes in `/benchmarks/embedded_compare/data/bench_summary.json`.
  - Ensure that any feature adds or changes are documented in the @docs documentation.
  - Ensure that the root README is updated, accurate and complete. If you added significant new features these should be highlighted in the Features list ("## Features" section).
  - Ensure that all tests pass. This includes all DecentDB tests (run by `nimble test` and `nimble test_bindings`) and all tests for all bindings found in `/bindings` as well.
  - Ensure that all the examples (found in the `/examples` directory) run successfully with no exceptions thrown.
  - Ensure there are no build warnings for building DecentDB, and all DecentDB bindings.