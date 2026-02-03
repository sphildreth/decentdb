# TESTING_STRATEGY — DecentDB (focus: benchmarks pipeline)
This file complements the main DecentDB engine testing strategy. It specifies tests required to keep benchmarks honest.

## 1. Unit tests (required)
- Aggregator:
  - Parses raw JSON records.
  - Correctly computes p95 selection and median-of-runs rule.
  - Correctly converts units (us→ms, bytes→MB).
  - Produces deterministic output.

- Chart generator:
  - Smoke test that it runs and writes expected files when given a valid `bench_summary.json`.

## 2. Self-check dataset (required)
Provide a tiny dataset and a tiny set of raw JSON records committed to the repo under:
- `benchmarks/raw/sample/`

CI must run:
- `python3 scripts/aggregate_benchmarks.py --input benchmarks/raw/sample --output data/bench_summary.json`
- `python3 scripts/make_readme_chart.py`

This ensures README chart tooling never breaks.

## 3. Guardrails
- The README chart **must not** be generated from Python ORM benchmarks.
- The aggregator must validate required fields and fail fast on malformed inputs.
