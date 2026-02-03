## Performance (at a glance)

![DecentDB benchmark chart](assets/decentdb-benchmarks.svg)

**How this chart is produced**
- The chart is generated from benchmark runs using `scripts/make_readme_chart.py`.
- Values are **normalized vs SQLite** (baseline = 1.0).
- For "lower is better" metrics (latency, DB size), the score is inverted so **higher bars mean better**.
- Full methodology and raw results live in `benchmarks/` (or your chosen folder).

**Regenerate**
```bash
cp data/bench_summary.example.json data/bench_summary.json
python3 scripts/make_readme_chart.py
```
