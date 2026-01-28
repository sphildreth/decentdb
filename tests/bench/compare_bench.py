import json
import sys


def load_bench(path):
    with open(path, "r", encoding="utf-8") as f:
        doc = json.load(f)
    benchmarks = {}
    for entry in doc.get("benchmarks", []):
        name = entry.get("name")
        if name:
            benchmarks[name] = entry
    return benchmarks


def main():
    if len(sys.argv) < 4:
        print("usage: compare_bench.py CURRENT BASELINE THRESHOLDS", file=sys.stderr)
        return 2
    current_path, baseline_path, thresholds_path = sys.argv[1:4]
    current = load_bench(current_path)
    baseline = load_bench(baseline_path)
    with open(thresholds_path, "r", encoding="utf-8") as f:
        thresholds = json.load(f)

    failures = []
    for name, limits in thresholds.items():
        cur = current.get(name)
        base = baseline.get(name)
        if cur is None or base is None:
            failures.append(f"{name}: missing current or baseline")
            continue
        cur_p95 = float(cur.get("p95_ms", 0))
        base_p95 = float(base.get("p95_ms", 0))
        if base_p95 <= 0:
            failures.append(f"{name}: baseline p95 invalid ({base_p95})")
            continue
        increase = (cur_p95 - base_p95) / base_p95
        allowed = float(limits.get("p95_increase", 0))
        if increase > allowed:
            failures.append(
                f"{name}: p95 {cur_p95:.3f}ms > {base_p95:.3f}ms (+{increase*100:.1f}% > {allowed*100:.1f}%)"
            )

    if failures:
        print("Benchmark regressions detected:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1
    print("Benchmarks within thresholds.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
