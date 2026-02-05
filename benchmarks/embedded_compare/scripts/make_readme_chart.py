#!/usr/bin/env python3
"""Generate README-friendly benchmark bar chart(s).

Inputs:
  - data/bench_summary.json (aggregated numbers you compute from raw runs)

Outputs:
  - assets/decentdb-benchmarks.svg
  - assets/decentdb-benchmarks.png

Notes:
  - This script produces a single grouped bar chart with normalized values.
  - For "lower is better" metrics (latencies, size), normalization is inverted so higher bars mean better.
  - This is intended for README *at-a-glance* comparison, not a full benchmark report.
"""

import json
import math
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "bench_summary.json"
OUT_SVG = ROOT / "assets" / "decentdb-benchmarks.svg"
OUT_PNG = ROOT / "assets" / "decentdb-benchmarks.png"

# Define which metrics appear and how to treat them
METRICS = [
    ("read_p95_ms", "Point read p95", "lower"),
    ("join_p95_ms", "Join p95", "lower"),
    ("commit_p95_ms", "Commit p95", "lower"),
    ("insert_rows_per_sec", "Insert (rows/s)", "higher"),
]

BASELINE_ENGINE = "SQLite"  # normalize against this engine

def load():
    with DATA.open("r", encoding="utf-8") as f:
        doc = json.load(f)
    engines = doc["engines"]
    rows = []
    for eng, vals in engines.items():
        row = {"engine": eng}
        row.update(vals)
        rows.append(row)
    return pd.DataFrame(rows), doc.get("metadata", {})

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    if BASELINE_ENGINE not in set(df["engine"]):
        raise SystemExit(f"Baseline engine '{BASELINE_ENGINE}' not found in data.")
    base = df[df["engine"] == BASELINE_ENGINE].iloc[0]

    norm = df.copy()
    for key, _, direction in METRICS:
        if key not in norm.columns:
            continue
        b = base.get(key)
        # If baseline missing, skip
        if b is None or (isinstance(b, float) and math.isnan(b)):
            norm[key] = None
            continue

        def conv(x):
            if x is None or (isinstance(x, float) and math.isnan(x)):
                return None
            # Normalize such that 1.0 == baseline
            if direction == "higher":
                return x / b if b != 0 else None
            # lower-is-better: invert so higher is better in chart (baseline still at 1.0)
            return b / x if x != 0 else None

        norm[key] = norm[key].map(conv)
    return norm

def plot(norm: pd.DataFrame, meta: dict):
    # Prepare long-form data
    records = []
    for key, label, _dir in METRICS:
        for _, row in norm.iterrows():
            v = row.get(key)
            if v is None or (isinstance(v, float) and math.isnan(v)):
                continue
            records.append({"Metric": label, "Engine": row["engine"], "Score": float(v)})

    long = pd.DataFrame(records)
    if long.empty:
        raise SystemExit("No plottable data. Did you provide metrics?")

    # Order engines: DecentDB first, then baseline, then others
    engines = list(dict.fromkeys(
        ["DecentDB", BASELINE_ENGINE] + [e for e in long["Engine"].unique().tolist() if e not in ("DecentDB", BASELINE_ENGINE)]
    ))
    long["Engine"] = pd.Categorical(long["Engine"], categories=engines, ordered=True)

    # Pivot for grouped bars
    piv = long.pivot_table(index="Metric", columns="Engine", values="Score", aggfunc="mean")

    # Plot
    plt.figure(figsize=(12, 5))
    ax = piv.plot(kind="bar")  # default colors (per instructions)
    ax.set_ylabel("Normalized score vs SQLite (higher is better)")
    ax.set_xlabel("")
    ax.set_title("DecentDB vs common embedded engines (normalized)")

    # Reference line at 1.0 baseline
    ax.axhline(1.0, linewidth=1)

    # Tight layout and save
    plt.tight_layout()
    OUT_SVG.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUT_SVG, format="svg")
    plt.savefig(OUT_PNG, format="png", dpi=180)

    # Print a small note to console for CI logs
    print(f"Wrote: {OUT_SVG}")
    print(f"Wrote: {OUT_PNG}")
    if meta:
        print("Metadata:", meta)

def main():
    df, meta = load()
    norm = normalize(df)
    plot(norm, meta)

if __name__ == "__main__":
    main()
