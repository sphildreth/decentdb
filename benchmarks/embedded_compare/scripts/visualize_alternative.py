#!/usr/bin/env python3
"""
Generate alternative visualizations for benchmark data.
- Radar Chart (Spider Plot) to show overall coverage.
- Relative Speedup Chart to show performance vs Baseline.
"""

import json
import math
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import pandas as pd

# Configurations
ROOT = Path(__file__).resolve().parents[1]
DATA_FILE = ROOT / "data" / "bench_summary.json"
OUT_RADAR = ROOT / "assets" / "decentdb-radar.png"
OUT_SPEEDUP = ROOT / "assets" / "decentdb-speedup.png"

BASELINE_ENGINE = "SQLite"

# Metrics configuration: (key, label, direction)
METRICS = [
    ("read_p95_ms", "Read Latency", "lower"),
    ("join_p95_ms", "Join Latency", "lower"),
    ("commit_p95_ms", "Commit Latency", "lower"),
    ("insert_rows_per_sec", "Insert Throughput", "higher"),
    ("db_size_mb", "Storage Efficiency", "lower"),
]

def load_data():
    if not DATA_FILE.exists():
        print(f"Error: {DATA_FILE} not found.")
        return None
    
    with open(DATA_FILE, "r") as f:
        data = json.load(f)
    return data["engines"]

def normalize_radar(engines):
    """
    Normalize all metrics to [0, 1] where 1.0 is the BEST in the group.
    """
    df = pd.DataFrame(engines).T
    normalized = pd.DataFrame(index=df.index, columns=[m[1] for m in METRICS])
    
    for key, label, direction in METRICS:
        if key not in df.columns:
            continue
            
        values = df[key].astype(float)
        
        if direction == "lower":
            # For latency/size: Best is MIN.
            # Score = Min / Value
            # This makes the best engine 1.0, and others < 1.0
            best_val = values.min()
            normalized[label] = values.apply(lambda x: best_val / x if x > 0 else 0)
        else:
            # For throughput: Best is MAX.
            # Score = Value / Max
            best_val = values.max()
            normalized[label] = values.apply(lambda x: x / best_val if best_val > 0 else 0)
            
    return normalized

def plot_radar(normalized_df):
    """
    Create a Radar Chart (Spider Plot).
    """
    categories = list(normalized_df.columns)
    N = len(categories)
    
    # Angles for each axis
    angles = [n / float(N) * 2 * math.pi for n in range(N)]
    angles += angles[:1] # Close the loop
    
    plt.figure(figsize=(10, 10))
    ax = plt.subplot(111, polar=True)
    
    # Draw one axe per variable + labels
    plt.xticks(angles[:-1], categories, color='grey', size=10)
    
    # Y-labels
    ax.set_rlabel_position(0)
    plt.yticks([0.25, 0.5, 0.75, 1.0], ["0.25", "0.50", "0.75", "1.00"], color="grey", size=7)
    plt.ylim(0, 1.1)  # Give a bit of headroom
    
    # Plot each engine
    colors = ['b', 'r', 'g', 'm', 'c']
    for i, engine in enumerate(normalized_df.index):
        values = normalized_df.loc[engine].values.flatten().tolist()
        values += values[:1] # Close the loop
        
        ax.plot(angles, values, linewidth=2, linestyle='solid', label=engine, color=colors[i % len(colors)])
        ax.fill(angles, values, colors[i % len(colors)], alpha=0.1)
    
    plt.title("Overall Performance (Outer is Better)", size=16, y=1.1)
    plt.legend(loc='upper right', bbox_to_anchor=(0.1, 1.1))
    
    OUT_RADAR.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUT_RADAR, dpi=150)
    print(f"Generated Radar Chart: {OUT_RADAR}")

def normalize_speedup(engines):
    """
    Normalize relative to Baseline (SQLite).
    Result > 1.0 means FASTER/BETTER than Baseline.
    """
    df = pd.DataFrame(engines).T
    if BASELINE_ENGINE not in df.index:
        print(f"Warning: Baseline engine {BASELINE_ENGINE} not found.")
        return None

    baseline = df.loc[BASELINE_ENGINE]
    normalized = pd.DataFrame(index=df.index, columns=[m[1] for m in METRICS])

    for key, label, direction in METRICS:
        if key not in df.columns:
            continue
        
        base_val = float(baseline[key])
        values = df[key].astype(float)
        
        if direction == "lower":
            # Latency: Speedup = Baseline / Value
            # e.g. Baseline 10ms, Target 5ms -> 10/5 = 2.0x speedup
            normalized[label] = values.apply(lambda x: base_val / x if x > 0 else 0)
        else:
            # Throughput: Speedup = Value / Baseline
            # e.g. Baseline 100ops, Target 200ops -> 200/100 = 2.0x speedup
            normalized[label] = values.apply(lambda x: x / base_val if base_val > 0 else 0)
            
    return normalized

def plot_speedup(speedup_df):
    """
    Create a horizontal bar chart showing speedup vs Baseline.
    """
    if speedup_df is None:
        return

    # Drop baseline row for cleaner visualization (it's always 1.0)
    # Actually, keeping it as reference is sometimes good, but let's drop it to focus on others if many.
    # But usually showing it at 1.0 is good for context.
    
    # Melt for plotting
    df_melted = speedup_df.reset_index().melt(id_vars='index', var_name='Metric', value_name='Speedup')
    df_melted.rename(columns={'index': 'Engine'}, inplace=True)
    
    engines = [e for e in speedup_df.index if e != BASELINE_ENGINE]
    # Put DecentDB first if present
    if "DecentDB" in engines:
        engines.remove("DecentDB")
        engines.insert(0, "DecentDB")
    
    # We want to group by Metric
    metrics = list(speedup_df.columns)
    
    y = np.arange(len(metrics))
    height = 0.8 / len(engines)
    
    plt.figure(figsize=(12, 6))
    ax = plt.subplot(111)
    
    for i, engine in enumerate(engines):
        vals = speedup_df.loc[engine]
        ax.barh(y + i*height, vals, height, label=engine)
        
        # Add value labels
        for j, v in enumerate(vals):
            ax.text(v + 0.05, y[j] + i*height, f"{v:.2f}x", va='center', size=9)

    # Add baseline line
    ax.axvline(1.0, color='k', linestyle='--', linewidth=1, label=f"{BASELINE_ENGINE} Baseline (1.0x)")
    
    ax.set_yticks(y + height * (len(engines)-1) / 2)
    ax.set_yticklabels(metrics)
    ax.set_xlabel(f"Speedup / Efficiency vs {BASELINE_ENGINE} (Higher is Better)")
    ax.set_title(f"Relative Performance vs {BASELINE_ENGINE}")
    ax.legend()
    
    plt.tight_layout()
    plt.savefig(OUT_SPEEDUP, dpi=150)
    print(f"Generated Speedup Chart: {OUT_SPEEDUP}")

def main():
    engines = load_data()
    if not engines:
        return

    # 1. Radar Chart
    norm_radar = normalize_radar(engines)
    plot_radar(norm_radar)
    
    # 2. Speedup Chart
    norm_speedup = normalize_speedup(engines)
    plot_speedup(norm_speedup)

if __name__ == "__main__":
    main()
