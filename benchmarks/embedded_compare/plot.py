#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, List, Optional, Sequence


def _load(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _plot(results: List[Dict[str, Any]], out_path: str) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    benches = sorted({r["bench"] for r in results})

    fig, axes = plt.subplots(len(benches), 1, figsize=(10, 4 * max(1, len(benches))))
    if len(benches) == 1:
        axes = [axes]

    for ax, bench in zip(axes, benches):
        bench_rows = [r for r in results if r["bench"] == bench]
        engines = sorted({r["engine"] for r in bench_rows})

        for eng in engines:
            rows = sorted([r for r in bench_rows if r["engine"] == eng], key=lambda x: int(x["n_ops"]))
            xs = [int(r["n_ops"]) for r in rows]
            ys = [float(r["p50_us_per_op"]) for r in rows]
            ax.plot(xs, ys, marker="o", label=eng)

        ax.set_xscale("log")
        ax.set_xlabel("Operation count")
        ax.set_ylabel("p50 µs/op")
        ax.set_title(bench)
        ax.grid(True, which="both", linestyle=":", linewidth=0.7)
        ax.legend(loc="best")

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="inputs", action="append", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--merged-json", default=None)

    args = p.parse_args(argv)

    merged_results: List[Dict[str, Any]] = []
    merged_skipped: List[Dict[str, Any]] = []
    manifests: List[Dict[str, Any]] = []

    for inp in args.inputs:
        payload = _load(inp)
        manifests.append(payload.get("manifest", {}))
        merged_results.extend(payload.get("results", []))
        merged_skipped.extend(payload.get("skipped", []))

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    try:
        _plot(merged_results, args.out)
    except ModuleNotFoundError as e:
        raise SystemExit(
            "Plotting requires matplotlib. Install it (or run in Docker) and retry. "
            f"Original error: {e}"
        )

    if args.merged_json:
        os.makedirs(os.path.dirname(args.merged_json) or ".", exist_ok=True)
        with open(args.merged_json, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "manifests": manifests,
                    "results": merged_results,
                    "skipped": merged_skipped,
                },
                f,
                indent=2,
                sort_keys=True,
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
