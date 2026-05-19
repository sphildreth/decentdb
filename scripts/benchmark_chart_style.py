"""Shared benchmark chart display names and colors."""

import matplotlib.pyplot as plt

BASELINE_ENGINE = "sqlite"

ENGINE_LABELS = {
    "decentdb_default_durable": "DecentDB (default durable)",
    "decentdb_tuned_durable": "DecentDB (tuned durable)",
    "duckdb": "DuckDB",
    "sqlite": "SQLite",
}

ENGINE_COLORS = {
    "DecentDB (tuned durable)": "#1f77b4",
    "DecentDB (default durable)": "#ff7f0e",
    "SQLite": "#2ca02c",
    "DuckDB": "#d62728",
    "H2": "#9467bd",
    "Apache Derby": "#8c564b",
    "HSQLDB": "#17becf",
    "LiteDB": "#e377c2",
    "Firebird": "#bcbd22",
}


def display_engine_name(engine: str) -> str:
    return ENGINE_LABELS.get(engine, engine)


def display_engine_color(engine: str, index: int) -> str:
    fallback = plt.rcParams["axes.prop_cycle"].by_key().get(
        "color",
        ["C0", "C1", "C2", "C3", "C4", "C5", "C6"],
    )
    return ENGINE_COLORS.get(engine, fallback[index % len(fallback)])


def ordered_display_engines(engines: list[str]) -> list[str]:
    engine_set = set(engines)
    preferred = [
        display_engine_name("decentdb_tuned_durable"),
        display_engine_name("decentdb_default_durable"),
        display_engine_name(BASELINE_ENGINE),
        display_engine_name("duckdb"),
        "H2",
        "LiteDB",
        "Apache Derby",
        "HSQLDB",
        "Firebird",
    ]
    ordered = [engine for engine in preferred if engine in engine_set]
    ordered.extend(engine for engine in engines if engine not in ordered)
    return ordered
