"""Shared benchmark chart display names and colors."""

import matplotlib.pyplot as plt

BASELINE_ENGINE = "sqlite"

ENGINE_LABELS = {
    "decentdb_default_durable": "DecentDB (default durable)",
    "decentdb_balanced_durable": "DecentDB (balanced durable)",
    "decentdb_low_memory_durable": "DecentDB (low-memory durable)",
    "decentdb_tuned_durable": "DecentDB (tuned durable)",
    "duckdb": "DuckDB (legacy duckdb key)",
    "duckdb_engine_default": "DuckDB (engine default)",
    "sqlite": "SQLite",
    "H2": "H2 (read-only partial)",
    "HSQLDB": "HSQLDB (read-only partial)",
}

ENGINE_COLORS = {
    "DecentDB (default durable)": "#ff7f0e",
    "DecentDB (balanced durable)": "#ff7f0e",
    "DecentDB (low-memory durable)": "#f4a261",
    "DecentDB (tuned durable)": "#1f77b4",
    "DuckDB": "#d62728",
    "DuckDB (engine default)": "#d62728",
    "DuckDB (legacy duckdb key)": "#d62728",
    "SQLite": "#2ca02c",
    "H2 (read-only partial)": "#9467bd",
    "HSQLDB (read-only partial)": "#17becf",
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
        display_engine_name("decentdb_balanced_durable"),
        display_engine_name("decentdb_default_durable"),
        display_engine_name("decentdb_low_memory_durable"),
        display_engine_name(BASELINE_ENGINE),
        display_engine_name("duckdb_engine_default"),
        display_engine_name("duckdb"),
        display_engine_name("H2"),
        "LiteDB",
        "Apache Derby",
        display_engine_name("HSQLDB"),
        "Firebird",
    ]
    ordered = []
    seen = set()
    for engine in preferred:
        if engine in engine_set and engine not in seen:
            ordered.append(engine)
            seen.add(engine)
    ordered.extend(engine for engine in engines if engine not in ordered)
    return ordered
