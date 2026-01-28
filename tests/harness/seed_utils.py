import os
import time


def choose_seed(seed: int | None) -> int:
    if seed is not None:
        return seed
    return int(time.time_ns() % 2_147_483_647)


def record_seed(seed: int, seed_log_path: str) -> None:
    os.makedirs(os.path.dirname(seed_log_path), exist_ok=True)
    with open(seed_log_path, "a", encoding="utf-8") as handle:
        handle.write(f"{seed}\n")


def read_seed(seed_file: str) -> int:
    with open(seed_file, "r", encoding="utf-8") as handle:
        line = handle.readline().strip()
        return int(line)
