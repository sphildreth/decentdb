#!/usr/bin/env python3
"""Generate sequential ID test datasets."""

import csv
import os


def generate_sequential_dataset(filename, num_rows):
    """Generate CSV with sequential IDs."""
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "value"])
        for i in range(1, num_rows + 1):
            writer.writerow([i, f"item_{i}", i * 10])
    print(f"Generated {filename} with {num_rows} rows")


if __name__ == "__main__":
    os.makedirs("tests/data", exist_ok=True)

    generate_sequential_dataset("tests/data/sequential_1k.csv", 1000)
    generate_sequential_dataset("tests/data/sequential_10k.csv", 10000)
    generate_sequential_dataset("tests/data/sequential_100k.csv", 100000)
    generate_sequential_dataset("tests/data/sequential_1m.csv", 1000000)

    print("Done!")
