#!/usr/bin/env python3
"""Generate edge case test dataset."""

import csv
import os


def generate_edge_case_dataset(filename, num_rows):
    """Generate CSV with edge cases."""
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "empty_string", "null_value", "max_int", "long_text"])

        for i in range(1, num_rows + 1):
            empty = "" if i % 5 == 0 else f"value_{i}"
            null_val = None if i % 7 == 0 else i * 100
            max_int = 9223372036854775807 if i == 1 else i  # Max int64
            long_text = "x" * 1000 if i % 10 == 0 else f"text_{i}"  # Long text

            writer.writerow([i, empty, null_val, max_int, long_text])
    print(f"Generated {filename} with {num_rows} rows")


if __name__ == "__main__":
    os.makedirs("tests/data", exist_ok=True)
    generate_edge_case_dataset("tests/data/edge_cases.csv", 1000)
    print("Done!")
