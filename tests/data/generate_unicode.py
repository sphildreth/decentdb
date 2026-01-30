#!/usr/bin/env python3
"""Generate unicode text test dataset."""

import csv
import os


def generate_unicode_dataset(filename, num_rows):
    """Generate CSV with unicode text."""
    # Mix of different scripts
    names = [
        "Alice",
        "Bob",
        "Carol",
        "David",
        "Eve",
        "José",
        "François",
        "Müller",
        "García",
        "Rodríguez",  # Latin extended
        "Иван",
        "Мария",
        "Алексей",  # Cyrillic
        "太郎",
        "花子",
        "田中",  # Japanese
        "李明",
        "王芳",
        "张伟",  # Chinese
        "김철수",
        "박영희",
        "이민수",  # Korean
        "Αλέξανδρος",
        "Μαρία",
        "Γιώργος",  # Greek
    ]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "description"])
        for i in range(1, num_rows + 1):
            name = names[i % len(names)]
            desc = f"Description for {name} with unicode: αβγ δεζ"
            writer.writerow([i, name, desc])
    print(f"Generated {filename} with {num_rows} rows")


if __name__ == "__main__":
    os.makedirs("tests/data", exist_ok=True)
    generate_unicode_dataset("tests/data/unicode_text.csv", 10000)
    print("Done!")
