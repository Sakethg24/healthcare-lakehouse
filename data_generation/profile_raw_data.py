#!/usr/bin/env python3
"""
profile_raw_data.py

Quick data-quality / "messiness" profile of Synthea CSV output. Prints
row count, duplicate-Id count, and per-column null rate / distinct count
for every CSV in a folder.

Usage:
    python3 profile_raw_data.py --dir /path/to/csv/folder
"""
import argparse
import os
from collections import defaultdict

import pandas as pd

CHUNK_SIZE = 200_000


def profile_file(path):
    name = os.path.basename(path)
    total_rows = 0
    null_counts = defaultdict(int)
    distinct_samples = defaultdict(set)
    id_seen = defaultdict(int)
    columns = None

    for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, dtype=str, keep_default_na=True):
        if columns is None:
            columns = list(chunk.columns)
        total_rows += len(chunk)
        for col in columns:
            null_counts[col] += chunk[col].isna().sum()
            if len(distinct_samples[col]) < 50_000:
                distinct_samples[col].update(chunk[col].dropna().unique().tolist()[:5000])
        if "Id" in columns:
            for v in chunk["Id"].dropna():
                id_seen[v] += 1

    print(f"\n=== {name} ({total_rows} rows) ===")
    if columns and "Id" in columns:
        dupes = sum(1 for v in id_seen.values() if v > 1)
        print(f"duplicate Id values: {dupes}")

    for col in columns or []:
        rate = (null_counts[col] / total_rows) if total_rows else 0
        distinct = len(distinct_samples[col])
        flag = "  <-- fully/near-fully null" if rate > 0.98 else ""
        flag = "  <-- looks fully populated, no missingness" if rate == 0 and distinct <= 1 else flag
        print(f"  {col:35s} null={rate:6.2%}  distinct(sampled)={distinct:7d}{flag}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", required=True, help="Folder containing CSV files")
    args = parser.parse_args()

    files = sorted(f for f in os.listdir(args.dir) if f.endswith(".csv"))
    print(f"Found {len(files)} CSV files in {args.dir}")

    for f in files:
        try:
            profile_file(os.path.join(args.dir, f))
        except Exception as e:
            print(f"!! failed to profile {f}: {e}")


if __name__ == "__main__":
    main()
