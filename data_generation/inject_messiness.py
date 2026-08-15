#!/usr/bin/env python3
"""
inject_messiness.py

Takes the clean Synthea CSV output and writes out a "dirty" copy with
deliberate, configurable, realistic data-quality problems: duplicate
rows, missing values, placeholder sentinels standing in for nulls,
invalid formats (phone numbers, zip codes), inconsistent casing, a
handful of implausible outliers, and orphaned foreign keys.

The original clean CSVs are never modified. This reads --in-dir and
writes a full copy into --out-dir -- files/columns not explicitly
targeted below pass through unchanged.

Usage:
    python3 inject_messiness.py \
        --in-dir  /path/to/csv_run_13k/output/csv \
        --out-dir /path/to/csv_run_13k_dirty
"""
import argparse
import random
import re
from pathlib import Path

import pandas as pd

random.seed(42)

DUPLICATE_RATE = 0.02
MISSING_RATE = 0.05
PLACEHOLDER_RATE = 0.02
INVALID_PHONE_RATE = 0.08
ZIP_LEADING_ZERO_LOSS_RATE = 0.10
CASING_WHITESPACE_RATE = 0.06
OUTLIER_RATE = 0.01
ORPHAN_FK_RATE = 0.005

PLACEHOLDERS = ["N/A", "UNKNOWN", "9999-99-99", "000-000-0000", "NONE"]


def dirty_phone(value):
    if pd.isna(value) or value == "":
        return value
    digits = re.sub(r"\D", "", str(value))
    choice = random.random()
    if choice < 0.4:
        digits = digits + str(random.randint(0, 9))
    elif choice < 0.7 and len(digits) > 4:
        digits = digits[:-2]
    if len(digits) >= 10:
        style = random.choice(["plain", "dashes", "parens"])
        if style == "dashes":
            return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
        if style == "parens":
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return digits


def strip_leading_zip_zero(value):
    s = str(value)
    return s.lstrip("0") or "0" if s.startswith("0") else value


def mangle_case(value):
    if pd.isna(value):
        return value
    s = str(value)
    choice = random.random()
    if choice < 0.33:
        return s.upper()
    if choice < 0.66:
        return s.lower()
    return f"  {s}  "


def duplicate_rows(df, rate):
    n = int(len(df) * rate)
    if n == 0:
        return df
    dupes = df.sample(n=n, random_state=random.randint(0, 10_000))
    return pd.concat([df, dupes], ignore_index=True)


def inject_missing(df, cols, rate):
    for col in cols:
        if col not in df.columns:
            continue
        mask = df.sample(frac=rate, random_state=random.randint(0, 10_000)).index
        df.loc[mask, col] = pd.NA
    return df


def inject_placeholders(df, cols, rate):
    for col in cols:
        if col not in df.columns:
            continue
        mask = df.sample(frac=rate, random_state=random.randint(0, 10_000)).index
        df.loc[mask, col] = [random.choice(PLACEHOLDERS) for _ in mask]
    return df


def dirty_patients(df):
    df = duplicate_rows(df, DUPLICATE_RATE)
    df = inject_missing(df, ["ADDRESS", "CITY", "STATE", "ZIP"], MISSING_RATE)
    df = inject_placeholders(df, ["MAIDEN", "SUFFIX"], PLACEHOLDER_RATE)

    zip_mask = df.sample(frac=ZIP_LEADING_ZERO_LOSS_RATE, random_state=1).index
    df.loc[zip_mask, "ZIP"] = df.loc[zip_mask, "ZIP"].apply(strip_leading_zip_zero)

    case_mask = df.sample(frac=CASING_WHITESPACE_RATE, random_state=2).index
    df.loc[case_mask, "FIRST"] = df.loc[case_mask, "FIRST"].apply(mangle_case)
    df.loc[case_mask, "LAST"] = df.loc[case_mask, "LAST"].apply(mangle_case)

    has_death = df["DEATHDATE"].notna()
    if has_death.any():
        outliers = df[has_death].sample(frac=min(OUTLIER_RATE, 1.0), random_state=3).index
        df.loc[outliers, "DEATHDATE"] = df.loc[outliers, "BIRTHDATE"]

    return df


def dirty_org_or_payer(df):
    df = duplicate_rows(df, DUPLICATE_RATE)
    df = inject_missing(df, ["PHONE"], MISSING_RATE)

    phone_mask = df.sample(frac=INVALID_PHONE_RATE, random_state=4).index
    df.loc[phone_mask, "PHONE"] = df.loc[phone_mask, "PHONE"].apply(dirty_phone)

    if "ZIP" in df.columns:
        zip_mask = df.sample(frac=ZIP_LEADING_ZERO_LOSS_RATE, random_state=5).index
        df.loc[zip_mask, "ZIP"] = df.loc[zip_mask, "ZIP"].apply(strip_leading_zip_zero)

    return df


def dirty_encounters(df):
    df = duplicate_rows(df, DUPLICATE_RATE)

    orphan_mask = df.sample(frac=ORPHAN_FK_RATE, random_state=6).index
    df.loc[orphan_mask, "PATIENT"] = ["MISSING-" + str(i) for i in orphan_mask]

    outlier_mask = df.sample(frac=OUTLIER_RATE, random_state=7).index
    df.loc[outlier_mask, "TOTAL_CLAIM_COST"] = (
        -pd.to_numeric(df.loc[outlier_mask, "TOTAL_CLAIM_COST"], errors="coerce").abs()
    )

    return df


def dirty_claims(df):
    df = duplicate_rows(df, DUPLICATE_RATE)

    orphan_mask = df.sample(frac=ORPHAN_FK_RATE, random_state=8).index
    df.loc[orphan_mask, "PROVIDERID"] = ["MISSING-" + str(i) for i in orphan_mask]

    return df


DIRTY_HANDLERS = {
    "patients.csv": dirty_patients,
    "organizations.csv": dirty_org_or_payer,
    "payers.csv": dirty_org_or_payer,
    "encounters.csv": dirty_encounters,
    "claims.csv": dirty_claims,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-dir", required=True)
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    for csv_path in sorted(in_dir.glob("*.csv")):
        name = csv_path.name
        df = pd.read_csv(csv_path, dtype=str)
        before = len(df)

        handler = DIRTY_HANDLERS.get(name)
        if handler:
            df = handler(df)

        df.to_csv(out_dir / name, index=False)
        tag = " (dirtied)" if handler else " (unchanged)"
        print(f"{name}: {before} -> {len(df)} rows{tag}")

    print(f"\nDone. Dirty output written to {out_dir}")


if __name__ == "__main__":
    main()
