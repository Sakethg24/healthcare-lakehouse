#!/usr/bin/env python3
"""
load_legacy_db.py

Loads the deliberately dirty Synthea CSV output (csv_run_13k_dirty) into the
legacy_claims_db Postgres database, applying the "legacy shaping"
transformations documented in migration/schema_mapping.md and
legacy_source/init/schema.sql:

  - patient_master: folds ADDRESS/CITY/STATE/ZIP into one free-text `addr`
    column instead of separate structured fields.
  - encountr: keeps enc_type_cd/proc_code/reason_cd as bare codes and drops
    the human-readable DESCRIPTION columns Synthea provides (no in-database
    lookup table -- silver joins these against a reference dimension).
  - dx_condition: denormalizes patient_name onto every row.
  - rx_med: reformats medication start/stop dates into legacy-style
    'MM/DD/YYYY' text instead of a native date type.
  - claim_line: reformats amount as legacy-style '$123.45' text.

Design notes:
  - This script does NOT try to clean the injected messiness on the way in
    (duplicate rows, missing values, malformed phone/zip, orphaned FK
    values). It loads that data as-is, on purpose, so it actually lands in
    the legacy system for the silver layer to find and clean later. See
    legacy_source/init/schema.sql for why patient_master/org_master/
    payer_master/encountr/claim_hdr don't enforce a PRIMARY KEY.
  - Uses PostgreSQL COPY (via psycopg2 copy_expert) for bulk loading rather
    than row-by-row INSERTs -- realistic for an actual legacy bulk load and
    far faster at this data volume.
  - Reads CSVs in chunks (default 50,000 rows) instead of loading entire
    files into memory -- some of these run into the millions of rows across
    ~13,000 patients' full lifetimes.
  - Column mapping is defensive: if an expected source column isn't found
    in a CSV's actual header, that target column loads as NULL and a
    one-time warning prints, rather than the script crashing outright.
    Synthea's exact column set can vary slightly by version.
  - Each chunk is loaded in its own transaction. If a chunk fails (e.g. an
    unexpected value that can't cast to the target type), that chunk is
    rolled back and reported, and the script moves on to the next chunk/
    table rather than aborting the whole run.

Usage:
    python3 load_legacy_db.py --csv-dir /path/to/csv_run_13k_dirty

Connection defaults match legacy_source/docker-compose.yml (localhost:5432,
db legacy_claims_db, user meridian_admin). Override with --db-host/--db-port/
--db-name/--db-user/--db-password, or the PGHOST/PGPORT/PGDATABASE/PGUSER/
PGPASSWORD environment variables.
"""
import argparse
import io
import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2

CHUNK_SIZE = 50_000
_WARNED_MISSING_COLS = set()


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def col(df, name, table_hint=""):
    """Return df[name] if present, else a NULL column, warning once."""
    if name in df.columns:
        return df[name]
    key = f"{table_hint}.{name}"
    if key not in _WARNED_MISSING_COLS:
        print(f"  [warn] expected source column '{name}' not found ({table_hint}) -- loading NULL for it")
        _WARNED_MISSING_COLS.add(key)
    return pd.Series([None] * len(df), index=df.index)


def col_any(df, candidates, table_hint=""):
    """Try a list of candidate source column names in order; first match wins."""
    for name in candidates:
        if name in df.columns:
            return df[name]
    key = f"{table_hint}.{'/'.join(candidates)}"
    if key not in _WARNED_MISSING_COLS:
        print(f"  [warn] none of {candidates} found ({table_hint}) -- loading NULL for it")
        _WARNED_MISSING_COLS.add(key)
    return pd.Series([None] * len(df), index=df.index)


def to_legacy_date_text(series):
    """Reformat a date/timestamp-like text column into 'MM/DD/YYYY' text
    strings -- the deliberate rx_med legacy quirk."""
    parsed = pd.to_datetime(series, errors="coerce", utc=False)
    return parsed.dt.strftime("%m/%d/%Y")


def to_legacy_dollar_text(series):
    """Reformat a numeric column into legacy-style '$123.45' text -- the
    deliberate claim_line.amount quirk."""
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.apply(lambda v: f"${v:,.2f}" if pd.notna(v) else None)


def build_addr(chunk):
    """Fold ADDRESS/CITY/STATE/ZIP into one free-text column -- the
    deliberate patient_master legacy quirk."""
    def _addr(row):
        parts = [row.get("ADDRESS"), row.get("CITY"), row.get("STATE"), row.get("ZIP")]
        parts = [str(p).strip() for p in parts if pd.notna(p) and str(p).strip() != ""]
        return ", ".join(parts) if parts else None
    return chunk.apply(_addr, axis=1)


def bulk_copy(conn, df, table, columns):
    """Bulk-load a dataframe into `table` via Postgres COPY."""
    if df.empty:
        return 0
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, columns=columns, na_rep="")
    buf.seek(0)
    cols_sql = ", ".join(columns)
    with conn.cursor() as cur:
        cur.copy_expert(f"COPY {table} ({cols_sql}) FROM STDIN WITH (FORMAT csv, NULL '')", buf)
    conn.commit()
    return len(df)


# -----------------------------------------------------------------------------
# Per-table transforms. Each returns a DataFrame whose columns exactly match
# `insert_columns` for that table (order matters for bulk_copy).
# -----------------------------------------------------------------------------

def transform_org_master(chunk, ctx):
    out = pd.DataFrame()
    out["org_id"] = col(chunk, "Id", "org_master")
    out["name"] = col(chunk, "NAME", "org_master")
    out["address"] = col(chunk, "ADDRESS", "org_master")
    out["city"] = col(chunk, "CITY", "org_master")
    out["state"] = col(chunk, "STATE", "org_master")
    out["zip"] = col(chunk, "ZIP", "org_master")
    out["lat"] = col(chunk, "LAT", "org_master")
    out["lon"] = col(chunk, "LON", "org_master")
    out["phone"] = col(chunk, "PHONE", "org_master")
    out["revenue"] = col(chunk, "REVENUE", "org_master")
    out["utilization"] = col(chunk, "UTILIZATION", "org_master")
    return out


def transform_provider_master(chunk, ctx):
    out = pd.DataFrame()
    out["provider_id"] = col(chunk, "Id", "provider_master")
    out["org_id"] = col(chunk, "ORGANIZATION", "provider_master")
    out["name"] = col(chunk, "NAME", "provider_master")
    out["gender"] = col(chunk, "GENDER", "provider_master")
    out["specialty"] = col(chunk, "SPECIALITY", "provider_master")
    out["address"] = col(chunk, "ADDRESS", "provider_master")
    out["city"] = col(chunk, "CITY", "provider_master")
    out["state"] = col(chunk, "STATE", "provider_master")
    out["zip"] = col(chunk, "ZIP", "provider_master")
    out["lat"] = col(chunk, "LAT", "provider_master")
    out["lon"] = col(chunk, "LON", "provider_master")
    out["num_encounters"] = col(chunk, "ENCOUNTERS", "provider_master")
    out["num_procedures"] = col(chunk, "PROCEDURES", "provider_master")
    return out


def transform_payer_master(chunk, ctx):
    out = pd.DataFrame()
    out["payer_id"] = col(chunk, "Id", "payer_master")
    out["name"] = col(chunk, "NAME", "payer_master")
    out["address"] = col(chunk, "ADDRESS", "payer_master")
    out["city"] = col(chunk, "CITY", "payer_master")
    out["state_headquartered"] = col_any(chunk, ["STATE_HEADQUARTERED", "STATE"], "payer_master")
    out["zip"] = col(chunk, "ZIP", "payer_master")
    out["phone"] = col(chunk, "PHONE", "payer_master")
    out["amount_covered"] = col(chunk, "AMOUNT_COVERED", "payer_master")
    out["amount_uncovered"] = col(chunk, "AMOUNT_UNCOVERED", "payer_master")
    out["revenue"] = col(chunk, "REVENUE", "payer_master")
    out["covered_encounters"] = col(chunk, "COVERED_ENCOUNTERS", "payer_master")
    out["uncovered_encounters"] = col(chunk, "UNCOVERED_ENCOUNTERS", "payer_master")
    out["covered_medications"] = col(chunk, "COVERED_MEDICATIONS", "payer_master")
    out["uncovered_medications"] = col(chunk, "UNCOVERED_MEDICATIONS", "payer_master")
    out["covered_procedures"] = col(chunk, "COVERED_PROCEDURES", "payer_master")
    out["uncovered_procedures"] = col(chunk, "UNCOVERED_PROCEDURES", "payer_master")
    out["covered_immunizations"] = col(chunk, "COVERED_IMMUNIZATIONS", "payer_master")
    out["uncovered_immunizations"] = col(chunk, "UNCOVERED_IMMUNIZATIONS", "payer_master")
    out["unique_customers"] = col(chunk, "UNIQUE_CUSTOMERS", "payer_master")
    out["qols_avg"] = col(chunk, "QOLS_AVG", "payer_master")
    out["member_months"] = col(chunk, "MEMBER_MONTHS", "payer_master")
    return out


def transform_patient_master(chunk, ctx):
    out = pd.DataFrame()
    out["patient_id"] = col(chunk, "Id", "patient_master")
    out["ssn"] = col(chunk, "SSN", "patient_master")
    out["drivers"] = col(chunk, "DRIVERS", "patient_master")
    out["passport"] = col(chunk, "PASSPORT", "patient_master")
    out["prefix"] = col(chunk, "PREFIX", "patient_master")
    out["first_name"] = col(chunk, "FIRST", "patient_master")
    out["middle_name"] = col(chunk, "MIDDLE", "patient_master")
    out["last_name"] = col(chunk, "LAST", "patient_master")
    out["suffix"] = col(chunk, "SUFFIX", "patient_master")
    out["maiden_name"] = col(chunk, "MAIDEN", "patient_master")
    out["marital_status"] = col(chunk, "MARITAL", "patient_master")
    out["race"] = col(chunk, "RACE", "patient_master")
    out["ethnicity"] = col(chunk, "ETHNICITY", "patient_master")
    out["gender"] = col(chunk, "GENDER", "patient_master")
    out["birthplace"] = col(chunk, "BIRTHPLACE", "patient_master")
    out["addr"] = build_addr(chunk)
    out["fips"] = col(chunk, "FIPS", "patient_master")
    out["lat"] = col(chunk, "LAT", "patient_master")
    out["lon"] = col(chunk, "LON", "patient_master")
    out["birthdate"] = col(chunk, "BIRTHDATE", "patient_master")
    out["deathdate"] = col(chunk, "DEATHDATE", "patient_master")
    out["healthcare_expenses"] = col(chunk, "HEALTHCARE_EXPENSES", "patient_master")
    out["healthcare_coverage"] = col(chunk, "HEALTHCARE_COVERAGE", "patient_master")
    out["income"] = col(chunk, "INCOME", "patient_master")
    return out


def transform_encountr(chunk, ctx):
    out = pd.DataFrame()
    out["encounter_id"] = col(chunk, "Id", "encountr")
    out["patient_id"] = col(chunk, "PATIENT", "encountr")
    out["org_id"] = col(chunk, "ORGANIZATION", "encountr")
    out["provider_id"] = col(chunk, "PROVIDER", "encountr")
    out["payer_id"] = col(chunk, "PAYER", "encountr")
    out["enc_type_cd"] = col(chunk, "ENCOUNTERCLASS", "encountr")   # bare code, no lookup -- quirk
    out["proc_code"] = col(chunk, "CODE", "encountr")               # bare code, no lookup -- quirk
    out["reason_cd"] = col(chunk, "REASONCODE", "encountr")         # bare code, no lookup -- quirk
    out["start_ts"] = col(chunk, "START", "encountr")
    out["stop_ts"] = col(chunk, "STOP", "encountr")
    out["base_encounter_cost"] = col(chunk, "BASE_ENCOUNTER_COST", "encountr")
    out["total_claim_cost"] = col(chunk, "TOTAL_CLAIM_COST", "encountr")
    out["payer_coverage"] = col(chunk, "PAYER_COVERAGE", "encountr")
    return out


def transform_dx_condition(chunk, ctx):
    out = pd.DataFrame()
    patient_ids = col(chunk, "PATIENT", "dx_condition")
    out["patient_id"] = patient_ids
    out["patient_name"] = patient_ids.map(ctx["patient_name_lookup"])  # denormalized -- quirk
    out["encounter_id"] = col(chunk, "ENCOUNTER", "dx_condition")
    out["dx_system"] = col(chunk, "SYSTEM", "dx_condition")
    out["dx_code"] = col(chunk, "CODE", "dx_condition")
    out["dx_description"] = col(chunk, "DESCRIPTION", "dx_condition")
    out["onset_date"] = col(chunk, "START", "dx_condition")
    out["resolved_date"] = col(chunk, "STOP", "dx_condition")
    return out


def transform_rx_med(chunk, ctx):
    out = pd.DataFrame()
    out["patient_id"] = col(chunk, "PATIENT", "rx_med")
    out["payer_id"] = col(chunk, "PAYER", "rx_med")
    out["encounter_id"] = col(chunk, "ENCOUNTER", "rx_med")
    out["med_code"] = col(chunk, "CODE", "rx_med")
    out["med_description"] = col(chunk, "DESCRIPTION", "rx_med")
    out["start_dt"] = to_legacy_date_text(col(chunk, "START", "rx_med"))   # text date -- quirk
    out["stop_dt"] = to_legacy_date_text(col(chunk, "STOP", "rx_med"))     # text date -- quirk
    out["base_cost"] = col(chunk, "BASE_COST", "rx_med")
    out["payer_coverage"] = col(chunk, "PAYER_COVERAGE", "rx_med")
    out["dispenses"] = col(chunk, "DISPENSES", "rx_med")
    out["total_cost"] = col_any(chunk, ["TOTALCOST", "TOTAL_COST"], "rx_med")
    out["reason_cd"] = col(chunk, "REASONCODE", "rx_med")
    out["reason_description"] = col(chunk, "REASONDESCRIPTION", "rx_med")
    return out


def transform_procedur(chunk, ctx):
    out = pd.DataFrame()
    out["patient_id"] = col(chunk, "PATIENT", "procedur")
    out["encounter_id"] = col(chunk, "ENCOUNTER", "procedur")
    out["proc_system"] = col(chunk, "SYSTEM", "procedur")
    out["proc_code"] = col(chunk, "CODE", "procedur")
    out["proc_description"] = col(chunk, "DESCRIPTION", "procedur")
    out["proc_date"] = col_any(chunk, ["START", "DATE"], "procedur")
    out["base_cost"] = col(chunk, "BASE_COST", "procedur")
    out["reason_cd"] = col(chunk, "REASONCODE", "procedur")
    out["reason_description"] = col(chunk, "REASONDESCRIPTION", "procedur")
    return out


def transform_immunizatn(chunk, ctx):
    out = pd.DataFrame()
    out["patient_id"] = col(chunk, "PATIENT", "immunizatn")
    out["encounter_id"] = col(chunk, "ENCOUNTER", "immunizatn")
    out["immun_date"] = col(chunk, "DATE", "immunizatn")
    out["immun_code"] = col(chunk, "CODE", "immunizatn")
    out["immun_description"] = col(chunk, "DESCRIPTION", "immunizatn")
    out["base_cost"] = col(chunk, "BASE_COST", "immunizatn")
    return out


def transform_careplan(chunk, ctx):
    out = pd.DataFrame()
    out["careplan_id"] = col(chunk, "Id", "careplan")
    out["patient_id"] = col(chunk, "PATIENT", "careplan")
    out["encounter_id"] = col(chunk, "ENCOUNTER", "careplan")
    out["start_date"] = col(chunk, "START", "careplan")
    out["stop_date"] = col(chunk, "STOP", "careplan")
    out["code"] = col(chunk, "CODE", "careplan")
    out["description"] = col(chunk, "DESCRIPTION", "careplan")
    out["reason_cd"] = col(chunk, "REASONCODE", "careplan")
    out["reason_description"] = col(chunk, "REASONDESCRIPTION", "careplan")
    return out


def transform_allergy(chunk, ctx):
    out = pd.DataFrame()
    out["patient_id"] = col(chunk, "PATIENT", "allergy")
    out["encounter_id"] = col(chunk, "ENCOUNTER", "allergy")
    out["allergy_system"] = col(chunk, "SYSTEM", "allergy")
    out["allergy_code"] = col(chunk, "CODE", "allergy")
    out["allergy_description"] = col(chunk, "DESCRIPTION", "allergy")
    out["allergy_type"] = col(chunk, "TYPE", "allergy")
    out["category"] = col(chunk, "CATEGORY", "allergy")
    out["start_date"] = col(chunk, "START", "allergy")
    out["stop_date"] = col(chunk, "STOP", "allergy")
    out["reaction1_code"] = col(chunk, "REACTION1", "allergy")
    out["reaction1_description"] = col(chunk, "DESCRIPTION1", "allergy")
    out["severity1"] = col(chunk, "SEVERITY1", "allergy")
    out["reaction2_code"] = col(chunk, "REACTION2", "allergy")
    out["reaction2_description"] = col(chunk, "DESCRIPTION2", "allergy")
    out["severity2"] = col(chunk, "SEVERITY2", "allergy")
    return out


def transform_device(chunk, ctx):
    out = pd.DataFrame()
    out["patient_id"] = col(chunk, "PATIENT", "device")
    out["encounter_id"] = col(chunk, "ENCOUNTER", "device")
    out["device_code"] = col(chunk, "CODE", "device")
    out["device_description"] = col(chunk, "DESCRIPTION", "device")
    out["udi"] = col(chunk, "UDI", "device")
    out["start_date"] = col(chunk, "START", "device")
    out["stop_date"] = col(chunk, "STOP", "device")
    return out


def transform_img_study(chunk, ctx):
    out = pd.DataFrame()
    out["study_id"] = col(chunk, "Id", "img_study")
    out["patient_id"] = col(chunk, "PATIENT", "img_study")
    out["encounter_id"] = col(chunk, "ENCOUNTER", "img_study")
    out["study_date"] = col(chunk, "DATE", "img_study")
    out["series_uid"] = col(chunk, "SERIES_UID", "img_study")
    out["bodysite_code"] = col(chunk, "BODYSITE_CODE", "img_study")
    out["bodysite_description"] = col(chunk, "BODYSITE_DESCRIPTION", "img_study")
    out["modality_code"] = col(chunk, "MODALITY_CODE", "img_study")
    out["modality_description"] = col(chunk, "MODALITY_DESCRIPTION", "img_study")
    out["instance_uid"] = col(chunk, "INSTANCE_UID", "img_study")
    out["sop_code"] = col(chunk, "SOP_CODE", "img_study")
    out["sop_description"] = col(chunk, "SOP_DESCRIPTION", "img_study")
    out["procedure_code"] = col(chunk, "PROCEDURE_CODE", "img_study")
    return out


def transform_observatn(chunk, ctx):
    out = pd.DataFrame()
    out["patient_id"] = col(chunk, "PATIENT", "observatn")
    out["encounter_id"] = col(chunk, "ENCOUNTER", "observatn")
    out["obs_date"] = col(chunk, "DATE", "observatn")
    out["category"] = col(chunk, "CATEGORY", "observatn")
    out["obs_code"] = col(chunk, "CODE", "observatn")
    out["obs_description"] = col(chunk, "DESCRIPTION", "observatn")
    out["obs_value"] = col(chunk, "VALUE", "observatn")
    out["units"] = col(chunk, "UNITS", "observatn")
    out["value_type"] = col(chunk, "TYPE", "observatn")
    return out


def transform_payer_xfer(chunk, ctx):
    out = pd.DataFrame()
    out["patient_id"] = col(chunk, "PATIENT", "payer_xfer")
    out["start_year"] = col(chunk, "START_YEAR", "payer_xfer")
    out["end_year"] = col(chunk, "END_YEAR", "payer_xfer")
    out["payer_id"] = col(chunk, "PAYER", "payer_xfer")
    out["ownership"] = col(chunk, "OWNERSHIP", "payer_xfer")
    return out


def transform_supply(chunk, ctx):
    out = pd.DataFrame()
    out["supply_date"] = col(chunk, "DATE", "supply")
    out["patient_id"] = col(chunk, "PATIENT", "supply")
    out["encounter_id"] = col(chunk, "ENCOUNTER", "supply")
    out["supply_code"] = col(chunk, "CODE", "supply")
    out["supply_description"] = col(chunk, "DESCRIPTION", "supply")
    out["quantity"] = col(chunk, "QUANTITY", "supply")
    return out


def transform_claim_hdr(chunk, ctx):
    out = pd.DataFrame()
    out["claim_id"] = col(chunk, "Id", "claim_hdr")
    out["patient_id"] = col(chunk, "PATIENTID", "claim_hdr")
    out["provider_id"] = col(chunk, "PROVIDERID", "claim_hdr")
    out["primary_payer_id"] = col(chunk, "PRIMARYPATIENTINSURANCEID", "claim_hdr")
    out["secondary_payer_id"] = col(chunk, "SECONDARYPATIENTINSURANCEID", "claim_hdr")
    out["department_id"] = col(chunk, "DEPARTMENTID", "claim_hdr")
    out["referring_provider_id"] = col(chunk, "REFERRINGPROVIDERID", "claim_hdr")
    out["supervising_provider_id"] = col(chunk, "SUPERVISINGPROVIDERID", "claim_hdr")
    out["appointment_id"] = col(chunk, "APPOINTMENTID", "claim_hdr")
    out["current_illness_date"] = col(chunk, "CURRENTILLNESSDATE", "claim_hdr")
    out["service_date"] = col(chunk, "SERVICEDATE", "claim_hdr")
    out["status1"] = col(chunk, "STATUS1", "claim_hdr")
    out["status2"] = col(chunk, "STATUS2", "claim_hdr")
    out["outstanding1"] = col(chunk, "OUTSTANDING1", "claim_hdr")
    out["outstanding2"] = col(chunk, "OUTSTANDING2", "claim_hdr")
    out["last_billed_date1"] = col(chunk, "LASTBILLEDDATE1", "claim_hdr")
    out["last_billed_date2"] = col(chunk, "LASTBILLEDDATE2", "claim_hdr")
    return out


def transform_claim_line(chunk, ctx):
    out = pd.DataFrame()
    # no line_id here -- claims_transactions.csv has no unique per-row Id
    # column; txn_id is a SERIAL surrogate assigned by Postgres on insert.
    out["claim_id"] = col(chunk, "CLAIMID", "claim_line")
    out["charge_id"] = col(chunk, "CHARGEID", "claim_line")
    out["patient_id"] = col(chunk, "PATIENTID", "claim_line")
    out["txn_type"] = col(chunk, "TYPE", "claim_line")
    out["amount"] = to_legacy_dollar_text(col_any(chunk, ["AMOUNT", "UNITAMOUNT"], "claim_line"))  # text $ -- quirk
    out["method"] = col(chunk, "METHOD", "claim_line")
    out["from_date"] = col(chunk, "FROMDATE", "claim_line")
    out["to_date"] = col(chunk, "TODATE", "claim_line")
    out["place_of_service"] = col(chunk, "PLACEOFSERVICE", "claim_line")
    out["procedure_code"] = col(chunk, "PROCEDURECODE", "claim_line")
    out["units"] = col(chunk, "UNITS", "claim_line")
    out["department_id"] = col(chunk, "DEPARTMENTID", "claim_line")
    out["payments"] = col(chunk, "PAYMENTS", "claim_line")
    out["adjustments"] = col(chunk, "ADJUSTMENTS", "claim_line")
    out["transfers"] = col(chunk, "TRANSFERS", "claim_line")
    out["outstanding"] = col(chunk, "OUTSTANDING", "claim_line")
    out["provider_id"] = col(chunk, "PROVIDERID", "claim_line")
    return out


# -----------------------------------------------------------------------------
# Table registry -- load order matters a little (reference tables first) but
# since no FKs are enforced, it's not strictly required.
# -----------------------------------------------------------------------------

TABLES = [
    {"source_file": "organizations.csv", "target_table": "org_master", "transform": transform_org_master,
     "insert_columns": ["org_id", "name", "address", "city", "state", "zip", "lat", "lon", "phone", "revenue", "utilization"]},
    {"source_file": "providers.csv", "target_table": "provider_master", "transform": transform_provider_master,
     "insert_columns": ["provider_id", "org_id", "name", "gender", "specialty", "address", "city", "state", "zip", "lat", "lon", "num_encounters", "num_procedures"]},
    {"source_file": "payers.csv", "target_table": "payer_master", "transform": transform_payer_master,
     "insert_columns": ["payer_id", "name", "address", "city", "state_headquartered", "zip", "phone", "amount_covered",
                         "amount_uncovered", "revenue", "covered_encounters", "uncovered_encounters", "covered_medications",
                         "uncovered_medications", "covered_procedures", "uncovered_procedures", "covered_immunizations",
                         "uncovered_immunizations", "unique_customers", "qols_avg", "member_months"]},
    {"source_file": "patients.csv", "target_table": "patient_master", "transform": transform_patient_master,
     "insert_columns": ["patient_id", "ssn", "drivers", "passport", "prefix", "first_name", "middle_name", "last_name",
                         "suffix", "maiden_name", "marital_status", "race", "ethnicity", "gender", "birthplace", "addr",
                         "fips", "lat", "lon", "birthdate", "deathdate", "healthcare_expenses", "healthcare_coverage", "income"]},
    {"source_file": "encounters.csv", "target_table": "encountr", "transform": transform_encountr,
     "insert_columns": ["encounter_id", "patient_id", "org_id", "provider_id", "payer_id", "enc_type_cd", "proc_code",
                         "reason_cd", "start_ts", "stop_ts", "base_encounter_cost", "total_claim_cost", "payer_coverage"]},
    {"source_file": "conditions.csv", "target_table": "dx_condition", "transform": transform_dx_condition,
     "insert_columns": ["patient_id", "patient_name", "encounter_id", "dx_system", "dx_code", "dx_description", "onset_date", "resolved_date"]},
    {"source_file": "medications.csv", "target_table": "rx_med", "transform": transform_rx_med,
     "insert_columns": ["patient_id", "payer_id", "encounter_id", "med_code", "med_description", "start_dt", "stop_dt",
                         "base_cost", "payer_coverage", "dispenses", "total_cost", "reason_cd", "reason_description"]},
    {"source_file": "procedures.csv", "target_table": "procedur", "transform": transform_procedur,
     "insert_columns": ["patient_id", "encounter_id", "proc_system", "proc_code", "proc_description", "proc_date",
                         "base_cost", "reason_cd", "reason_description"]},
    {"source_file": "immunizations.csv", "target_table": "immunizatn", "transform": transform_immunizatn,
     "insert_columns": ["patient_id", "encounter_id", "immun_date", "immun_code", "immun_description", "base_cost"]},
    {"source_file": "careplans.csv", "target_table": "careplan", "transform": transform_careplan,
     "insert_columns": ["careplan_id", "patient_id", "encounter_id", "start_date", "stop_date", "code", "description",
                         "reason_cd", "reason_description"]},
    {"source_file": "allergies.csv", "target_table": "allergy", "transform": transform_allergy,
     "insert_columns": ["patient_id", "encounter_id", "allergy_system", "allergy_code", "allergy_description",
                         "allergy_type", "category", "start_date", "stop_date", "reaction1_code", "reaction1_description",
                         "severity1", "reaction2_code", "reaction2_description", "severity2"]},
    {"source_file": "devices.csv", "target_table": "device", "transform": transform_device,
     "insert_columns": ["patient_id", "encounter_id", "device_code", "device_description", "udi", "start_date", "stop_date"]},
    {"source_file": "imaging_studies.csv", "target_table": "img_study", "transform": transform_img_study,
     "insert_columns": ["study_id", "patient_id", "encounter_id", "study_date", "series_uid", "bodysite_code",
                         "bodysite_description", "modality_code", "modality_description", "instance_uid", "sop_code",
                         "sop_description", "procedure_code"]},
    {"source_file": "observations.csv", "target_table": "observatn", "transform": transform_observatn,
     "insert_columns": ["patient_id", "encounter_id", "obs_date", "category", "obs_code", "obs_description", "obs_value", "units", "value_type"]},
    {"source_file": "payer_transitions.csv", "target_table": "payer_xfer", "transform": transform_payer_xfer,
     "insert_columns": ["patient_id", "start_year", "end_year", "payer_id", "ownership"]},
    {"source_file": "supplies.csv", "target_table": "supply", "transform": transform_supply,
     "insert_columns": ["supply_date", "patient_id", "encounter_id", "supply_code", "supply_description", "quantity"]},
    {"source_file": "claims.csv", "target_table": "claim_hdr", "transform": transform_claim_hdr,
     "insert_columns": ["claim_id", "patient_id", "provider_id", "primary_payer_id", "secondary_payer_id",
                         "department_id", "referring_provider_id", "supervising_provider_id", "appointment_id",
                         "current_illness_date", "service_date", "status1", "status2", "outstanding1", "outstanding2",
                         "last_billed_date1", "last_billed_date2"]},
    {"source_file": "claims_transactions.csv", "target_table": "claim_line", "transform": transform_claim_line,
     "insert_columns": ["claim_id", "charge_id", "patient_id", "txn_type", "amount", "method", "from_date",
                         "to_date", "place_of_service", "procedure_code", "units", "department_id", "payments",
                         "adjustments", "transfers", "outstanding", "provider_id"]},
]


def build_patient_name_lookup(csv_dir):
    """Small, full read of patients.csv (it's the smallest file) to build the
    patient_id -> 'FIRST LAST' lookup used to denormalize dx_condition.patient_name."""
    path = csv_dir / "patients.csv"
    if not path.exists():
        print(f"  [warn] {path} not found -- dx_condition.patient_name will be NULL")
        return {}
    df = pd.read_csv(path, dtype=str, usecols=lambda c: c in ("Id", "FIRST", "LAST"))
    names = (df.get("FIRST", "").fillna("") + " " + df.get("LAST", "").fillna("")).str.strip()
    return dict(zip(df["Id"], names))


def load_table(conn, csv_dir, table_cfg, ctx):
    path = csv_dir / table_cfg["source_file"]
    target = table_cfg["target_table"]
    if not path.exists():
        print(f"[skip] {table_cfg['source_file']} not found in {csv_dir}")
        return

    print(f"[load] {table_cfg['source_file']} -> {target}")
    total_rows = 0
    total_loaded = 0
    chunk_num = 0
    for chunk in pd.read_csv(path, dtype=str, chunksize=CHUNK_SIZE):
        chunk_num += 1
        total_rows += len(chunk)
        try:
            transformed = table_cfg["transform"](chunk, ctx)
            loaded = bulk_copy(conn, transformed, target, table_cfg["insert_columns"])
            total_loaded += loaded
        except Exception as e:
            conn.rollback()
            print(f"  [error] chunk {chunk_num} of {table_cfg['source_file']} failed and was skipped: {e}")

    print(f"  -> read {total_rows} source rows, loaded {total_loaded} into {target}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-dir", required=True, help="Path to the dirty CSV directory (e.g. csv_run_13k_dirty)")
    parser.add_argument("--db-host", default=os.environ.get("PGHOST", "localhost"))
    parser.add_argument("--db-port", default=os.environ.get("PGPORT", "5432"))
    parser.add_argument("--db-name", default=os.environ.get("PGDATABASE", "legacy_claims_db"))
    parser.add_argument("--db-user", default=os.environ.get("PGUSER", "meridian_admin"))
    parser.add_argument("--db-password", default=os.environ.get("PGPASSWORD", "changeme_local_only"))
    parser.add_argument("--only", default=None,
                         help="Comma-separated target table names to (re)load, e.g. --only img_study. "
                              "Useful for retrying a single table after a schema fix without reloading "
                              "everything else (most tables have no PK/uniqueness guard, so a full rerun "
                              "would duplicate already-loaded data).")
    args = parser.parse_args()

    csv_dir = Path(args.csv_dir)
    if not csv_dir.is_dir():
        print(f"ERROR: {csv_dir} is not a directory")
        sys.exit(1)

    tables_to_run = TABLES
    if args.only:
        wanted = {t.strip() for t in args.only.split(",")}
        tables_to_run = [t for t in TABLES if t["target_table"] in wanted]
        missing = wanted - {t["target_table"] for t in tables_to_run}
        if missing:
            print(f"ERROR: unknown table name(s) in --only: {missing}")
            sys.exit(1)

    print(f"Connecting to {args.db_user}@{args.db_host}:{args.db_port}/{args.db_name} ...")
    conn = psycopg2.connect(
        host=args.db_host, port=args.db_port, dbname=args.db_name,
        user=args.db_user, password=args.db_password,
    )

    print("Building patient_id -> name lookup for dx_condition denormalization ...")
    ctx = {"patient_name_lookup": build_patient_name_lookup(csv_dir)}

    for table_cfg in tables_to_run:
        load_table(conn, csv_dir, table_cfg, ctx)

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
