# Meridian Health — Legacy-to-Lakehouse Migration

A hands-on data engineering project simulating a real healthcare system's migration from a legacy on-prem estate (OLTP database + file share) into a governed, medallion-architecture cloud lakehouse on Azure Databricks.

This isn't a toy demo against sample data. It's built end-to-end against a realistic ~15GB synthetic healthcare dataset (Synthea-generated patient/claims records plus the NIH ChestX-ray14 imaging set), with a legacy schema deliberately designed with real-world messiness — denormalized columns, text-typed dates and currency, bare reference codes, no foreign keys — so the migration and cleanup work is genuine, not simulated.

## Why this project

Most portfolio data projects start from a clean CSV and load it into a warehouse. This one starts from a deliberately messy, legacy-shaped source system and works through the actual problems a migration engineer hits in practice: schema quirks discovered only by loading real data, cloud infrastructure that doesn't behave the way the docs imply, credentials that go stale, disk failures mid-run. The goal was to build something that reflects what the job actually looks like, not a happy-path tutorial.

## Architecture

**Source (simulated legacy estate, local Docker):**
- PostgreSQL 16 — 18-table OLTP schema modeling claims, encounters, patients, providers, prescriptions, and more, with intentional legacy quirks (see `migration/schema_mapping.md`)
- MinIO (S3-compatible) — flat file share holding FHIR bundles, C-CDA documents, and diagnostic imaging

**Target (Azure Databricks lakehouse):**
- Azure Data Lake Storage Gen2 (hierarchical namespace) — bronze / silver / gold containers
- Unity Catalog — governs access across bronze/silver/gold schemas via storage credentials and external locations
- Databricks compute — medallion-architecture transformations (raw → cleaned/conformed → business-ready)
- Connectivity from Databricks back to the local legacy Postgres instance via a Cloudflare Tunnel, standing in for what a real migration would do with a Site-to-Site VPN or ExpressRoute private link

**Medallion layers:**
- **Bronze** — raw, unmodified ingestion from the legacy system, with full lineage columns
- **Silver** — cleaned, typed, conformed (denormalized columns dropped, bare codes joined against reference dimensions, currency/date text fields properly typed, SCD2 dimensions for patients/providers)
- **Gold** — business-ready aggregates and masked/governed views for downstream consumption

Full mapping of every legacy table to its bronze/silver target, including every known data-quality issue and how it's handled, is documented in `migration/schema_mapping.md`.

## What's built so far

- Legacy Postgres schema (18 tables) and Docker Compose environment for the full simulated legacy estate
- Bulk data loader (`load_legacy_db.py`) — chunked, `COPY`-based ingestion handling ~15GB / 30M+ rows across 18 tables, with defensive column mapping and per-table reload support
- File share loader (`load_legacy_fileshare.py`) — resumable, concurrent upload of 100K+ FHIR/C-CDA/imaging files to MinIO with integrity verification
- Azure infrastructure: ADLS Gen2 storage account, Unity Catalog (credentials, external locations, catalog/schemas), Databricks Premium workspace with a working compute cluster
- Cloudflare Tunnel connectivity from Databricks back to the local legacy database for JDBC-based extraction
- End-to-end verification: a live Databricks cluster reading/writing through Unity Catalog into real ADLS Gen2 storage

**In progress:** the medallion ETL pipeline (bronze → silver → gold transformations), data quality monitoring, and orchestration.

## Notable engineering problems solved along the way

- **Real schema bugs found only by loading real data.** Synthea's `imaging_studies.csv` turned out to have one row per series/instance sharing a study-level ID (not unique per row as assumed), and `claims_transactions.csv` has no unique row identifier at all. Both required schema patches (surrogate keys) and isolated table reloads without touching already-loaded data.
- **Full local infrastructure recovery.** A disk-space exhaustion incident corrupted the local Docker VM mid-upload. Recovered via a clean restart and verified zero data loss by re-checking exact row counts against pre-incident values.
- **Azure vCPU quota exhaustion.** Cluster creation repeatedly failed against different VM families for different reasons — confidential-computing SKUs can't be self-service quota-approved, and Azure tracks each VM sub-family (`Dv5`, `Dsv5`, `Dasv5`, `Ddsv5`) as an entirely separate quota pool. Root-caused to a freshly upgraded trial subscription with no billing history, resolved via a manually-reviewed Azure support ticket rather than the automated self-service path.
- **Stale Unity Catalog credential after workspace recreation.** An orphaned storage credential reference blocked table creation with a `STORAGE_CREDENTIAL_DOES_NOT_EXIST` error, compounded by a chicken-and-egg dependency (couldn't tear down the credential's managed file-event queue without a valid credential to do it with). Resolved by recreating the external locations cleanly with file events disabled.

## Repo structure

```
legacy_source/          # Simulated legacy system
  init/schema.sql        # Legacy Postgres DDL (18 tables)
  docker-compose.yml      # Postgres + MinIO + bucket init
  load_scripts/
    load_legacy_db.py      # Bulk OLTP data loader
    load_legacy_fileshare.py  # File share uploader (FHIR/C-CDA/images)
migration/
  schema_mapping.md       # Legacy -> bronze -> silver mapping, per table
docs/                    # Supporting documentation
```

## Tech stack

PostgreSQL 16, MinIO, Docker Compose, Python (psycopg2, pandas, boto3), Azure Data Lake Storage Gen2, Azure Databricks (Unity Catalog, Delta Lake, PySpark/SQL), Cloudflare Tunnel.
