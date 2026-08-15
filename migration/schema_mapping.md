# Schema Mapping — Legacy → Target

Documents how each legacy Postgres table maps to its bronze/silver counterpart. The legacy schema is deliberately kept a little messier than the target (older naming, a couple of denormalized columns, inconsistent casing) so this mapping is real work rather than a 1:1 rename.

| Legacy table (Postgres) | Legacy quirks | Target (bronze → silver) | Notes |
|---|---|---|---|
| `PATIENT_MASTER` | upper_snake naming, `SSN` stored unmasked, single `ADDR` free-text column | `bronze.patients` → `silver.dim_patient` (SCD2) | Address gets parsed into structured fields in silver; SSN gets masked at the silver boundary, never exposed downstream unmasked except via the `auditor` UC role. |
| `ENCOUNTR` | truncated table name (legacy 8-char convention), `ENC_TYPE_CD` is a bare code with no lookup table in source | `bronze.encounters` → `silver.fact_encounters` | Code lookup joined in silver against a maintained `ref_encounter_type` dimension. |
| `DX_CONDITION` | one row per diagnosis with a denormalized `PATIENT_NAME` copied in (data-quality smell to preserve and then clean) | `bronze.conditions` → `silver.fact_conditions` | Denormalized name column dropped in silver; existence is used as a deliberate data-quality profiling example. |
| `RX_MED` | dates stored as `VARCHAR` in `MM/DD/YYYY` text format | `bronze.medications` → `silver.fact_medications` | Explicit type-casting/parsing step in silver; a documented example of "why bronze stays raw." |
| `PROVIDER_DIM` (loaded from NPPES, not part of core OLTP) | already fairly clean; arrives as monthly full-replacement files, not via the OLTP DB | `bronze.providers` → `silver.dim_provider` (SCD2) | Not part of the JDBC migration — lands via the existing external vendor-feed path, unchanged from the original design. |
| `CLAIM_HDR` / `CLAIM_LINE` | header/line split, `AMOUNT` stored as text with `$` prefix in a legacy export quirk | `bronze.claims` / `bronze.claims_transactions` → `silver.fact_claims` | Currency parsing and header/line join handled in silver. |
| legacy file share (MinIO paths) | flat directory, filename is the only "key" (`patient_id` embedded in filename, no metadata table) | `bronze.imaging_files` / `bronze.document_files` (Volumes + metadata Delta table) | Migration step parses `patient_id`/`claim_id` out of filenames into a proper metadata table — another realistic "legacy system" problem worth solving visibly. |

## General rules applied during migration

- All legacy `_CD` columns get joined against a reference/lookup dimension in silver rather than staying as bare codes.
- Any column touching direct identifiers (name, SSN, address, DOB) is retained unmasked only through bronze/silver-internal processing; gold and all UC grants apply masking per `governance/column_masking.sql`.
- Every migrated row carries `_legacy_source_table`, `_migration_batch_id`, and `_migrated_at` for traceability back to the source system, independent of the generic bronze lineage columns.
