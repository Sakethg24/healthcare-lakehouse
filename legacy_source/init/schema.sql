-- =============================================================================
-- Meridian Health -- Legacy Claims DB (legacy_claims_db)
-- =============================================================================
-- This is the simulated legacy OLTP system the migration project extracts
-- from. It is loaded from Synthea's "Run 1" CSV output (~13,000 patients,
-- ~11GB, deliberately dirtied by inject_messiness.py before loading -- see
-- data_generation/inject_messiness.py and docs/DATASETS.md).
--
-- Design choices worth calling out (see migration/schema_mapping.md for the
-- full legacy -> target crosswalk):
--
--   1. Six tables carry deliberate "legacy" quirks so mapping them to the
--      target schema is real work, not a rename: patient_master (free-text
--      ADDR, unmasked SSN), encountr (truncated name, bare enc_type_cd/proc
--      code with no lookup table), dx_condition (denormalized patient_name),
--      rx_med (dates stored as text), claim_hdr/claim_line (header/line
--      split, $-prefixed text amount).
--
--   2. NO foreign key constraints are enforced anywhere in this schema.
--      This is intentional, not an oversight, for two reasons: (a) real
--      legacy OLTP systems accumulate referential integrity gaps over time,
--      and (b) inject_messiness.py deliberately writes orphaned FK values
--      (e.g. "MISSING-123" into encounters.PATIENT and claims.PROVIDERID)
--      so the silver-layer data-quality/quarantine step has real bad rows
--      to catch. Enforcing FKs here would make the dirty CSVs fail to load,
--      defeating the point. Indexes are still added on join columns for
--      query performance.
--
--   3. All identifier/FK-like columns are TEXT, not a UUID type, so that
--      injected malformed values (orphaned FKs, etc.) load without error.
--
--   4. provider_master holds the legacy system's OWN sparse internal
--      provider directory (from Synthea's providers.csv) -- this is NOT
--      the real NPPES registry. It's deliberately thin, which is exactly
--      why the silver-layer Synthea-provider <-> NPPES-NPI crosswalk table
--      has real work to do (see docs/DATA_MODEL.md).
--
--   5. Identifiers are lowercase snake_case Postgres convention rather than
--      quoted ALL_CAPS. The "legacy" flavor here is in table/column naming
--      and structure (truncated names, denormalization, bare codes, text
--      dates), not in SQL case-sensitivity mechanics -- quoting every
--      identifier in every downstream query (load scripts, JDBC, Spark)
--      would be friction without adding realism.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Reference / dimension-ish tables (loaded first; nothing here enforces FKs
-- against them, but load order mirrors real dependency for sanity)
-- -----------------------------------------------------------------------------

CREATE TABLE org_master (
    org_id          TEXT PRIMARY KEY,
    name            TEXT,
    address         TEXT,
    city            TEXT,
    state           TEXT,
    zip             TEXT,
    lat             NUMERIC,
    lon             NUMERIC,
    phone           TEXT,
    revenue         NUMERIC,
    utilization     NUMERIC
);

-- The legacy system's OWN internal provider directory -- sparse and
-- disconnected from the real NPPES registry on purpose. See note 4 above.
CREATE TABLE provider_master (
    provider_id     TEXT PRIMARY KEY,
    org_id          TEXT,
    name            TEXT,
    gender          TEXT,
    specialty       TEXT,
    address         TEXT,
    city            TEXT,
    state           TEXT,
    zip             TEXT,
    lat             NUMERIC,
    lon             NUMERIC,
    num_encounters  INTEGER,
    num_procedures  INTEGER
);

CREATE TABLE payer_master (
    payer_id                    TEXT PRIMARY KEY,
    name                        TEXT,
    address                     TEXT,
    city                        TEXT,
    state_headquartered         TEXT,
    zip                         TEXT,
    phone                       TEXT,
    amount_covered              NUMERIC,
    amount_uncovered            NUMERIC,
    revenue                     NUMERIC,
    covered_encounters          INTEGER,
    uncovered_encounters        INTEGER,
    covered_medications         INTEGER,
    uncovered_medications       INTEGER,
    covered_procedures          INTEGER,
    uncovered_procedures        INTEGER,
    covered_immunizations       INTEGER,
    uncovered_immunizations     INTEGER,
    unique_customers            INTEGER,
    qols_avg                    NUMERIC,
    member_months                INTEGER
);

-- -----------------------------------------------------------------------------
-- PATIENT_MASTER -- legacy quirk: single free-text ADDR column instead of
-- separate address/city/state/zip (parsed back into structured fields in
-- silver); SSN kept unmasked here (masked at the silver boundary, unmasked
-- only via the `auditor` Unity Catalog role downstream).
-- -----------------------------------------------------------------------------

CREATE TABLE patient_master (
    patient_id                 TEXT PRIMARY KEY,
    ssn                        TEXT,
    drivers                    TEXT,
    passport                   TEXT,
    prefix                     TEXT,
    first_name                 TEXT,
    middle_name                TEXT,
    last_name                  TEXT,
    suffix                     TEXT,
    maiden_name                TEXT,
    marital_status              TEXT,
    race                       TEXT,
    ethnicity                  TEXT,
    gender                     TEXT,
    birthplace                 TEXT,
    addr                       TEXT,   -- free-text: address + city + state + zip, deliberately unstructured
    fips                       TEXT,
    lat                        NUMERIC,
    lon                        NUMERIC,
    birthdate                  DATE,
    deathdate                  DATE,
    healthcare_expenses        NUMERIC,
    healthcare_coverage        NUMERIC,
    income                     NUMERIC
);

CREATE INDEX idx_patient_master_last_name ON patient_master (last_name);

-- -----------------------------------------------------------------------------
-- ENCOUNTR -- legacy quirk: truncated table name; enc_type_cd and proc_code
-- are bare codes with no in-database lookup/description (silver joins them
-- against a maintained ref_encounter_type dimension).
-- -----------------------------------------------------------------------------

CREATE TABLE encountr (
    encounter_id        TEXT PRIMARY KEY,
    patient_id           TEXT,   -- no FK enforced; injected orphan values expected (see note 2)
    org_id               TEXT,
    provider_id           TEXT,
    payer_id              TEXT,
    enc_type_cd           TEXT,   -- bare code, was ENCOUNTERCLASS in source, no lookup table
    proc_code             TEXT,   -- bare code, was CODE in source, no lookup table
    reason_cd             TEXT,   -- bare code, was REASONCODE in source, no lookup table
    start_ts              TIMESTAMP,
    stop_ts               TIMESTAMP,
    base_encounter_cost   NUMERIC,
    total_claim_cost       NUMERIC,
    payer_coverage         NUMERIC
);

CREATE INDEX idx_encountr_patient_id ON encountr (patient_id);
CREATE INDEX idx_encountr_provider_id ON encountr (provider_id);

-- -----------------------------------------------------------------------------
-- DX_CONDITION -- legacy quirk: denormalized patient_name copied onto every
-- row (a real data-quality smell, preserved deliberately and cleaned/dropped
-- in silver, used as a profiling example).
-- -----------------------------------------------------------------------------

CREATE TABLE dx_condition (
    dx_id           SERIAL PRIMARY KEY,
    patient_id       TEXT,
    patient_name     TEXT,   -- denormalized: FIRST || ' ' || LAST at load time, on purpose
    encounter_id     TEXT,
    dx_system        TEXT,
    dx_code          TEXT,
    dx_description   TEXT,
    onset_date       DATE,
    resolved_date    DATE
);

CREATE INDEX idx_dx_condition_patient_id ON dx_condition (patient_id);
CREATE INDEX idx_dx_condition_encounter_id ON dx_condition (encounter_id);

-- -----------------------------------------------------------------------------
-- RX_MED -- legacy quirk: dates stored as free-text VARCHAR in MM/DD/YYYY
-- format instead of a native date type (explicit parsing step required in
-- silver; a documented example of "why bronze stays raw").
-- -----------------------------------------------------------------------------

CREATE TABLE rx_med (
    rx_id                SERIAL PRIMARY KEY,
    patient_id            TEXT,
    payer_id               TEXT,
    encounter_id           TEXT,
    med_code               TEXT,
    med_description        TEXT,
    start_dt               VARCHAR(10),  -- 'MM/DD/YYYY' text, not a DATE column -- deliberate quirk
    stop_dt                VARCHAR(10),  -- 'MM/DD/YYYY' text, not a DATE column -- deliberate quirk
    base_cost               NUMERIC,
    payer_coverage           NUMERIC,
    dispenses               INTEGER,
    total_cost               NUMERIC,
    reason_cd                TEXT,
    reason_description        TEXT
);

CREATE INDEX idx_rx_med_patient_id ON rx_med (patient_id);

-- -----------------------------------------------------------------------------
-- Remaining OLTP-core tables -- part of the Postgres-bound Run 1 CSV set,
-- kept structurally close to Synthea's own schema (no specific legacy quirk
-- was assigned to these, beyond the no-FK / TEXT-id conventions above).
-- -----------------------------------------------------------------------------

CREATE TABLE procedur (
    proc_id              SERIAL PRIMARY KEY,
    patient_id            TEXT,
    encounter_id           TEXT,
    proc_system            TEXT,
    proc_code               TEXT,
    proc_description         TEXT,
    proc_date                TIMESTAMP,
    base_cost                 NUMERIC,
    reason_cd                  TEXT,
    reason_description          TEXT
);

CREATE INDEX idx_procedur_patient_id ON procedur (patient_id);

CREATE TABLE immunizatn (
    immun_id         SERIAL PRIMARY KEY,
    patient_id         TEXT,
    encounter_id        TEXT,
    immun_date           DATE,
    immun_code            TEXT,
    immun_description      TEXT,
    base_cost               NUMERIC
);

CREATE INDEX idx_immunizatn_patient_id ON immunizatn (patient_id);

CREATE TABLE careplan (
    careplan_id       TEXT PRIMARY KEY,
    patient_id          TEXT,
    encounter_id         TEXT,
    start_date            DATE,
    stop_date              DATE,
    code                    TEXT,
    description              TEXT,
    reason_cd                 TEXT,
    reason_description          TEXT
);

CREATE INDEX idx_careplan_patient_id ON careplan (patient_id);

CREATE TABLE allergy (
    allergy_id          SERIAL PRIMARY KEY,
    patient_id            TEXT,
    encounter_id           TEXT,
    allergy_system          TEXT,
    allergy_code             TEXT,
    allergy_description       TEXT,
    allergy_type                TEXT,
    category                     TEXT,
    start_date                    DATE,
    stop_date                      DATE,
    reaction1_code                  TEXT,
    reaction1_description            TEXT,
    severity1                         TEXT,
    reaction2_code                     TEXT,
    reaction2_description               TEXT,
    severity2                            TEXT
);

CREATE INDEX idx_allergy_patient_id ON allergy (patient_id);

CREATE TABLE device (
    device_id        SERIAL PRIMARY KEY,
    patient_id         TEXT,
    encounter_id        TEXT,
    device_code           TEXT,
    device_description      TEXT,
    udi                       TEXT,
    start_date                 TIMESTAMP,
    stop_date                    TIMESTAMP
);

CREATE INDEX idx_device_patient_id ON device (patient_id);

-- imaging_studies rows are the ones that later get randomly linked to a real
-- NIH ChestX-ray14 image in silver (see docs/DATA_MODEL.md, decision #1).
--
-- Note: study_id is NOT unique per row -- Synthea's imaging_studies.csv has
-- one row per series/instance within a study, so a single study_id can
-- legitimately repeat across several rows. This isn't injected messiness,
-- it's a genuine structural fact about the source file, discovered when the
-- original TEXT PRIMARY KEY on study_id failed to load real data. Fixed
-- with a surrogate img_id and a plain (non-unique) index on study_id.
CREATE TABLE img_study (
    img_id                SERIAL PRIMARY KEY,
    study_id              TEXT NOT NULL,
    patient_id              TEXT,
    encounter_id              TEXT,
    study_date                  TIMESTAMP,
    series_uid                    TEXT,
    bodysite_code                   TEXT,
    bodysite_description              TEXT,
    modality_code                       TEXT,
    modality_description                  TEXT,
    instance_uid                            TEXT,
    sop_code                                  TEXT,
    sop_description                             TEXT,
    procedure_code                                TEXT
);

CREATE INDEX idx_img_study_patient_id ON img_study (patient_id);
CREATE INDEX idx_img_study_study_id ON img_study (study_id);

CREATE TABLE observatn (
    obs_id          SERIAL PRIMARY KEY,
    patient_id        TEXT,
    encounter_id       TEXT,
    obs_date             TIMESTAMP,
    category               TEXT,
    obs_code                TEXT,
    obs_description           TEXT,
    obs_value                  TEXT,
    units                        TEXT,
    value_type                     TEXT
);

CREATE INDEX idx_observatn_patient_id ON observatn (patient_id);

CREATE TABLE payer_xfer (
    transition_id    SERIAL PRIMARY KEY,
    patient_id         TEXT,
    start_year           INTEGER,
    end_year               INTEGER,
    payer_id                 TEXT,
    ownership                  TEXT
);

CREATE INDEX idx_payer_xfer_patient_id ON payer_xfer (patient_id);

CREATE TABLE supply (
    supply_id        SERIAL PRIMARY KEY,
    supply_date        DATE,
    patient_id           TEXT,
    encounter_id           TEXT,
    supply_code               TEXT,
    supply_description          TEXT,
    quantity                      INTEGER
);

CREATE INDEX idx_supply_patient_id ON supply (patient_id);

-- -----------------------------------------------------------------------------
-- CLAIM_HDR / CLAIM_LINE -- legacy quirk: header/line split (rather than one
-- flat claims table), and claim_line.amount stored as TEXT with a literal
-- '$' prefix (a legacy export quirk; currency parsing happens in silver).
-- -----------------------------------------------------------------------------

CREATE TABLE claim_hdr (
    claim_id                    TEXT PRIMARY KEY,
    patient_id                    TEXT,
    provider_id                     TEXT,   -- no FK enforced; injected orphan values expected (see note 2)
    primary_payer_id                  TEXT,
    secondary_payer_id                  TEXT,
    department_id                         TEXT,
    referring_provider_id                   TEXT,
    supervising_provider_id                   TEXT,
    appointment_id                              TEXT,
    current_illness_date                          DATE,
    service_date                                    DATE,
    status1                                           TEXT,
    status2                                             TEXT,
    outstanding1                                          NUMERIC,
    outstanding2                                            NUMERIC,
    last_billed_date1                                         DATE,
    last_billed_date2                                           DATE
);

CREATE INDEX idx_claim_hdr_patient_id ON claim_hdr (patient_id);
CREATE INDEX idx_claim_hdr_provider_id ON claim_hdr (provider_id);

-- Note: claims_transactions.csv has no unique per-row "Id" column at all
-- (unlike claims.csv) -- CLAIMID/CHARGEID/PATIENTID identify the claim,
-- charge, and patient, but not the row itself. Discovered when the
-- original TEXT PRIMARY KEY on line_id (assumed sourced from an "Id"
-- column) came back NULL for every row. Fixed with a surrogate txn_id.
CREATE TABLE claim_line (
    txn_id                  SERIAL PRIMARY KEY,
    claim_id                 TEXT,
    charge_id                  TEXT,
    patient_id                   TEXT,
    txn_type                       TEXT,
    amount                           TEXT,   -- '$123.45' -- deliberate legacy text-with-symbol quirk
    method                             TEXT,
    from_date                           DATE,
    to_date                               DATE,
    place_of_service                       TEXT,
    procedure_code                           TEXT,
    units                                      INTEGER,
    department_id                                TEXT,
    payments                                       NUMERIC,
    adjustments                                      NUMERIC,
    transfers                                          NUMERIC,
    outstanding                                          NUMERIC,
    provider_id                                            TEXT
);

CREATE INDEX idx_claim_line_claim_id ON claim_line (claim_id);
CREATE INDEX idx_claim_line_patient_id ON claim_line (patient_id);

-- =============================================================================
-- End of schema. Applied automatically on first container start via
-- legacy_source/docker-compose.yml mounting this file into
-- /docker-entrypoint-initdb.d/ on the Postgres image.
-- =============================================================================
