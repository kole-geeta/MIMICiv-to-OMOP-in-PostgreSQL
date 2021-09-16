
-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_drug_exposure table
-- 
-- Dependencies: run after 
--      lk_drug_prescriptions.sql
--      cdm_person.sql,
--      cdm_visit_occurrence.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- cdm_drug_exposure
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
DROP TABLE IF EXISTS etl_dataset.cdm_drug_exposure;
CREATE TABLE etl_dataset.cdm_drug_exposure
(
    drug_exposure_id              INTEGER       not null ,
    person_id                     INTEGER       not null ,
    drug_concept_id               INTEGER       not null ,
    drug_exposure_start_date      DATE        not null ,
    drug_exposure_start_datetime  TIMESTAMP             ,
    drug_exposure_end_date        DATE        not null ,
    drug_exposure_end_datetime    TIMESTAMP             ,
    verbatim_end_date             DATE                 ,
    drug_type_concept_id          INTEGER       not null ,
    stop_reason                   TEXT               ,
    refills                       INTEGER                ,
    quantity                      FLOAT              ,
    days_supply                   INTEGER                ,
    sig                           TEXT               ,
    route_concept_id              INTEGER                ,
    lot_number                    TEXT               ,
    provider_id                   INTEGER                ,
    visit_occurrence_id           INTEGER                ,
    visit_detail_id               INTEGER                ,
    drug_source_value             TEXT               ,
    drug_source_concept_id        INTEGER                ,
    route_source_value            TEXT               ,
    dose_unit_source_value        TEXT               ,
    -- 
    unit_id                       TEXT,
    load_table_id                 TEXT,
    load_row_id                   INTEGER,
    trace_id                      TEXT
)
;

INSERT INTO etl_dataset.cdm_drug_exposure
SELECT
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS drug_exposure_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS drug_concept_id,
    CAST(src.start_datetime AS DATE)            AS drug_exposure_start_date,
    src.start_datetime                          AS drug_exposure_start_datetime,
    CAST(src.end_datetime AS DATE)              AS drug_exposure_end_date,
    src.end_datetime                            AS drug_exposure_end_datetime,
    CAST(NULL AS DATE)                          AS verbatim_end_date,
    src.type_concept_id                         AS drug_type_concept_id,
    CAST(NULL AS TEXT)                        AS stop_reason,
    CAST(NULL AS INTEGER)                         AS refills,
    src.quantity                                AS quantity,
    CAST(NULL AS INTEGER)                         AS days_supply,
    CAST(NULL AS TEXT)                        AS sig,
    src.route_concept_id                        AS route_concept_id,
    CAST(NULL AS TEXT)                        AS lot_number,
    CAST(NULL AS INTEGER)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INTEGER)                         AS visit_detail_id,
    src.source_code                             AS drug_source_value,
    src.source_concept_id                       AS drug_source_concept_id,
    src.route_source_code                       AS route_source_value,
    src.dose_unit_source_code                   AS dose_unit_source_value,
    -- 
    CONCAT('drug.', src.unit_id)    AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    etl_dataset.lk_drug_mapped src
INNER JOIN
    etl_dataset.cdm_person per
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
INNER JOIN
    etl_dataset.cdm_visit_occurrence vis
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS TEXT), '|', CAST(src.hadm_id AS TEXT))
WHERE
    src.target_domain_id = 'Drug'
;

