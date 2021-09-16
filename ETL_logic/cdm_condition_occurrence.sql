
-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_condition_occurrence table
-- 
-- Dependencies: run after 
--      st_core.sql,
--      st_hosp.sql,
--      lk_cond_diagnoses.sql,
--      lk_meas_chartevents.sql,
--      cdm_person.sql,
--      cdm_visit_occurrence.sql
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
--
-- -------------------------------------------------------------------

-- 4,520 rows on demo

-- -------------------------------------------------------------------
-- cdm_condition_occurrence
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
DROP TABLE IF EXISTS etl_dataset.cdm_condition_occurrence;
CREATE TABLE etl_dataset.cdm_condition_occurrence
(
    condition_occurrence_id       INTEGER     not null ,
    person_id                     INTEGER     not null ,
    condition_concept_id          INTEGER     not null ,
    condition_start_date          DATE      not null ,
    condition_start_datetime      TIMESTAMP           ,
    condition_end_date            DATE               ,
    condition_end_datetime        TIMESTAMP           ,
    condition_type_concept_id     INTEGER     not null ,
    stop_reason                   TEXT             ,
    provider_id                   INTEGER              ,
    visit_occurrence_id           INTEGER              ,
    visit_detail_id               INTEGER              ,
    condition_source_value        TEXT             ,
    condition_source_concept_id   INTEGER              ,
    condition_status_source_value TEXT             ,
    condition_status_concept_id   INTEGER              ,
    -- 
    unit_id                       TEXT,
    load_table_id                 TEXT,
    load_row_id                   INTEGER,
    trace_id                      TEXT
)
;

-- -------------------------------------------------------------------
-- Rule 1
-- diagnoses
-- -------------------------------------------------------------------

INSERT INTO etl_dataset.cdm_condition_occurrence
SELECT
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int       AS condition_occurrence_id,
    per.person_id                           AS person_id,
    COALESCE(src.target_concept_id, 0)      AS condition_concept_id,
    CAST(src.start_datetime AS DATE)        AS condition_start_date,
    src.start_datetime                      AS condition_start_datetime,
    CAST(src.end_datetime AS DATE)          AS condition_end_date,
    src.end_datetime                        AS condition_end_datetime,
    src.type_concept_id                     AS condition_type_concept_id,
    CAST(NULL AS TEXT)                    AS stop_reason,
    CAST(NULL AS INTEGER)                     AS provider_id,
    vis.visit_occurrence_id                 AS visit_occurrence_id,
    CAST(NULL AS INTEGER)                     AS visit_detail_id,
    src.source_code                         AS condition_source_value,
    COALESCE(src.source_concept_id, 0)      AS condition_source_concept_id,
    CAST(NULL AS TEXT)                    AS condition_status_source_value,
    CAST(NULL AS INTEGER)                     AS condition_status_concept_id,
    --
    CONCAT('condition.', src.unit_id) AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    etl_dataset.lk_diagnoses_icd_mapped src
INNER JOIN
    etl_dataset.cdm_person per
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
INNER JOIN
    etl_dataset.cdm_visit_occurrence vis
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS TEXT), '|', CAST(src.hadm_id AS TEXT))
WHERE
    src.target_domain_id = 'Condition'
;

-- -------------------------------------------------------------------
-- rule 2
-- Chartevents.value
-- -------------------------------------------------------------------

INSERT INTO etl_dataset.cdm_condition_occurrence
SELECT
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int       AS condition_occurrence_id,
    per.person_id                           AS person_id,
    COALESCE(src.target_concept_id, 0)      AS condition_concept_id,
    CAST(src.start_datetime AS DATE)        AS condition_start_date,
    src.start_datetime                      AS condition_start_datetime,
    CAST(src.start_datetime AS DATE)        AS condition_end_date,
    src.start_datetime                      AS condition_end_datetime,
    32817                                   AS condition_type_concept_id, -- EHR  Type Concept    Type Concept
    CAST(NULL AS TEXT)                    AS stop_reason,
    CAST(NULL AS INTEGER)                     AS provider_id,
    vis.visit_occurrence_id                 AS visit_occurrence_id,
    CAST(NULL AS INTEGER)                     AS visit_detail_id,
    src.source_code                         AS condition_source_value,
    COALESCE(src.source_concept_id, 0)      AS condition_source_concept_id,
    CAST(NULL AS TEXT)                    AS condition_status_source_value,
    CAST(NULL AS INTEGER)                     AS condition_status_concept_id,
    --
    CONCAT('condition.', src.unit_id) AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    etl_dataset.lk_chartevents_condition_mapped src
INNER JOIN
    etl_dataset.cdm_person per
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
INNER JOIN
    etl_dataset.cdm_visit_occurrence vis
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS TEXT), '|', CAST(src.hadm_id AS TEXT))
WHERE
    src.target_domain_id = 'Condition'
;



-- -------------------------------------------------------------------
-- rule 3
-- Chartevents
-- -------------------------------------------------------------------

INSERT INTO etl_dataset.cdm_condition_occurrence
SELECT
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int       AS condition_occurrence_id,
    per.person_id                           AS person_id,
    COALESCE(src.target_concept_id, 0)      AS condition_concept_id,
    CAST(src.start_datetime AS DATE)        AS condition_start_date,
    src.start_datetime                      AS condition_start_datetime,
    CAST(src.start_datetime AS DATE)        AS condition_end_date,
    src.start_datetime                      AS condition_end_datetime,
    src.type_concept_id                     AS condition_type_concept_id,
    CAST(NULL AS TEXT)                    AS stop_reason,
    CAST(NULL AS INTEGER)                     AS provider_id,
    vis.visit_occurrence_id                 AS visit_occurrence_id,
    CAST(NULL AS INTEGER)                     AS visit_detail_id,
    src.source_code                         AS condition_source_value,
    COALESCE(src.source_concept_id, 0)      AS condition_source_concept_id,
    CAST(NULL AS TEXT)                    AS condition_status_source_value,
    CAST(NULL AS INTEGER)                     AS condition_status_concept_id,
    --
    CONCAT('condition.', src.unit_id) AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    etl_dataset.lk_chartevents_mapped src
INNER JOIN
    etl_dataset.cdm_person per
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
INNER JOIN
    etl_dataset.cdm_visit_occurrence vis
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS TEXT), '|', CAST(src.hadm_id AS TEXT))
WHERE
    src.target_domain_id = 'Condition'
;



