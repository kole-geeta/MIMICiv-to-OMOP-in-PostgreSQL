
-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_observation_period table
-- 
-- Dependencies: run after 
--      cdm_visit_occurrence
--      all event tables
--      cdm_death
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
-- -------------------------------------------------------------------


-- -------------------------------------------------------------------
-- tmp_observation_period_clean
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS etl_dataset.tmp_observation_period_clean;
CREATE TABLE etl_dataset.tmp_observation_period_clean AS
SELECT
    src.person_id               AS person_id,
    MIN(src.visit_start_date)   AS start_date,
    MAX(src.visit_end_date)     AS end_date,
    src.unit_id                 AS unit_id
FROM
    etl_dataset.cdm_visit_occurrence src
GROUP BY
    src.person_id, src.unit_id
;

INSERT INTO etl_dataset.tmp_observation_period_clean
SELECT
    src.person_id               AS person_id,
    MIN(src.condition_start_date)   AS start_date,
    MAX(src.condition_end_date)     AS end_date,
    src.unit_id                 AS unit_id
FROM
    etl_dataset.cdm_condition_occurrence src
GROUP BY
    src.person_id, src.unit_id
;

INSERT INTO etl_dataset.tmp_observation_period_clean
SELECT
    src.person_id               AS person_id,
    MIN(src.procedure_date)   AS start_date,
    MAX(src.procedure_date)     AS end_date,
    src.unit_id                 AS unit_id
FROM
    etl_dataset.cdm_procedure_occurrence src
GROUP BY
    src.person_id, src.unit_id
;

INSERT INTO etl_dataset.tmp_observation_period_clean
SELECT
    src.person_id               AS person_id,
    MIN(src.drug_exposure_start_date)   AS start_date,
    MAX(src.drug_exposure_end_date)     AS end_date,
    src.unit_id                 AS unit_id
FROM
    etl_dataset.cdm_drug_exposure src
GROUP BY
    src.person_id, src.unit_id
;

INSERT INTO etl_dataset.tmp_observation_period_clean
SELECT
    src.person_id               AS person_id,
    MIN(src.device_exposure_start_date)   AS start_date,
    MAX(src.device_exposure_end_date)     AS end_date,
    src.unit_id                 AS unit_id
FROM
    etl_dataset.cdm_device_exposure src
GROUP BY
    src.person_id, src.unit_id
;

INSERT INTO etl_dataset.tmp_observation_period_clean
SELECT
    src.person_id               AS person_id,
    MIN(src.measurement_date)   AS start_date,
    MAX(src.measurement_date)     AS end_date,
    src.unit_id                 AS unit_id
FROM
    etl_dataset.cdm_measurement src
GROUP BY
    src.person_id, src.unit_id
;

INSERT INTO etl_dataset.tmp_observation_period_clean
SELECT
    src.person_id               AS person_id,
    MIN(src.specimen_date)   AS start_date,
    MAX(src.specimen_date)     AS end_date,
    src.unit_id                 AS unit_id
FROM
    etl_dataset.cdm_specimen src
GROUP BY
    src.person_id, src.unit_id
;

INSERT INTO etl_dataset.tmp_observation_period_clean
SELECT
    src.person_id               AS person_id,
    MIN(src.observation_date)   AS start_date,
    MAX(src.observation_date)     AS end_date,
    src.unit_id                 AS unit_id
FROM
    etl_dataset.cdm_observation src
GROUP BY
    src.person_id, src.unit_id
;

INSERT INTO etl_dataset.tmp_observation_period_clean
SELECT
    src.person_id               AS person_id,
    MIN(src.death_date)         AS start_date,
    MAX(src.death_date)         AS end_date,
    src.unit_id                 AS unit_id
FROM
    etl_dataset.cdm_death src
GROUP BY
    src.person_id, src.unit_id
;


-- -------------------------------------------------------------------
-- tmp_observation_period
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS etl_dataset.tmp_observation_period;
CREATE TABLE etl_dataset.tmp_observation_period AS
SELECT
    src.person_id               AS person_id,
    MIN(src.start_date)   AS start_date,
    MAX(src.end_date)     AS end_date,
    src.unit_id                 AS unit_id
FROM
    etl_dataset.tmp_observation_period_clean src
GROUP BY
    src.person_id, src.unit_id
;

-- -------------------------------------------------------------------
-- cdm_observation_period
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
DROP TABLE IF EXISTS etl_dataset.cdm_observation_period;
CREATE TABLE etl_dataset.cdm_observation_period
(
    observation_period_id             INTEGER   not null ,
    person_id                         INTEGER   not null ,
    observation_period_start_date     DATE    not null ,
    observation_period_end_date       DATE    not null ,
    period_type_concept_id            INTEGER   not null ,
    -- 
    unit_id                       TEXT,
    load_table_id                 TEXT,
    load_row_id                   INTEGER,
    trace_id                      TEXT
)
;

INSERT INTO etl_dataset.cdm_observation_period
SELECT
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int          AS observation_period_id,
    src.person_id                               AS person_id,
    MIN(src.start_date)                         AS observation_period_start_date,
    MAX(src.end_date)                           AS observation_period_end_date,
    32828                                       AS period_type_concept_id,  -- 32828    OMOP4976901 EHR episode record
    --
    'observation_period'                        AS unit_id,
    'event tables'                              AS load_table_id,
    0                                           AS load_row_id,
    CAST(NULL AS TEXT)                        AS trace_id
FROM 
    etl_dataset.tmp_observation_period src
GROUP BY
    src.person_id
;

-- -------------------------------------------------------------------
-- cleanup
-- -------------------------------------------------------------------

-- DROP TABLE IF EXISTS etl_dataset.tmp_observation_period_clean;
-- DROP TABLE IF EXISTS etl_dataset.tmp_observation_period;



