
-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_observation table
-- 
-- Dependencies: run after 
--      lk_observation
--      lk_procedure
--      lk_meas_chartevents
--      lk_cond_diagnoses
--      cdm_person.sql
--      cdm_visit_occurrence
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- cdm_observation
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
DROP TABLE IF EXISTS etl_dataset.cdm_observation;
CREATE TABLE etl_dataset.cdm_observation
(
    observation_id                INTEGER     not null ,
    person_id                     INTEGER     not null ,
    observation_concept_id        INTEGER     not null ,
    observation_date              DATE      not null ,
    observation_datetime          TIMESTAMP           ,
    observation_type_concept_id   INTEGER     not null ,
    value_as_number               FLOAT        ,
    value_as_string               TEXT         ,
    value_as_concept_id           INTEGER          ,
    qualifier_concept_id          INTEGER          ,
    unit_concept_id               INTEGER          ,
    provider_id                   INTEGER          ,
    visit_occurrence_id           INTEGER          ,
    visit_detail_id               INTEGER          ,
    observation_source_value      TEXT         ,
    observation_source_concept_id INTEGER          ,
    unit_source_value             TEXT         ,
    qualifier_source_value        TEXT         ,
    -- 
    unit_id                       TEXT,
    load_table_id                 TEXT,
    load_row_id                   INTEGER,
    trace_id                      TEXT
)
;

-- -------------------------------------------------------------------
-- Rules 1-4
-- lk_observation_mapped (demographics and DRG codes)
-- -------------------------------------------------------------------

INSERT INTO etl_dataset.cdm_observation
SELECT
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS observation_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS observation_concept_id,
    CAST(src.start_datetime AS DATE)            AS observation_date,
    src.start_datetime                          AS observation_datetime,
    src.type_concept_id                         AS observation_type_concept_id,
    CAST(NULL AS FLOAT)                       AS value_as_number,
    src.value_as_string                         AS value_as_string,
    CASE
    WHEN src.value_as_string IS NOT NULL THEN COALESCE(src.value_as_concept_id, 0)
    ELSE NULL
    END                                   AS value_as_concept_id,
    CAST(NULL AS INTEGER)                         AS qualifier_concept_id,
    CAST(NULL AS INTEGER)                         AS unit_concept_id,
    CAST(NULL AS INTEGER)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INTEGER)                         AS visit_detail_id,
    src.source_code                             AS observation_source_value,
    src.source_concept_id                       AS observation_source_concept_id,
    CAST(NULL AS TEXT)                        AS unit_source_value,
    CAST(NULL AS TEXT)                        AS qualifier_source_value,
    -- 
    CONCAT('observation.', src.unit_id)         AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    etl_dataset.lk_observation_mapped src
INNER JOIN
    etl_dataset.cdm_person per
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
INNER JOIN
    etl_dataset.cdm_visit_occurrence vis
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS TEXT), '|', CAST(src.hadm_id AS TEXT))
WHERE
    src.target_domain_id = 'Observation'
;

-- -------------------------------------------------------------------
-- Rule 5
-- chartevents
-- -------------------------------------------------------------------

INSERT INTO etl_dataset.cdm_observation
SELECT
    src.measurement_id                          AS observation_id, -- id is generated already
    per.person_id                               AS person_id,
    src.target_concept_id                       AS observation_concept_id,
    CAST(src.start_datetime AS DATE)            AS observation_date,
    src.start_datetime                          AS observation_datetime,
    src.type_concept_id                         AS observation_type_concept_id,
    src.value_as_number                         AS value_as_number,
    src.value_source_value                      AS value_as_string,
    CASE
    WHEN src.value_source_value IS NOT NULL THEN COALESCE(src.value_as_concept_id, 0)
    ELSE NULL
    END                                  AS value_as_concept_id,
    CAST(NULL AS INTEGER)                         AS qualifier_concept_id,
    src.unit_concept_id                         AS unit_concept_id,
    CAST(NULL AS INTEGER)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INTEGER)                         AS visit_detail_id,
    src.source_code                             AS observation_source_value,
    src.source_concept_id                       AS observation_source_concept_id,
    src.unit_source_value                       AS unit_source_value,
    CAST(NULL AS TEXT)                        AS qualifier_source_value,
    -- 
    CONCAT('observation.', src.unit_id)         AS unit_id,
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
    src.target_domain_id = 'Observation'
;

-- -------------------------------------------------------------------
-- Rule 6
-- lk_procedure_mapped
-- -------------------------------------------------------------------

INSERT INTO etl_dataset.cdm_observation
SELECT
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS observation_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS observation_concept_id,
    CAST(src.start_datetime AS DATE)            AS observation_date,
    src.start_datetime                          AS observation_datetime,
    src.type_concept_id                         AS observation_type_concept_id,
    CAST(NULL AS FLOAT)                       AS value_as_number,
    CAST(NULL AS TEXT)                        AS value_as_string,
    CAST(NULL AS INTEGER)                         AS value_as_concept_id,
    CAST(NULL AS INTEGER)                         AS qualifier_concept_id,
    CAST(NULL AS INTEGER)                         AS unit_concept_id,
    CAST(NULL AS INTEGER)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INTEGER)                         AS visit_detail_id,
    src.source_code                             AS observation_source_value,
    src.source_concept_id                       AS observation_source_concept_id,
    CAST(NULL AS TEXT)                        AS unit_source_value,
    CAST(NULL AS TEXT)                        AS qualifier_source_value,
    -- 
    CONCAT('observation.', src.unit_id)         AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    etl_dataset.lk_procedure_mapped src
INNER JOIN
    etl_dataset.cdm_person per
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
INNER JOIN
    etl_dataset.cdm_visit_occurrence vis
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS TEXT), '|', CAST(src.hadm_id AS TEXT))
WHERE
    src.target_domain_id = 'Observation'
;

-- -------------------------------------------------------------------
-- Rule 7
-- diagnoses
-- -------------------------------------------------------------------

INSERT INTO etl_dataset.cdm_observation
SELECT
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS observation_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS observation_concept_id, -- to rename fields in *_mapped
    CAST(src.start_datetime AS DATE)            AS observation_date,
    src.start_datetime                          AS observation_datetime,
    src.type_concept_id                         AS observation_type_concept_id,
    CAST(NULL AS FLOAT)                       AS value_as_number,
    CAST(NULL AS TEXT)                        AS value_as_string,
    CAST(NULL AS INTEGER)                         AS value_as_concept_id,
    CAST(NULL AS INTEGER)                         AS qualifier_concept_id,
    CAST(NULL AS INTEGER)                         AS unit_concept_id,
    CAST(NULL AS INTEGER)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INTEGER)                         AS visit_detail_id,
    src.source_code                             AS observation_source_value,
    src.source_concept_id                       AS observation_source_concept_id,
    CAST(NULL AS TEXT)                        AS unit_source_value,
    CAST(NULL AS TEXT)                        AS qualifier_source_value,
    -- 
    CONCAT('observation.', src.unit_id)         AS unit_id,
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
    src.target_domain_id = 'Observation'
;

-- -------------------------------------------------------------------
-- Rule 8
-- lk_specimen_mapped
-- -------------------------------------------------------------------

INSERT INTO etl_dataset.cdm_observation
SELECT
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS observation_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS observation_concept_id,
    CAST(src.start_datetime AS DATE)            AS observation_date,
    src.start_datetime                          AS observation_datetime,
    src.type_concept_id                         AS observation_type_concept_id,
    CAST(NULL AS FLOAT)                       AS value_as_number,
    CAST(NULL AS TEXT)                        AS value_as_string,
    CAST(NULL AS INTEGER)                         AS value_as_concept_id,
    CAST(NULL AS INTEGER)                         AS qualifier_concept_id,
    CAST(NULL AS INTEGER)                         AS unit_concept_id,
    CAST(NULL AS INTEGER)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INTEGER)                         AS visit_detail_id,
    src.source_code                             AS observation_source_value,
    src.source_concept_id                       AS observation_source_concept_id,
    CAST(NULL AS TEXT)                        AS unit_source_value,
    CAST(NULL AS TEXT)                        AS qualifier_source_value,
    -- 
    CONCAT('observation.', src.unit_id)         AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    etl_dataset.lk_specimen_mapped src
INNER JOIN
    etl_dataset.cdm_person per
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
INNER JOIN
    etl_dataset.cdm_visit_occurrence vis
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS TEXT), '|', 
                COALESCE(CAST(src.hadm_id AS TEXT), CAST(src.date_id AS TEXT)))
WHERE
    src.target_domain_id = 'Observation'
;



