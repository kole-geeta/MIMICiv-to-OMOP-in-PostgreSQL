
-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_measurement table
-- 
-- Dependencies: run after 
--      cdm_person.sql,
--      cdm_visit_occurrence,
--      cdm_visit_detail,
--          lk_meas_labevents.sql,
--          lk_meas_chartevents,
--          lk_meas_specimen,
--          lk_meas_waveform.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
--
-- src_labevents: look closer to fields priority and specimen_id
-- src_labevents.value: 
--      investigate if there are formatted values with thousand separators,
--      and if we need to use more complicated parsing.
-- -------------------------------------------------------------------



--HINT DISTRIBUTE_ON_KEY(person_id)
DROP TABLE IF EXISTS etl_dataset.cdm_measurement;
CREATE TABLE etl_dataset.cdm_measurement
(
    measurement_id                INTEGER     not null ,
    person_id                     INTEGER     not null ,
    measurement_concept_id        INTEGER     not null ,
    measurement_date              DATE      not null ,
    measurement_datetime          TIMESTAMP           ,
    measurement_time              TEXT             ,
    measurement_type_concept_id   INTEGER     not null ,
    operator_concept_id           INTEGER              ,
    value_as_number               FLOAT            ,
    value_as_concept_id           INTEGER              ,
    unit_concept_id               INTEGER              ,
    range_low                     FLOAT            ,
    range_high                    FLOAT            ,
    provider_id                   INTEGER              ,
    visit_occurrence_id           INTEGER              ,
    visit_detail_id               INTEGER              ,
    measurement_source_value      TEXT             ,
    measurement_source_concept_id INTEGER              ,
    unit_source_value             TEXT             ,
    value_source_value            TEXT             ,
    -- 
    unit_id                       TEXT,
    load_table_id                 TEXT,
    load_row_id                   INTEGER,
    trace_id                      TEXT  
)
;

-- -------------------------------------------------------------------
-- Rule 1
-- LABS from labevents
-- demo:  115,272 rows from mapped 107,209 rows. Remove duplicates
-- -------------------------------------------------------------------

INSERT INTO etl_dataset.cdm_measurement
SELECT
    src.measurement_id                      AS measurement_id,
    per.person_id                           AS person_id,
    COALESCE(src.target_concept_id, 0)      AS measurement_concept_id,
    CAST(src.start_datetime AS DATE)        AS measurement_date,
    src.start_datetime                      AS measurement_datetime,
    CAST(NULL AS TEXT)                    AS measurement_time,
    32856                                   AS measurement_type_concept_id, -- OMOP4976929 Lab
    src.operator_concept_id                 AS operator_concept_id,
    CAST(src.value_as_number AS FLOAT)    AS value_as_number,  -- to move CAST to mapped/clean
    CAST(NULL AS INTEGER)                     AS value_as_concept_id,
    src.unit_concept_id                     AS unit_concept_id,
    src.range_low                           AS range_low,
    src.range_high                          AS range_high,
    CAST(NULL AS INTEGER)                     AS provider_id,
    vis.visit_occurrence_id                 AS visit_occurrence_id,
    CAST(NULL AS INTEGER)                     AS visit_detail_id,
    src.source_code                         AS measurement_source_value,
    src.source_concept_id                   AS measurement_source_concept_id,
    src.unit_source_value                   AS unit_source_value,
    src.value_source_value                  AS value_source_value,
    --
    CONCAT('measurement.', src.unit_id)     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM  
    etl_dataset.lk_meas_labevents_mapped src -- 107,209 
INNER JOIN
    etl_dataset.cdm_person per -- 110,849
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
INNER JOIN
    etl_dataset.cdm_visit_occurrence vis -- 116,559
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS TEXT), '|', 
                COALESCE(CAST(src.hadm_id AS TEXT), CAST(src.date_id AS TEXT)))
WHERE
    src.target_domain_id = 'Measurement' -- 115,272
;

-- -------------------------------------------------------------------
-- Rule 2
-- chartevents
-- -------------------------------------------------------------------

INSERT INTO etl_dataset.cdm_measurement
SELECT
    src.measurement_id                      AS measurement_id,
    per.person_id                           AS person_id,
    COALESCE(src.target_concept_id, 0)      AS measurement_concept_id,
    CAST(src.start_datetime AS DATE)        AS measurement_date,
    src.start_datetime                      AS measurement_datetime,
    CAST(NULL AS TEXT)                    AS measurement_time,
    src.type_concept_id                     AS measurement_type_concept_id,
    CAST(NULL AS INTEGER)                     AS operator_concept_id,
    src.value_as_number                     AS value_as_number,
    src.value_as_concept_id                 AS value_as_concept_id,
    src.unit_concept_id                     AS unit_concept_id,
    CAST(NULL AS INTEGER)                     AS range_low,
    CAST(NULL AS INTEGER)                     AS range_high,
    CAST(NULL AS INTEGER)                     AS provider_id,
    vis.visit_occurrence_id                 AS visit_occurrence_id,
    CAST(NULL AS INTEGER)                     AS visit_detail_id,
    src.source_code                         AS measurement_source_value,
    src.source_concept_id                   AS measurement_source_concept_id,
    src.unit_source_value                   AS unit_source_value,
    src.value_source_value                  AS value_source_value,
    --
    CONCAT('measurement.', src.unit_id)     AS unit_id,
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
    src.target_domain_id = 'Measurement'
;

-- -------------------------------------------------------------------
-- Rule 3.1
-- Microbiology - organism
-- -------------------------------------------------------------------

INSERT INTO etl_dataset.cdm_measurement
SELECT
    src.measurement_id                      AS measurement_id,
    per.person_id                           AS person_id,
    COALESCE(src.target_concept_id, 0)      AS measurement_concept_id,
    CAST(src.start_datetime AS DATE)        AS measurement_date,
    src.start_datetime                      AS measurement_datetime,
    CAST(NULL AS TEXT)                    AS measurement_time,
    src.type_concept_id                     AS measurement_type_concept_id,
    CAST(NULL AS INTEGER)                     AS operator_concept_id,
    CAST(NULL AS FLOAT)                   AS value_as_number,
    COALESCE(src.value_as_concept_id, 0)    AS value_as_concept_id,
    CAST(NULL AS INTEGER)                     AS unit_concept_id,
    CAST(NULL AS INTEGER)                     AS range_low,
    CAST(NULL AS INTEGER)                     AS range_high,
    CAST(NULL AS INTEGER)                     AS provider_id,
    vis.visit_occurrence_id                 AS visit_occurrence_id,
    CAST(NULL AS INTEGER)                     AS visit_detail_id,
    src.source_code                         AS measurement_source_value,
    src.source_concept_id                   AS measurement_source_concept_id,
    CAST(NULL AS TEXT)                    AS unit_source_value,
    src.value_source_value                  AS value_source_value,
    --
    CONCAT('measurement.', src.unit_id)     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM  
    etl_dataset.lk_meas_organism_mapped src
INNER JOIN
    etl_dataset.cdm_person per
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
INNER JOIN
    etl_dataset.cdm_visit_occurrence vis -- 116,559
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS TEXT), '|', 
                COALESCE(CAST(src.hadm_id AS TEXT), CAST(src.date_id AS TEXT)))
WHERE
    src.target_domain_id = 'Measurement'
;

-- -------------------------------------------------------------------
-- Rule 3.2
-- Microbiology - antibiotics
-- -------------------------------------------------------------------

INSERT INTO etl_dataset.cdm_measurement
SELECT
    src.measurement_id                      AS measurement_id,
    per.person_id                           AS person_id,
    COALESCE(src.target_concept_id, 0)      AS measurement_concept_id,
    CAST(src.start_datetime AS DATE)        AS measurement_date,
    src.start_datetime                      AS measurement_datetime,
    CAST(NULL AS TEXT)                    AS measurement_time,
    src.type_concept_id                     AS measurement_type_concept_id,
    src.operator_concept_id                 AS operator_concept_id, -- dilution comparison
    src.value_as_number                     AS value_as_number, -- dilution value
    COALESCE(src.value_as_concept_id, 0)    AS value_as_concept_id, -- resistance (interpretation)
    CAST(NULL AS INTEGER)                     AS unit_concept_id,
    CAST(NULL AS INTEGER)                     AS range_low,
    CAST(NULL AS INTEGER)                     AS range_high,
    CAST(NULL AS INTEGER)                     AS provider_id,
    vis.visit_occurrence_id                 AS visit_occurrence_id,
    CAST(NULL AS INTEGER)                     AS visit_detail_id,
    src.source_code                         AS measurement_source_value, -- antibiotic name
    src.source_concept_id                   AS measurement_source_concept_id,
    CAST(NULL AS TEXT)                    AS unit_source_value,
    src.value_source_value                  AS value_source_value, -- resistance source value
    --
    CONCAT('measurement.', src.unit_id)     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM  
    etl_dataset.lk_meas_ab_mapped src
INNER JOIN
    etl_dataset.cdm_person per
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
INNER JOIN
    etl_dataset.cdm_visit_occurrence vis -- 116,559
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS TEXT), '|', 
                COALESCE(CAST(src.hadm_id AS TEXT), CAST(src.date_id AS TEXT)))
WHERE
    src.target_domain_id = 'Measurement'
;

-- -------------------------------------------------------------------
-- cdm_measurement
-- Rule 10 (waveform)
-- wf demo poc: 1,500 rows from 1,500 rows in mapped
-- -------------------------------------------------------------------

INSERT INTO etl_dataset.cdm_measurement
SELECT
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int       AS measurement_id,
    per.person_id                           AS person_id,
    COALESCE(src.target_concept_id, 0)      AS measurement_concept_id,
    CAST(src.start_datetime AS DATE)        AS measurement_date,
    src.start_datetime                      AS measurement_datetime,
    CAST(NULL AS TEXT)                    AS measurement_time, -- deprecated, to be removed in later versions
    32817                                   AS measurement_type_concept_id, -- OMOP4976890 EHR
    CAST(NULL AS INTEGER)                     AS operator_concept_id,
    src.value_as_number                     AS value_as_number,
    CAST(NULL AS INTEGER)                     AS value_as_concept_id, -- to add values
    src.unit_concept_id                     AS unit_concept_id,
    CAST(NULL AS FLOAT)                   AS range_low,
    CAST(NULL AS FLOAT)                   AS range_high,
    CAST(NULL AS INTEGER)                     AS provider_id,
    vd.visit_occurrence_id                  AS visit_occurrence_id,
    vd.visit_detail_id                      AS visit_detail_id,
    CONCAT(src.source_code)                 AS measurement_source_value,  -- source value is changed
    src.source_concept_id                           AS measurement_source_concept_id,
    src.unit_source_value                   AS unit_source_value,
    CAST(src.value_as_number AS TEXT)     AS value_source_value, -- ?
    -- 
    CONCAT('measurement.', src.unit_id)     AS unit_id,
    src.load_table_id                       AS load_table_id,
    src.load_row_id                         AS load_row_id,
    src.trace_id                            AS trace_id
FROM
    etl_dataset.lk_meas_waveform_mapped src
INNER JOIN
    etl_dataset.cdm_person per 
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
INNER JOIN
    etl_dataset.cdm_visit_detail vd 
        ON src.reference_id = vd.visit_detail_source_value
WHERE
    src.target_domain_id = 'Measurement'
;

