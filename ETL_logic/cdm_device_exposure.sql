
-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_device_exposure table
-- 
-- Dependencies: run after 
--      lk_drug_prescriptions.sql
--      lk_meas_chartevents.sql
--      cdm_person.sql
--      cdm_visit_occurrence.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- cdm_device_exposure
-- Rule 1 lk_drug_mapped
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
DROP TABLE IF EXISTS etl_dataset.cdm_device_exposure;
CREATE TABLE etl_dataset.cdm_device_exposure
(
    device_exposure_id              INTEGER       not null ,
    person_id                       INTEGER       not null ,
    device_concept_id               INTEGER       not null ,
    device_exposure_start_date      DATE        not null ,
    device_exposure_start_datetime  TIMESTAMP             ,
    device_exposure_end_date        DATE                 ,
    device_exposure_end_datetime    TIMESTAMP             ,
    device_type_concept_id          INTEGER       not null ,
    unique_device_id                TEXT               ,
    quantity                        INTEGER                ,
    provider_id                     INTEGER                ,
    visit_occurrence_id             INTEGER                ,
    visit_detail_id                 INTEGER                ,
    device_source_value             TEXT               ,
    device_source_concept_id        INTEGER                ,
    -- 
    unit_id                       TEXT,
    load_table_id                 TEXT,
    load_row_id                   INTEGER,
    trace_id                      TEXT
)
;


INSERT INTO etl_dataset.cdm_device_exposure
SELECT
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS device_exposure_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS device_concept_id,
    CAST(src.start_datetime AS DATE)            AS device_exposure_start_date,
    src.start_datetime                          AS device_exposure_start_datetime,
    CAST(src.end_datetime AS DATE)              AS device_exposure_end_date,
    src.end_datetime                            AS device_exposure_end_datetime,
    src.type_concept_id                         AS device_type_concept_id,
    CAST(NULL AS TEXT)                        AS unique_device_id,
    CAST(
    CASE
    WHEN ROUND(src.quantity) = src.quantity THEN src.quantity
    ELSE NULL
    END
        AS INTEGER)                               AS quantity,
    CAST(NULL AS INTEGER)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INTEGER)                         AS visit_detail_id,
    src.source_code                             AS device_source_value,
    src.source_concept_id                       AS device_source_concept_id,
    -- 
    CONCAT('device.', src.unit_id)  AS unit_id,
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
    src.target_domain_id = 'Device'
;


INSERT INTO etl_dataset.cdm_device_exposure
SELECT
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS device_exposure_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS device_concept_id,
    CAST(src.start_datetime AS DATE)            AS device_exposure_start_date,
    src.start_datetime                          AS device_exposure_start_datetime,
    CAST(src.start_datetime AS DATE)            AS device_exposure_end_date,
    src.start_datetime                          AS device_exposure_end_datetime,
    src.type_concept_id                         AS device_type_concept_id,
    CAST(NULL AS TEXT)                        AS unique_device_id,
    CAST(
    CASE 
    WHEN ROUND(src.value_as_number) = src.value_as_number THEN src.value_as_number
    ELSE NULL
    END
        AS INTEGER)                               AS quantity,
    CAST(NULL AS INTEGER)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INTEGER)                         AS visit_detail_id,
    src.source_code                             AS device_source_value,
    src.source_concept_id                       AS device_source_concept_id,
    -- 
    CONCAT('device.', src.unit_id)  AS unit_id,
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
    src.target_domain_id = 'Device'
;



