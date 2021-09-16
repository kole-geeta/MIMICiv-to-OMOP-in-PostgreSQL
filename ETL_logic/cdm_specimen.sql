
-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_specimen table
-- 
-- Dependencies: run after 
--      cdm_person.sql,
--      lk_meas_specimen.sql
--      
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Rule 1 specimen from microbiology
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- cdm_specimen
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
DROP TABLE IF EXISTS etl_dataset.cdm_specimen;
CREATE TABLE etl_dataset.cdm_specimen
(
    specimen_id                 INTEGER     not null ,
    person_id                   INTEGER     not null ,
    specimen_concept_id         INTEGER     not null ,
    specimen_type_concept_id    INTEGER     not null ,
    specimen_date               DATE      not null ,
    specimen_datetime           TIMESTAMP           ,
    quantity                    FLOAT            ,
    unit_concept_id             INTEGER              ,
    anatomic_site_concept_id    INTEGER              ,
    disease_status_concept_id   INTEGER              ,
    specimen_source_id          TEXT             ,
    specimen_source_value       TEXT             ,
    unit_source_value           TEXT             ,
    anatomic_site_source_value  TEXT             ,
    disease_status_source_value TEXT             ,
    -- 
    unit_id                       TEXT,
    load_table_id                 TEXT,
    load_row_id                   INTEGER,
    trace_id                      TEXT
)
;


INSERT INTO etl_dataset.cdm_specimen
SELECT
    src.specimen_id                             AS specimen_id,
    per.person_id                               AS person_id,
    COALESCE(src.target_concept_id, 0)          AS specimen_concept_id,
    32856                                       AS specimen_type_concept_id, -- OMOP4976929 Lab
    CAST(src.start_datetime AS DATE)            AS specimen_date,
    src.start_datetime                          AS specimen_datetime,
    CAST(NULL AS FLOAT)                       AS quantity,
    CAST(NULL AS INTEGER)                         AS unit_concept_id,
    0                                           AS anatomic_site_concept_id,
    0                                           AS disease_status_concept_id,
    src.trace_id                                AS specimen_source_id,
    src.source_code                             AS specimen_source_value,
    CAST(NULL AS TEXT)                        AS unit_source_value,
    CAST(NULL AS TEXT)                        AS anatomic_site_source_value,
    CAST(NULL AS TEXT)                        AS disease_status_source_value,
    -- 
    CONCAT('specimen.', src.unit_id)    AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    etl_dataset.lk_specimen_mapped src
INNER JOIN
    etl_dataset.cdm_person per
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
WHERE
    src.target_domain_id = 'Specimen'
;

