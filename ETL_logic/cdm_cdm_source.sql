
-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_cdm_source table
-- 
-- Dependencies: no 
--      run in the end of the ETL workflow
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
-- 
-- To define source release date as (?)
--      SELECT MAX(creation_time)
--      FROM (loop through source datasets).INFORMATION_SCHEMA.TABLES
-- Add second row for Waveform POC?
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS etl_dataset.cdm_cdm_source;
CREATE TABLE etl_dataset.cdm_cdm_source
(
    cdm_source_name                 TEXT        not null ,
    cdm_source_abbreviation         TEXT             ,
    cdm_holder                      TEXT             ,
    source_description              TEXT             ,
    source_documentation_reference  TEXT             ,
    cdm_etl_reference               TEXT             ,
    source_release_date             DATE               ,
    cdm_release_date                DATE               ,
    cdm_version                     TEXT             ,
    vocabulary_version              TEXT             ,
    -- 
    unit_id                       TEXT,
    load_table_id                 TEXT,
    load_row_id                   INTEGER,
    trace_id                      TEXT
)
;

INSERT INTO etl_dataset.cdm_cdm_source
SELECT
    'MIMIC IV'                              AS cdm_source_name,
    'mimiciv'                               AS cdm_source_abbreviation,
    'PhysioNet'                             AS cdm_holder,          
    CONCAT('MIMIC-IV is a publicly available database of patients ',
        'admitted to the Beth Israel Deaconess Medical Center in Boston, MA, USA.') AS source_description,
    'https://mimic-iv.mit.edu/docs/'        AS source_documentation_reference,
    'https://github.com/OHDSI/MIMIC/'       AS cdm_etl_reference,
    TO_DATE('2020-09-01', 'YYYY-MM-DD')    AS source_release_date, -- to look up
    CURRENT_DATE()                          AS cdm_release_date,
    '5.3.1'                                 AS cdm_version,
    v.vocabulary_version                    AS vocabulary_version,
    -- 
    'cdm.source'            AS unit_id,
    'none'                  AS load_table_id,
    1                       AS load_row_id,
    TO_JSON_TEXT(STRUCT(
        'mimiciv' AS trace_id
    ))                                  AS trace_id

FROM 
    voc_dataset.vocabulary v
WHERE
    v.vocabulary_id = 'None'
;

