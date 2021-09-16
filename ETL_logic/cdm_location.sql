-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate cdm_care_site table
-- 
-- Dependencies: run after st_core.sql
-- on Demo: 
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize "create or replace"
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- cdm_location
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS etl_dataset.cdm_location;
CREATE TABLE etl_dataset.cdm_location
(
    location_id           INTEGER     not null ,
    address_1             TEXT             ,
    address_2             TEXT             ,
    city                  TEXT             ,
    state                 TEXT             ,
    zip                   TEXT             ,
    county                TEXT             ,
    location_source_value TEXT             ,
    -- 
    unit_id                       TEXT,
    load_table_id                 TEXT,
    load_row_id                   INTEGER,
    trace_id                      TEXT
)
;

INSERT INTO etl_dataset.cdm_location
SELECT
    1                           AS location_id,
    CAST(NULL AS TEXT)        AS address_1,
    CAST(NULL AS TEXT)        AS address_2,
    CAST(NULL AS TEXT)        AS city,
    'MA'                        AS state,
    CAST(NULL AS TEXT)        AS zip,
    CAST(NULL AS TEXT)        AS county,
    'Beth Israel Hospital'      AS location_source_value,
    -- 
    'location.null'             AS unit_id,
    'null'                      AS load_table_id,
    0                           AS load_row_id,
    CAST(NULL AS TEXT)        AS trace_id
;
