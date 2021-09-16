
-- -------------------------------------------------------------------
-- Populate cdm_care_site table
-- 
-- Dependencies: run after st_core.sql (transfers)
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize "create or replace"
--
-- negative unique id from FARM_FINGERPRINT(GENERATE_UUID())
--
-- custom mapping: 
--      gcpt_care_site -> mimiciv_cs_place_of_service
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- lk_trans_careunit_clean
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS etl_dataset.lk_trans_careunit_clean;
CREATE TABLE etl_dataset.lk_trans_careunit_clean AS
SELECT
    src.careunit                        AS source_code,
    src.load_table_id                   AS load_table_id,
    0                                   AS load_row_id,
	JSONB_AGG(src.trace_id)->> 0 as trace_id
FROM 
    etl_dataset.src_transfers src

WHERE
    src.careunit IS NOT NULL
GROUP BY
    careunit,
    load_table_id	
;

DROP TABLE IF EXISTS etl_dataset.cdm_care_site;
CREATE TABLE etl_dataset.cdm_care_site
(
    care_site_id                  INTEGER       not null ,
    care_site_name                TEXT               ,
    place_of_service_concept_id  INTEGER                ,
    location_id                   INTEGER               ,
    care_site_source_value        TEXT               ,
    place_of_service_source_value TEXT               ,
    -- 
    unit_id                       TEXT,
    load_table_id                 TEXT,
    load_row_id                   INTEGER,
    trace_id                      TEXT
)
;

INSERT INTO etl_dataset.cdm_care_site
SELECT
   ('x'||substr(md5(random():: text),1,8))::bit(32)::int    
   AS care_site_id,
    src.source_code                     AS care_site_name,
    vc2.concept_id                      AS place_of_service_concept_id,
    1                                   AS location_id,  -- hard-coded BIDMC
    src.source_code                     AS care_site_source_value,
    src.source_code                     AS place_of_service_source_value,
    'care_site.transfers'       AS unit_id,
    src.load_table_id           AS load_table_id,
    src.load_row_id             AS load_row_id,
  src.trace_id                AS trace_id
FROM 
   etl_dataset.lk_trans_careunit_clean src
LEFT JOIN
   voc_dataset.concept vc
        ON  vc.concept_code = src.source_code
        AND vc.vocabulary_id = 'mimiciv_cs_place_of_service' -- gcpt_care_site
LEFT JOIN
    voc_dataset.concept_relationship vcr
        ON  vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
   voc_dataset.concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
;




