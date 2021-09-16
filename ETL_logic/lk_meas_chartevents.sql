
-- -------------------------------------------------------------------
-- Populate lookup tables for cdm_measurement table
-- Rule 2
-- Labs from chartevents
-- 
-- Dependencies: run after 
--      st_core.sql,
--      st_hosp.sql,
--      st_icu.sql,
--      lk_meas_units
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- to add more measurements from chartevents (see table d_items and analysis for custom mapping)
-- 
-- custom mapping:
--      brand new vocabulary -> mimiciv_meas_chartevents_value -- mapped partially
--          src_chartevents.value = measurement.value_source_value -> value_as_concept_id
-- 
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Rule 2
-- chartevents
-- chartevents keeps all possible records about events during an ICU stay
-- it repeats labs, but in case of discrepancies labs is the final truth
-- both HR and RR obviously do not repeat labs
-- Saturation probably repeats labs. Compare to Lab with source_value = "Oxygen Saturation | 50817"
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS etl_dataset.lk_chartevents_clean;
CREATE TABLE etl_dataset.lk_chartevents_clean AS
SELECT
    src.subject_id                  AS subject_id,
    src.hadm_id                     AS hadm_id,
    src.stay_id                     AS stay_id,
    src.itemid                      AS itemid,
    CAST(src.itemid AS TEXT)      AS source_code,
    di.label                        AS source_label,
    src.charttime                   AS start_datetime,
    TRIM(src.value)                 AS value,
	CASE
		WHEN REGEXP_MATCH(TRIM(src.value), '^[-]?[\d]+[.]?[\d]*[ ]*[a-z]+$') IS NOT NULL THEN CAST((REGEXP_MATCH(src.value, '[-]?[\d]+[.]?[\d]*'))[1] AS FLOAT)
		ELSE src.valuenum																			   
    END                        AS valuenum,
	
	CASE
		WHEN REGEXP_MATCH(TRIM(src.value), '^[-]?[\d]+[.]?[\d]*[ ]*[a-z]+$') IS NOT NULL THEN REGEXP_MATCH(src.value, '[a-z]+')::character varying(20)
		ELSE src.valueuom
	END                AS valueuom, -- unit of measurement
    --
    'chartevents'           AS unit_id,
    src.load_table_id       AS load_table_id,
    src.load_row_id         AS load_row_id,
    src.trace_id            AS trace_id
FROM
    etl_dataset.src_chartevents src -- ce
INNER JOIN
    etl_dataset.src_d_items di
        ON  src.itemid = di.itemid
WHERE
    di.label NOT LIKE '%Temperature'
    OR di.label LIKE '%Temperature' 
    AND 
	CASE
		WHEN valueuom LIKE '%F%' THEN (valuenum - 32) * 5 / 9
		ELSE valuenum
	END BETWEEN 25 and 44
    
;

-- -------------------------------------------------------------------
-- tmp_chartevents_code_dist
-- it is a temporary table to collect distinct codes to be mapped
-- we are going to store source_code, source_label, and row_count in the concept lookup table,
-- to provide enough information for a mapping team for possible future mapping
--
-- brand new custom vocabulary -> mimiciv_meas_chart
-- brand new custom vocabulary -> mimiciv_meas_chartevents_value
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS etl_dataset.tmp_chartevents_code_dist;
CREATE TABLE etl_dataset.tmp_chartevents_code_dist AS
-- source codes to be mapped
SELECT
    itemid                      AS itemid,
    source_code                 AS source_code,
    source_label                AS source_label,
    'mimiciv_meas_chart'        AS source_vocabulary_id,
    COUNT(*)                    AS row_count
FROM
    etl_dataset.lk_chartevents_clean
GROUP BY
    itemid,
    source_code,
    source_label
UNION ALL
-- values to be mapped
SELECT
    CAST(NULL AS INTEGER)                 AS itemid,
    value                               AS source_code,
    value                               AS source_label,
    'mimiciv_meas_chartevents_value'    AS source_vocabulary_id, -- both obs values and conditions
    COUNT(*)                            AS row_count
FROM
    etl_dataset.lk_chartevents_clean
GROUP BY
    value,
    source_code,
    source_label
;

-- -------------------------------------------------------------------
-- lk_chartevents_concept
-- collect the mapping and keep source_code, source_label, and row_count
-- for possible future mapping
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS etl_dataset.lk_chartevents_concept;
CREATE TABLE etl_dataset.lk_chartevents_concept AS
SELECT
    src.itemid                  AS itemid,
    src.source_code             AS source_code,
    src.source_label            AS source_label,
    src.source_vocabulary_id    AS source_vocabulary_id,
    -- source concept
    vc.domain_id                AS source_domain_id,
    vc.concept_id               AS source_concept_id,
    -- target concept
    vc2.domain_id               AS target_domain_id,
    vc2.concept_id              AS target_concept_id,
    src.row_count               AS row_count
FROM
    etl_dataset.tmp_chartevents_code_dist src
LEFT JOIN
    voc_dataset.concept vc
        ON  vc.concept_code = src.source_code
        AND vc.vocabulary_id = src.source_vocabulary_id
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

DROP TABLE IF EXISTS etl_dataset.tmp_chartevents_code_dist;

-- -------------------------------------------------------------------
-- lk_chartevents_mapped
-- src_chartevents to measurement and measurement value
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS etl_dataset.lk_chartevents_mapped;
CREATE TABLE etl_dataset.lk_chartevents_mapped AS
SELECT
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS measurement_id,
    src.subject_id                              AS subject_id,
    src.hadm_id                                 AS hadm_id,
    src.stay_id                                 AS stay_id,
    src.start_datetime                          AS start_datetime,
    32817                                       AS type_concept_id,  -- OMOP4976890 EHR
    src.itemid                                  AS itemid,
    src.source_code                             AS source_code,
    src.source_label                            AS source_label,
    c_main.source_vocabulary_id                 AS source_vocabulary_id,
    c_main.source_domain_id                     AS source_domain_id,
    c_main.source_concept_id                    AS source_concept_id,
    c_main.target_domain_id                     AS target_domain_id,
    c_main.target_concept_id                    AS target_concept_id,
    CASE
		WHEN src.valuenum IS NULL THEN src.value 
		ELSE NULL
	END											 AS value_source_value,
	
	CASE
		WHEN  (src.valuenum IS NULL) AND (src.value IS NOT NULL)  THEN COALESCE(c_value.target_concept_id, 0)
		ELSE  NULL
	END 										 AS value_as_concept_id,

        src.valuenum                                AS value_as_number,
    src.valueuom                                AS unit_source_value, -- unit of measurement
 
   CASE
		WHEN src.valueuom IS NOT NULL THEN  COALESCE(uc.target_concept_id, 0)
		ELSE NULL								
	END 						AS unit_concept_id,

    --
    CONCAT('meas.', src.unit_id)                AS unit_id,
    src.load_table_id       AS load_table_id,
    src.load_row_id         AS load_row_id,
    src.trace_id            AS trace_id
FROM
    etl_dataset.lk_chartevents_clean src -- ce
LEFT JOIN
    etl_dataset.lk_chartevents_concept c_main -- main
        ON c_main.source_code = src.source_code 
        AND c_main.source_vocabulary_id = 'mimiciv_meas_chart'
LEFT JOIN
    etl_dataset.lk_chartevents_concept c_value -- values for main
        ON c_value.source_code = src.value
        AND c_value.source_vocabulary_id = 'mimiciv_meas_chartevents_value'
        AND c_value.target_domain_id = 'Meas Value'
LEFT JOIN 
    etl_dataset.lk_meas_unit_concept uc
        ON uc.source_code = src.valueuom
;


-- -------------------------------------------------------------------
-- lk_chartevents_condition_mapped
-- src_chartevents to condition
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS etl_dataset.lk_chartevents_condition_mapped;
CREATE TABLE etl_dataset.lk_chartevents_condition_mapped AS
SELECT
    src.subject_id                              AS subject_id,
    src.hadm_id                                 AS hadm_id,
    src.stay_id                                 AS stay_id,
    src.start_datetime                          AS start_datetime,
    src.value                                   AS source_code,
    c_main.source_vocabulary_id                 AS source_vocabulary_id,
    c_main.source_concept_id                    AS source_concept_id,
    c_main.target_domain_id                     AS target_domain_id,
    c_main.target_concept_id                    AS target_concept_id,
    --
    CONCAT('cond.', src.unit_id)                AS unit_id,
    src.load_table_id       AS load_table_id,
    src.load_row_id         AS load_row_id,
    src.trace_id            AS trace_id
FROM
    etl_dataset.lk_chartevents_clean src -- ce
INNER JOIN
    etl_dataset.lk_chartevents_concept c_main -- condition domain from values, mapped
        ON c_main.source_code = src.value
        AND c_main.source_vocabulary_id = 'mimiciv_meas_chartevents_value'
        AND c_main.target_domain_id = 'Condition'
;

