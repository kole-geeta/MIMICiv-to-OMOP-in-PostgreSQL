-- -------------------------------------------------------------------
-- Populate cdm_visit_occurrence table
-- 
-- Dependencies: run after 
--      st_core.sql,
--      cdm_person.sql,
--      cdm_care_site
-- -------------------------------------------------------------------

-- Using cdm_care_site:
--      care_site_name = 'BIDMC' -- Beth Israel hospital for all
--      (populate with departments)
--
-- Field diagnosis is not found in admissions table.
--      diagnosis is used to set admission/discharge concepts for organ donors
--      use hosp.diagnosis_icd + hosp.d_icd_diagnoses/voc_concept?
--
-- Review logic for organ donors. Concepts used in MIMIC III:
--      4216643 -- DEAD/EXPIRED
--      4022058 -- ORGAN DONOR
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- cdm_visit_occurrence
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
DROP TABLE IF EXISTS etl_dataset.cdm_visit_occurrence;
CREATE TABLE etl_dataset.cdm_visit_occurrence
(
    visit_occurrence_id           INTEGER     not null ,
    person_id                     INTEGER     not null ,
    visit_concept_id              INTEGER     not null ,
    visit_start_date              DATE      not null ,
    visit_start_datetime          TIMESTAMP           ,
    visit_end_date                DATE      not null ,
    visit_end_datetime            TIMESTAMP           ,
    visit_type_concept_id         INTEGER     not null ,
    provider_id                   INTEGER              ,
    care_site_id                  INTEGER              ,
    visit_source_value            TEXT             ,
    visit_source_concept_id       INTEGER              ,
    admitting_source_concept_id   INTEGER              ,
    admitting_source_value        TEXT             ,
    discharge_to_concept_id       INTEGER              ,
    discharge_to_source_value     TEXT             ,
    preceding_visit_occurrence_id INTEGER              ,
    -- 
    unit_id                       TEXT,
    load_table_id                 TEXT,
    load_row_id                   INTEGER,
    trace_id                      TEXT
)
;

INSERT INTO etl_dataset.cdm_visit_occurrence
SELECT
    src.visit_occurrence_id                 AS visit_occurrence_id,
    per.person_id                           AS person_id,
    COALESCE(lat.target_concept_id, 0)      AS visit_concept_id,
    CAST(src.start_datetime AS DATE)        AS visit_start_date,
    src.start_datetime                      AS visit_start_datetime,
    CAST(src.end_datetime AS DATE)          AS visit_end_date,
    src.end_datetime                        AS visit_end_datetime,
    32817                                   AS visit_type_concept_id,   -- EHR   Type Concept    Standard                          
    CAST(NULL AS INTEGER)                     AS provider_id,
    cs.care_site_id                         AS care_site_id,
    src.source_value                        AS visit_source_value, -- it should be an ID for visits
    COALESCE(lat.source_concept_id, 0)      AS visit_source_concept_id, -- it is where visit_concept_id comes from
   CASE
		WHEN src.admission_location IS NOT NULL THEN COALESCE(la.target_concept_id, 0)
		ELSE NULL 
	END 									AS admitting_source_concept_id,
    src.admission_location                  AS admitting_source_value,
	CASE
		WHEN src.discharge_location IS NOT NULL THEN COALESCE(ld.target_concept_id, 0)
		ELSE NULL
	END 									 AS discharge_to_concept_id,         
    src.discharge_location                  AS discharge_to_source_value,
    LAG(src.visit_occurrence_id) OVER ( 
        PARTITION BY subject_id, hadm_id 
        ORDER BY start_datetime
    )                                   AS preceding_visit_occurrence_id,
    --
    CONCAT('visit.', src.unit_id)   AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM 
    etl_dataset.lk_visit_clean src
INNER JOIN
    etl_dataset.cdm_person per
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
LEFT JOIN 
    etl_dataset.lk_visit_concept lat
        ON lat.source_code = src.admission_type
LEFT JOIN 
    etl_dataset.lk_visit_concept la 
        ON la.source_code = src.admission_location
LEFT JOIN 
    etl_dataset.lk_visit_concept ld
        ON ld.source_code = src.discharge_location
LEFT JOIN 
    etl_dataset.cdm_care_site cs
        ON care_site_name = 'BIDMC' -- Beth Israel hospital for all
;

