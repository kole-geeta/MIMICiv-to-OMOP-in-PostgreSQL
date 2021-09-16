-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_procedure_occurrence table
-- 
-- Dependencies: run after 
--      cdm_person.sql,
--      cdm_visit_occurrence,
--      lk_procedure_occurrence

-----------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
--
-- -------------------------------------------------------------------


-- -------------------------------------------------------------------
-- cdm_procedure_occurrence
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
DROP TABLE IF EXISTS etl_dataset.cdm_procedure_occurrence;
CREATE TABLE etl_dataset.cdm_procedure_occurrence
(
    procedure_occurrence_id     INTEGER     not null ,
    person_id                   INTEGER     not null ,
    procedure_concept_id        INTEGER     not null ,
    procedure_date              DATE      not null ,
    procedure_datetime          TIMESTAMP           ,
    procedure_type_concept_id   INTEGER     not null ,
    modifier_concept_id         INTEGER              ,
    quantity                    INTEGER              ,
    provider_id                 INTEGER              ,
    visit_occurrence_id         INTEGER              ,
    visit_detail_id             INTEGER              ,
    procedure_source_value      TEXT             ,
    procedure_source_concept_id INTEGER              ,
    modifier_source_value      TEXT              ,
    -- 
    unit_id                       TEXT,
    load_table_id                 TEXT,
    load_row_id                   INTEGER,
    trace_id                      TEXT
)
;

-- -------------------------------------------------------------------
-- Rules 1-4
-- lk_procedure_mapped
-- -------------------------------------------------------------------

INSERT INTO etl_dataset.cdm_procedure_occurrence
SELECT
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int          AS procedure_occurrence_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS procedure_concept_id,
    CAST(src.start_datetime AS DATE)            AS procedure_date,
    src.start_datetime                          AS procedure_datetime,
    src.type_concept_id                         AS procedure_type_concept_id,
    0                                           AS modifier_concept_id,
    CAST(src.quantity AS INTEGER)                 AS quantity,
    CAST(NULL AS INTEGER)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INTEGER)                         AS visit_detail_id,
    src.source_code                             AS procedure_source_value,
    src.source_concept_id                       AS procedure_source_concept_id,
    CAST(NULL AS TEXT)                        AS modifier_source_value,
    -- 
    CONCAT('procedure.', src.unit_id)           AS unit_id,
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
    src.target_domain_id = 'Procedure'
;

-- -------------------------------------------------------------------
-- Rule 5
-- lk_observation_mapped, possible DRG codes
-- -------------------------------------------------------------------

INSERT INTO etl_dataset.cdm_procedure_occurrence
SELECT
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int          AS procedure_occurrence_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS procedure_concept_id,
    CAST(src.start_datetime AS DATE)            AS procedure_date,
    src.start_datetime                          AS procedure_datetime,
    src.type_concept_id                         AS procedure_type_concept_id,
    0                                           AS modifier_concept_id,
    CAST(NULL AS INTEGER)                         AS quantity,
    CAST(NULL AS INTEGER)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INTEGER)                         AS visit_detail_id,
    src.source_code                             AS procedure_source_value,
    src.source_concept_id                       AS procedure_source_concept_id,
    CAST(NULL AS TEXT)                        AS modifier_source_value,
    -- 
    CONCAT('procedure.', src.unit_id)           AS unit_id,
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
    src.target_domain_id = 'Procedure'
;

-- -------------------------------------------------------------------
-- Rule 6
-- lk_specimen_mapped, small part of specimen is mapped to Procedure
-- -------------------------------------------------------------------

INSERT INTO etl_dataset.cdm_procedure_occurrence
SELECT
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int          AS procedure_occurrence_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS procedure_concept_id,
    CAST(src.start_datetime AS DATE)            AS procedure_date,
    src.start_datetime                          AS procedure_datetime,
    src.type_concept_id                         AS procedure_type_concept_id,
    0                                           AS modifier_concept_id,
    CAST(NULL AS INTEGER)                         AS quantity,
    CAST(NULL AS INTEGER)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INTEGER)                         AS visit_detail_id,
    src.source_code                             AS procedure_source_value,
    src.source_concept_id                       AS procedure_source_concept_id,
    CAST(NULL AS TEXT)                        AS modifier_source_value,
    -- 
    CONCAT('procedure.', src.unit_id)           AS unit_id,
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
    src.target_domain_id = 'Procedure'
;


-- -------------------------------------------------------------------
-- Rule 7
-- lk_chartevents_mapped, a part of chartevents table is mapped to Procedure
-- -------------------------------------------------------------------

INSERT INTO etl_dataset.cdm_procedure_occurrence
SELECT
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS procedure_occurrence_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS procedure_concept_id,
    CAST(src.start_datetime AS DATE)            AS procedure_date,
    src.start_datetime                          AS procedure_datetime,
    src.type_concept_id                         AS procedure_type_concept_id,
    0                                           AS modifier_concept_id,
    CAST(NULL AS INTEGER)                         AS quantity,
    CAST(NULL AS INTEGER)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INTEGER)                         AS visit_detail_id,
    src.source_code                             AS procedure_source_value,
    src.source_concept_id                       AS procedure_source_concept_id,
    CAST(NULL AS TEXT)                        AS modifier_source_value,
    -- 
    CONCAT('procedure.', src.unit_id)           AS unit_id,
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
    src.target_domain_id = 'Procedure'
;
-- Dependencies: run after 
--      cdm_person.sql,
--      cdm_visit_occurrence,
--      lk_procedure_occurrence

