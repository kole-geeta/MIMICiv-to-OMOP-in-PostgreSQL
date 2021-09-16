-- tmp_custom_concept
DROP TABLE IF EXISTS voc_dataset.tmp_custom_concept;

CREATE TABLE voc_dataset.tmp_custom_concept AS
SELECT
    voc.source_concept_id           AS concept_id,
    voc.concept_name                AS concept_name,
    voc.source_domain_id            AS domain_id,
    voc.source_vocabulary_id        AS vocabulary_id,
    voc.source_concept_class_id     AS concept_class_id,
    CASE
        WHEN voc.target_concept_id = 0 THEN 'S'
        ELSE voc.standard_concept 
    END                             AS standard_concept,
    voc.concept_code                AS concept_code,
    TO_DATE( voc.valid_start_date,'YYYY-MM-DD')    AS valid_start_date,
    TO_DATE( voc.valid_end_date,'YYYY-MM-DD')      AS valid_end_date,
    voc.invalid_reason              AS invalid_reason,

    'tmp_custom_mapping'            AS load_table_id,
    CAST(NULL AS INTEGER)             AS load_row_id
    -- voc.load_table_id               AS load_table_id,
    -- MIN(voc.load_row_id)            AS load_row_id
FROM
    etl_dataset.tmp_custom_mapping voc
GROUP BY
    voc.source_concept_id,
    voc.concept_name,
    voc.source_domain_id,
    voc.source_vocabulary_id,
    voc.source_concept_class_id,
    CASE
        WHEN voc.target_concept_id = 0 THEN 'S'
        ELSE voc.standard_concept 
    END,
    voc.concept_code,
    voc.valid_start_date,
    voc.valid_end_date,
    voc.invalid_reason
    -- voc.load_table_id
;

-- tmp_custom_concept_relationship

DROP TABLE IF EXISTS voc_dataset.tmp_custom_concept_relationship;

CREATE TABLE voc_dataset.tmp_custom_concept_relationship AS
SELECT
    tcr.source_concept_id               AS concept_id_1,
    CASE
        WHEN tcr.target_concept_id = 0 THEN tcr.source_concept_id
        ELSE tcr.target_concept_id
    END                                 AS concept_id_2,
    tcr.relationship_id                 AS relationship_id,
    TO_DATE(tcr.relationship_valid_start_date,'YYYY-MM-DD')   AS valid_start_date,
    TO_DATE( tcr.relationship_end_date,'YYYY-MM-DD')           AS valid_end_date,
    tcr.invalid_reason_cr               AS invalid_reason,

    'tmp_custom_mapping'            AS load_table_id,
    CAST(NULL AS INTEGER)             AS load_row_id
    -- tcr.load_table_id                   AS load_table_id,
    -- tcr.load_row_id                     AS load_row_id
FROM
    etl_dataset.tmp_custom_mapping tcr
WHERE
    tcr.target_concept_id IS NOT NULL

UNION ALL

SELECT
    CASE
        WHEN tcr.target_concept_id = 0 THEN tcr.source_concept_id
        ELSE tcr.target_concept_id
    END                                 AS concept_id_1,
    tcr.source_concept_id               AS concept_id_2,
    tcr.reverese_relationship_id        AS relationship_id,
    TO_DATE(tcr.relationship_valid_start_date, 'YYYY-MM-DD')   AS valid_start_date,
    TO_DATE(tcr.relationship_end_date,'YYYY-MM-DD')           AS valid_end_date,
    tcr.invalid_reason_cr               AS invalid_reason,

    'tmp_custom_mapping'            AS load_table_id,
    CAST(NULL AS INTEGER)             AS load_row_id
    -- tcr.load_table_id                   AS load_table_id,
    -- tcr.load_row_id                     AS load_row_id
FROM
    etl_dataset.tmp_custom_mapping tcr
WHERE
    tcr.target_concept_id IS NOT NULL
;

-- tmp_custom_vocabulary

DROP TABLE IF EXISTS voc_dataset.tmp_custom_vocabulary_dist;

CREATE TABLE voc_dataset.tmp_custom_vocabulary_dist AS
SELECT
    voc.source_vocabulary_id        AS source_vocabulary_id,

    'tmp_custom_mapping'            AS load_table_id,
    CAST(NULL AS INTEGER)             AS load_row_id
    -- voc.load_table_id               AS load_table_id,
    -- MIN(voc.load_row_id)            AS load_row_id
FROM
    etl_dataset.tmp_custom_mapping voc
GROUP BY
    voc.source_vocabulary_id
    -- voc.load_table_id
;

DROP TABLE IF EXISTS voc_dataset.tmp_custom_vocabulary;

CREATE TABLE voc_dataset.tmp_custom_vocabulary AS
SELECT
    voc.source_vocabulary_id        AS vocabulary_id,
    voc.source_vocabulary_id        AS vocabulary_name,
    'Odysseus generated'            AS vocabulary_reference,
    CAST(NULL AS TEXT)            AS vocabulary_version,
    2110000001 + 
        ROW_NUMBER() OVER (
            ORDER BY voc.source_vocabulary_id
        )                           AS vocabulary_concept_id,

    voc.load_table_id               AS load_table_id,
    voc.load_row_id                 AS load_row_id
FROM
    voc_dataset.tmp_custom_vocabulary_dist voc
;

DROP TABLE IF EXISTS voc_dataset.tmp_custom_vocabulary_dist;

DROP TABLE IF EXISTS voc_dataset.tmp_voc_concept;

CREATE TABLE voc_dataset.tmp_voc_concept AS
SELECT *
FROM
    voc_dataset.concept
WHERE
    concept_id < 2000000000
;


DROP TABLE IF EXISTS voc_dataset.tmp_voc_concept_relationship;

CREATE TABLE voc_dataset.tmp_voc_concept_relationship AS
SELECT vr.*
FROM
    voc_dataset.concept_relationship vr
INNER JOIN
    voc_dataset.tmp_voc_concept vc1
        ON  vc1.concept_id = vr.concept_id_1
INNER JOIN
    voc_dataset.tmp_voc_concept vc2
        ON  vc2.concept_id = vr.concept_id_2
;

INSERT INTO voc_dataset.tmp_voc_concept
SELECT
    voc.concept_id              AS concept_id,
    voc.concept_name            AS concept_name,
    voc.domain_id               AS domain_id,
    voc.vocabulary_id           AS vocabulary_id,
    voc.concept_class_id        AS concept_class_id,
    voc.standard_concept        AS standard_concept,
    voc.concept_code            AS concept_code,
    voc.valid_start_date        AS valid_start_date,
    voc.valid_end_date          AS valid_end_date,
    voc.invalid_reason          AS invalid_reason,

    voc.load_table_id           AS load_table_id,
    voc.load_row_id              AS load_row_id
FROM 
    voc_dataset.tmp_custom_concept voc
;
	
DROP TABLE IF EXISTS voc_dataset.concept;

CREATE TABLE voc_dataset.concept AS
SELECT * 
FROM voc_dataset.tmp_voc_concept;


INSERT INTO voc_dataset.tmp_voc_concept_relationship
SELECT
    tcr.concept_id_1             AS concept_id_1,
    tcr.concept_id_2             AS concept_id_2,
    tcr.relationship_id          AS relationship_id,
    tcr.valid_start_date         AS valid_start_date,
    tcr.valid_end_date           AS valid_end_date,
    tcr.invalid_reason           AS invalid_reason,

    tcr.load_table_id            AS load_table_id,
    tcr.load_row_id              AS load_row_id
FROM 
    voc_dataset.tmp_custom_concept_relationship tcr	
;

DROP TABLE IF EXISTS voc_dataset.concept_relationship;

CREATE TABLE voc_dataset.concept_relationship AS
SELECT *
FROM voc_dataset.tmp_voc_concept_relationship;


DROP TABLE IF EXISTS voc_dataset.tmp_voc_vocabulary;

CREATE TABLE voc_dataset.tmp_voc_vocabulary AS
SELECT *
FROM
    voc_dataset.vocabulary
WHERE
    vocabulary_concept_id < 2000000000
;

-- ----------------------------------------------------------------------
-- Add custom vocabularies to Vocabulary and Concept table
-- ----------------------------------------------------------------------

INSERT INTO voc_dataset.tmp_voc_vocabulary
SELECT
    voc.vocabulary_id         AS vocabulary_id,
    voc.vocabulary_name       AS vocabulary_name,
    voc.vocabulary_reference  AS vocabulary_reference,
    voc.vocabulary_version    AS vocabulary_version,
    voc.vocabulary_concept_id AS vocabulary_concept_id,

    voc.load_table_id           AS load_table_id,
    voc.load_row_id             AS load_row_id
FROM 
    voc_dataset.tmp_custom_vocabulary voc
;

DROP TABLE IF EXISTS voc_dataset.vocabulary;

CREATE TABLE voc_dataset.vocabulary AS
SELECT *
FROM voc_dataset.tmp_voc_vocabulary;

INSERT INTO voc_dataset.concept
SELECT
    vcv.vocabulary_concept_id   AS concept_id,
    vcv.vocabulary_name         AS concept_name,
    'Metadata'                  AS domain_id,
    'Vocabulary'                AS vocabulary_id,
    'Vocabulary'                AS concept_class_id,
    'S'                         AS standard_concept,
    vcv.vocabulary_reference    AS concept_code,
    CAST('1970-01-01' AS DATE)  AS valid_start_date,
    CAST('2099-12-31' AS DATE)  AS valid_end_date,
    NULL                        AS invalid_reason,

    NULL                        AS load_table_id,
    NULL                        AS load_row_id
FROM 
    voc_dataset.tmp_custom_vocabulary vcv 
;

------------------------------------------------------------------
-- clean up
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS voc_dataset.tmp_custom_concept;
DROP TABLE IF EXISTS voc_dataset.tmp_custom_concept_relationship;
DROP TABLE IF EXISTS voc_dataset.tmp_custom_vocabulary;
DROP TABLE IF EXISTS voc_dataset.tmp_voc_concept;
DROP TABLE IF EXISTS voc_dataset.tmp_voc_concept_relationship;
DROP TABLE IF EXISTS voc_dataset.tmp_voc_vocabulary;


