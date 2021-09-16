
CREATE SCHEMA voc_dataset
-- ------------------------------------------------------------------------------
-- Create vocabulary tables from tmp_*, loaded from CSV
-- add load_row_id fields and format dates
-- ------------------------------------------------------------------------------
-- concept
-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS voc_dataset.concept;
CREATE TABLE voc_dataset.concept AS
SELECT
    *,
    'concept' AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int AS load_row_id
FROM
    etl_dataset.voc_concept
;

--DROP TABLE IF EXISTS etl_dataset.voc_concept;

-- ------------------------------------------------------------------------------
-- concept_relationship
-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS voc_dataset.concept_relationship;
CREATE TABLE voc_dataset.concept_relationship AS
SELECT
    *,
    'concept_relationship' AS load_table_id,
    0 AS load_row_id
FROM
    etl_dataset.voc_concept_relationship
;

--DROP TABLE IF EXISTS etl_dataset.voc_concept_relationship;

-- ------------------------------------------------------------------------------
-- vocabulary
-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS voc_dataset.vocabulary;
CREATE  TABLE voc_dataset.vocabulary AS
SELECT
    *,
    'vocabulary' AS load_table_id,
    0 AS load_row_id
FROM
    etl_dataset.voc_vocabulary
;

--DROP TABLE IF EXISTS etl_dataset.voc_vocabulary;

-- ------------------------------------------------------------------------------
-- tables which are NOT affected in generating custom concepts 
-- ------------------------------------------------------------------------------

-- ------------------------------------------------------------------------------
-- drug_strength
-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS voc_dataset.drug_strength;
CREATE TABLE voc_dataset.drug_strength AS
SELECT
    
    *,
    'drug_strength' AS load_table_id,
    0 AS load_row_id
FROM
    etl_dataset.voc_drug_strength
;

--DROP TABLE IF EXISTS etl_dataset.voc_drug_strength;

-- ------------------------------------------------------------------------------
-- concept_class
-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS voc_dataset.concept_class;
CREATE TABLE voc_dataset.concept_class AS
SELECT
    *,
    'concept_class' AS load_table_id,
    0 AS load_row_id
FROM
    etl_dataset.voc_concept_class
;

--DROP TABLE IF EXISTS etl_dataset.voc_concept_class;

-- ------------------------------------------------------------------------------
-- concept_ancestor
-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS voc_dataset.concept_ancestor;
CREATE TABLE voc_dataset.concept_ancestor AS
SELECT
    *,
    'concept_ancestor' AS load_table_id,
    0 AS load_row_id
FROM
    etl_dataset.voc_concept_ancestor
;

------------------------------------------------------------------------------
-- concept_synonym
-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS voc_dataset.concept_synonym;
CREATE TABLE voc_dataset.concept_synonym AS
SELECT
    *,
    'concept_synonym' AS load_table_id,
    0 AS load_row_id
FROM
    etl_dataset.voc_concept_synonym
;
------------------------------------------------------------------------------
-- domain
-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS voc_dataset.domain;
CREATE  TABLE voc_dataset.domain AS
SELECT
    *,
    'domain' AS load_table_id,
    0 AS load_row_id
FROM
    etl_dataset.voc_domain
;

------------------------------------------------------------------------------
-- relationship
-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS voc_dataset.relationship ;
CREATE TABLE voc_dataset.relationship AS
SELECT
    *,
    'relationship' AS load_table_id,
    0 AS load_row_id
FROM
    etl_dataset.voc_relationship
;

--DROP TABLE IF EXISTS etl_dataset.voc_relationship;




