CREATE SCHEMA OMOP_CDM;

--Copy Vocabulary Tables

CREATE TABLE OMOP_CDM.concept AS 
SELECT
    concept_id,
    concept_name,
    domain_id,
    vocabulary_id,
    concept_class_id,
    standard_concept,
    concept_code,
    valid_start_DATE,
    valid_end_DATE,
    invalid_reason
FROM voc_dataset.concept;

CREATE TABLE OMOP_CDM.vocabulary AS 
SELECT
    vocabulary_id,
    vocabulary_name,
    vocabulary_reference,
    vocabulary_version,
    vocabulary_concept_id
FROM voc_dataset.vocabulary;

CREATE TABLE OMOP_CDM.domain AS 
SELECT
    domain_id,
    domain_name,
    domain_concept_id
FROM voc_dataset.domain;

CREATE TABLE OMOP_CDM.concept_class AS 
SELECT
    concept_class_id,
    concept_class_name,
    concept_class_concept_id
FROM voc_dataset.concept_class;

CREATE TABLE OMOP_CDM.concept_relationship AS 
SELECT
    concept_id_1,
    concept_id_2,
    relationship_id,
    valid_start_DATE,
    valid_end_DATE,
    invalid_reason
FROM voc_dataset.concept_relationship;

CREATE TABLE OMOP_CDM.relationship AS 
SELECT
    relationship_id,
    relationship_name,
    is_hierarchical,
    defines_ancestry,
    reverse_relationship_id,
    relationship_concept_id
FROM voc_dataset.relationship;

CREATE TABLE OMOP_CDM.concept_synonym AS 
SELECT
    concept_id,
    concept_synonym_name,
    language_concept_id
FROM voc_dataset.concept_synonym;

CREATE TABLE OMOP_CDM.concept_ancestor AS 
SELECT
    ancestor_concept_id,
    descendant_concept_id,
    min_levels_of_separation,
    max_levels_of_separation
FROM voc_dataset.concept_ancestor;

CREATE TABLE OMOP_CDM.drug_strength AS 
SELECT
    drug_concept_id,
    ingredient_concept_id,
    amount_value,
    amount_unit_concept_id,
    numerator_value,
    numerator_unit_concept_id,
    denominator_value,
    denominator_unit_concept_id,
    box_size,
    valid_start_DATE,
    valid_end_DATE,
    invalid_reason
FROM voc_dataset.drug_strength;


-- Copy CDM tables


CREATE TABLE OMOP_CDM.cdm_source AS 
SELECT
    cdm_source_name,
    cdm_source_abbreviation,
    cdm_holder,
    source_description,
    source_documentation_reference,
    cdm_etl_reference,
    source_release_date,
    cdm_release_date,
    cdm_version,
    vocabulary_version
FROM etl_dataset.cdm_cdm_source;

CREATE TABLE OMOP_CDM.person AS 
SELECT
    person_id,
    gender_concept_id,
    year_of_birth,
    month_of_birth,
    day_of_birth,
    birth_datetime,
    race_concept_id,
    ethnicity_concept_id,
    location_id,
    provider_id,
    care_site_id,
    person_source_value,
    gender_source_value,
    gender_source_concept_id,
    race_source_value,
    race_source_concept_id,
    ethnicity_source_value,
    ethnicity_source_concept_id
FROM etl_dataset.cdm_person;

CREATE TABLE OMOP_CDM.observation_period AS 
SELECT
    observation_period_id,
    person_id,
    observation_period_start_date,
    observation_period_end_date,
    period_type_concept_id
FROM etl_dataset.cdm_observation_period;

CREATE TABLE OMOP_CDM.specimen AS 
SELECT
    specimen_id,
    person_id,
    specimen_concept_id,
    specimen_type_concept_id,
    specimen_date,
    specimen_datetime,
    quantity,
    unit_concept_id,
    anatomic_site_concept_id,
    disease_status_concept_id,
    specimen_source_id,
    specimen_source_value,
    unit_source_value,
    anatomic_site_source_value,
    disease_status_source_value
FROM etl_dataset.cdm_specimen;

CREATE TABLE OMOP_CDM.death AS 
SELECT
    person_id,
    death_date,
    death_datetime,
    death_type_concept_id,
    cause_concept_id,
    cause_source_value,
    cause_source_concept_id
FROM etl_dataset.cdm_death;

CREATE TABLE OMOP_CDM.visit_occurrence AS 
SELECT
    visit_occurrence_id,
    person_id,
    visit_concept_id,
    visit_start_date,
    visit_start_datetime,
    visit_end_date,
    visit_end_datetime,
    visit_type_concept_id,
    provider_id,
    care_site_id,
    visit_source_value,
    visit_source_concept_id,
    admitting_source_concept_id,
    admitting_source_value,
    discharge_to_concept_id,
    discharge_to_source_value,
    preceding_visit_occurrence_id
FROM etl_dataset.cdm_visit_occurrence;

CREATE TABLE OMOP_CDM.visit_detail AS 
SELECT
    visit_detail_id,
    person_id,
    visit_detail_concept_id,
    visit_detail_start_date,
    visit_detail_start_datetime,
    visit_detail_end_date,
    visit_detail_end_datetime,
    visit_detail_type_concept_id,
    provider_id,
    care_site_id,
    admitting_source_concept_id,
    discharge_to_concept_id,
    preceding_visit_detail_id,
    visit_detail_source_value,
    visit_detail_source_concept_id,
    admitting_source_value,
    discharge_to_source_value,
    visit_detail_parent_id,
    visit_occurrence_id
FROM etl_dataset.cdm_visit_detail;

CREATE TABLE OMOP_CDM.procedure_occurrence AS 
SELECT
    procedure_occurrence_id,
    person_id,
    procedure_concept_id,
    procedure_date,
    procedure_datetime,
    procedure_type_concept_id,
    modifier_concept_id,
    quantity,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    procedure_source_value,
    procedure_source_concept_id,
    modifier_source_value
FROM etl_dataset.cdm_procedure_occurrence;

CREATE TABLE OMOP_CDM.drug_exposure AS 
SELECT
    drug_exposure_id,
    person_id,
    drug_concept_id,
    drug_exposure_start_date,
    drug_exposure_start_datetime,
    drug_exposure_end_date,
    drug_exposure_end_datetime,
    verbatim_end_date,
    drug_type_concept_id,
    stop_reason,
    refills,
    quantity,
    days_supply,
    sig,
    route_concept_id,
    lot_number,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    drug_source_value,
    drug_source_concept_id,
    route_source_value,
    dose_unit_source_value
FROM etl_dataset.cdm_drug_exposure;

CREATE TABLE OMOP_CDM.device_exposure AS 
SELECT
    device_exposure_id,
    person_id,
    device_concept_id,
    device_exposure_start_date,
    device_exposure_start_datetime,
    device_exposure_end_date,
    device_exposure_end_datetime,
    device_type_concept_id,
    unique_device_id,
    quantity,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    device_source_value,
    device_source_concept_id
FROM etl_dataset.cdm_device_exposure;

CREATE TABLE OMOP_CDM.condition_occurrence AS 
SELECT
    condition_occurrence_id,
    person_id,
    condition_concept_id,
    condition_start_date,
    condition_start_datetime,
    condition_end_date,
    condition_end_datetime,
    condition_type_concept_id,
    stop_reason,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    condition_source_value,
    condition_source_concept_id,
    condition_status_source_value,
    condition_status_concept_id
FROM etl_dataset.cdm_condition_occurrence;

CREATE TABLE OMOP_CDM.measurement AS 
SELECT
    measurement_id,
    person_id,
    measurement_concept_id,
    measurement_date,
    measurement_datetime,
    measurement_time,
    measurement_type_concept_id,
    operator_concept_id,
    value_as_number,
    value_as_concept_id,
    unit_concept_id,
    range_low,
    range_high,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    measurement_source_value,
    measurement_source_concept_id,
    unit_source_value,
    value_source_value
FROM etl_dataset.cdm_measurement;

CREATE TABLE OMOP_CDM.observation AS 
SELECT
    observation_id,
    person_id,
    observation_concept_id,
    observation_date,
    observation_datetime,
    observation_type_concept_id,
    value_as_number,
    value_as_string,
    value_as_concept_id,
    qualifier_concept_id,
    unit_concept_id,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    observation_source_value,
    observation_source_concept_id,
    unit_source_value,
    qualifier_source_value
FROM etl_dataset.cdm_observation;

CREATE TABLE OMOP_CDM.fact_relationship AS 
SELECT
    domain_concept_id_1,
    fact_id_1,
    domain_concept_id_2,
    fact_id_2,
    relationship_concept_id
FROM etl_dataset.cdm_fact_relationship;

CREATE TABLE OMOP_CDM.location AS 
SELECT
    location_id,
    address_1,
    address_2,
    city,
    state,
    zip,
    county,
    location_source_value
FROM etl_dataset.cdm_location;

CREATE TABLE OMOP_CDM.care_site AS 
SELECT
    care_site_id,
    care_site_name,
    place_of_service_concept_id,
    location_id,
    care_site_source_value,
    place_of_service_source_value
FROM etl_dataset.cdm_care_site;


CREATE TABLE OMOP_CDM.drug_era AS 
SELECT
    drug_era_id,
    person_id,
    drug_concept_id,
    drug_era_start_date,
    drug_era_end_date,
    drug_exposure_count,
    gap_days
FROM etl_dataset.cdm_drug_era;

CREATE TABLE OMOP_CDM.dose_era AS 
SELECT
    dose_era_id,
    person_id,
    drug_concept_id,
    unit_concept_id,
    dose_value,
    dose_era_start_date,
    dose_era_end_date
FROM etl_dataset.cdm_dose_era;

CREATE TABLE OMOP_CDM.condition_era AS 
SELECT
    condition_era_id,
    person_id,
    condition_concept_id,
    condition_era_start_date,
    condition_era_end_date,
    condition_occurrence_count
FROM etl_dataset.cdm_condition_era;
