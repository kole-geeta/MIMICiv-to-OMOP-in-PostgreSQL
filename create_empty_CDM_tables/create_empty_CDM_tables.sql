CREATE SCHEMA etl_dataset;
CREATE TABLE etl_dataset.cdm_cohort_definition (
  cohort_definition_id            INTEGER       not null,
  cohort_definition_name          TEXT      not null,
  cohort_definition_description   TEXT              ,
  definition_type_concept_id      INTEGER       not null,
  cohort_definition_syntax        TEXT              ,
  subject_concept_id              INTEGER       not null,
  cohort_initiation_date          DATE
)
;


CREATE TABLE etl_dataset.cdm_attribute_definition (
  attribute_definition_id     INTEGER       not null,
  attribute_name              TEXT      not null,
  attribute_description       TEXT              ,
  attribute_type_concept_id   INTEGER       not null,
  attribute_syntax            TEXT
)
;


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
  vocabulary_version              TEXT
)
;


CREATE TABLE etl_dataset.cdm_metadata
(
  metadata_concept_id       INTEGER       not null ,
  metadata_type_concept_id  INTEGER       not null ,
  name                      TEXT      not null ,
  value_as_TEXT           TEXT               ,
  value_as_concept_id       INTEGER                ,
  metadata_date             DATE                 ,
  metadata_DATETIME        TIMESTAMP
)
;



--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE etl_dataset.cdm_person
(
  person_id                   INTEGER     not null ,
  gender_concept_id           INTEGER     not null ,
  year_of_birth               INTEGER     not null ,
  month_of_birth              INTEGER              ,
  day_of_birth                INTEGER              ,
  birth_DATETIME             TIMESTAMP           ,
  race_concept_id             INTEGER     not null,
  ethnicity_concept_id        INTEGER     not null,
  location_id                 INTEGER              ,
  provider_id                 INTEGER              ,
  care_site_id                INTEGER              ,
  person_source_value         TEXT             ,
  gender_source_value         TEXT             ,
  gender_source_concept_id    INTEGER              ,
  race_source_value           TEXT             ,
  race_source_concept_id      INTEGER              ,
  ethnicity_source_value      TEXT             ,
  ethnicity_source_concept_id INTEGER
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE  TABLE etl_dataset.cdm_observation_period
(
  observation_period_id             INTEGER   not null ,
  person_id                         INTEGER   not null ,
  observation_period_start_date     DATE    not null ,
  observation_period_end_date       DATE    not null ,
  period_type_concept_id            INTEGER   not null
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE  TABLE etl_dataset.cdm_specimen
(
  specimen_id                 INTEGER     not null ,
  person_id                   INTEGER     not null ,
  specimen_concept_id         INTEGER     not null ,
  specimen_type_concept_id    INTEGER     not null ,
  specimen_date               DATE      not null ,
  specimen_DATETIME          TIMESTAMP           ,
  quantity                    FLOAT            ,
  unit_concept_id             INTEGER              ,
  anatomic_site_concept_id    INTEGER              ,
  disease_status_concept_id   INTEGER              ,
  specimen_source_id          TEXT             ,
  specimen_source_value       TEXT             ,
  unit_source_value           TEXT             ,
  anatomic_site_source_value  TEXT             ,
  disease_status_source_value TEXT
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE etl_dataset.cdm_death
(
  person_id               INTEGER     not null ,
  death_date              DATE      not null ,
  death_DATETIME         TIMESTAMP           ,
  death_type_concept_id   INTEGER     not null ,
  cause_concept_id        INTEGER              ,
  cause_source_value      TEXT             ,
  cause_source_concept_id INTEGER
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE etl_dataset.cdm_visit_occurrence
(
  visit_occurrence_id           INTEGER     not null ,
  person_id                     INTEGER     not null ,
  visit_concept_id              INTEGER     not null ,
  visit_start_date              DATE      not null ,
  visit_start_DATETIME         TIMESTAMP           ,
  visit_end_date                DATE      not null ,
  visit_end_DATETIME           TIMESTAMP           ,
  visit_type_concept_id         INTEGER     not null ,
  provider_id                   INTEGER              ,
  care_site_id                  INTEGER              ,
  visit_source_value            TEXT             ,
  visit_source_concept_id       INTEGER              ,
  admitting_source_concept_id   INTEGER              ,
  admitting_source_value        TEXT             ,
  discharge_to_concept_id       INTEGER              ,
  discharge_to_source_value     TEXT             ,
  preceding_visit_occurrence_id INTEGER
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE etl_dataset.cdm_visit_detail
(
  visit_detail_id                    INTEGER     not null ,
  person_id                          INTEGER     not null ,
  visit_detail_concept_id            INTEGER     not null ,
  visit_detail_start_date            DATE      not null ,
  visit_detail_start_DATETIME       TIMESTAMP           ,
  visit_detail_end_date              DATE      not null ,
  visit_detail_end_DATETIME         TIMESTAMP           ,
  visit_detail_type_concept_id       INTEGER     not null ,
  provider_id                        INTEGER              ,
  care_site_id                       INTEGER              ,
  admitting_source_concept_id        INTEGER              ,
  discharge_to_concept_id            INTEGER              ,
  preceding_visit_detail_id          INTEGER              ,
  visit_detail_source_value          TEXT             ,
  visit_detail_source_concept_id     INTEGER              ,
  admitting_source_value             TEXT             ,
  discharge_to_source_value          TEXT             ,
  visit_detail_parent_id             INTEGER              ,
  visit_occurrence_id                INTEGER     not null
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE etl_dataset.cdm_procedure_occurrence
(
  procedure_occurrence_id     INTEGER     not null ,
  person_id                   INTEGER     not null ,
  procedure_concept_id        INTEGER     not null ,
  procedure_date              DATE      not null ,
  procedure_DATETIME         TIMESTAMP           ,
  procedure_type_concept_id   INTEGER     not null ,
  modifier_concept_id         INTEGER              ,
  quantity                    INTEGER              ,
  provider_id                 INTEGER              ,
  visit_occurrence_id         INTEGER              ,
  visit_detail_id             INTEGER              ,
  procedure_source_value      TEXT             ,
  procedure_source_concept_id INTEGER              ,
  modifier_source_value      TEXT
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE etl_dataset.cdm_drug_exposure
(
  drug_exposure_id              INTEGER       not null ,
  person_id                     INTEGER       not null ,
  drug_concept_id               INTEGER       not null ,
  drug_exposure_start_date      DATE        not null ,
  drug_exposure_start_DATETIME TIMESTAMP             ,
  drug_exposure_end_date        DATE        not null ,
  drug_exposure_end_DATETIME   TIMESTAMP             ,
  verbatim_end_date             DATE                 ,
  drug_type_concept_id          INTEGER       not null ,
  stop_reason                   TEXT               ,
  refills                       INTEGER                ,
  quantity                      FLOAT              ,
  days_supply                   INTEGER                ,
  sig                           TEXT               ,
  route_concept_id              INTEGER                ,
  lot_number                    TEXT               ,
  provider_id                   INTEGER                ,
  visit_occurrence_id           INTEGER                ,
  visit_detail_id               INTEGER                ,
  drug_source_value             TEXT               ,
  drug_source_concept_id        INTEGER                ,
  route_source_value            TEXT               ,
  dose_unit_source_value        TEXT
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE etl_dataset.cdm_device_exposure
(
  device_exposure_id              INTEGER       not null ,
  person_id                       INTEGER       not null ,
  device_concept_id               INTEGER       not null ,
  device_exposure_start_date      DATE        not null ,
  device_exposure_start_DATETIME TIMESTAMP             ,
  device_exposure_end_date        DATE                 ,
  device_exposure_end_DATETIME   TIMESTAMP             ,
  device_type_concept_id          INTEGER       not null ,
  unique_device_id                TEXT               ,
  quantity                        INTEGER                ,
  provider_id                     INTEGER                ,
  visit_occurrence_id             INTEGER                ,
  visit_detail_id                 INTEGER                ,
  device_source_value             TEXT               ,
  device_source_concept_id        INTEGER
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE etl_dataset.cdm_condition_occurrence
(
  condition_occurrence_id       INTEGER     not null ,
  person_id                     INTEGER     not null ,
  condition_concept_id          INTEGER     not null ,
  condition_start_date          DATE      not null ,
  condition_start_DATETIME     TIMESTAMP           ,
  condition_end_date            DATE               ,
  condition_end_DATETIME       TIMESTAMP           ,
  condition_type_concept_id     INTEGER     not null ,
  stop_reason                   TEXT             ,
  provider_id                   INTEGER              ,
  visit_occurrence_id           INTEGER              ,
  visit_detail_id               INTEGER              ,
  condition_source_value        TEXT             ,
  condition_source_concept_id   INTEGER              ,
  condition_status_source_value TEXT             ,
  condition_status_concept_id   INTEGER
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE etl_dataset.cdm_measurement
(
  measurement_id                INTEGER     not null ,
  person_id                     INTEGER     not null ,
  measurement_concept_id        INTEGER     not null ,
  measurement_date              DATE      not null ,
  measurement_DATETIME         TIMESTAMP           ,
  measurement_time              TEXT             ,
  measurement_type_concept_id   INTEGER     not null ,
  operator_concept_id           INTEGER              ,
  value_as_number               FLOAT            ,
  value_as_concept_id           INTEGER              ,
  unit_concept_id               INTEGER              ,
  range_low                     FLOAT            ,
  range_high                    FLOAT            ,
  provider_id                   INTEGER              ,
  visit_occurrence_id           INTEGER              ,
  visit_detail_id               INTEGER              ,
  measurement_source_value      TEXT             ,
  measurement_source_concept_id INTEGER              ,
  unit_source_value             TEXT             ,
  value_source_value            TEXT
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE etl_dataset.cdm_note
(
  note_id               INTEGER       not null ,
  person_id             INTEGER       not null ,
  note_date             DATE        not null ,
  note_DATETIME        TIMESTAMP             ,
  note_type_concept_id  INTEGER       not null ,
  note_class_concept_id INTEGER       not null ,
  note_title            TEXT               ,
  note_text             TEXT               ,
  encoding_concept_id   INTEGER       not null ,
  language_concept_id   INTEGER       not null ,
  provider_id           INTEGER                ,
  visit_occurrence_id   INTEGER                ,
  visit_detail_id       INTEGER                ,
  note_source_value     TEXT
)
;



CREATE TABLE etl_dataset.cdm_note_nlp
(
  note_nlp_id                 INTEGER                ,
  note_id                     INTEGER                ,
  section_concept_id          INTEGER                ,
  snippet                     TEXT               ,
  "offset"                      TEXT               ,
  lexical_variant             TEXT      not null ,
  note_nlp_concept_id         INTEGER                ,
  note_nlp_source_concept_id  INTEGER                ,
  nlp_system                  TEXT               ,
  nlp_date                    DATE        not null ,
  nlp_DATETIME               TIMESTAMP             ,
  term_exists                 TEXT               ,
  term_temporal               TEXT               ,
  term_modifiers              TEXT
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE etl_dataset.cdm_observation
(
  observation_id                INTEGER     not null ,
  person_id                     INTEGER     not null ,
  observation_concept_id        INTEGER     not null ,
  observation_date              DATE      not null ,
  observation_DATETIME         TIMESTAMP           ,
  observation_type_concept_id   INTEGER     not null ,
  value_as_number               FLOAT        ,
  value_as_TEXT               TEXT         ,
  value_as_concept_id           INTEGER          ,
  qualifier_concept_id          INTEGER          ,
  unit_concept_id               INTEGER          ,
  provider_id                   INTEGER          ,
  visit_occurrence_id           INTEGER          ,
  visit_detail_id               INTEGER          ,
  observation_source_value      TEXT         ,
  observation_source_concept_id INTEGER          ,
  unit_source_value             TEXT         ,
  qualifier_source_value        TEXT
)
;


CREATE TABLE etl_dataset.cdm_fact_relationship
(
  domain_concept_id_1     INTEGER     not null ,
  fact_id_1               INTEGER     not null ,
  domain_concept_id_2     INTEGER     not null ,
  fact_id_2               INTEGER     not null ,
  relationship_concept_id INTEGER     not null
)
;


CREATE TABLE etl_dataset.cdm_location
(
  location_id           INTEGER     not null ,
  address_1             TEXT             ,
  address_2             TEXT             ,
  city                  TEXT             ,
  state                 TEXT             ,
  zip                   TEXT             ,
  county                TEXT             ,
  location_source_value TEXT
)
;


CREATE TABLE etl_dataset.cdm_care_site
(
  care_site_id                  INTEGER       not null ,
  care_site_name                TEXT               ,
  place_of_service_concept_id   INTEGER                ,
  location_id                   INTEGER                ,
  care_site_source_value        TEXT               ,
  place_of_service_source_value TEXT
)
;


CREATE TABLE etl_dataset.cdm_provider
(
  provider_id                 INTEGER       not null ,
  provider_name               TEXT               ,
  npi                         TEXT               ,
  dea                         TEXT               ,
  specialty_concept_id        INTEGER                ,
  care_site_id                INTEGER                ,
  year_of_birth               INTEGER                ,
  gender_concept_id           INTEGER                ,
  provider_source_value       TEXT               ,
  specialty_source_value      TEXT               ,
  specialty_source_concept_id INTEGER                ,
  gender_source_value         TEXT               ,
  gender_source_concept_id    INTEGER
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE etl_dataset.cdm_payer_plan_period
(
  payer_plan_period_id          INTEGER     not null ,
  person_id                     INTEGER     not null ,
  payer_plan_period_start_date  DATE      not null ,
  payer_plan_period_end_date    DATE      not null ,
  payer_concept_id              INTEGER              ,
  payer_source_value            TEXT             ,
  payer_source_concept_id       INTEGER              ,
  plan_concept_id               INTEGER              ,
  plan_source_value             TEXT             ,
  plan_source_concept_id        INTEGER              ,
  sponsor_concept_id            INTEGER              ,
  sponsor_source_value          TEXT             ,
  sponsor_source_concept_id     INTEGER              ,
  family_source_value           TEXT             ,
  stop_reason_concept_id        INTEGER              ,
  stop_reason_source_value      TEXT             ,
  stop_reason_source_concept_id INTEGER
)
;


CREATE TABLE etl_dataset.cdm_cost
(
  cost_id                   INTEGER     not null ,
  cost_event_id             INTEGER     not null ,
  cost_domain_id            TEXT    not null ,
  cost_type_concept_id      INTEGER     not null ,
  currency_concept_id       INTEGER              ,
  total_charge              FLOAT            ,
  total_cost                FLOAT            ,
  total_paid                FLOAT            ,
  paid_by_payer             FLOAT            ,
  paid_by_patient           FLOAT            ,
  paid_patient_copay        FLOAT            ,
  paid_patient_coinsurance  FLOAT            ,
  paid_patient_deductible   FLOAT            ,
  paid_by_primary           FLOAT            ,
  paid_ingredient_cost      FLOAT            ,
  paid_dispensing_fee       FLOAT            ,
  payer_plan_period_id      INTEGER              ,
  amount_allowed            FLOAT            ,
  revenue_code_concept_id   INTEGER              ,
  revenue_code_source_value  TEXT            ,
  drg_concept_id            INTEGER              ,
  drg_source_value          TEXT
)
;


--HINT DISTRIBUTE_ON_KEY(subject_id)
CREATE TABLE etl_dataset.cdm_cohort
(
  cohort_definition_id  INTEGER   not null ,
  subject_id            INTEGER   not null ,
  cohort_start_date     DATE      not null ,
  cohort_end_date       DATE      not null
)
;


--HINT DISTRIBUTE_ON_KEY(subject_id)
CREATE TABLE etl_dataset.cdm_cohort_attribute
(
  cohort_definition_id    INTEGER     not null ,
  subject_id              INTEGER     not null ,
  cohort_start_date       DATE      not null ,
  cohort_end_date         DATE      not null ,
  attribute_definition_id INTEGER     not null ,
  value_as_number         FLOAT            ,
  value_as_concept_id     INTEGER
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE etl_dataset.cdm_drug_era
(
  drug_era_id         INTEGER     not null ,
  person_id           INTEGER     not null ,
  drug_concept_id     INTEGER     not null ,
  drug_era_start_date DATE      not null ,
  drug_era_end_date   DATE      not null ,
  drug_exposure_count INTEGER              ,
  gap_days            INTEGER
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE etl_dataset.cdm_dose_era
(
  dose_era_id           INTEGER     not null ,
  person_id             INTEGER     not null ,
  drug_concept_id       INTEGER     not null ,
  unit_concept_id       INTEGER     not null ,
  dose_value            FLOAT   not null ,
  dose_era_start_date   DATE      not null ,
  dose_era_end_date     DATE      not null
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE etl_dataset.cdm_condition_era
(
  condition_era_id            INTEGER     not null ,
  person_id                   INTEGER     not null ,
  condition_concept_id        INTEGER     not null ,
  condition_era_start_date    DATE      not null ,
  condition_era_end_date      DATE      not null ,
  condition_occurrence_count  INTEGER
)
;


