CREATE TABLE etl_dataset.voc_concept (
  concept_id          INTEGER       not null ,
  concept_name        TEXT      ,
  domain_id           TEXT      not null ,
  vocabulary_id       TEXT      not null ,
  concept_class_id    TEXT      not null ,
  standard_concept    TEXT               ,
  concept_code        TEXT      not null ,
  valid_start_DATE    DATE        not null ,
  valid_end_DATE      DATE        not null ,
  invalid_reason      TEXT
)
;


CREATE TABLE etl_dataset.voc_vocabulary (
  vocabulary_id         TEXT      not null,
  vocabulary_name       TEXT      not null,
  vocabulary_reference  TEXT      not null,
  vocabulary_version    TEXT              ,
  vocabulary_concept_id INTEGER       not null
)
;


CREATE TABLE etl_dataset.voc_domain (
  domain_id         TEXT      not null,
  domain_name       TEXT      not null,
  domain_concept_id INTEGER       not null
)
;


CREATE TABLE etl_dataset.voc_concept_class (
  concept_class_id          TEXT      not null,
  concept_class_name        TEXT      not null,
  concept_class_concept_id  INTEGER       not null
)
;


CREATE TABLE etl_dataset.voc_concept_relationship (
  concept_id_1      INTEGER     not null,
  concept_id_2      INTEGER     not null,
  relationship_id   TEXT    not null,
  valid_start_DATE  DATE      not null,
  valid_end_DATE    DATE      not null,
  invalid_reason    TEXT
  )
;


CREATE TABLE etl_dataset.voc_relationship (
  relationship_id         TEXT      not null,
  relationship_name       TEXT      not null,
  is_hierarchical         TEXT      not null,
  defines_ancestry        TEXT      not null,
  reverse_relationship_id TEXT      not null,
  relationship_concept_id INTEGER       not null
)
;


CREATE TABLE etl_dataset.voc_concept_synonym (
  concept_id            INTEGER       not null,
  concept_synonym_name  TEXT      not null,
  language_concept_id   INTEGER       not null
)
;


CREATE TABLE etl_dataset.voc_concept_ancestor (
  ancestor_concept_id       INTEGER   not null,
  descendant_concept_id     INTEGER   not null,
  min_levels_of_separation  INTEGER   not null,
  max_levels_of_separation  INTEGER   not null
)
;


CREATE TABLE etl_dataset.voc_source_to_concept_map (
  source_code             TEXT      not null,
  source_concept_id       INTEGER       not null,
  source_vocabulary_id    TEXT      not null,
  source_code_description TEXT              ,
  target_concept_id       INTEGER       not null,
  target_vocabulary_id    TEXT      not null,
  valid_start_DATE        DATE        not null,
  valid_end_DATE          DATE        not null,
  invalid_reason          TEXT
)
;


CREATE TABLE etl_dataset.voc_drug_strength (
  drug_concept_id             INTEGER     not null,
  ingredient_concept_id       INTEGER     not null,
  amount_value                FLOAT           ,
  amount_unit_concept_id      INTEGER             ,
  numerator_value             FLOAT           ,
  numerator_unit_concept_id   INTEGER             ,
  denominator_value           FLOAT           ,
  denominator_unit_concept_id INTEGER             ,
  box_size                    INTEGER             ,
  valid_start_DATE            DATE       not null,
  valid_end_DATE              DATE       not null,
  invalid_reason              TEXT
)
;


