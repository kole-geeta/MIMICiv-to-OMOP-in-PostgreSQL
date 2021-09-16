The majority of source code is implemented in PostgreSQL (Postgres) because it is the primary support for the MIMIC database. The OMOP CDM version 5.3.3.1 (OMOP) tables were created from the provided scripts ( OHDSI/MIMIC: MIMIC (Medical Information Mart for Intensive Care) is a large, single-center database comprising information relating to patients admitted to critical care units at a large tertiary care hospital. This repository contains the ETL to the OMOP CDM, https://github.com/OHDSI/MIMIC ) with some changes documented in our scripts. The vocabulary tables were loaded from concepts downloaded from Athena 3 and the clinical and derived tables were loaded from MIMIC.
Five steps to be done for final conversion :
  - *vocabulary refresh*, which loads vocabulary and custom mapping data from local folders to the vocabulary dataset
  - *ddl*, which creates empty cdm tables in the ETL dataset
  - *staging*, which creates a snapshot of the source tables in the ETL dataset
  - *etl*, which performs the ETL logic
  - *unloading*,  which copies CDM and vocabulary tables to the final CDM OMOP dataset.

All workflows from ddl to etl operate with the so called "ETL" dataset, where intermediate tables are created, and all tables have prefixes according to their roles. i.e. voc for vocabulary tables, src for snapshot of source tables, lk for intermediate aka lookup tables, cdm for target CDM tables. Most of the tables have additional fields: unit_id, load_table_id, load_row_id, trace_id.

The last step, unload, populates the final OMOP CDM dataset, also referred to as "ATLAS" dataset. Only CDM and vocabulary tables are kept here, prefixes and additional fields are removed. The final OMOP CDM dataset can be analysed with OHDSI tools such as ATLAS or DQD.

MIMIC iv tables were loaded from modules core, icu and hosp into a single schema named mimiciv. An empty schema named etl_dataset was created in the same database. 

Among the 37 OMOP tables, the one related to hospital costs were not applicable, some related to derived data were not populated and some tables related to vocabulary were pre-loaded with terminology information. The 27 tables of MIMIC have been dispatched into 18 OMOP tables. The reduced number of tables results from the differences in design of both models. OMOP stores all the terminologies into one table whereas MIMIC has one table for each terminology and the same applies for facts data that are grouped by nature in OMOP while MIMIC tables are more specialized and respect the source EHR’s design.
|S.No|TABLE|ROWS|COLUMNS|
| :-------------: |:-----------------:|:-------------:| :-----:|
|1|cdm_visit_occurrence|2435481|17|
|2|cdm_care_site|42|6|
|3|cdm_cdm_source|1|10|
|4|cdm_person|337942|18|
|5|cdm_death|9331|7|
|6|cdm_visit_detail|1667753|19|
|7|cdm_specimen|1499926|15|
|8|cdm_condition_occurrence|10816850|16|
|9|cdm_measurement|208106453|20|
|10|cdm_drug_exposure|17045781|23|
|11|cdm_device_exposure|1841699|15|
|12|cdm_observation|31734205|18|
|13|cdm_observation_period|337926|5|
|14|cdm_fact_relationship|6916536|5|
|15|cdm_conditon_era|4222099|6|
|16|cdm_drug_era|7842030|7|
|17|cdm_dose_era|129136|7|
|18|cdm_procedure_occurrence|12451068|14|
|19|cdm_location|1|8|

**STEP 1: Vocabulary Refresh**

All vocabularies of the Standardized Vocabularies are consolidated into the same common format. This relieves the researchers from having to understand and handle multiple different formats and life-cycle conventions of the originating vocabularies.In order to obtain the Standardized Vocabularies, you can download the latest version from ATHENA and load it into your local database. To download a zip file with all Standardized Vocabularies tables, select all the vocabularies you need for your OMOP CDM. Vocabulary with Standard Concepts and very common usage are preselected. Add vocabularies that are used in your source data. https://ohdsi.github.io/TheBookOfOhdsi/StandardizedVocabularies.html

The conceptual mapping aims at aligning the MIMIC local terminologies to the OMOP’s standard ones. It consists of two phases: integration and alignment.
The integration phase is about loading terminologies into the OMOP vocabulary tables. The OMOP terminologies are provided by the Athena tool and were loaded in schema etl_dataset with prefix ‘voc_’

The alignment phase to standardize local MIMIC codes into OMOP standard codes has three distinct cases. In the first case, some MIMIC data is by chance already coded according to OMOP standard terminologies (e.g. LOINC laboratory results) and, therefore, the standard and local concepts are the same. In the second case, MIMIC data is not coded in the standard OMOP terminologies, but the mapping is already provided by OMOP (ex: ICD9/SNOMEDCT), so the domain tables have been loaded accordingly. In the third case, terminology mapping is not provided and consists of a large set of local terms (admission diagnosis, drugs). When a manual terminology mapping concept is required, a tmp_custom_mapping csv file is built. MIMIC local concepts were loaded with a concept_id identifier starting from 2 billion (lower numbers are reserved for OMOP terminologies)

Another schema named voc_dataset was created which combined the standard Athena vocabulary tables from etl_dataset with tmp_custom mapping. 

**STEP 2: Create empty CDM tables**

Creates empty cdm tables in the ETL dataset.

**STEP 3: Staging tables (temporary intermediate tables)**

Create a snapshot of the source data. The snapshot data is stored in staging source tables with prefix "src_".

**STEP 4: Perform ETL Logic**
  - Clean source data: filter out rows to be not used, format values, apply some business rules. This step results in creating "clean" intermediate tables with prefix "lk_" and suffix "clean".

  - Map distinct source codes to concepts in vocabulary tables. The step results in creating intermediate tables with prefix "lk_" and suffix "concept".
  
    -Custom mapping is implemented in custom concepts generated in vocabulary tables beforehand.
    
  - Join cleaned data and mapped codes. The step results in creating intermediate tables with prefix "lk_" and suffix "mapped".
  
  - Distribute mapped data by target CDM tables according to target_domain_id values.
  

**STEP 5: Unloading**

The last step, unload, populates the final OMOP CDM dataset. Only CDM and vocabulary tables are kept here, prefixes and additional fields are removed. The final OMOP CDM dataset can be analysed with OHDSI tools as ATLAS or DQD.


