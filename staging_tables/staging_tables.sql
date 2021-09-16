
-- -------------------------------------------------------------------
-- src_patients
-- -------------------------------------------------------------------


DROP TABLE IF EXISTS etl_dataset.src_patients;
CREATE TABLE etl_dataset.src_patients AS
SELECT
    subject_id                          AS subject_id,
    anchor_year                         AS anchor_year,
    anchor_age                          AS anchor_age,
    anchor_year_group                   AS anchor_year_group,
    gender                              AS gender,

    'patients'                          AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
    jsonb_build_object('subject_id',subject_id)                                  AS trace_id
FROM
    mimiciv.patients
;

-- -------------------------------------------------------------------
-- src_admissions
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS etl_dataset.src_admissions;
CREATE TABLE etl_dataset.src_admissions AS
SELECT
    hadm_id                             AS hadm_id, -- PK
    subject_id                          AS subject_id,
    admittime                           AS admittime,
    dischtime                           AS dischtime,
    deathtime                           AS deathtime,
    admission_type                      AS admission_type,
    admission_location                  AS admission_location,
    discharge_location                  AS discharge_location,
    ethnicity                           AS ethnicity,
    edregtime                           AS edregtime,
    insurance                           AS insurance,
    marital_status                      AS marital_status,
    language                            AS language,
    -- edouttime
    -- hospital_expire_flag
    --
    'admissions'                        AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
    jsonb_build_object('subject_id',subject_id, 'hadm_id',hadm_id)                                  AS trace_id
FROM
    mimiciv.admissions
;

-- -------------------------------------------------------------------
-- src_transfers
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS etl_dataset.src_transfers;
CREATE TABLE etl_dataset.src_transfers AS
SELECT
    transfer_id                         AS transfer_id,
    hadm_id                             AS hadm_id,
    subject_id                          AS subject_id,
    careunit                            AS careunit,
    intime                              AS intime,
    outtime                             AS outtime,
    eventtype                           AS eventtype,
    --
    'transfers'                         AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
    jsonb_build_object('subject_id',subject_id, 'hadm_id',hadm_id, 'transfer_id',  transfer_id)                                 AS trace_id
FROM
    mimiciv.transfers
;


DROP TABLE IF EXISTS etl_dataset.src_diagnoses_icd;
CREATE TABLE etl_dataset.src_diagnoses_icd AS
SELECT
    subject_id      AS subject_id,
    hadm_id         AS hadm_id,
    seq_num         AS seq_num,
    icd_code        AS icd_code,
    icd_version     AS icd_version,
    --
    'diagnoses_icd'                     AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
    jsonb_build_object('hadm_id',hadm_id,'seq_num',seq_num)                                 AS trace_id
FROM
    mimiciv.diagnoses_icd
;

-- -------------------------------------------------------------------
-- for Measurement
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- src_services
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS etl_dataset.src_services;

CREATE TABLE etl_dataset.src_services AS
SELECT
    subject_id                          AS subject_id,
    hadm_id                             AS hadm_id,
    transfertime                        AS transfertime,
    prev_service                        AS prev_service,
    curr_service                        AS curr_service,
    --
    'services'                          AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
    jsonb_build_object('subject_id',subject_id, 'hadm_id',hadm_id, 'transfertime',  transfertime)                                 AS trace_id
FROM
    mimiciv.services
;

-- -------------------------------------------------------------------
-- src_labevents
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS etl_dataset.src_labevents;
CREATE TABLE etl_dataset.src_labevents AS
SELECT
    labevent_id                         AS labevent_id,
    subject_id                          AS subject_id,
    charttime                           AS charttime,
    hadm_id                             AS hadm_id,
    itemid                              AS itemid,
    valueuom                            AS valueuom,
    value                               AS value,
    flag                                AS flag,
    ref_range_lower                     AS ref_range_lower,
    ref_range_upper                     AS ref_range_upper,
    --
    'labevents'                         AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
    jsonb_build_object('labevent_id',labevent_id)                                 AS trace_id
FROM
    mimiciv.labevents
;

-- -------------------------------------------------------------------
-- src_d_labitems
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS etl_dataset.src_d_labitems;
CREATE TABLE etl_dataset.src_d_labitems AS
SELECT
    itemid                              AS itemid,
    label                               AS label,
    fluid                               AS fluid,
    category                            AS category,
    loinc_code                          AS loinc_code,
    --
    'd_labitems'                        AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
    jsonb_build_object('itemid',itemid)                                 AS trace_id
FROM
    mimiciv.d_labitems
;


-- -------------------------------------------------------------------
-- for Procedure
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- src_procedures_icd
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS etl_dataset.src_procedures_icd;
CREATE TABLE etl_dataset.src_procedures_icd AS
SELECT
    subject_id                          AS subject_id,
    hadm_id                             AS hadm_id,
    icd_code        AS icd_code,
    icd_version     AS icd_version,
    --
    'procedures_icd'                    AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
    jsonb_build_object('subject_id',subject_id, 'hadm_id',hadm_id, 'icd_code',  icd_code, 'icd_version',icd_version )                                 AS trace_id -- this set of fields is not unique. To set quantity?
FROM
    mimiciv.procedures_icd
;

-- -------------------------------------------------------------------
-- src_hcpcsevents
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS etl_dataset.src_hcpcsevents;
CREATE TABLE etl_dataset.src_hcpcsevents AS
SELECT
    hadm_id                             AS hadm_id,
    subject_id                          AS subject_id,
    hcpcs_cd                            AS hcpcs_cd,
    seq_num                             AS seq_num,
    short_description                   AS short_description,
    --
    'hcpcsevents'                       AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
    jsonb_build_object('subject_id',subject_id, 'hadm_id',hadm_id, 'hcpcs_cd',  hcpcs_cd, 'seq_num',seq_num)                                 AS trace_id -- this set of fields is not unique. To set quantity?
FROM
    mimiciv.hcpcsevents
;


-- -------------------------------------------------------------------
-- src_drgcodes
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS etl_dataset.src_drgcodes;
CREATE TABLE etl_dataset.src_drgcodes AS
SELECT
    hadm_id                             AS hadm_id,
    subject_id                          AS subject_id,
    drg_code                            AS drg_code,
    description                         AS description,
    --
    'drgcodes'                       AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
    jsonb_build_object('subject_id',subject_id, 'hadm_id',hadm_id, 'drg_code',  COALESCE(drg_code, null))  AS trace_id                             
FROM
    mimiciv.drgcodes
;

-- -------------------------------------------------------------------
-- src_prescriptions
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS etl_dataset.src_prescriptions;
CREATE TABLE etl_dataset.src_prescriptions AS
SELECT
    hadm_id                             AS hadm_id,
    subject_id                          AS subject_id,
    pharmacy_id                         AS pharmacy_id,
    starttime                           AS starttime,
    stoptime                            AS stoptime,
    drug_type                           AS drug_type,
    drug                                AS drug,
    gsn                                 AS gsn,
    ndc                                 AS ndc,
    prod_strength                       AS prod_strength,
    form_rx                             AS form_rx,
    dose_val_rx                         AS dose_val_rx,
    dose_unit_rx                        AS dose_unit_rx,
    form_val_disp                       AS form_val_disp,
    form_unit_disp                      AS form_unit_disp,
    doses_per_24_hrs                    AS doses_per_24_hrs,
    route                               AS route,
    --
    'prescriptions'                     AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
    jsonb_build_object('subject_id',subject_id, 'hadm_id',hadm_id, 'pharmacy_id',  pharmacy_id, 'starttime', starttime)                                 AS trace_id
FROM
    mimiciv.prescriptions
;


-- -------------------------------------------------------------------
-- src_microbiologyevents
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS etl_dataset.src_microbiologyevents;
CREATE TABLE etl_dataset.src_microbiologyevents AS
SELECT
    microevent_id               AS microevent_id,
    subject_id                  AS subject_id,
    hadm_id                     AS hadm_id,
    chartdate                   AS chartdate,
    charttime                   AS charttime, -- usage: COALESCE(charttime, chartdate)
    spec_itemid                 AS spec_itemid, -- d_micro, type of specimen taken. If no grouth, then all other fields is null
    spec_type_desc              AS spec_type_desc, -- for reference
    test_itemid                 AS test_itemid, -- d_micro, what test is taken, goes to measurement
    test_name                   AS test_name, -- for reference
    org_itemid                  AS org_itemid, -- d_micro, what bacteria have grown
    org_name                    AS org_name, -- for reference
    ab_itemid                   AS ab_itemid, -- d_micro, antibiotic tested on the bacteria
    ab_name                     AS ab_name, -- for reference
    dilution_comparison         AS dilution_comparison, -- operator sign
    dilution_value              AS dilution_value, -- numeric value
    interpretation              AS interpretation, -- bacteria's degree of resistance to the antibiotic
    --
    'microbiologyevents'                AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
    jsonb_build_object('subject_id',subject_id, 'hadm_id',hadm_id, 'microevent_id', microevent_id)                                  AS trace_id
FROM
    mimiciv.microbiologyevents
;

-- -------------------------------------------------------------------
-- src_d_micro
-- -------------------------------------------------------------------


DROP TABLE IF EXISTS etl_dataset.src_d_micro;
CREATE TABLE etl_dataset.src_d_micro(
	itemid						INTEGER,
	label						CHARACTER VARYING(100),
	category					CHARACTER VARYING(50),
	load_table_id				TEXT,
	load_row_id					INTEGER,
	trace_id					TEXT
)
;

INSERT INTO etl_dataset.src_d_micro
SELECT 
	spec_itemid as itemid,
	spec_type_desc as label,
	'SPECIMEN' as category,
	'd_micro'                   AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int  AS load_row_id,
    jsonb_build_object('itemid', spec_itemid)                   AS trace_id

FROM 
	etl_dataset.src_microbiologyevents 
GROUP BY
itemid,
label;

INSERT INTO etl_dataset.src_d_micro
SELECT 
	test_itemid as itemid,
	test_name as label,
	'MICROTEST' as category,
	'd_micro'                   AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int  AS load_row_id,
    jsonb_build_object('itemid', test_itemid)                   AS trace_id

FROM 
	etl_dataset.src_microbiologyevents
GROUP BY
itemid,
label;

INSERT INTO etl_dataset.src_d_micro
SELECT 
	org_itemid as itemid,
	org_name as label,
	'ORGANISM' as category,
	'd_micro'                   AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int  AS load_row_id,
    jsonb_build_object('itemid', org_itemid)                   AS trace_id

FROM 
	etl_dataset.src_microbiologyevents
GROUP BY
itemid,
label;


INSERT INTO etl_dataset.src_d_micro
SELECT 
	ab_itemid as itemid,
	ab_name as label,
	'ANTIBIOTIC' as category,
	'd_micro'                   AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int  AS load_row_id,
    jsonb_build_object('itemid', ab_itemid)                   AS trace_id

FROM 
	etl_dataset.src_microbiologyevents
GROUP BY
itemid,
label;

delete from etl_dataset.src_d_micro
where not (itemid is not null);


-- -------------------------------------------------------------------
-- src_pharmacy
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS etl_dataset.src_pharmacy;
CREATE TABLE etl_dataset.src_pharmacy AS
SELECT
    pharmacy_id                         AS pharmacy_id,
    medication                          AS medication,
    -- hadm_id                             AS hadm_id,
    -- subject_id                          AS subject_id,
    -- starttime                           AS starttime,
    -- stoptime                            AS stoptime,
    -- route                               AS route,
    --
    'pharmacy'                          AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
    jsonb_build_object('pharmacy_id',pharmacy_id)                                 AS trace_id
FROM
    mimiciv.pharmacy
;

DROP TABLE IF EXISTS etl_dataset.src_procedureevents;
CREATE TABLE etl_dataset.src_procedureevents AS
SELECT
    hadm_id                             AS hadm_id,
    subject_id                          AS subject_id,
    stay_id                             AS stay_id,
    itemid                              AS itemid,
    starttime                           AS starttime,
    value                               AS value,
    cancelreason                        AS cancelreason,
    --
    'procedureevents'                   AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
    jsonb_build_object('subject_id',subject_id, 'hadm_id',hadm_id, 'starttime', starttime)                                 AS trace_id
FROM
    mimiciv.procedureevents
;

-- -------------------------------------------------------------------
-- src_d_items
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS etl_dataset.src_d_items;
CREATE TABLE etl_dataset.src_d_items AS
SELECT
    itemid                              AS itemid,
    label                               AS label,
    linksto                             AS linksto,
    -- abbreviation
    -- category
    -- unitname
    -- param_type
    -- lownormalvalue
    -- highnormalvalue
    --
    'd_items'                           AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
    jsonb_build_object('itemid', itemid, 'linksto', linksto)                                 AS trace_id
FROM
    mimiciv.d_items
;

-- -------------------------------------------------------------------
-- src_datetimeevents
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS etl_dataset.src_datetimeevents;
CREATE TABLE etl_dataset.src_datetimeevents AS
SELECT
    subject_id  AS subject_id,
    hadm_id     AS hadm_id,
    stay_id     AS stay_id,
    itemid      AS itemid,
    charttime   AS charttime,
    value       AS value,
    --
    'datetimeevents'                    AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
    jsonb_build_object('subject_id',subject_id, 'hadm_id',hadm_id, 'stay_id',  stay_id, 'charttime', charttime)                                 AS trace_id
FROM
    mimiciv.datetimeevents
;

DROP TABLE IF EXISTS etl_dataset.src_chartevents;
CREATE TABLE etl_dataset.src_chartevents AS
SELECT
    subject_id  AS subject_id,
    hadm_id     AS hadm_id,
    stay_id     AS stay_id,
    itemid      AS itemid,
    charttime   AS charttime,
    value       AS value,
    valuenum    AS valuenum,
    valueuom    AS valueuom,
    --
    'chartevents'                       AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
    jsonb_build_object('subject_id',subject_id, 'hadm_id',hadm_id, 'stay_id',  stay_id, 'charttime', charttime)                                 AS trace_id
FROM
    mimiciv.chartevents
;



