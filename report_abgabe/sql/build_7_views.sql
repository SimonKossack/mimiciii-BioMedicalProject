CREATE SCHEMA IF NOT EXISTS mimiciii_derived;

SET search_path TO mimiciii_derived, mimic, public;

-- Load helper functions (DATETIME_ADD, DATETIME_SUB, DATE_ADD)
\i sql/concepts_postgres/postgres-functions.sql

-- 1) urine output
\i sql/concepts_postgres/firstday/urine_output_first_day.sql

-- 2) vitals
\i sql/concepts_postgres/firstday/vitals_first_day.sql

-- 3) gcs
\i sql/concepts_postgres/firstday/gcs_first_day.sql

-- 4) labs
\i sql/concepts_postgres/firstday/labs_first_day.sql

-- 5) blood gas (dependency first)
\i sql/concepts_postgres/firstday/blood_gas_first_day.sql
\i sql/concepts_postgres/firstday/blood_gas_first_day_arterial.sql

-- 6) ventilation (dependency first)
\i sql/concepts_postgres/durations/ventilation_classification.sql
\i sql/concepts_postgres/durations/ventilation_durations.sql

-- 7) echodata (safe stub)
CREATE OR REPLACE VIEW mimiciii_derived.echodata AS
SELECT
  NULL::int AS subject_id,
  NULL::int AS hadm_id,
  NULL::timestamp AS charttime,
  NULL::text AS text
WHERE FALSE;
