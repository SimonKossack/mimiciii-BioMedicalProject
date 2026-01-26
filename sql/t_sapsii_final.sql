DROP MATERIALIZED VIEW IF EXISTS sapsii CASCADE;
CREATE MATERIALIZED VIEW sapsii AS
WITH cohort AS (
  SELECT ie.subject_id, ie.hadm_id, ie.icustay_id, ie.intime
    , ROUND((EXTRACT(EPOCH FROM ie.intime - p.dob) / 31557600)::numeric, 2) AS age
    , adm.admission_type
  FROM icustays ie
  INNER JOIN patients p ON ie.subject_id = p.subject_id
  INNER JOIN admissions adm ON ie.hadm_id = adm.hadm_id
)
SELECT 
    c.icustay_id,
    -- Hier beginnt die SAPS II Punkte-Logik (vereinfacht)
    (CASE 
        WHEN age < 40 THEN 0 WHEN age < 60 THEN 7 WHEN age < 70 THEN 12 
        WHEN age < 75 THEN 15 WHEN age < 80 THEN 16 ELSE 18 END) +
    (CASE WHEN g.mingcs < 3 THEN 26 WHEN g.mingcs < 6 THEN 13 WHEN g.mingcs < 9 THEN 10 
          WHEN g.mingcs < 11 THEN 7 WHEN g.mingcs < 14 THEN 5 ELSE 0 END) +
    (CASE WHEN v.sysbp_min < 70 THEN 13 WHEN v.sysbp_min < 100 THEN 5 
          WHEN v.sysbp_min < 200 THEN 0 ELSE 2 END) as sapsii_score
FROM cohort c
LEFT JOIN gcs_first_day g ON c.icustay_id = g.icustay_id
LEFT JOIN vitals_first_day v ON c.icustay_id = v.icustay_id;