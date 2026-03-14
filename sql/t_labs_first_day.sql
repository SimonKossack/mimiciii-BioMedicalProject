-- Erstellt den Laborwerte-Baustein für SOFA
DROP MATERIALIZED VIEW IF EXISTS labs_first_day CASCADE;
CREATE MATERIALIZED VIEW labs_first_day AS
SELECT pvt.subject_id, pvt.hadm_id, pvt.icustay_id
  , min(case when label = 'PLATELET' then valuenum else null end) as platelet_min
  , max(case when label = 'BILIRUBIN' then valuenum else null end) as bilirubin_max
  , max(case when label = 'CREATININE' then valuenum else null end) as creatinine_max
FROM (
  SELECT ie.subject_id, ie.hadm_id, ie.icustay_id
  , CASE
        WHEN itemid IN (50912, 51081) THEN 'CREATININE'
        WHEN itemid IN (50885) THEN 'BILIRUBIN'
        WHEN itemid IN (51265) THEN 'PLATELET'
    END AS label
  , valuenum
  FROM icustays ie
  LEFT JOIN labevents l ON ie.hadm_id = l.hadm_id
    AND l.charttime BETWEEN (ie.intime - INTERVAL '6 hours') AND (ie.intime + INTERVAL '1 day')
    AND l.itemid IN (50912, 51081, 50885, 51265)
) pvt
GROUP BY pvt.subject_id, pvt.hadm_id, pvt.icustay_id;