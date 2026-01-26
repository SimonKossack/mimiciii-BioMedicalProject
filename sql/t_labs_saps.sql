DROP MATERIALIZED VIEW IF EXISTS labs_saps CASCADE;
CREATE MATERIALIZED VIEW labs_saps AS
SELECT pvt.icustay_id
  , max(case when label = 'BUN' then valuenum else null end) as bun_max
  , max(case when label = 'SODIUM' then valuenum else null end) as sodium_max
  , min(case when label = 'SODIUM' then valuenum else null end) as sodium_min
  , max(case when label = 'POTASSIUM' then valuenum else null end) as potassium_max
  , min(case when label = 'POTASSIUM' then valuenum else null end) as potassium_min
  , max(case when label = 'WBC' then valuenum else null end) as wbc_max
  , min(case when label = 'WBC' then valuenum else null end) as wbc_min
FROM (
  SELECT ie.icustay_id
  , CASE
        WHEN itemid IN (51006) THEN 'BUN'
        WHEN itemid IN (50824, 50983) THEN 'SODIUM'
        WHEN itemid IN (50822, 50971) THEN 'POTASSIUM'
        WHEN itemid IN (51300, 51301) THEN 'WBC'
    END AS label
  , valuenum
  FROM icustays ie
  LEFT JOIN labevents l ON ie.hadm_id = l.hadm_id
    AND l.charttime BETWEEN (ie.intime - INTERVAL '6 hours') AND (ie.intime + INTERVAL '1 day')
) pvt
GROUP BY pvt.icustay_id;