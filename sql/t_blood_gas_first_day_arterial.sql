DROP MATERIALIZED VIEW IF EXISTS blood_gas_first_day_arterial CASCADE;
CREATE MATERIALIZED VIEW blood_gas_first_day_arterial AS
WITH stg_fio2 AS (
  SELECT icustay_id, charttime, 
    -- Normalisierung: FiO2 wird oft als 21-100 oder 0.21-1.0 gespeichert
    CASE WHEN valuenum > 0.20 AND valuenum <= 1.0 THEN valuenum * 100
         WHEN valuenum > 20 AND valuenum <= 100 THEN valuenum
         ELSE NULL END AS fio2
  FROM chartevents
  WHERE itemid IN (3420, 190, 223835, 3422) -- ItemIDs für FiO2
),
stg_spo2 AS (
  SELECT icustay_id, charttime, valuenum AS spo2
  FROM chartevents
  WHERE itemid IN (646, 220227) AND valuenum > 0 AND valuenum <= 100
)
SELECT 
    ie.icustay_id,
    -- Wir berechnen den minimalen S/F-Quotienten (Ersatz für Horovitz)
    MIN(s.spo2 / (f.fio2/100.0)) as sf_ratio_min
FROM icustays ie
LEFT JOIN stg_spo2 s ON ie.icustay_id = s.icustay_id
    AND s.charttime BETWEEN ie.intime AND (ie.intime + INTERVAL '1 day')
LEFT JOIN stg_fio2 f ON ie.icustay_id = f.icustay_id
    AND f.charttime BETWEEN ie.intime AND (ie.intime + INTERVAL '1 day')
GROUP BY ie.icustay_id;