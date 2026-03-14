-- Erstellt den Vitalwerte-Baustein (Blutdruck etc.) für SOFA
DROP MATERIALIZED VIEW IF EXISTS vitals_first_day CASCADE;
CREATE MATERIALIZED VIEW vitals_first_day AS
SELECT pvt.subject_id, pvt.hadm_id, pvt.icustay_id
  , min(case when VitalID = 'HeartRate' then valuenum else null end) as heartrate_min
  , max(case when VitalID = 'HeartRate' then valuenum else null end) as heartrate_max
  , avg(case when VitalID = 'HeartRate' then valuenum else null end) as heartrate_mean
  , min(case when VitalID = 'SysBP' then valuenum else null end) as sysbp_min
  , max(case when VitalID = 'SysBP' then valuenum else null end) as sysbp_max
  , avg(case when VitalID = 'SysBP' then valuenum else null end) as sysbp_mean
  , min(case when VitalID = 'DiasBP' then valuenum else null end) as diasbp_min
  , max(case when VitalID = 'DiasBP' then valuenum else null end) as diasbp_max
  , avg(case when VitalID = 'DiasBP' then valuenum else null end) as diasbp_mean
  , min(case when VitalID = 'MeanBP' then valuenum else null end) as meanbp_min
  , max(case when VitalID = 'MeanBP' then valuenum else null end) as meanbp_max
  , avg(case when VitalID = 'MeanBP' then valuenum else null end) as meanbp_mean
FROM (
  select ie.subject_id, ie.hadm_id, ie.icustay_id
  , case
      when itemid in (211,220045) then 'HeartRate'
      when itemid in (51,442,455,6701,220179,220050) then 'SysBP'
      when itemid in (8368,8440,8441,8555,220180,220051) then 'DiasBP'
      when itemid in (456,52,6702,443,220052,220181,225312) then 'MeanBP'
    end as VitalID
  , valuenum
  from icustays ie
  left join chartevents c on ie.icustay_id = c.icustay_id
    and c.charttime between ie.intime and (ie.intime + INTERVAL '1 day')
    and c.error IS DISTINCT FROM 1
  where itemid in (211,220045,51,442,455,6701,220179,220050,8368,8440,8441,8555,220180,220051,456,52,6702,443,220052,220181,225312)
) pvt
group by pvt.subject_id, pvt.hadm_id, pvt.icustay_id;