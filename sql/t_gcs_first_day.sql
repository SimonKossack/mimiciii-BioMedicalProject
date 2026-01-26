-- Erstellt den GCS-Baustein für SOFA/SAPS II
DROP MATERIALIZED VIEW IF EXISTS gcs_first_day CASCADE;
CREATE MATERIALIZED VIEW gcs_first_day AS
with base as
(
  SELECT pvt.ICUSTAY_ID
  , pvt.charttime
  , max(case when pvt.itemid = 454 then pvt.valuenum else null end) as GCSMotor
  , max(case when pvt.itemid = 723 then pvt.valuenum else null end) as GCSVerbal
  , max(case when pvt.itemid = 184 then pvt.valuenum else null end) as GCSEyes
  , case
      when max(case when pvt.itemid = 723 then pvt.valuenum else null end) = 0 then 1
      else 0
    end as EndoTrachFlag
  , ROW_NUMBER () OVER (PARTITION BY pvt.ICUSTAY_ID ORDER BY pvt.charttime ASC) as rn
  FROM (
    select l.ICUSTAY_ID
    , case
        when l.ITEMID in (723,223900) then 723
        when l.ITEMID in (454,223901) then 454
        when l.ITEMID in (184,220739) then 184
        else l.ITEMID end as ITEMID
    , case
        when l.ITEMID = 723 and l.VALUE = '1.0 ET/Trach' then 0 
        when l.ITEMID = 223900 and l.VALUE = 'No Response-ETT' then 0 
        else VALUENUM
      end as VALUENUM
    , l.CHARTTIME
    FROM chartevents l
    inner join icustays b on l.icustay_id = b.icustay_id
    where l.ITEMID in (184, 454, 723, 223900, 223901, 220739)
    -- Zeitfenster: Ersten 24 Stunden
    and l.charttime between b.intime and (b.intime + INTERVAL '1 day')
    AND (l.error IS NULL OR l.error = 0)
  ) pvt
  group by pvt.ICUSTAY_ID, pvt.charttime
)
, gcs as (
  select b.*
  , b2.GCSVerbal as GCSVerbalPrev
  , b2.GCSMotor as GCSMotorPrev
  , b2.GCSEyes as GCSEyesPrev
  , case
      when b.GCSVerbal = 0 then 15
      when b.GCSVerbal is null and b2.GCSVerbal = 0 then 15
      when b2.GCSVerbal = 0 then
          coalesce(b.GCSMotor,6) + coalesce(b.GCSVerbal,5) + coalesce(b.GCSEyes,4)
      else
          coalesce(b.GCSMotor,coalesce(b2.GCSMotor,6)) + 
          coalesce(b.GCSVerbal,coalesce(b2.GCSVerbal,5)) + 
          coalesce(b.GCSEyes,coalesce(b2.GCSEyes,4))
      end as GCS
  from base b
  left join base b2 on b.ICUSTAY_ID = b2.ICUSTAY_ID and b.rn = b2.rn+1 
    and b2.charttime > (b.charttime - INTERVAL '6 hours')
)
, gcs_final as (
  select gcs.*
  , ROW_NUMBER () OVER (PARTITION BY gcs.ICUSTAY_ID ORDER BY gcs.GCS ASC) as IsMinGCS
  from gcs
)
select ie.subject_id, ie.hadm_id, ie.icustay_id
, GCS as mingcs
, coalesce(GCSMotor,GCSMotorPrev) as gcsmotor
, coalesce(GCSVerbal,GCSVerbalPrev) as gcsverbal
, coalesce(GCSEyes,GCSEyesPrev) as gcseyes
, EndoTrachFlag as endotrachflag
FROM icustays ie
left join gcs_final gs on ie.icustay_id = gs.icustay_id and gs.IsMinGCS = 1;