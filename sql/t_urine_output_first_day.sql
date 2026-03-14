-- Erstellt den Urin-Baustein für SOFA
DROP MATERIALIZED VIEW IF EXISTS urine_output_first_day CASCADE;
CREATE MATERIALIZED VIEW urine_output_first_day AS
select
  ie.subject_id, ie.hadm_id, ie.icustay_id
  , sum(VALUE) as urineoutput
from icustays ie
left join outputevents oe
  on ie.icustay_id = oe.icustay_id
  -- Zeitfenster: Ersten 24 Stunden
  and oe.charttime between ie.intime and (ie.intime + INTERVAL '1 day')
where oe.itemid in
(
  -- Alle relevanten Urin-ItemIDs
  40055, -- Urine Out Foley
  43175, -- Urine .
  40069, -- Urine Out Void
  40094, -- Urine Out Condom Cath
  40715, -- Urine Out Suprapubic
  40473, -- Urine Out IleoConduit
  40085, -- Urine Out Incontinent
  40057, -- Urine Out Rt Nephrostomy
  40056, -- Urine Out Lt Nephrostomy
  40405, -- Urine Out Other
  226559, -- Foley
  226560, -- Void
  226561, -- Condom Cath
  226584, -- Ileoconduit
  226563, -- Suprapubic
  226564, -- R Nephrostomy
  226565, -- L Nephrostomy
  226567, -- Straight Cath
  226557, -- R Ureteral Stent
  226558  -- L Ureteral Stent
)
group by ie.subject_id, ie.hadm_id, ie.icustay_id;