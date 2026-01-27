# src/trajectories.py
import pandas as pd
from src.db import q

def get_parameter_trajectory(df_aki, parameter_type='creatinine', hours=72):
    """
    Extrahiert den Verlauf eines Parameters für die AKI-Kohorte.
    Korrigiert für Tabellen ohne icustay_id (wie labevents).
    """
    ids = tuple(df_aki['icustay_id'].unique())
    
    # Konfiguration der MIMIC-Parameter
    # WICHTIG: Wir unterscheiden jetzt, ob wir über icustay_id oder hadm_id joinen
    config = {
        'creatinine': {'table': 'labevents', 'ids': '(50912)', 'col': 'valuenum', 'join_col': 'hadm_id'},
        'urine': {'table': 'outputevents', 'ids': '(40055, 43175, 40069, 40094, 40715)', 'col': 'value', 'join_col': 'icustay_id'},
        'map': {'table': 'chartevents', 'ids': '(456, 52, 6702, 443, 220052, 220181, 225312)', 'col': 'valuenum', 'join_col': 'icustay_id'}
    }
    
    c = config[parameter_type]
    
    query = f"""
    WITH buckets AS (
        SELECT generate_series(0, {hours-6}, 6) AS hr_start
    ),
    measurements AS (
        SELECT 
            ie.icustay_id,
            EXTRACT(EPOCH FROM (m.charttime - ie.intime))/3600 AS hrs,
            m.{c['col']} AS val
        FROM icustays ie
        JOIN {c['table']} m ON ie.{c['join_col']} = m.{c['join_col']}
        WHERE ie.icustay_id IN {ids}
          AND m.itemid IN {c['ids']}
          AND m.charttime >= ie.intime
          AND m.charttime <= ie.intime + interval '{hours} hours'
    )
    SELECT 
        m.icustay_id,
        b.hr_start,
        AVG(m.val) as value
    FROM buckets b
    LEFT JOIN measurements m ON m.hrs >= b.hr_start AND m.hrs < b.hr_start + 6
    WHERE m.icustay_id IS NOT NULL
    GROUP BY m.icustay_id, b.hr_start
    ORDER BY m.icustay_id, b.hr_start
    """
    return q(query)

def add_time_to_vaso(df_aki):
    # Holen der Startzeitpunkte der Vasopressoren
    first_vaso = q("""
        SELECT icustay_id, MIN(starttime) as first_vaso_time
        FROM inputevents_mv
        WHERE itemid IN (221906, 221289, 221749, 222315) -- Norepi, Epi, Phenylephrin, Vasopressin
        GROUP BY icustay_id
    """)
    
    # Merge mit df_aki um intime zu erhalten
    df = df_aki.merge(first_vaso, on='icustay_id', how='left')
    
    # Differenz in Stunden berechnen
    df['hours_until_vaso'] = (df['first_vaso_time'] - df['intime']).dt.total_seconds() / 3600
    return df

def add_dialysis_flag(df_aki):
    """
    Markiert Patienten, die während ihres Aufenthalts eine Dialyse (RRT) erhalten haben.
    """
    # ItemIDs für Dialyse in MIMIC-III (CRRT, Hämodialyse, etc.)
    dialysis_ids = (
        152, 148, 149, 146, 147, 151, 150, # Alte IDs
        227036, 224308, 224385, 225135, 225183, 225802 # Metavision IDs
    )
    
    query = f"""
    SELECT DISTINCT icustay_id
    FROM chartevents
    WHERE itemid IN {dialysis_ids}
      AND icustay_id IN {tuple(df_aki['icustay_id'].unique())}
    """
    dialysis_patients = q(query)
    
    # Flag setzen: 1 für Dialyse, 0 für keine Dialyse
    df_aki['had_dialysis'] = 0
    df_aki.loc[df_aki['icustay_id'].isin(dialysis_patients['icustay_id']), 'had_dialysis'] = 1
    
    return df_aki


def get_sofa_at_time(hours):
    """
    Berechnet den SOFA-Score für ein spezifisches Zeitfenster (Snapshot).
    Ersetzt das Standard-Intervall von 24h durch den Parameter 'hours'.
    """
    # Der bereinigte SQL-String ohne psql-Sonderbefehle
    raw_sofa_sql = """
    with wt AS
    (
      SELECT ie.icustay_id
        , avg(CASE
            WHEN itemid IN (762, 763, 3723, 3580, 226512) THEN valuenum
            WHEN itemid IN (3581) THEN valuenum * 0.45359237
            WHEN itemid IN (3582) THEN valuenum * 0.0283495231
            ELSE null
          END) AS weight
      FROM icustays ie
      left join chartevents c on ie.icustay_id = c.icustay_id
      WHERE valuenum IS NOT NULL
      AND itemid IN (762, 763, 3723, 3580, 3581, 3582, 226512)
      AND valuenum != 0
      and charttime between DATETIME_SUB(ie.intime, INTERVAL '1' DAY) and DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
      AND (c.error IS NULL OR c.error = 0)
      group by ie.icustay_id
    ),
    echo2 as(
      select ie.icustay_id, avg(weight * 0.45359237) as weight
      FROM icustays ie
      left join echo_data echo on ie.hadm_id = echo.hadm_id
        and echo.charttime > DATETIME_SUB(ie.intime, INTERVAL '7' DAY)
        and echo.charttime < DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
      group by ie.icustay_id
    ),
    vaso_cv as (
      select ie.icustay_id
        , max(case
                when itemid = 30047 then rate / coalesce(wt.weight,ec.weight)
                when itemid = 30120 then rate
                else null
              end) as rate_norepinephrine
        , max(case
                when itemid =  30044 then rate / coalesce(wt.weight,ec.weight)
                when itemid in (30119,30309) then rate
                else null
              end) as rate_epinephrine
        , max(case when itemid in (30043,30307) then rate end) as rate_dopamine
        , max(case when itemid in (30042,30306) then rate end) as rate_dobutamine
      FROM icustays ie
      inner join inputevents_cv cv on ie.icustay_id = cv.icustay_id 
        and cv.charttime between ie.intime and DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
      left join wt on ie.icustay_id = wt.icustay_id
      left join echo2 ec on ie.icustay_id = ec.icustay_id
      where itemid in (30047,30120,30044,30119,30309,30043,30307,30042,30306)
      and rate is not null
      group by ie.icustay_id
    ),
    vaso_mv as (
      select ie.icustay_id
        , max(case when itemid = 221906 then rate end) as rate_norepinephrine
        , max(case when itemid = 221289 then rate end) as rate_epinephrine
        , max(case when itemid = 221662 then rate end) as rate_dopamine
        , max(case when itemid = 221653 then rate end) as rate_dobutamine
      FROM icustays ie
      inner join inputevents_mv mv on ie.icustay_id = mv.icustay_id 
        and mv.starttime between ie.intime and DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
      where itemid in (221906,221289,221662,221653)
      and statusdescription != 'Rewritten'
      group by ie.icustay_id
    ),
    pafi1 as (
      select bg.icustay_id, bg.charttime, pao2fio2
      , case when vd.icustay_id is not null then 1 else 0 end as isvent
      from blood_gas_first_day_arterial bg
      left join ventilation_durations vd on bg.icustay_id = vd.icustay_id
        and bg.charttime >= vd.starttime and bg.charttime <= vd.endtime
    ),
    pafi2 as (
      select icustay_id
      , min(case when isvent = 0 then pao2fio2 else null end) as pao2fio2_novent_min
      , min(case when isvent = 1 then pao2fio2 else null end) as pao2fio2_vent_min
      from pafi1 group by icustay_id
    ),
    scorecomp as (
      select ie.icustay_id, v.meanbp_min
      , coalesce(cv.rate_norepinephrine, mv.rate_norepinephrine) as rate_norepinephrine
      , coalesce(cv.rate_epinephrine, mv.rate_epinephrine) as rate_epinephrine
      , coalesce(cv.rate_dopamine, mv.rate_dopamine) as rate_dopamine
      , coalesce(cv.rate_dobutamine, mv.rate_dobutamine) as rate_dobutamine
      , l.creatinine_max, l.bilirubin_max, l.platelet_min
      , pf.pao2fio2_novent_min, pf.pao2fio2_vent_min
      , uo.urineoutput, gcs.mingcs
      FROM icustays ie
      left join vaso_cv cv on ie.icustay_id = cv.icustay_id
      left join vaso_mv mv on ie.icustay_id = mv.icustay_id
      left join pafi2 pf on ie.icustay_id = pf.icustay_id
      left join vitals_first_day v on ie.icustay_id = v.icustay_id
      left join labs_first_day l on ie.icustay_id = l.icustay_id
      left join urine_output_first_day uo on ie.icustay_id = uo.icustay_id
      left join gcs_first_day gcs on ie.icustay_id = gcs.icustay_id
    ),
    scorecalc as (
      select icustay_id
      , case
          when pao2fio2_vent_min < 100 OR pao2fio2_novent_min < 100 then 4
          when pao2fio2_vent_min < 200 OR pao2fio2_novent_min < 200 then 3
          when pao2fio2_novent_min < 300 then 2
          when pao2fio2_novent_min < 400 then 1
          when coalesce(pao2fio2_vent_min, pao2fio2_novent_min) is null then null
          else 0
        end as respiration
      , case
          when platelet_min < 20  then 4
          when platelet_min < 50  then 3
          when platelet_min < 100 then 2
          when platelet_min < 150 then 1
          when platelet_min is null then null
          else 0
        end as coagulation
      , case
            when bilirubin_max >= 12.0 then 4
            when bilirubin_max >= 6.0  then 3
            when bilirubin_max >= 2.0  then 2
            when bilirubin_max >= 1.2  then 1
            when bilirubin_max is null then null
            else 0
          end as liver
      , case
          when rate_dopamine > 15 or rate_epinephrine > 0.1 or rate_norepinephrine > 0.1 then 4
          when rate_dopamine > 5 or rate_epinephrine <= 0.1 or rate_norepinephrine <= 0.1 then 3
          when rate_dopamine > 0 or rate_dobutamine > 0 then 2
          when meanbp_min < 70 then 1
          when coalesce(meanbp_min, rate_dopamine, rate_dobutamine, rate_epinephrine, rate_norepinephrine) is null then null
          else 0
        end as cardiovascular
      , case
          when (mingcs >= 13 and mingcs <= 14) then 1
          when (mingcs >= 10 and mingcs <= 12) then 2
          when (mingcs >= 6 and mingcs <= 9) then 3
          when mingcs < 6 then 4
          when mingcs is null then null
          else 0 end as cns
      , case
        when (creatinine_max >= 5.0) then 4
        when urineoutput < 200 then 4
        when (creatinine_max >= 3.5 and creatinine_max < 5.0) then 3
        when urineoutput < 500 then 3
        when (creatinine_max >= 2.0 and creatinine_max < 3.5) then 2
        when (creatinine_max >= 1.2 and creatinine_max < 2.0) then 1
        when coalesce(urineoutput, creatinine_max) is null then null
        else 0 end as renal
      from scorecomp
    )
    select icustay_id
      , coalesce(respiration,0) + coalesce(coagulation,0) + coalesce(liver,0) + 
        coalesce(cardiovascular,0) + coalesce(cns,0) + coalesce(renal,0) as SOFA
      , respiration, coagulation, liver, cardiovascular, cns, renal
    FROM scorecalc
    """
    
    # Hier wird das Zeitfenster dynamisch angepasst
    current_query = raw_sofa_sql.replace("INTERVAL '1' DAY", f"INTERVAL '{hours}' HOUR")
    
    return q(current_query)

def check_data_availability(hours):
    query = f"""
    WITH t_patients AS (
        SELECT icustay_id, hadm_id, intime, 
               DATETIME_ADD(intime, INTERVAL '{hours}' HOUR) as outtime
        FROM icustays
    ),
    bp AS (
        SELECT ie.icustay_id, COUNT(*) as cnt
        FROM t_patients ie
        INNER JOIN chartevents ce ON ie.icustay_id = ce.icustay_id
        WHERE ce.itemid IN (456,52,6702,443,220052,220181,225312)
        AND ce.charttime BETWEEN ie.intime AND ie.outtime
        GROUP BY ie.icustay_id
    ),
    crea AS (
        SELECT ie.icustay_id, COUNT(*) as cnt
        FROM t_patients ie
        INNER JOIN labevents le ON ie.hadm_id = le.hadm_id
        WHERE le.itemid IN (50912)
        AND le.charttime BETWEEN ie.intime AND ie.outtime
        GROUP BY ie.icustay_id
    )
    SELECT 
        (SELECT COUNT(*) FROM t_patients) as total_patients,
        (SELECT COUNT(*) FROM bp) as bp_data,
        (SELECT COUNT(*) FROM crea) as creatinine_data
    """
    return q(query)