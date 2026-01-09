/* Datei: sql/create_cohort_aki.sql
   Zweck: Erstellt eine feste Tabelle 'cohort_aki' in der Datenbank.
*/

-- 1. Alte Tabelle löschen, falls sie existiert (für sauberen Neustart)
DROP TABLE IF EXISTS cohort_aki;

-- 2. Neue Tabelle erstellen
CREATE TABLE cohort_aki AS
WITH aki_diagnoses AS (
    -- Finde alle Admissions mit AKI (ICD9 beginnt mit 584)
    SELECT DISTINCT hadm_id
    FROM diagnoses_icd
    WHERE icd9_code LIKE '584%%' -- Wichtig: %% für Python
),
rrt_procedures AS (
    -- Finde alle Admissions mit Dialyse (RRT) Prozeduren
    SELECT DISTINCT hadm_id
    FROM procedures_icd
    WHERE icd9_code IN ('3995', '5498', 'V560', 'V561', 'V451')
)

SELECT 
    pat.subject_id,
    adm.hadm_id,
    
    -- Zeitstempel (wichtig für spätere Zeitreihen)
    adm.admittime,
    adm.dischtime,
    
    -- Demografie
    pat.gender,
    -- Alter berechnen
    ROUND( (CAST(EXTRACT(epoch FROM adm.admittime - pat.dob) / (60*60*24*365.242) AS numeric)), 1) AS age,
    
    -- Intervention (Feature): Hat Dialyse bekommen? (1=Ja, 0=Nein)
    CASE 
        WHEN rrt.hadm_id IS NOT NULL THEN 1 
        ELSE 0 
    END AS intervention_dialysis,
    
    -- Outcome (Label): Tod im Krankenhaus?
    adm.hospital_expire_flag AS outcome_death,
    
    -- Aufenthaltsdauer (LOS) in Tagen
    ROUND( (CAST(EXTRACT(epoch FROM adm.dischtime - adm.admittime) / 86400.0 AS numeric)), 2) AS los_days

FROM admissions adm
INNER JOIN patients pat ON adm.subject_id = pat.subject_id
INNER JOIN aki_diagnoses aki ON adm.hadm_id = aki.hadm_id
LEFT JOIN rrt_procedures rrt ON adm.hadm_id = rrt.hadm_id

WHERE 
    -- Nur Erwachsene
    (EXTRACT(epoch FROM adm.admittime - pat.dob) / (60*60*24*365.242)) >= 18;

-- 3. Kleiner Index, damit zukünftige Abfragen rasend schnell sind
CREATE INDEX idx_aki_hadm ON cohort_aki(hadm_id);