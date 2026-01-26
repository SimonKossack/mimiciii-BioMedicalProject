/* Datei: sql/create_cohort_respiratory.sql
   Zweck: Erstellt eine feste Tabelle 'cohort_respiratory' für Patienten mit Lungenversagen.
*/

-- 1. Alte Tabelle löschen
DROP TABLE IF EXISTS cohort_respiratory;

-- 2. Neue Tabelle erstellen
CREATE TABLE cohort_respiratory AS
WITH resp_diagnoses AS (
    SELECT DISTINCT hadm_id
    FROM diagnoses_icd
    WHERE icd9_code = '51881' 
),
vent_procedures AS (
    SELECT DISTINCT hadm_id
    FROM procedures_icd
    WHERE icd9_code IN ('9670', '9671', '9672')
)

SELECT 
    pat.subject_id,
    adm.hadm_id,
    adm.admittime,
    adm.dischtime,
    pat.gender,
    ROUND( (CAST(EXTRACT(epoch FROM adm.admittime - pat.dob) / (60*60*24*365.242) AS numeric)), 1) AS age,
    -- Hier war der Fehler: Wir prüfen gegen 'vent_procedures' (nicht rrt)
    CASE 
        WHEN vent.hadm_id IS NOT NULL THEN 1 
        ELSE 0 
    END AS intervention_ventilation,
    adm.hospital_expire_flag AS outcome_death,
    ROUND( (CAST(EXTRACT(epoch FROM adm.dischtime - adm.admittime) / 86400.0 AS numeric)), 2) AS los_days

FROM admissions adm
INNER JOIN patients pat ON adm.subject_id = pat.subject_id
INNER JOIN resp_diagnoses resp ON adm.hadm_id = resp.hadm_id
-- WICHTIG: Der Tabellenname hier muss mit dem Namen oben im WITH-Block übereinstimmen!
LEFT JOIN vent_procedures vent ON adm.hadm_id = vent.hadm_id

WHERE 
    (EXTRACT(epoch FROM adm.admittime - pat.dob) / (60*60*24*365.242)) >= 18;

-- 3. Index erstellen
CREATE INDEX idx_resp_hadm ON cohort_respiratory(hadm_id);