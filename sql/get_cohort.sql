/* Datei: sql/get_cohort.sql 
VERSION: MIMIC-III (3)
Zweck: Basis-Kohorte aller erwachsenen ICU-Patienten
*/

SELECT 
    ie.subject_id,
    ie.hadm_id,
    ie.icustay_id,          -- In MIMIC-3 heißt das icustay_id (nicht stay_id)
    ie.intime,
    ie.outtime,
    
    pat.gender,
    
    -- Alter berechnen: Aufnahmezeitpunkt minus Geburtsdatum
    -- (MIMIC-3 speichert kein fertiges Alter)
    ROUND( (CAST(EXTRACT(epoch FROM ie.intime - pat.dob) / (60*60*24*365.242) AS numeric)), 1) AS age,

    -- Aufenthaltsdauer in Tagen
    ROUND( (CAST(EXTRACT(epoch FROM ie.outtime - ie.intime) / 86400.0 AS numeric)), 2) AS los_days,
    
    -- Mortalität (hospital_expire_flag)
    adm.hospital_expire_flag

FROM icustays ie
INNER JOIN patients pat
    ON ie.subject_id = pat.subject_id
INNER JOIN admissions adm
    ON ie.hadm_id = adm.hadm_id

WHERE 
    -- Filter: Nur Erwachsene (Alter >= 18)
    (EXTRACT(epoch FROM ie.intime - pat.dob) / (60*60*24*365.242)) >= 18
    
    -- Filter: Mindestens 24h auf ICU
    AND (EXTRACT(epoch FROM ie.outtime - ie.intime) / 3600.0) >= 24

ORDER BY ie.subject_id, ie.intime
LIMIT 10000;