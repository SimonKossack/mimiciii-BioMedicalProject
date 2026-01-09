/* Datei: sql/check_diseases.sql
   Zweck: Vergleich der Fallzahlen (Korrigiert für Python: %% statt %)
*/

SELECT 
    CASE 
        -- WICHTIG: In Python müssen wir %% schreiben, damit ein % ankommt!
        WHEN icd9_code LIKE '584%%' THEN 'Akutes Nierenversagen (AKI)'
        WHEN icd9_code LIKE '428%%' THEN 'Herzinsuffizienz (CHF)'
        WHEN icd9_code LIKE '578%%' THEN 'GI Blutung'
        WHEN icd9_code LIKE '430%%' OR icd9_code LIKE '431%%' OR icd9_code LIKE '432%%' THEN 'Hirnblutung/Stroke'
        ELSE 'Andere'
    END AS disease_group,
    COUNT(*) as num_patients,
    -- Berechnung der Sterblichkeit innerhalb dieser Gruppen
    ROUND(CAST(SUM(CASE WHEN adm.hospital_expire_flag = 1 THEN 1 ELSE 0 END) AS numeric) / COUNT(*) * 100, 2) as mortality_rate_percent

FROM diagnoses_icd d
INNER JOIN admissions adm
    ON d.hadm_id = adm.hadm_id

WHERE 
    icd9_code LIKE '584%%'  -- Kidney Failure
    OR icd9_code LIKE '428%%'  -- Heart Failure
    OR icd9_code LIKE '578%%'  -- GI Bleeding
    OR icd9_code LIKE '430%%' OR icd9_code LIKE '431%%' -- Stroke

GROUP BY disease_group
ORDER BY num_patients DESC;