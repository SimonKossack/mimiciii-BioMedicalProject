SELECT 
    s.icustay_id,
    s.sofa_score,
    sap.sapsii_score,
    -- Wir schauen uns die Bausteine an, die beide nutzen
    g.mingcs as gcs_wert,
    v.sysbp_min as niedrigster_blutdruck
FROM sofa s
JOIN sapsii sap ON s.icustay_id = sap.icustay_id
JOIN gcs_first_day g ON s.icustay_id = g.icustay_id
JOIN vitals_first_day v ON s.icustay_id = v.icustay_id
WHERE s.sofa_score IS NOT NULL
ORDER BY s.sofa_score DESC
LIMIT 10;


SELECT 
    COUNT(*) as gesamt_patienten,
    COUNT(g.mingcs) as gcs_vorhanden,
    COUNT(v.sysbp_min) as blutdruck_vorhanden,
    COUNT(l.bun_max) as harnstoff_vorhanden, -- für SAPS II
    COUNT(l.sodium_max) as natrium_vorhanden,
    -- Berechnung der Prozentrate
    ROUND(100.0 * COUNT(g.mingcs) / COUNT(*), 2) as gcs_rate_prozent,
    ROUND(100.0 * COUNT(v.sysbp_min) / COUNT(*), 2) as blutdruck_rate_prozent
FROM sapsii s
LEFT JOIN gcs_first_day g ON s.icustay_id = g.icustay_id
LEFT JOIN vitals_first_day v ON s.icustay_id = v.icustay_id
LEFT JOIN labs_saps l ON s.icustay_id = l.icustay_id;


SELECT 
    COUNT(*) as gesamt_patienten,
    -- GCS & Vitals
    ROUND(100.0 * COUNT(g.mingcs) / COUNT(*), 2) as gcs_fuellrate,
    ROUND(100.0 * COUNT(v.sysbp_min) / COUNT(*), 2) as blutdruck_fuellrate,
    ROUND(100.0 * COUNT(v.heartrate_mean) / COUNT(*), 2) as puls_fuellrate,
    -- Laborwerte (SOFA & SAPS II)
    ROUND(100.0 * COUNT(l.creatinine_max) / COUNT(*), 2) as kreatinin_fuellrate,
    ROUND(100.0 * COUNT(l.bilirubin_max) / COUNT(*), 2) as bilirubin_fuellrate,
    ROUND(100.0 * COUNT(l.platelet_min) / COUNT(*), 2) as thrombozyten_fuellrate,
    -- Spezifische SAPS II Laborwerte
    ROUND(100.0 * COUNT(ls.bun_max) / COUNT(*), 2) as harnstoff_fuellrate,
    ROUND(100.0 * COUNT(ls.sodium_max) / COUNT(*), 2) as natrium_fuellrate,
    ROUND(100.0 * COUNT(ls.wbc_max) / COUNT(*), 2) as leukozyten_fuellrate,
    -- Output
    ROUND(100.0 * COUNT(uo.urineoutput) / COUNT(*), 2) as urin_fuellrate
FROM icustays ie
LEFT JOIN gcs_first_day g ON ie.icustay_id = g.icustay_id
LEFT JOIN vitals_first_day v ON ie.icustay_id = v.icustay_id
LEFT JOIN labs_first_day l ON ie.icustay_id = l.icustay_id
LEFT JOIN labs_saps ls ON ie.icustay_id = ls.icustay_id
LEFT JOIN urine_output_first_day uo ON ie.icustay_id = uo.icustay_id;



SELECT 
    'PaO2 (Sauerstoffpartialdruck)' AS parameter, 
    COUNT(*) AS anzahl 
FROM labevents 
WHERE itemid = 50821 -- Arterieller PaO2

UNION ALL

SELECT 
    'FiO2 (Eingestellter Sauerstoff)' AS parameter, 
    COUNT(*) 
FROM chartevents 
WHERE itemid IN (190, 3420, 223835) -- Gängige FiO2 IDs

UNION ALL

SELECT 
    'SpO2 (Sättigung - Fingerclip)' AS parameter, 
    COUNT(*) 
FROM chartevents 
WHERE itemid IN (646, 220227);



SELECT 
    COUNT(*) as total_rows,
    COUNT(rate_norepinephrine) as has_norepi,
    MAX(rate_norepinephrine) as max_norepi
FROM vaso_mv; -- oder vaso_cv

-- Prüfung für Metavision (neuere Daten)
SELECT 
    itemid, 
    label, 
    COUNT(*) as anzahl_einträge
FROM inputevents_mv mv
JOIN d_items di ON mv.itemid = di.itemid
WHERE di.label ILIKE '%norepinephrine%' 
   OR di.label ILIKE '%epinephrine%' 
   OR di.label ILIKE '%dopamine%'
GROUP BY itemid, label;


SELECT 
    mv.itemid, 
    di.label, 
    COUNT(*) as anzahl_einträge
FROM inputevents_mv mv
JOIN d_items di ON mv.itemid = di.itemid
WHERE di.label ILIKE '%norepinephrine%' 
   OR di.label ILIKE '%epinephrine%' 
   OR di.label ILIKE '%dopamine%'
   OR di.label ILIKE '%dobutamine%'
GROUP BY mv.itemid, di.label;


SELECT 
    mv.icustay_id,
    di.label,
    MAX(mv.rate) as max_rate
FROM inputevents_mv mv
JOIN d_items di ON mv.itemid = di.itemid
WHERE mv.itemid IN (221906, 221289, 221662, 221653)
AND mv.statusdescription != 'Rewritten'
GROUP BY mv.icustay_id, di.label
LIMIT 10;


SELECT cardiovascular, COUNT(*) 
FROM sofa 
GROUP BY cardiovascular 
ORDER BY cardiovascular;



WITH creatinine_counts AS (
    -- Zählt alle Kreatinin-Messungen pro Aufenthalt
    SELECT 
        hadm_id, 
        COUNT(*) as anzahl_kreatinin_messungen
    FROM labevents
    WHERE itemid IN (50912) -- Kreatinin
    GROUP BY hadm_id
)
SELECT 
    s.renal AS renal_sofa_score,
    ROUND(AVG(c.anzahl_kreatinin_messungen), 2) AS avg_messungen,
    COUNT(s.icustay_id) AS anzahl_patienten
FROM sofa s
JOIN creatinine_counts c ON s.hadm_id = c.hadm_id
GROUP BY s.renal
ORDER BY s.renal;