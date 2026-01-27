-- Wir löschen direkt im Ziel-Schema, egal was es ist
DROP MATERIALIZED VIEW IF EXISTS mimiciii_derived.urine_output_first_day CASCADE;
DROP VIEW IF EXISTS mimiciii_derived.urine_output_first_day CASCADE;
DROP TABLE IF EXISTS mimiciii_derived.urine_output_first_day CASCADE;

DROP MATERIALIZED VIEW IF EXISTS mimiciii_derived.vitals_first_day CASCADE;
DROP VIEW IF EXISTS mimiciii_derived.vitals_first_day CASCADE;
DROP TABLE IF EXISTS mimiciii_derived.vitals_first_day CASCADE;

DROP MATERIALIZED VIEW IF EXISTS mimiciii_derived.gcs_first_day CASCADE;
DROP VIEW IF EXISTS mimiciii_derived.gcs_first_day CASCADE;

DROP MATERIALIZED VIEW IF EXISTS mimiciii_derived.labs_first_day CASCADE;
DROP VIEW IF EXISTS mimiciii_derived.labs_first_day CASCADE;

DROP MATERIALIZED VIEW IF EXISTS mimiciii_derived.blood_gas_first_day CASCADE;
DROP VIEW IF EXISTS mimiciii_derived.blood_gas_first_day CASCADE;

DROP MATERIALIZED VIEW IF EXISTS mimiciii_derived.blood_gas_first_day_arterial CASCADE;
DROP VIEW IF EXISTS mimiciii_derived.blood_gas_first_day_arterial CASCADE;





DROP MATERIALIZED VIEW IF EXISTS mimiciii_derived.sofa CASCADE;
DROP VIEW IF EXISTS mimiciii_derived.sofa CASCADE;


DROP MATERIALIZED VIEW IF EXISTS mimiciii_derived.sofa CASCADE;

-- 2. Versuch: Falls es doch eine Tabelle war
DROP TABLE IF EXISTS mimiciii_derived.sofa CASCADE;


DROP MATERIALIZED VIEW IF EXISTS mimiciii_derived.sapsii CASCADE;


-- Beispiel für SAPS II
DROP MATERIALIZED VIEW IF EXISTS mimiciii_derived.sapsii CASCADE;
DROP VIEW IF EXISTS mimiciii_derived.sapsii CASCADE;
DROP TABLE IF EXISTS mimiciii_derived.sapsii CASCADE;




-- Löschen der Materialized Views im public Schema
DROP MATERIALIZED VIEW IF EXISTS public.urine_output_first_day CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.gcs_first_day CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.vitals_first_day CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.labs_first_day CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.sapsii CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.sofa CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.blood_gas_first_day_arterial CASCADE;

-- Beispiel für SAPS II im richtigen Schema
DROP MATERIALIZED VIEW IF EXISTS mimiciii_derived.sapsii CASCADE;
DROP VIEW IF EXISTS mimiciii_derived.sapsii CASCADE;
DROP TABLE IF EXISTS mimiciii_derived.sapsii CASCADE;



-- Löscht die Sichten im falschen Schema (public)
DROP MATERIALIZED VIEW IF EXISTS public.sapsii CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.sofa CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.urine_output_first_day CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.gcs_first_day CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.vitals_first_day CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.labs_first_day CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.blood_gas_first_day_arterial CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.ventilation_durations CASCADE;



SELECT count(*) FROM mimiciii_derived.vitals_6h;



SET search_path TO mimiciii_derived, mimic, public;

DROP TABLE IF EXISTS vitals_first_day CASCADE;
DROP TABLE IF EXISTS labs_first_day CASCADE;
DROP TABLE IF EXISTS gcs_first_day CASCADE;
DROP TABLE IF EXISTS urine_output_first_day CASCADE;
DROP TABLE IF EXISTS blood_gas_first_day CASCADE;
DROP TABLE IF EXISTS blood_gas_first_day_arterial CASCADE;

-- Falls du die 6h Namen schon angelegt hattest, löschen wir die auch einmal kurz:
DROP TABLE IF EXISTS vitals_6h CASCADE;
DROP TABLE IF EXISTS labs_6h CASCADE;
DROP TABLE IF EXISTS gcs_6h CASCADE;
DROP TABLE IF EXISTS urine_output_6h CASCADE;
DROP TABLE IF EXISTS blood_gas_6h CASCADE;
DROP TABLE IF EXISTS blood_gas_6h_arterial CASCADE;




SET search_path TO mimiciii_derived, mimic, public;

SELECT 'vitals_6h' as Tabelle, COUNT(*) as Anzahl_Zeilen FROM vitals_6h
UNION ALL
SELECT 'labs_6h', COUNT(*) FROM labs_6h
UNION ALL
SELECT 'gcs_6h', COUNT(*) FROM gcs_6h
UNION ALL
SELECT 'urine_output_6h', COUNT(*) FROM urine_output_6h
UNION ALL
SELECT 'blood_gas_6h_arterial', COUNT(*) FROM blood_gas_6h_arterial;


SET search_path TO mimiciii_derived, mimic, public;

SELECT 'vitals_6h' as Tabelle, COUNT(*) as Anzahl_Zeilen FROM vitals_6h
UNION ALL
SELECT 'labs_6h', COUNT(*) FROM labs_6h
UNION ALL
SELECT 'gcs_6h', COUNT(*) FROM gcs_6h
UNION ALL
SELECT 'urine_output_6h', COUNT(*) FROM urine_output_6h
UNION ALL
SELECT 'blood_gas_6h_arterial', COUNT(*) FROM blood_gas_6h_arterial;




SELECT 
    (SELECT COUNT(DISTINCT icustay_id) FROM mimiciii_derived.vitals_6h) as vitals_patients,
    (SELECT COUNT(DISTINCT icustay_id) FROM mimiciii_derived.labs_6h) as labs_patients,
    (SELECT COUNT(DISTINCT icustay_id) FROM mimiciii_derived.gcs_6h) as gcs_patients;





SELECT 
    s6.icustay_id, 
    s24.sofa as sofa_24h, 
    s6.sofa as sofa_6h,
    (s24.sofa - s6.sofa) as differenz
FROM mimiciii_derived.sofa6h s6
JOIN mimiciii_derived.sofa s24 ON s6.icustay_id = s24.icustay_id
WHERE s24.sofa IS NOT NULL
LIMIT 20;

SELECT 
    'SOFA 6h' as zeitpunkt, AVG(sofa) as avg_score, MAX(sofa) as max_score
FROM mimiciii_derived.sofa6h
UNION ALL
SELECT 
    'SOFA 24h', AVG(sofa), MAX(sofa)
FROM mimiciii_derived.sofa;



SELECT 
    MIN(DATETIME_DIFF(ce.charttime, ie.intime, 'HOUR')) as min_stunden,
    MAX(DATETIME_DIFF(ce.charttime, ie.intime, 'HOUR')) as max_stunden
FROM mimiciii_derived.vitals_12h v
JOIN mimiciii.icustays ie ON v.icustay_id = ie.icustay_id;


SELECT 
    sofa, 
    COUNT(*) as anzahl_patienten,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as prozent
FROM mimiciii_derived.sofa12h
GROUP BY sofa
ORDER BY sofa;



SELECT 
    '06h' AS Fenster, 
    COUNT(*) AS Patienten, 
    ROUND(AVG(sofa), 2) AS Avg_SOFA,
    ROUND(STDDEV(sofa), 2) AS Std_Dev
FROM mimiciii_derived.sofa6h
UNION ALL
SELECT 
    '12h', 
    COUNT(*), 
    ROUND(AVG(sofa), 2),
    ROUND(STDDEV(sofa), 2)
FROM mimiciii_derived.sofa12h
UNION ALL
SELECT 
    '24h (Standard)', 
    COUNT(*), 
    ROUND(AVG(sofa), 2),
    ROUND(STDDEV(sofa), 2)
FROM mimiciii_derived.sofa; -- Deine Original-Tabelle


SELECT 
    s12.icustay_id,
    s6.sofa as sofa_6h,
    s12.sofa as sofa_12h,
    s.sofa as sofa_24h,
    (s.sofa - s6.sofa) as delta_6_zu_24
FROM mimiciii_derived.sofa12h s12
JOIN mimiciii_derived.sofa6h s6 ON s12.icustay_id = s6.icustay_id
JOIN mimiciii_derived.sofa s ON s12.icustay_id = s.icustay_id
WHERE (s.sofa - s6.sofa) > 2 -- Zeige nur Patienten, die um mehr als 2 Punkte steigen
LIMIT 10;