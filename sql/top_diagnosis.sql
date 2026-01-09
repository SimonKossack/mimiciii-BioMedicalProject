/* Datei: sql/top_diagnoses.sql
   Zweck: Die 20 häufigsten Diagnosen in MIMIC-III finden (mit Klarnamen)
*/

SELECT 
    d.icd9_code,
    -- Wir holen den kurzen Titel für die Grafik und den langen für Details
    dict.short_title,
    dict.long_title,
    COUNT(*) as frequency
    
FROM diagnoses_icd d
-- Verknüpfung mit dem Wörterbuch, um Text statt Nummern zu bekommen
JOIN d_icd_diagnoses dict 
    ON d.icd9_code = dict.icd9_code

GROUP BY d.icd9_code, dict.short_title, dict.long_title
ORDER BY frequency DESC
LIMIT 20;