/* Datei: sql/exclusions.sql
   Zweck: Identifiziert Patienten mit Chronischer Nierenkrankheit (CKD) oder Krebs
*/

SELECT 
    d.hadm_id,
    
    -- 1. Chronische Nierenkrankheit (CKD)
    -- Codes: 585.x (Chronic kidney disease)
    MAX(CASE WHEN d.icd9_code LIKE '585%%' THEN 1 ELSE 0 END) as has_ckd,
    
    -- 2. Metastasierender Krebs / Lymphom / Leukämie
    -- Codes: 196.x-199.x (Metastasen), 200.x-208.x (Hämatologisch)
    MAX(CASE 
        WHEN d.icd9_code LIKE '196%%' OR d.icd9_code LIKE '197%%' 
          OR d.icd9_code LIKE '198%%' OR d.icd9_code LIKE '199%%'
          OR d.icd9_code LIKE '20%%' 
        THEN 1 
        ELSE 0 
    END) as has_cancer

FROM diagnoses_icd d
INNER JOIN cohort_aki c ON d.hadm_id = c.hadm_id
GROUP BY d.hadm_id;