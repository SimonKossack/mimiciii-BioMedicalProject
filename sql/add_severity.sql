/* Datei: sql/add_severity.sql
   Zweck: Schweregrad-Merkmale (Kreatinin & Beatmung) hinzufügen
*/

-- 1. Tabelle für Kreatinin (Laborkürzel: 50912)
WITH creat AS (
    SELECT 
        le.hadm_id, 
        MAX(le.valuenum) as max_creatinine
    FROM labevents le
    -- Nur unsere Kohorte betrachten (macht es schneller)
    INNER JOIN cohort_aki c ON le.hadm_id = c.hadm_id
    WHERE le.itemid = 50912 -- Code für Creatinine
    GROUP BY le.hadm_id
),

-- 2. Tabelle für Beatmung (Prozeduren)
vent AS (
    SELECT DISTINCT 
        p.hadm_id,
        1 as mech_vent
    FROM procedures_icd p
    INNER JOIN cohort_aki c ON p.hadm_id = c.hadm_id
    WHERE p.icd9_code IN (
        '9670', '9671', '9672' -- Codes für invasive Beatmung
    )
)

-- 3. Alles zusammenführen
SELECT 
    c.*,
    -- Kreatinin hinzufügen (wenn kein Wert da, nehmen wir NULL)
    creat.max_creatinine,
    -- Beatmung hinzufügen (wenn kein Wert, dann 0)
    COALESCE(vent.mech_vent, 0) as mech_vent
    
FROM cohort_aki c
LEFT JOIN creat ON c.hadm_id = creat.hadm_id
LEFT JOIN vent  ON c.hadm_id = vent.hadm_id;