/* Datei: sql/get_potassium.sql
   Zweck: Maximales Kalium für AKI Patienten finden
*/

SELECT 
    le.hadm_id,
    MAX(le.valuenum) as max_potassium
FROM labevents le
INNER JOIN cohort_aki c ON le.hadm_id = c.hadm_id
WHERE le.itemid = 50971 -- Potassium (Serum)
GROUP BY le.hadm_id;