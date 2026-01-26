-- DAS FINALE SOFA-SKRIPT (Vollständig mit Respiration)
DROP MATERIALIZED VIEW IF EXISTS sofa CASCADE;
CREATE MATERIALIZED VIEW sofa AS
WITH scorecomp AS (
    SELECT ie.icustay_id
      , v.meanbp_min
      , l.creatinine_max
      , l.bilirubin_max
      , l.platelet_min
      , uo.urineoutput
      , gcs.mingcs
      -- NEU: Zugriff auf deine aktualisierte Blutgas-View
      , bg.sf_ratio_min
      -- Daten aus deiner neuen View
      , vaso.rate_norepinephrine, vaso.rate_epinephrine, 
        vaso.rate_dopamine, vaso.rate_dobutamine
    FROM icustays ie
    LEFT JOIN vitals_first_day v ON ie.icustay_id = v.icustay_id
    LEFT JOIN labs_first_day l ON ie.icustay_id = l.icustay_id
    LEFT JOIN urine_output_first_day uo ON ie.icustay_id = uo.icustay_id
    LEFT JOIN gcs_first_day gcs ON ie.icustay_id = gcs.icustay_id
    LEFT JOIN blood_gas_first_day_arterial bg ON ie.icustay_id = bg.icustay_id
    LEFT JOIN vaso_first_day vaso ON ie.icustay_id = vaso.icustay_id -- EINFACHER JOIN
),
scorecalc AS (
    SELECT icustay_id
      -- 1. Respiration (Lunge) - Basierend auf SF-Ratio
      , CASE
          WHEN sf_ratio_min < 100 THEN 4
          WHEN sf_ratio_min < 200 THEN 3
          WHEN sf_ratio_min < 300 THEN 2
          WHEN sf_ratio_min < 400 THEN 1
          ELSE 0 END AS respiration
      -- 2. Koagulation
      , CASE
          WHEN platelet_min < 20  THEN 4
          WHEN platelet_min < 50  THEN 3
          WHEN platelet_min < 100 THEN 2
          WHEN platelet_min < 150 THEN 1
          ELSE 0 END AS coagulation
      -- 3. Leber
      , CASE
            WHEN bilirubin_max >= 12.0 THEN 4
            WHEN bilirubin_max >= 6.0  THEN 3
            WHEN bilirubin_max >= 2.0  THEN 2
            WHEN bilirubin_max >= 1.2  THEN 1
            ELSE 0 END AS liver
      -- Korrigiertes Herz-Kreislauf-System
      -- Korrigiertes Herz-Kreislauf-System (Präzise Logik)
      , CASE
            -- STUFE 4: Hohe Dosen (Grenze ist > 0.1)
            WHEN rate_norepinephrine > 0.1 
              OR rate_epinephrine > 0.1 
              OR rate_dopamine > 15 
              THEN 4
            
            -- STUFE 3: Niedrige Dosen (Epi/Norepi bis maximal 0.1)
            -- Da Stufe 4 bereits oben abgefangen wurde, reicht hier > 0
            WHEN rate_norepinephrine > 0 
              OR rate_epinephrine > 0 
              OR rate_dopamine > 5 
              THEN 3
            
            -- STUFE 2: Dopamin bis 5 oder Dobutamin (beliebige Dosis)
            WHEN rate_dopamine > 0 
              OR rate_dobutamine > 0 
              THEN 2
            
            -- STUFE 1: Nur der Blutdruck, keine Medikamente
            WHEN meanbp_min < 70 
              THEN 1
            
            ELSE 0 END AS cardiovascular
      -- 5. ZNS
      , CASE
          WHEN (mingcs >= 13 and mingcs <= 14) THEN 1
          WHEN (mingcs >= 10 and mingcs <= 12) THEN 2
          WHEN (mingcs >=  6 and mingcs <=  9) THEN 3
          WHEN  mingcs <   6 THEN 4
          ELSE 0 END AS cns
      -- 6. Niere
      , CASE
        WHEN (creatinine_max >= 5.0) OR (urineoutput < 200) THEN 4
        WHEN (creatinine_max >= 3.5 AND creatinine_max < 5.0) OR (urineoutput < 500) THEN 3
        WHEN (creatinine_max >= 2.0 AND creatinine_max < 3.5) THEN 2
        WHEN (creatinine_max >= 1.2 AND creatinine_max < 2.0) THEN 1
        ELSE 0 END AS renal
    FROM scorecomp
)
SELECT 
    ie.subject_id, ie.hadm_id, ie.icustay_id,
    -- Summe ALLER 6 Komponenten (mit COALESCE für fehlende Werte)
    (coalesce(respiration,0) + coalesce(coagulation,0) + coalesce(liver,0) + 
     coalesce(cardiovascular,0) + coalesce(cns,0) + coalesce(renal,0)) AS sofa_score,
    respiration, coagulation, liver, cardiovascular, cns, renal
FROM icustays ie
LEFT JOIN scorecalc s ON ie.icustay_id = s.icustay_id
ORDER BY ie.icustay_id;