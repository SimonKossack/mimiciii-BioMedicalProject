SELECT 
    c.subject_id,
    c.hadm_id,
    c.gender,
    c.age,
    c.intervention_ventilation, 
    c.outcome_death,
    s.sapsii
FROM cohort_respiratory c
LEFT JOIN mimic_derived.sapsii s ON c.hadm_id = s.hadm_id;