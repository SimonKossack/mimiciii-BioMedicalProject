DROP MATERIALIZED VIEW IF EXISTS vaso_first_day CASCADE;
CREATE MATERIALIZED VIEW vaso_first_day AS
SELECT 
    ie.icustay_id,
    MAX(CASE WHEN itemid = 221906 THEN rate END) as rate_norepinephrine,
    MAX(CASE WHEN itemid = 221289 THEN rate END) as rate_epinephrine,
    MAX(CASE WHEN itemid = 221662 THEN rate END) as rate_dopamine,
    MAX(CASE WHEN itemid = 221653 THEN rate END) as rate_dobutamine
FROM icustays ie
LEFT JOIN inputevents_mv mv ON ie.icustay_id = mv.icustay_id
    AND mv.starttime BETWEEN ie.intime AND (ie.intime + interval '1' day)
WHERE itemid IN (221906, 221289, 221662, 221653)
AND statusdescription != 'Rewritten'
GROUP BY ie.icustay_id;