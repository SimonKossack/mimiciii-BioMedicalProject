-- Build KDIGO stage summary restricted to the first 6 hours after ICU admission.
--
-- Output:
--   - kdigo_stage_first6h: one row per icustay_id
--       * aki_stage_6h: max KDIGO stage in [intime, intime + 6h]
--       * aki_6h: binary AKI flag in first 6h
--       * kdigo_points_6h: number of KDIGO charttime points in first 6h
--
-- Source preference:
--   1) kdigo_stages
--   2) mimiciii_derived.kdigo_stages
--   3) empty fallback (all NULL/0), if no source exists

DROP TABLE IF EXISTS kdigo_stage_first6h;

DO $$
DECLARE
    kdigo_src TEXT := NULL;
BEGIN
    IF to_regclass('kdigo_stages') IS NOT NULL THEN
        kdigo_src := 'kdigo_stages';
    ELSIF to_regclass('mimiciii_derived.kdigo_stages') IS NOT NULL THEN
        kdigo_src := 'mimiciii_derived.kdigo_stages';
    END IF;

    IF kdigo_src IS NOT NULL THEN
        EXECUTE format(
            $q$
            CREATE TABLE kdigo_stage_first6h AS
            SELECT
                ie.icustay_id,
                MAX(ks.aki_stage) AS aki_stage_6h,
                CASE WHEN COALESCE(MAX(ks.aki_stage), 0) > 0 THEN 1 ELSE 0 END AS aki_6h,
                COUNT(ks.charttime) AS kdigo_points_6h
            FROM icustays ie
            LEFT JOIN %1$s ks
                ON ie.icustay_id = ks.icustay_id
               AND ks.charttime >= ie.intime
               AND ks.charttime <= ie.intime + INTERVAL '6 hour'
            GROUP BY ie.icustay_id
            $q$,
            kdigo_src
        );
    ELSE
        EXECUTE '
            CREATE TABLE kdigo_stage_first6h AS
            SELECT
                ie.icustay_id,
                NULL::NUMERIC AS aki_stage_6h,
                NULL::INT AS aki_6h,
                0::BIGINT AS kdigo_points_6h
            FROM icustays ie
        ';
    END IF;
END $$;

-- Optional quality check:
-- SELECT
--     COUNT(*) AS n_stays,
--     SUM(CASE WHEN aki_stage_6h IS NULL THEN 1 ELSE 0 END) AS n_missing_stage,
--     AVG(aki_6h::float) AS aki_6h_rate
-- FROM kdigo_stage_first6h;
