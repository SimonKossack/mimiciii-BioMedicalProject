-- Build a sepsis summary table focused on the first 6 hours after ICU admission.
--
-- Output table:
--   sepsis_flag_first6h (one row per icustay_id)
--     - sepsis_6h: sepsis present in [intime, intime + 6h] when onset timing exists
--     - sepsis_any_hosp: fallback sepsis flag at hospital-stay level
--     - sepsis_source: source used to build the row
--
-- Notes:
--   1) If a timed sepsis3 table is available with an onset-like timestamp column,
--      sepsis_6h is populated from that timing.
--   2) If only hadm-level ICD-based sepsis concepts exist (angus/martin/explicit),
--      sepsis_6h remains NULL and sepsis_any_hosp is populated.

DROP TABLE IF EXISTS sepsis_flag_first6h;

DO $$
DECLARE
    has_sepsis3 BOOLEAN := FALSE;
    has_sepsis3_icu BOOLEAN := FALSE;
    has_sepsis3_flag BOOLEAN := FALSE;
    sepsis3_time_col TEXT := NULL;
    has_angus BOOLEAN := FALSE;
    has_martin BOOLEAN := FALSE;
    has_explicit BOOLEAN := FALSE;
    sepsis_any_expr TEXT := '0';
BEGIN
    has_sepsis3 := to_regclass('sepsis3') IS NOT NULL;

    IF has_sepsis3 THEN
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = 'sepsis3'
              AND column_name = 'icustay_id'
        ) INTO has_sepsis3_icu;

        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = 'sepsis3'
              AND column_name = 'sepsis3'
        ) INTO has_sepsis3_flag;

        SELECT c.column_name
        INTO sepsis3_time_col
        FROM information_schema.columns c
        WHERE c.table_schema = current_schema()
          AND c.table_name = 'sepsis3'
          AND c.column_name IN ('sepsis_time', 'onsettime', 'suspected_infection_time', 'charttime', 'starttime')
        ORDER BY CASE c.column_name
            WHEN 'sepsis_time' THEN 1
            WHEN 'onsettime' THEN 2
            WHEN 'suspected_infection_time' THEN 3
            WHEN 'charttime' THEN 4
            WHEN 'starttime' THEN 5
            ELSE 99
        END
        LIMIT 1;
    END IF;

    has_angus := to_regclass('angus') IS NOT NULL;
    has_martin := to_regclass('martin') IS NOT NULL;
    has_explicit := to_regclass('explicit') IS NOT NULL;

    IF has_sepsis3 AND has_sepsis3_icu AND has_sepsis3_flag AND sepsis3_time_col IS NOT NULL THEN
        EXECUTE format(
            $q$
            CREATE TABLE sepsis_flag_first6h AS
            SELECT
                ie.icustay_id,
                MAX(
                    CASE
                        WHEN s.sepsis3
                         AND s.%1$I >= ie.intime
                         AND s.%1$I <= ie.intime + INTERVAL '6 hour'
                        THEN 1 ELSE 0
                    END
                )::INT AS sepsis_6h,
                MAX(CASE WHEN s.sepsis3 THEN 1 ELSE 0 END)::INT AS sepsis_any_hosp,
                'sepsis3_timed'::TEXT AS sepsis_source
            FROM icustays ie
            LEFT JOIN sepsis3 s
                ON s.icustay_id = ie.icustay_id
            GROUP BY ie.icustay_id
            $q$,
            sepsis3_time_col
        );

    ELSIF has_angus OR has_martin OR has_explicit THEN
        IF has_angus THEN
            sepsis_any_expr := sepsis_any_expr ||
                ' + (CASE WHEN EXISTS (SELECT 1 FROM angus a WHERE a.hadm_id = ie.hadm_id AND COALESCE(a.sepsis, a.angus, 0) > 0) THEN 1 ELSE 0 END)';
        END IF;
        IF has_martin THEN
            sepsis_any_expr := sepsis_any_expr ||
                ' + (CASE WHEN EXISTS (SELECT 1 FROM martin m WHERE m.hadm_id = ie.hadm_id AND COALESCE(m.sepsis, 0) > 0) THEN 1 ELSE 0 END)';
        END IF;
        IF has_explicit THEN
            sepsis_any_expr := sepsis_any_expr ||
                ' + (CASE WHEN EXISTS (SELECT 1 FROM explicit e WHERE e.hadm_id = ie.hadm_id AND COALESCE(e.sepsis, 0) > 0) THEN 1 ELSE 0 END)';
        END IF;

        EXECUTE format(
            $q$
            CREATE TABLE sepsis_flag_first6h AS
            SELECT
                ie.icustay_id,
                NULL::INT AS sepsis_6h,
                CASE WHEN (%1$s) > 0 THEN 1 ELSE 0 END::INT AS sepsis_any_hosp,
                'icd_hadm_fallback'::TEXT AS sepsis_source
            FROM icustays ie
            $q$,
            sepsis_any_expr
        );

    ELSE
        EXECUTE '
            CREATE TABLE sepsis_flag_first6h AS
            SELECT
                ie.icustay_id,
                NULL::INT AS sepsis_6h,
                NULL::INT AS sepsis_any_hosp,
                ''no_source_available''::TEXT AS sepsis_source
            FROM icustays ie
        ';
    END IF;
END $$;

-- Optional quality check:
-- SELECT
--   sepsis_source,
--   COUNT(*) AS n,
--   AVG(sepsis_6h::FLOAT) AS sepsis_6h_rate,
--   AVG(sepsis_any_hosp::FLOAT) AS sepsis_any_rate
-- FROM sepsis_flag_first6h
-- GROUP BY sepsis_source;
