# src/utils.py
from __future__ import annotations

import numpy as np
import pandas as pd
from src.db import q


def add_icu_los_days(df_aki: pd.DataFrame) -> pd.DataFrame:
    """Adds ICU length-of-stay in days as column 'icu_los_days'."""
    df = df_aki.copy()
    df["icu_los_days"] = (df["outtime"] - df["intime"]).dt.total_seconds() / (3600 * 24)
    return df


def add_dialysis_flag(df_aki: pd.DataFrame) -> pd.DataFrame:
    """
    Adds 'dialysis' flag (0/1) to df_aki.

    Definition (MIMIC-III pragmatic, for "dialysis required" yes/no):
      - procedureevents_mv with dialysis-related labels (via d_items)
        OR
      - procedures_icd with ICD-9 dialysis codes (3995, 5498)

    Notes:
      - Suitable for cohort-level "dialysis required" analyses.
      - Not suitable for exact RRT start time (timing analyses) without refinement.
    """
    df = df_aki.copy()

    df_rrt_proc = q("""
        SELECT DISTINCT pe.icustay_id
        FROM procedureevents_mv pe
        JOIN d_items di ON pe.itemid = di.itemid
        WHERE
            LOWER(di.label) LIKE '%hemodial%'
         OR LOWER(di.label) LIKE '%haemodial%'
         OR LOWER(di.label) LIKE '%crrt%'
         OR LOWER(di.label) LIKE '%dialysis%'
    """)

    df_rrt_icd = q("""
        SELECT DISTINCT hadm_id
        FROM procedures_icd
        WHERE icd9_code IN ('3995','5498')
    """)

    df["dialysis"] = (
        df["icustay_id"].isin(df_rrt_proc["icustay_id"])
        | df["hadm_id"].isin(df_rrt_icd["hadm_id"])
    ).astype(int)

    return df


def add_early_dopamine_flag(df_aki: pd.DataFrame, window_hours: float = 24.0) -> pd.DataFrame:
    """
    Adds 'early_dopamine' flag (0/1): dopamine started within [0, window_hours] hours after ICU intime.

    Uses inputevents_mv joined to d_items via itemid.
    """
    df = df_aki.copy()

    df_dopamine = q("""
        SELECT ie.icustay_id, ie.starttime
        FROM inputevents_mv ie
        JOIN d_items di ON ie.itemid = di.itemid
        WHERE LOWER(di.label) LIKE '%dopamine%'
    """)

    # merge intime for delta calculation
    dopa = df_dopamine.merge(df[["icustay_id", "intime"]], on="icustay_id", how="inner")

    dopa["hours_since_icu"] = (dopa["starttime"] - dopa["intime"]).dt.total_seconds() / 3600

    early_ids = dopa.loc[
        (dopa["hours_since_icu"] >= 0) & (dopa["hours_since_icu"] <= window_hours),
        "icustay_id"
    ].unique()

    df["early_dopamine"] = df["icustay_id"].isin(early_ids).astype(int)
    return df


def add_sofa_score(df_aki: pd.DataFrame) -> pd.DataFrame:
    """
    Adds SOFA score columns from mimiciii_derived.sofa table.

    TIME WINDOW: first 24 hours of ICU stay (first-day definition from
    MIMIC-III derived tables using ``*_first_day`` views).

    Returns df with new columns:
      - sofa: Total SOFA score (0-24)
      - sofa_respiration: Respiratory component (0-4)
      - sofa_coagulation: Coagulation component (0-4)
      - sofa_liver: Liver component (0-4)
      - sofa_cardiovascular: Cardiovascular component (0-4)
      - sofa_cns: CNS component (0-4)
      - sofa_renal: Renal component (0-4)

    For scores over different time windows (e.g. 6h, 48h) use
    ``compute_sofa_from_raw(window_hours=...)``.
    """
    df = df_aki.copy()
    
    df_sofa = q("""
        SELECT icustay_id, 
               sofa, 
               respiration, 
               coagulation, 
               liver, 
               cardiovascular, 
               cns, 
               renal
        FROM mimiciii_derived.sofa
    """)
    
    # Rename columns for clarity
    df_sofa = df_sofa.rename(columns={
        'respiration': 'sofa_respiration',
        'coagulation': 'sofa_coagulation',
        'liver': 'sofa_liver',
        'cardiovascular': 'sofa_cardiovascular',
        'cns': 'sofa_cns',
        'renal': 'sofa_renal'
    })
    
    df = df.merge(df_sofa, on='icustay_id', how='left')
    return df


def add_sapsii_score(df_aki: pd.DataFrame) -> pd.DataFrame:
    """
    Adds SAPS II score columns from mimiciii_derived.sapsii table.

    TIME WINDOW: first 24 hours of ICU stay (first-day definition from
    MIMIC-III derived tables).

    Returns df with new columns:
      - sapsii: Total SAPS II score
      - sapsii_prob: Probability of mortality based on SAPS II
      - sapsii_age_score: Age component
      - sapsii_hr_score: Heart rate component
      - sapsii_sysbp_score: Systolic BP component
      - sapsii_temp_score: Temperature component
      - sapsii_pao2fio2_score: PaO2/FiO2 component
      - sapsii_uo_score: Urine output component
      - sapsii_bun_score: BUN component
      - sapsii_wbc_score: WBC component
      - sapsii_potassium_score: Potassium component
      - sapsii_sodium_score: Sodium component
      - sapsii_bicarbonate_score: Bicarbonate component
      - sapsii_bilirubin_score: Bilirubin component
      - sapsii_gcs_score: GCS component
      - sapsii_comorbidity_score: Comorbidity component
      - sapsii_admissiontype_score: Admission type component
    """
    df = df_aki.copy()
    
    df_saps = q("""
        SELECT icustay_id,
               sapsii,
               sapsii_prob,
               age_score,
               hr_score,
               sysbp_score,
               temp_score,
               pao2fio2_score,
               uo_score,
               bun_score,
               wbc_score,
               potassium_score,
               sodium_score,
               bicarbonate_score,
               bilirubin_score,
               gcs_score,
               comorbidity_score,
               admissiontype_score
        FROM mimiciii_derived.sapsii
    """)
    
    # Rename columns explicitly to avoid confusion with other scores 
    # (e.g., 'gcs_score' creates clarity vs just 'gcs' or potential overlaps)
    df_saps = df_saps.rename(columns={
        'age_score': 'sapsii_age_score',
        'hr_score': 'sapsii_hr_score',
        'sysbp_score': 'sapsii_sysbp_score',
        'temp_score': 'sapsii_temp_score',
        'pao2fio2_score': 'sapsii_pao2fio2_score',
        'uo_score': 'sapsii_uo_score',
        'bun_score': 'sapsii_bun_score',
        'wbc_score': 'sapsii_wbc_score',
        'potassium_score': 'sapsii_potassium_score',
        'sodium_score': 'sapsii_sodium_score',
        'bicarbonate_score': 'sapsii_bicarbonate_score',
        'bilirubin_score': 'sapsii_bilirubin_score',
        'gcs_score': 'sapsii_gcs_score',
        'comorbidity_score': 'sapsii_comorbidity_score',
        'admissiontype_score': 'sapsii_admissiontype_score'
    })
    
    df = df.merge(df_saps, on='icustay_id', how='left')
    return df

def add_vasopressor_flags(df_aki: pd.DataFrame, window_hours: float = 24.0) -> pd.DataFrame:
    """
    Adds vasopressor flags (0/1) for early use within window_hours after ICU intime.
    
    Returns df with new columns:
      - early_norepinephrine: Norepinephrine started early
      - early_epinephrine: Epinephrine started early
      - early_phenylephrine: Phenylephrine started early
      - any_vasopressor: Any vasopressor started early
    """
    df = df_aki.copy()
    
    # Get all vasopressor events
    vaso_events = q(f"""
        SELECT ie.icustay_id, ie.starttime, 
               CASE 
                   WHEN LOWER(di.label) LIKE '%norepinephrine%' THEN 'norepinephrine'
                   WHEN LOWER(di.label) LIKE '%epinephrine%' AND LOWER(di.label) NOT LIKE '%norepi%' THEN 'epinephrine'
                   WHEN LOWER(di.label) LIKE '%phenylephrine%' THEN 'phenylephrine'
                   WHEN LOWER(di.label) LIKE '%vasopressin%' THEN 'vasopressin'
                   ELSE 'other'
               END as vasopressor_type
        FROM inputevents_mv ie
        JOIN d_items di ON ie.itemid = di.itemid
        WHERE LOWER(di.label) LIKE '%norepinephrine%'
           OR LOWER(di.label) LIKE '%epinephrine%'
           OR LOWER(di.label) LIKE '%phenylephrine%'
           OR LOWER(di.label) LIKE '%vasopressin%'
    """)
    
    if len(vaso_events) == 0:
        # Add columns with all zeros if no vasopressors found
        df['early_norepinephrine'] = 0
        df['early_epinephrine'] = 0
        df['early_phenylephrine'] = 0
        df['any_vasopressor'] = 0
        return df
    
    # Merge with intime for delta calculation
    vaso = vaso_events.merge(df[["icustay_id", "intime"]], on="icustay_id", how="inner")
    vaso["hours_since_icu"] = (vaso["starttime"] - vaso["intime"]).dt.total_seconds() / 3600
    
    # Filter early use only
    vaso_early = vaso[(vaso["hours_since_icu"] >= 0) & (vaso["hours_since_icu"] <= window_hours)]
    
    # Create flags for each vasopressor type
    df['early_norepinephrine'] = df['icustay_id'].isin(
        vaso_early[vaso_early['vasopressor_type'] == 'norepinephrine']['icustay_id'].unique()
    ).astype(int)
    
    df['early_epinephrine'] = df['icustay_id'].isin(
        vaso_early[vaso_early['vasopressor_type'] == 'epinephrine']['icustay_id'].unique()
    ).astype(int)
    
    df['early_phenylephrine'] = df['icustay_id'].isin(
        vaso_early[vaso_early['vasopressor_type'] == 'phenylephrine']['icustay_id'].unique()
    ).astype(int)
    
    df['any_vasopressor'] = (
        (df['early_norepinephrine'] == 1) |
        (df['early_epinephrine'] == 1) |
        (df['early_phenylephrine'] == 1)
    ).astype(int)
    
    return df


def add_mechanical_ventilation_flag(df_aki: pd.DataFrame) -> pd.DataFrame:
    """
    Adds 'mechanical_ventilation' flag (0/1) during ICU stay.
    
    Uses procedureevents_mv or ventilation_durations derived table if available.
    """
    df = df_aki.copy()
    
    # Try derived table first, fall back to procedureevents
    try:
        df_vent = q("""
            SELECT DISTINCT icustay_id
            FROM mimiciii_derived.ventilation_durations
        """)
    except:
        df_vent = q("""
            SELECT DISTINCT icustay_id
            FROM procedureevents_mv
            WHERE LOWER(description) LIKE '%intubat%'
               OR LOWER(description) LIKE '%ventilat%'
        """)
    
    df['mechanical_ventilation'] = df['icustay_id'].isin(df_vent['icustay_id']).astype(int)
    return df


def _run_first_successful_query(sql_queries: list[str], required_cols: list[str]) -> pd.DataFrame:
    """
    Execute SQL statements in order and return the first result containing
    all required columns.
    """
    for sql in sql_queries:
        try:
            res = q(sql)
            if all(c in res.columns for c in required_cols):
                return res
        except Exception:
            continue

    return pd.DataFrame(columns=required_cols)


def add_kdigo_stage(
    df_aki: pd.DataFrame,
    col_name: str = "aki_stage",
) -> pd.DataFrame:
    """
    Add KDIGO AKI stage per ICU stay.

    This tries multiple common concept tables/schemas and uses the first
    available source. If a dedicated first-6h summary table
    (``kdigo_stage_first6h``) exists, it is preferred.
    Output is merged by ``icustay_id`` and written to ``col_name``.
    """
    df = df_aki.copy()

    if "icustay_id" not in df.columns:
        raise ValueError("df_aki muss 'icustay_id' enthalten.")

    kdigo_queries = [
        """
        SELECT icustay_id, aki_stage_6h AS aki_stage
        FROM kdigo_stage_first6h
        """,
        """
        SELECT icustay_id, aki_stage_48hr AS aki_stage
        FROM kdigo_stages_48hr
        """,
        """
        SELECT icustay_id, aki_stage_7day AS aki_stage
        FROM kdigo_stages_7day
        """,
        """
        SELECT icustay_id, MAX(aki_stage) AS aki_stage
        FROM kdigo_stages
        GROUP BY icustay_id
        """,
        """
        SELECT icustay_id, MAX(aki_stage) AS aki_stage
        FROM mimiciii_derived.kdigo_stages
        GROUP BY icustay_id
        """,
        """
        SELECT stay_id AS icustay_id, MAX(aki_stage) AS aki_stage
        FROM mimiciv_derived.kdigo_stages
        GROUP BY stay_id
        """,
    ]

    kdigo = _run_first_successful_query(
        sql_queries=kdigo_queries,
        required_cols=["icustay_id", "aki_stage"],
    )

    if kdigo.empty:
        df[col_name] = np.nan
        return df

    kdigo = kdigo[["icustay_id", "aki_stage"]].drop_duplicates(subset=["icustay_id"])
    kdigo["aki_stage"] = pd.to_numeric(kdigo["aki_stage"], errors="coerce")

    df = df.merge(kdigo, on="icustay_id", how="left")
    if col_name != "aki_stage":
        df = df.rename(columns={"aki_stage": col_name})

    return df


def add_sepsis_flag(
    df_aki: pd.DataFrame,
    col_name: str = "sepsis",
) -> pd.DataFrame:
    """
    Add sepsis yes/no flag.

    This tries multiple concept definitions (6h summary table, Sepsis-3,
    Angus, Martin, Explicit) and merges by ``icustay_id`` when available,
    otherwise by ``hadm_id``.
    """
    df = df_aki.copy()

    if "icustay_id" not in df.columns and "hadm_id" not in df.columns:
        raise ValueError("df_aki muss mindestens 'icustay_id' oder 'hadm_id' enthalten.")

    sepsis_queries = [
        """
        SELECT icustay_id, COALESCE(sepsis_6h, sepsis_any_hosp) AS sepsis
        FROM sepsis_flag_first6h
        """,
        """
        SELECT icustay_id, MAX(CASE WHEN sepsis3 THEN 1 ELSE 0 END) AS sepsis
        FROM sepsis3
        GROUP BY icustay_id
        """,
        """
        SELECT icustay_id, MAX(CASE WHEN sepsis3 THEN 1 ELSE 0 END) AS sepsis
        FROM mimiciii_derived.sepsis3
        GROUP BY icustay_id
        """,
        """
        SELECT hadm_id, MAX(sepsis) AS sepsis
        FROM angus
        GROUP BY hadm_id
        """,
        """
        SELECT hadm_id, MAX(sepsis) AS sepsis
        FROM martin
        GROUP BY hadm_id
        """,
        """
        SELECT hadm_id, MAX(sepsis) AS sepsis
        FROM explicit
        GROUP BY hadm_id
        """,
        """
        SELECT hadm_id, MAX(angus) AS sepsis
        FROM angus
        GROUP BY hadm_id
        """,
    ]

    sepsis_df = _run_first_successful_query(
        sql_queries=sepsis_queries,
        required_cols=["sepsis"],
    )

    if sepsis_df.empty:
        df[col_name] = np.nan
        return df

    if "icustay_id" in sepsis_df.columns and "icustay_id" in df.columns:
        tmp = sepsis_df[["icustay_id", "sepsis"]].drop_duplicates(subset=["icustay_id"])
        df = df.merge(tmp, on="icustay_id", how="left")
    elif "hadm_id" in sepsis_df.columns and "hadm_id" in df.columns:
        tmp = sepsis_df[["hadm_id", "sepsis"]].drop_duplicates(subset=["hadm_id"])
        df = df.merge(tmp, on="hadm_id", how="left")
    else:
        df[col_name] = np.nan
        return df

    if col_name != "sepsis":
        df = df.rename(columns={"sepsis": col_name})

    sepsis_num = pd.to_numeric(df[col_name], errors="coerce")
    df[col_name] = np.where(sepsis_num.notna(), (sepsis_num > 0).astype(float), np.nan)
    return df


def add_first6h_baseline_confounders(
    df_aki: pd.DataFrame,
    kdigo_col: str = "aki_stage",
    sepsis_col: str = "sepsis",
    ventilation_col: str = "mechanical_ventilation",
) -> pd.DataFrame:
    """
    Convenience helper to enrich a cohort with baseline confounders used in
    the 6h landmark design.

    Adds/updates:
      - KDIGO stage via ``add_kdigo_stage`` (prefers kdigo_stage_first6h)
      - Sepsis flag via ``add_sepsis_flag`` (prefers sepsis_flag_first6h)
      - Mechanical ventilation flag via ``add_mechanical_ventilation_flag``
    """
    df = df_aki.copy()
    df = add_kdigo_stage(df, col_name=kdigo_col)
    df = add_sepsis_flag(df, col_name=sepsis_col)
    df = add_mechanical_ventilation_flag(df)

    if ventilation_col != "mechanical_ventilation" and "mechanical_ventilation" in df.columns:
        df = df.rename(columns={"mechanical_ventilation": ventilation_col})

    return df


def add_early_late_dialysis_flags(
    df_aki: pd.DataFrame,
    window_hours: float = 24.0,
    include_inputevents: bool = True,
    allow_negative_hours: bool = False
) -> pd.DataFrame:
    """
    Adds timing-aware dialysis flags to df_aki:

      - early_dialysis (0/1): first RRT start within [0, window_hours] hours after ICU intime
      - late_dialysis  (0/1): first RRT start  > window_hours after ICU intime
      - dialysis_timed (0/1): any RRT event with a starttime found (timing available)
      - dialysis_icd_only (0/1): dialysis==1 but timing not available (typically ICD-based)

    IMPORTANT:
      This function assumes df_aki already contains 'dialysis' (your pragmatic flag),
      and uses ONLY event tables with starttime to define early/late.
    """

    df = df_aki.copy()

    required = {"icustay_id", "intime", "dialysis"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"df_aki fehlt Spalten: {missing}")

    # --- 1) RRT events from procedureevents_mv (timed)
    pe = q("""
        SELECT pe.icustay_id, pe.starttime
        FROM procedureevents_mv pe
        JOIN d_items di ON pe.itemid = di.itemid
        WHERE
            LOWER(di.label) LIKE '%hemodial%'
         OR LOWER(di.label) LIKE '%haemodial%'
         OR LOWER(di.label) LIKE '%crrt%'
         OR LOWER(di.label) LIKE '%dialysis%'
    """)

    events = pe.copy()

    # --- 2) Optional: inputevents_mv (some CRRT signals appear here)
    if include_inputevents:
        ie = q("""
            SELECT ie.icustay_id, ie.starttime
            FROM inputevents_mv ie
            JOIN d_items di ON ie.itemid = di.itemid
            WHERE
                LOWER(di.label) LIKE '%crrt%'
             OR LOWER(di.label) LIKE '%cvvh%'
             OR LOWER(di.label) LIKE '%hemofiltration%'
             OR LOWER(di.label) LIKE '%dialysis%'
        """)
        events = pd.concat([events, ie], ignore_index=True)

    # Clean & merge intime
    events = events.dropna(subset=["icustay_id", "starttime"])
    ev = events.merge(df[["icustay_id", "intime"]], on="icustay_id", how="inner").dropna(subset=["intime"])

    ev["hours_since_icu"] = (ev["starttime"] - ev["intime"]).dt.total_seconds() / 3600

    if not allow_negative_hours:
        ev = ev[ev["hours_since_icu"] >= 0].copy()

    # earliest event per icustay
    first_ev = (
        ev.sort_values(["icustay_id", "hours_since_icu"])
          .groupby("icustay_id", as_index=False)
          .first()
    )

    timed_ids = first_ev["icustay_id"].unique()

    early_ids = first_ev.loc[first_ev["hours_since_icu"] <= window_hours, "icustay_id"].unique()
    late_ids  = first_ev.loc[first_ev["hours_since_icu"] >  window_hours, "icustay_id"].unique()

    df["dialysis_timed"] = df["icustay_id"].isin(timed_ids).astype(int)
    df["early_dialysis"] = df["icustay_id"].isin(early_ids).astype(int)
    df["late_dialysis"]  = df["icustay_id"].isin(late_ids).astype(int)

    # ICD-only (or otherwise untimed): dialysis==1 but no timed event found
    df["dialysis_icd_only"] = ((df["dialysis"].astype(int) == 1) & (df["dialysis_timed"] == 0)).astype(int)

    return df


def extract_dialysis_timing(df_aki: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts dialysis start/end/duration per icustay_id
    using procedureevents_mv + inputevents_mv.

    Returns df with:
      - dialysis_start
      - dialysis_end
      - dialysis_duration_hours
    """

    df = df_aki.copy()

    # --- Procedure-based dialysis (IHD etc.)
    pe = q("""
        SELECT pe.icustay_id, pe.starttime, pe.endtime
        FROM procedureevents_mv pe
        JOIN d_items di ON pe.itemid = di.itemid
        WHERE
            LOWER(di.label) LIKE '%hemodial%'
         OR LOWER(di.label) LIKE '%haemodial%'
         OR LOWER(di.label) LIKE '%dialysis%'
         OR LOWER(di.label) LIKE '%crrt%'
    """)

    # --- CRRT from inputevents
    ie = q("""
        SELECT ie.icustay_id, ie.starttime, ie.endtime
        FROM inputevents_mv ie
        JOIN d_items di ON ie.itemid = di.itemid
        WHERE
            LOWER(di.label) LIKE '%crrt%'
         OR LOWER(di.label) LIKE '%cvvh%'
         OR LOWER(di.label) LIKE '%hemofiltration%'
    """)

    events = pd.concat([pe, ie], ignore_index=True)
    events = events.dropna(subset=["icustay_id", "starttime"])

    # join ICU intime
    events = events.merge(
        df[["icustay_id", "intime"]],
        on="icustay_id",
        how="inner"
    )

    # keep only after ICU admission
    events = events[events["starttime"] >= events["intime"]]

    # earliest start, latest end per icustay
    agg = (
        events
        .groupby("icustay_id")
        .agg(
            dialysis_start=("starttime", "min"),
            dialysis_end=("endtime", "max")
        )
        .reset_index()
    )

    # duration
    agg["dialysis_duration_hours"] = (
        agg["dialysis_end"] - agg["dialysis_start"]
    ).dt.total_seconds() / 3600

    return df.merge(agg, on="icustay_id", how="left")


def add_dialysis_near_icu_discharge_flag(
    df_aki: pd.DataFrame,
    hours_before_discharge: float = 6.0,
    include_inputevents: bool = True
) -> pd.DataFrame:
    """
    Adds 'dialysis_last_Xh' flag (0/1):
    Dialysis occurred within the last `hours_before_discharge`
    before ICU outtime.

    Counts any dialysis event that overlaps with
    [outtime - hours_before_discharge, outtime].

    Requires df_aki to contain:
      - icustay_id
      - outtime
    """

    df = df_aki.copy()

    required = {"icustay_id", "outtime"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"df_aki fehlt Spalten: {missing}")

    # -----------------------------
    # 1) Dialysis events (timed)
    # -----------------------------
    pe = q("""
        SELECT pe.icustay_id, pe.starttime, pe.endtime
        FROM procedureevents_mv pe
        JOIN d_items di ON pe.itemid = di.itemid
        WHERE
            LOWER(di.label) LIKE '%hemodial%'
         OR LOWER(di.label) LIKE '%haemodial%'
         OR LOWER(di.label) LIKE '%dialysis%'
         OR LOWER(di.label) LIKE '%crrt%'
    """)

    events = pe.copy()

    if include_inputevents:
        ie = q("""
            SELECT ie.icustay_id, ie.starttime, ie.endtime
            FROM inputevents_mv ie
            JOIN d_items di ON ie.itemid = di.itemid
            WHERE
                LOWER(di.label) LIKE '%crrt%'
             OR LOWER(di.label) LIKE '%cvvh%'
             OR LOWER(di.label) LIKE '%hemofiltration%'
        """)
        events = pd.concat([events, ie], ignore_index=True)

    events = events.dropna(subset=["icustay_id", "starttime"])

    # -----------------------------
    # 2) Merge outtime
    # -----------------------------
    ev = events.merge(
        df[["icustay_id", "outtime"]],
        on="icustay_id",
        how="inner"
    ).dropna(subset=["outtime"])

    # If endtime missing (common), treat as instantaneous at starttime
    ev["endtime"] = ev["endtime"].fillna(ev["starttime"])

    # -----------------------------
    # 3) Define discharge window
    # -----------------------------
    ev["window_start"] = ev["outtime"] - pd.to_timedelta(hours_before_discharge, unit="h")
    ev["window_end"] = ev["outtime"]

    # -----------------------------
    # 4) Overlap check
    # -----------------------------
    ev["overlaps"] = (
        (ev["starttime"] <= ev["window_end"]) &
        (ev["endtime"]   >= ev["window_start"])
    )

    flagged_ids = ev.loc[ev["overlaps"], "icustay_id"].unique()

    df[f"dialysis_last_{int(hours_before_discharge)}h"] = (
        df["icustay_id"].isin(flagged_ids).astype(int)
    )

    return df


def add_rrt_persistence_near_discharge(
    df_aki: pd.DataFrame,
    hours_before_discharge: float = 6.0,
    min_overlap_hours: float = 5.0,
    gap_tolerance_hours: float = 2.0,
    include_inputevents: bool = True,
) -> pd.DataFrame:
    """
    Robust proxy for "RRT still needed near ICU discharge".

    Builds RRT sessions (merging close events), then computes:
      - rrt_any_in_last6h: any overlap with [outtime-6h, outtime]
      - rrt_active_at_outtime: session overlaps outtime (RRT running at discharge time)
      - rrt_persistent_last6h: active_at_outtime OR overlap_duration_in_window >= min_overlap_hours

    Parameters
    ----------
    hours_before_discharge : float
        Window size before ICU outtime (default 6h).
    min_overlap_hours : float
        Minimum overlap within the window to count as "persistent" (default 3h).
        This helps exclude very short / potentially transient exposures.
    gap_tolerance_hours : float
        Merge events into the same session if the gap between them is <= this (default 2h).
        Helps when CRRT is charted in multiple adjacent chunks.
    """

    df = df_aki.copy()
    required = {"icustay_id", "outtime"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"df_aki fehlt Spalten: {missing}")

    # --- timed RRT events (start/end). endtime can be missing -> treat as instantaneous
    pe = q("""
        SELECT pe.icustay_id, pe.starttime, pe.endtime
        FROM procedureevents_mv pe
        JOIN d_items di ON pe.itemid = di.itemid
        WHERE
            LOWER(di.label) LIKE '%hemodial%'
         OR LOWER(di.label) LIKE '%haemodial%'
         OR LOWER(di.label) LIKE '%dialysis%'
         OR LOWER(di.label) LIKE '%crrt%'
         OR LOWER(di.label) LIKE '%cvvh%'
         OR LOWER(di.label) LIKE '%hemofiltration%'
    """)

    events = pe.copy()

    if include_inputevents:
        ie = q("""
            SELECT ie.icustay_id, ie.starttime, ie.endtime
            FROM inputevents_mv ie
            JOIN d_items di ON ie.itemid = di.itemid
            WHERE
                LOWER(di.label) LIKE '%crrt%'
             OR LOWER(di.label) LIKE '%cvvh%'
             OR LOWER(di.label) LIKE '%cvvhd%'
             OR LOWER(di.label) LIKE '%cvvhdf%'
             OR LOWER(di.label) LIKE '%hemofiltration%'
             OR LOWER(di.label) LIKE '%dialysis%'
        """)
        events = pd.concat([events, ie], ignore_index=True)

    events = events.dropna(subset=["icustay_id", "starttime"]).copy()
    events["endtime"] = events["endtime"].fillna(events["starttime"])

    # join outtime
    ev = events.merge(df[["icustay_id", "outtime"]], on="icustay_id", how="inner").dropna(subset=["outtime"])

    # keep only events that start before outtime (conservative)
    ev = ev[ev["starttime"] <= ev["outtime"]].copy()

    # -----------------------------
    # Build sessions per icustay_id
    # -----------------------------
    gap_tol = pd.to_timedelta(gap_tolerance_hours, unit="h")

    ev = ev.sort_values(["icustay_id", "starttime", "endtime"]).copy()

    sessions = []
    for icu_id, g in ev.groupby("icustay_id", sort=False):
        # iterate in time order
        cur_start = None
        cur_end = None
        outt = g["outtime"].iloc[0]

        for _, row in g.iterrows():
            s = row["starttime"]
            e = row["endtime"]

            if cur_start is None:
                cur_start, cur_end = s, e
                continue

            # merge if overlapping or close gap
            if s <= (cur_end + gap_tol):
                cur_end = max(cur_end, e)
            else:
                sessions.append((icu_id, cur_start, cur_end, outt))
                cur_start, cur_end = s, e

        if cur_start is not None:
            sessions.append((icu_id, cur_start, cur_end, outt))

    sess = pd.DataFrame(sessions, columns=["icustay_id", "sess_start", "sess_end", "outtime"])
    if sess.empty:
        # no events found -> all zeros
        df[f"rrt_any_in_last{int(hours_before_discharge)}h"] = 0
        df["rrt_active_at_outtime"] = 0
        df[f"rrt_persistent_last{int(hours_before_discharge)}h"] = 0
        return df

    # define window
    window_hours = hours_before_discharge
    sess["win_start"] = sess["outtime"] - pd.to_timedelta(window_hours, unit="h")
    sess["win_end"] = sess["outtime"]

    # overlap duration within window (in hours)
    overlap_start = sess[["sess_start", "win_start"]].max(axis=1)
    overlap_end = sess[["sess_end", "win_end"]].min(axis=1)
    sess["overlap_hours"] = (overlap_end - overlap_start).dt.total_seconds() / 3600
    sess["overlap_hours"] = sess["overlap_hours"].clip(lower=0)

    # flags
    sess["any_in_window"] = (sess["overlap_hours"] > 0).astype(int)
    sess["active_at_outtime"] = ((sess["sess_start"] <= sess["outtime"]) & (sess["sess_end"] >= sess["outtime"])).astype(int)
    sess["persistent_in_window"] = ((sess["active_at_outtime"] == 1) | (sess["overlap_hours"] >= min_overlap_hours)).astype(int)

    # aggregate per icustay
    agg = (
        sess.groupby("icustay_id", as_index=False)
            .agg(
                any_in_window=("any_in_window", "max"),
                active_at_outtime=("active_at_outtime", "max"),
                persistent_in_window=("persistent_in_window", "max"),
                max_overlap_hours=("overlap_hours", "max"),
            )
    )

    any_col = f"rrt_any_in_last{int(hours_before_discharge)}h"
    pers_col = f"rrt_persistent_last{int(hours_before_discharge)}h"

    df = df.merge(agg, on="icustay_id", how="left")
    df[any_col] = df["any_in_window"].fillna(0).astype(int)
    df["rrt_active_at_outtime"] = df["active_at_outtime"].fillna(0).astype(int)
    df[pers_col] = df["persistent_in_window"].fillna(0).astype(int)
    df["rrt_max_overlap_hours_in_window"] = df["max_overlap_hours"].fillna(0.0)

    df = df.drop(columns=["any_in_window", "active_at_outtime", "persistent_in_window", "max_overlap_hours"])

    return df


def recode_ethnicity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a new column 'ethnicity_grp' to df based on df['ethnicity'].
    """

    df = df.copy()

    def _map_eth(e):
        if pd.isna(e):
            return "Unknown"
        e = str(e).upper()
        if "WHITE" in e:
            return "White"
        if "BLACK" in e:
            return "Black"
        if "HISPANIC" in e:
            return "Hispanic"
        if "ASIAN" in e:
            return "Asian"
        return "Other"

    df["ethnicity_grp"] = df["ethnicity"].apply(_map_eth)
    return df
# ============================================================
# Intervention flags: fluids & diuretics (early + anytime)
# ============================================================

# --- Pattern definitions (central, reusable)
DIURETIC_PATTERNS = [
    "%furosemide%", "%lasix%",
    "%bumetanide%",
    "%torsemide%",
    "%chlorothiazide%",
    "%metolazone%",
    "%mannitol%",
    "%acetazolamide%",
    "%spironolactone%",
]

FLUID_PATTERNS = [
    "%normal saline%", "%0.9%saline%", "%saline%",
    "%lactated ring%", "%ringer%",
    "%plasmalyte%", "%plasma-lyte%",
    "%d5w%", "%dextrose%",
    "%albumin%",
    "%hetastarch%", "%starch%",
    "%packed red%", "%prbc%", "%red blood cell%",
    "%fresh frozen plasma%", "%ffp%",
    "%platelet%",
]


def add_inputevents_flag(
    df_aki: pd.DataFrame,
    col_early: str,
    patterns: list[str],
    window_hours: float = 24.0,
    col_any: str | None = None,
) -> pd.DataFrame:
    df = df_aki.copy()

    where = " OR ".join([f"LOWER(di.label) LIKE '{p}'" for p in patterns])

    ev = q(f"""
        SELECT ie.icustay_id, ie.starttime
        FROM inputevents_mv ie
        JOIN d_items di ON ie.itemid = di.itemid
        WHERE {where}
    """)

    if len(ev) == 0:
        df[col_early] = 0
        if col_any:
            df[col_any] = 0
        return df

    if col_any:
        any_ids = ev["icustay_id"].dropna().unique()
        df[col_any] = df["icustay_id"].isin(any_ids).astype(int)

    ev = ev.merge(
        df[["icustay_id", "intime"]],
        on="icustay_id",
        how="inner"
    ).dropna(subset=["intime", "starttime"])

    ev["hours_since_icu"] = (
        ev["starttime"] - ev["intime"]
    ).dt.total_seconds() / 3600

    ev = ev[(ev["hours_since_icu"] >= 0) & (ev["hours_since_icu"] <= window_hours)]

    early_ids = ev["icustay_id"].unique()
    df[col_early] = df["icustay_id"].isin(early_ids).astype(int)

    if col_any and col_any not in df.columns:
        df[col_any] = 0

    return df


def add_early_fluid_flag(
    df_aki: pd.DataFrame,
    window_hours: float = 24.0
) -> pd.DataFrame:
    return add_inputevents_flag(
        df_aki,
        col_early="early_fluid",
        col_any="any_fluid",
        patterns=FLUID_PATTERNS,
        window_hours=window_hours,
    )


def add_early_diuretic_flag(
    df_aki: pd.DataFrame,
    window_hours: float = 24.0
) -> pd.DataFrame:
    return add_inputevents_flag(
        df_aki,
        col_early="early_diuretic",
        col_any="any_diuretic",
        patterns=DIURETIC_PATTERNS,
        window_hours=window_hours,
    )


# ============================================================
# Flexible time-window functions (raw MIMIC-III tables)
# ============================================================

# -- Lab ItemIDs used across MIMIC-III (labevents) -----------
_LAB_ITEMS = {
    "creatinine":  (50912,),
    "bilirubin":   (50885,),
    "platelets":   (51265,),
    "bun":         (51006,),
    "wbc":         (51301, 51300),
    "potassium":   (50971, 50822),
    "sodium":      (50983, 50824),
    "bicarbonate": (50882,),
    "lactate":     (50813,),
    "pao2":        (50821,),
    "fio2_lab":    (50816,),
}

# -- Vital / Chart ItemIDs (chartevents) --------------------
_VITAL_ITEMS = {
    "heart_rate":  (220045,),
    "sbp":         (220179, 220050),
    "dbp":         (220180, 220051),
    "mbp":         (220181, 220052),
    "resp_rate":   (220210, 224690),
    "temperature": (223761, 223762),
    "spo2":        (220277,),
    "gcs_total":   (198,),
    "fio2_chart":  (223835, 3420),
}


def get_labs_for_window(
    df_cohort: pd.DataFrame,
    window_hours: float = 24.0,
    agg: str = "worst",
    end_hours_col: str | None = None,
) -> pd.DataFrame:
    """
    Retrieve lab values from ``labevents`` within ``[intime, intime + window_hours]``.

    Parameters
    ----------
    df_cohort : DataFrame
        Must contain ``subject_id``, ``hadm_id``, ``icustay_id``, ``intime``.
    window_hours : float
        Length of observation window after ICU intime (default 24).
    agg : str
        Aggregation strategy per lab per ICU stay.
        ``"worst"`` picks the clinically worst value (max for creatinine /
        bilirubin / BUN / lactate / WBC / potassium; min for platelets /
        bicarbonate / sodium).
        ``"first"`` picks the earliest measurement.
        ``"mean"`` averages.

    Returns
    -------
    DataFrame with one row per ``icustay_id`` and one column per lab analyte,
    suffixed with ``_<window_hours>h`` (e.g. ``creatinine_6h``).
    """
    ids = tuple(df_cohort["hadm_id"].dropna().astype(int).unique().tolist())
    if not ids:
        return df_cohort.copy()

    in_clause = f"({ids[0]})" if len(ids) == 1 else str(ids)

    all_itemids = []
    item_to_lab: dict[int, str] = {}
    for lab_name, itemids in _LAB_ITEMS.items():
        for iid in itemids:
            all_itemids.append(iid)
            item_to_lab[iid] = lab_name
    itemid_str = ",".join(str(i) for i in all_itemids)

    raw = q(f"""
        SELECT le.hadm_id, le.itemid, le.charttime, le.valuenum
        FROM labevents le
        WHERE le.hadm_id IN {in_clause}
          AND le.itemid IN ({itemid_str})
          AND le.valuenum IS NOT NULL
    """)

    if raw.empty:
        return df_cohort.copy()

    raw["lab"] = raw["itemid"].map(item_to_lab)

    # Merge-Spalten: bei end_hours_col patientenspezifisches Fensterende einbeziehen
    _merge_cols = ["hadm_id", "icustay_id", "intime"]
    if end_hours_col is not None:
        if end_hours_col not in df_cohort.columns:
            raise ValueError(
                f"Spalte '{end_hours_col}' fehlt in df_cohort. "
                f"Sie sollte den patientenspezifischen Fensterende in Stunden enthalten "
                f"(z.B. 'first_vaso_hours' aus der Vasopressor-Timing-Berechnung)."
            )
        _merge_cols.append(end_hours_col)

    merged = raw.merge(
        df_cohort[_merge_cols].drop_duplicates(),
        on="hadm_id",
        how="inner",
    )
    merged["charttime"] = pd.to_datetime(merged["charttime"])
    merged["intime"] = pd.to_datetime(merged["intime"])
    merged["hours"] = (merged["charttime"] - merged["intime"]).dt.total_seconds() / 3600

    # Zeitfilterung: fixes oder patientenspezifisches Fenster
    if end_hours_col is not None:
        merged = merged[(merged["hours"] >= 0) & (merged["hours"] <= merged[end_hours_col])]
    else:
        merged = merged[(merged["hours"] >= 0) & (merged["hours"] <= window_hours)]

    _WORST_MAX = {"creatinine", "bilirubin", "bun", "lactate", "wbc", "potassium"}

    def _agg_fn(group: pd.DataFrame) -> float:
        lab_name = group["lab"].iloc[0]
        if agg == "first":
            return group.sort_values("charttime").iloc[0]["valuenum"]
        if agg == "mean":
            return group["valuenum"].mean()
        if lab_name in _WORST_MAX:
            return group["valuenum"].max()
        return group["valuenum"].min()

    result = (
        merged.groupby(["icustay_id", "lab"])
        .apply(_agg_fn)
        .unstack("lab")
    )

    # Suffix: patientenspezifisches Fenster → '_t_star', fixes Fenster → '_{N}h'
    sfx = "_t_star" if end_hours_col is not None else f"_{int(window_hours)}h"
    result.columns = [c + sfx for c in result.columns]
    result = result.reset_index()

    return df_cohort.merge(result, on="icustay_id", how="left")


def get_vitals_for_window(
    df_cohort: pd.DataFrame,
    window_hours: float = 24.0,
    agg: str = "worst",
    end_hours_col: str | None = None,
) -> pd.DataFrame:
    """
    Retrieve vital signs from ``chartevents`` within ``[intime, intime + window_hours]``.

    Parameters
    ----------
    df_cohort : DataFrame
        Must contain ``icustay_id``, ``intime``.
    window_hours : float
        Length of observation window after ICU intime (default 24).
    agg : str
        ``"worst"`` picks clinically worst (min for BP/GCS/SpO2, max for HR/temp/RR).
        ``"first"`` picks earliest.  ``"mean"`` averages.

    Returns
    -------
    DataFrame with one column per vital, suffixed ``_<window_hours>h``.
    """
    icu_ids = tuple(df_cohort["icustay_id"].dropna().astype(int).unique().tolist())
    if not icu_ids:
        return df_cohort.copy()

    in_clause = f"({icu_ids[0]})" if len(icu_ids) == 1 else str(icu_ids)

    all_itemids = []
    item_to_vital: dict[int, str] = {}
    for vital_name, itemids in _VITAL_ITEMS.items():
        for iid in itemids:
            all_itemids.append(iid)
            item_to_vital[iid] = vital_name
    itemid_str = ",".join(str(i) for i in all_itemids)

    raw = q(f"""
        SELECT ce.icustay_id, ce.itemid, ce.charttime, ce.valuenum
        FROM chartevents ce
        WHERE ce.icustay_id IN {in_clause}
          AND ce.itemid IN ({itemid_str})
          AND ce.valuenum IS NOT NULL
    """)

    if raw.empty:
        return df_cohort.copy()

    raw["vital"] = raw["itemid"].map(item_to_vital)

    # Merge-Spalten: bei end_hours_col patientenspezifisches Fensterende einbeziehen
    _merge_cols_v = ["icustay_id", "intime"]
    if end_hours_col is not None:
        if end_hours_col not in df_cohort.columns:
            raise ValueError(
                f"Spalte '{end_hours_col}' fehlt in df_cohort. "
                f"Sie sollte den patientenspezifischen Fensterende in Stunden enthalten "
                f"(z.B. 'first_vaso_hours' aus der Vasopressor-Timing-Berechnung)."
            )
        _merge_cols_v.append(end_hours_col)

    merged = raw.merge(
        df_cohort[_merge_cols_v].drop_duplicates(),
        on="icustay_id",
        how="inner",
    )
    merged["charttime"] = pd.to_datetime(merged["charttime"])
    merged["intime"] = pd.to_datetime(merged["intime"])
    merged["hours"] = (merged["charttime"] - merged["intime"]).dt.total_seconds() / 3600

    # Zeitfilterung: fixes oder patientenspezifisches Fenster
    if end_hours_col is not None:
        merged = merged[(merged["hours"] >= 0) & (merged["hours"] <= merged[end_hours_col])]
    else:
        merged = merged[(merged["hours"] >= 0) & (merged["hours"] <= window_hours)]

    _WORST_MIN = {"sbp", "dbp", "mbp", "gcs_total", "spo2"}

    def _agg_fn(group: pd.DataFrame) -> float:
        vital_name = group["vital"].iloc[0]
        if agg == "first":
            return group.sort_values("charttime").iloc[0]["valuenum"]
        if agg == "mean":
            return group["valuenum"].mean()
        if vital_name in _WORST_MIN:
            return group["valuenum"].min()
        return group["valuenum"].max()

    result = (
        merged.groupby(["icustay_id", "vital"])
        .apply(_agg_fn)
        .unstack("vital")
    )

    # Suffix: patientenspezifisches Fenster → '_t_star', fixes Fenster → '_{N}h'
    sfx = "_t_star" if end_hours_col is not None else f"_{int(window_hours)}h"
    result.columns = [c + sfx for c in result.columns]
    result = result.reset_index()

    return df_cohort.merge(result, on="icustay_id", how="left")


def get_urine_output_for_window(
    df_cohort: pd.DataFrame,
    window_hours: float = 24.0,
    end_hours_col: str | None = None,
) -> pd.DataFrame:
    """
    Total urine output (mL) from ``outputevents`` within
    ``[intime, intime + window_hours]`` (oder patientenspezifisch bis ``end_hours_col``).

    Returns df with new column ``uo_ml_<window_hours>h`` (bzw. ``uo_ml_t_star``).
    """
    icu_ids = tuple(df_cohort["icustay_id"].dropna().astype(int).unique().tolist())
    if not icu_ids:
        return df_cohort.copy()

    in_clause = f"({icu_ids[0]})" if len(icu_ids) == 1 else str(icu_ids)

    uo_items = (40055, 43175, 40069, 40094, 40715, 40473, 40085, 40057, 40056,
                227488, 226559, 226560, 226561, 226563, 226564, 226565,
                226567, 226557)
    uo_str = ",".join(str(i) for i in uo_items)

    raw = q(f"""
        SELECT oe.icustay_id, oe.charttime, oe.value
        FROM outputevents oe
        WHERE oe.icustay_id IN {in_clause}
          AND oe.itemid IN ({uo_str})
          AND oe.value IS NOT NULL
          AND oe.value > 0
    """)

    # Spaltenname für UO-Ergebnis
    _uo_col = "uo_ml_t_star" if end_hours_col is not None else f"uo_ml_{int(window_hours)}h"

    if raw.empty:
        df_out = df_cohort.copy()
        df_out[_uo_col] = np.nan
        return df_out

    # Merge-Spalten: bei end_hours_col patientenspezifisches Fensterende einbeziehen
    _merge_cols_uo = ["icustay_id", "intime"]
    if end_hours_col is not None:
        if end_hours_col not in df_cohort.columns:
            raise ValueError(
                f"Spalte '{end_hours_col}' fehlt in df_cohort. "
                f"Sie sollte den patientenspezifischen Fensterende in Stunden enthalten "
                f"(z.B. 'first_vaso_hours' aus der Vasopressor-Timing-Berechnung)."
            )
        _merge_cols_uo.append(end_hours_col)

    merged = raw.merge(
        df_cohort[_merge_cols_uo].drop_duplicates(),
        on="icustay_id",
        how="inner",
    )
    merged["charttime"] = pd.to_datetime(merged["charttime"])
    merged["intime"] = pd.to_datetime(merged["intime"])
    merged["hours"] = (merged["charttime"] - merged["intime"]).dt.total_seconds() / 3600

    # Zeitfilterung: fixes oder patientenspezifisches Fenster
    if end_hours_col is not None:
        merged = merged[(merged["hours"] >= 0) & (merged["hours"] <= merged[end_hours_col])]
    else:
        merged = merged[(merged["hours"] >= 0) & (merged["hours"] <= window_hours)]

    uo_total = merged.groupby("icustay_id")["value"].sum().reset_index()
    uo_total = uo_total.rename(columns={"value": _uo_col})

    return df_cohort.merge(uo_total, on="icustay_id", how="left")


def get_vasopressor_features_for_window(
    df_cohort: pd.DataFrame,
    window_hours: float = 24.0,
    end_hours_col: str | None = None,
) -> pd.DataFrame:
    """
    Retrieve vasopressor/inotrope exposure within a time window and derive
    coarse dose features usable for SOFA cardiovascular scoring.

    Returns one row per icustay_id with:
      - vaso_any_<suffix>, dopamine_any_<suffix>, dobutamine_any_<suffix>,
        norepinephrine_any_<suffix>, epinephrine_any_<suffix>,
        phenylephrine_any_<suffix>, vasopressin_any_<suffix>
      - dopamine_rate_mcgkgmin_<suffix>, norepinephrine_rate_mcgkgmin_<suffix>,
        epinephrine_rate_mcgkgmin_<suffix>
    """
    df = df_cohort.copy()
    icu_ids = tuple(df["icustay_id"].dropna().astype(int).unique().tolist())
    sfx = "_t_star" if end_hours_col is not None else f"_{int(window_hours)}h"

    out_cols = [
        f"vaso_any{sfx}",
        f"dopamine_any{sfx}",
        f"dobutamine_any{sfx}",
        f"norepinephrine_any{sfx}",
        f"epinephrine_any{sfx}",
        f"phenylephrine_any{sfx}",
        f"vasopressin_any{sfx}",
        f"dopamine_rate_mcgkgmin{sfx}",
        f"norepinephrine_rate_mcgkgmin{sfx}",
        f"epinephrine_rate_mcgkgmin{sfx}",
    ]

    if not icu_ids:
        for c in out_cols:
            df[c] = np.nan
        return df

    in_clause = f"({icu_ids[0]})" if len(icu_ids) == 1 else str(icu_ids)

    ev = q(f"""
        SELECT
            ie.icustay_id,
            ie.starttime,
            ie.rate,
            ie.rateuom,
            LOWER(di.label) AS label
        FROM inputevents_mv ie
        JOIN d_items di ON ie.itemid = di.itemid
        WHERE ie.icustay_id IN {in_clause}
          AND (
               LOWER(di.label) LIKE '%norepinephrine%'
            OR (LOWER(di.label) LIKE '%epinephrine%' AND LOWER(di.label) NOT LIKE '%norepi%')
            OR LOWER(di.label) LIKE '%dopamine%'
            OR LOWER(di.label) LIKE '%dobutamine%'
            OR LOWER(di.label) LIKE '%phenylephrine%'
            OR LOWER(di.label) LIKE '%vasopressin%'
          )
    """)

    if ev.empty:
        for c in out_cols:
            df[c] = 0.0 if c.endswith(f"_any{sfx}") else np.nan
        return df

    merge_cols = ["icustay_id", "intime"]
    if end_hours_col is not None:
        if end_hours_col not in df.columns:
            raise ValueError(
                f"Spalte '{end_hours_col}' fehlt in df_cohort. "
                f"Sie sollte den patientenspezifischen Fensterende in Stunden enthalten."
            )
        merge_cols.append(end_hours_col)

    ev = ev.merge(df[merge_cols].drop_duplicates(), on="icustay_id", how="inner")
    ev["starttime"] = pd.to_datetime(ev["starttime"])
    ev["intime"] = pd.to_datetime(ev["intime"])
    ev["hours"] = (ev["starttime"] - ev["intime"]).dt.total_seconds() / 3600

    if end_hours_col is not None:
        ev = ev[(ev["hours"] >= 0) & (ev["hours"] <= ev[end_hours_col])].copy()
    else:
        ev = ev[(ev["hours"] >= 0) & (ev["hours"] <= window_hours)].copy()

    if ev.empty:
        for c in out_cols:
            df[c] = 0.0 if c.endswith(f"_any{sfx}") else np.nan
        return df

    def _drug_from_label(lbl: str) -> str:
        l = str(lbl).lower()
        if "norepinephrine" in l:
            return "norepinephrine"
        if "epinephrine" in l and "norepi" not in l:
            return "epinephrine"
        if "dobutamine" in l:
            return "dobutamine"
        if "dopamine" in l:
            return "dopamine"
        if "phenylephrine" in l:
            return "phenylephrine"
        if "vasopressin" in l:
            return "vasopressin"
        return "other"

    def _to_mcgkgmin(rate: float, uom: str) -> float:
        if pd.isna(rate) or pd.isna(uom):
            return np.nan
        u = str(uom).lower().replace(" ", "")
        if "mcg/kg/min" in u or "mcg/kg/minute" in u:
            return float(rate)
        return np.nan

    ev["drug"] = ev["label"].map(_drug_from_label)
    ev["rate_mcgkgmin"] = [
        _to_mcgkgmin(r, u) for r, u in zip(ev["rate"], ev["rateuom"])
    ]

    by_icu = ev.groupby("icustay_id")
    out = pd.DataFrame({"icustay_id": by_icu.size().index})

    # Any exposure flags per class.
    for drug in ["dopamine", "dobutamine", "norepinephrine", "epinephrine", "phenylephrine", "vasopressin"]:
        drug_ids = ev.loc[ev["drug"] == drug, "icustay_id"].dropna().unique()
        out[f"{drug}_any{sfx}"] = out["icustay_id"].isin(drug_ids).astype(float)

    out[f"vaso_any{sfx}"] = (
        out[f"dopamine_any{sfx}"]
        + out[f"dobutamine_any{sfx}"]
        + out[f"norepinephrine_any{sfx}"]
        + out[f"epinephrine_any{sfx}"]
        + out[f"phenylephrine_any{sfx}"]
        + out[f"vasopressin_any{sfx}"]
    ).gt(0).astype(float)

    # Rate features (only where unit is already mcg/kg/min).
    for drug in ["dopamine", "norepinephrine", "epinephrine"]:
        tmp = (
            ev.loc[ev["drug"] == drug, ["icustay_id", "rate_mcgkgmin"]]
            .groupby("icustay_id", as_index=False)["rate_mcgkgmin"]
            .max()
            .rename(columns={"rate_mcgkgmin": f"{drug}_rate_mcgkgmin{sfx}"})
        )
        out = out.merge(tmp, on="icustay_id", how="left")

    for c in out_cols:
        if c not in out.columns:
            out[c] = 0.0 if c.endswith(f"_any{sfx}") else np.nan

    return df.merge(out[["icustay_id"] + out_cols], on="icustay_id", how="left")


def summarize_map_coverage(
    df_cohort: pd.DataFrame,
    windows_hours: tuple[int, ...] = (6, 24),
) -> pd.DataFrame:
    """
    Summarize availability of MAP measurements across fixed time windows.

    Returns one row per requested window with counts and percentages.
    """
    if "icustay_id" not in df_cohort.columns or "intime" not in df_cohort.columns:
        raise ValueError("df_cohort muss mindestens 'icustay_id' und 'intime' enthalten.")

    base = df_cohort[["icustay_id", "intime"]].drop_duplicates().copy()
    n_total = len(base)
    rows: list[dict[str, float]] = []

    for w in windows_hours:
        d = get_vitals_for_window(base, window_hours=float(w), agg="worst", end_hours_col=None)
        col = f"mbp_{int(w)}h"
        n_map = int(d[col].notna().sum()) if col in d.columns else 0
        pct = (100.0 * n_map / n_total) if n_total > 0 else np.nan
        rows.append(
            {
                "window_hours": int(w),
                "n_total": int(n_total),
                "n_with_map": int(n_map),
                "map_coverage_pct": float(pct),
            }
        )

    return pd.DataFrame(rows)


def pick_first_existing_column(
    df: pd.DataFrame,
    candidates: list[str],
) -> str | None:
    """Return the first column from candidates that exists in df."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _coerce_binary_series(s: pd.Series) -> pd.Series:
    """Coerce common binary encodings to float {0.0, 1.0} (NaN if unknown)."""
    if s.dtype == bool:
        return s.astype(float)

    num = pd.to_numeric(s, errors="coerce")
    if num.notna().mean() > 0.8:
        return (num > 0).astype(float)

    txt = s.astype(str).str.strip().str.lower()
    out = pd.Series(np.nan, index=s.index, dtype=float)
    out[txt.isin({"1", "true", "yes", "y", "ja", "vent", "sepsis"})] = 1.0
    out[txt.isin({"0", "false", "no", "n", "nein", "none", "nan"})] = 0.0
    return out


def _coerce_kdigo_stage_series(s: pd.Series) -> pd.Series:
    """
    Coerce KDIGO/AKI stage representations to numeric stage values.

    Supports direct numeric values and text containing stage digits (0-3).
    """
    num = pd.to_numeric(s, errors="coerce")
    if num.notna().mean() > 0.8:
        return num

    txt = s.astype(str).str.lower()
    extracted = txt.str.extract(r"([0-3])", expand=False)
    return pd.to_numeric(extracted, errors="coerce")


def build_mandatory_matching_confounders(
    df: pd.DataFrame,
    source_df: pd.DataFrame | None = None,
    id_col: str = "icustay_id",
    strict: bool = True,
) -> tuple[pd.DataFrame, dict[str, str | None], list[str]]:
    """
    Build canonical mandatory confounders for matching.

    Mandatory set (canonical output columns):
      - KDIGO stage: ``match_kdigo_stage``
      - SOFA cardiovascular: ``match_sofa_cardio``
      - MAP 6h: ``match_map_6h``
      - Sepsis yes/no: ``match_sepsis``
      - Ventilation yes/no: ``match_ventilation``
      - Age: ``match_age``

    The function searches first in ``df`` and then in ``source_df`` (if provided),
    using common candidate column names. If ``strict=True`` and one of the six
    mandatory variables cannot be mapped, a ValueError is raised.

    Returns
    -------
    df_out : pd.DataFrame
        Copy of df with canonical ``match_*`` columns added.
    used_mapping : dict[str, str | None]
        Mapping from canonical variable key to detected source column name.
    missing_required : list[str]
        Canonical keys that could not be mapped.
    """
    df_out = df.copy()

    if source_df is not None:
        if id_col not in df_out.columns or id_col not in source_df.columns:
            raise ValueError(f"'{id_col}' muss in df und source_df vorhanden sein.")
        merged_src = source_df.drop_duplicates(subset=[id_col]).copy()
    else:
        merged_src = None

    candidate_map: dict[str, list[str]] = {
        "kdigo_stage": [
            "aki_stage_6h",
            "aki_stage",
            "kdigo_stage",
            "kdigo",
            "kdigo_max",
            "match_kdigo_stage",
        ],
        "sofa_cardio": [
            "sofa_cardiovascular_6h",
            "sofa_cardiovascular",
            "sofa_cardiovascular_24h",
            "match_sofa_cardio",
        ],
        "map_6h": [
            "mbp_6h",
            "map_6h",
            "mbp",
            "map",
            "match_map_6h",
        ],
        "sepsis": [
            "sepsis_6h",
            "sepsis_any_hosp",
            "sepsis",
            "sepsis_flag",
            "is_sepsis",
            "septic_shock",
            "match_sepsis",
        ],
        "ventilation": [
            "mechanical_ventilation",
            "ventilation",
            "ventilated",
            "is_ventilated",
            "match_ventilation",
        ],
        "age": [
            "age",
            "anchor_age",
            "admission_age",
            "match_age",
        ],
    }

    out_col_map = {
        "kdigo_stage": "match_kdigo_stage",
        "sofa_cardio": "match_sofa_cardio",
        "map_6h": "match_map_6h",
        "sepsis": "match_sepsis",
        "ventilation": "match_ventilation",
        "age": "match_age",
    }

    used_mapping: dict[str, str | None] = {}
    missing_required: list[str] = []

    for key, candidates in candidate_map.items():
        src_col = pick_first_existing_column(df_out, candidates)

        if src_col is None and merged_src is not None:
            src_col = pick_first_existing_column(merged_src, candidates)
            if src_col is not None and src_col != id_col:
                df_out = df_out.merge(
                    merged_src[[id_col, src_col]],
                    on=id_col,
                    how="left",
                    suffixes=("", "_src"),
                )

        used_mapping[key] = src_col
        target_col = out_col_map[key]

        if src_col is None:
            df_out[target_col] = np.nan
            missing_required.append(key)
            continue

        raw = df_out[src_col]
        if key in {"sepsis", "ventilation"}:
            df_out[target_col] = _coerce_binary_series(raw)
        elif key == "kdigo_stage":
            df_out[target_col] = _coerce_kdigo_stage_series(raw)
        else:
            df_out[target_col] = pd.to_numeric(raw, errors="coerce")

    if strict and missing_required:
        miss = ", ".join(missing_required)
        raise ValueError(
            "Pflicht-Confounder fehlen und konnten nicht gemappt werden: "
            f"{miss}. Bitte Spalten in df/source_df bereitstellen oder Candidate-Listen erweitern."
        )

    return df_out, used_mapping, missing_required


def get_mandatory_matching_columns() -> list[str]:
    """Canonical mandatory confounder columns used for strict matching."""
    return [
        "match_kdigo_stage",
        "match_sofa_cardio",
        "match_map_6h",
        "match_sepsis",
        "match_ventilation",
        "match_age",
    ]


def compute_sofa_from_raw(
    df_cohort: pd.DataFrame,
    window_hours: float = 24.0,
    end_hours_col: str | None = None,
) -> pd.DataFrame:
    """
    Compute SOFA total and component scores from raw MIMIC-III tables
    for an arbitrary time window ``[intime, intime + window_hours]``.

    This bypasses ``mimiciii_derived.sofa`` (which is fixed to first 24h)
    and allows computing e.g. a 6h-baseline SOFA or a 48h-SOFA.

    Scoring follows the standard SOFA definition (Vincent et al., 1996).

    Requirements on ``df_cohort``: ``icustay_id``, ``hadm_id``,
    ``subject_id``, ``intime``.

    Returns df with new columns (suffixed ``_<window_hours>h``):
      sofa_respiration, sofa_coagulation, sofa_liver,
      sofa_cardiovascular, sofa_cns, sofa_renal, sofa_total.
    """
    df = df_cohort.copy()
    # Suffix: patientenspezifisches Fenster → '_t_star', fixes Fenster → '_{N}h'
    sfx = "_t_star" if end_hours_col is not None else f"_{int(window_hours)}h"

    df = get_labs_for_window(df, window_hours=window_hours, agg="worst", end_hours_col=end_hours_col)
    df = get_vitals_for_window(df, window_hours=window_hours, agg="worst", end_hours_col=end_hours_col)
    df = get_urine_output_for_window(df, window_hours=window_hours, end_hours_col=end_hours_col)

    pao2_col = f"pao2{sfx}"
    fio2_col = f"fio2_lab{sfx}"
    plat_col = f"platelets{sfx}"
    bili_col = f"bilirubin{sfx}"
    mbp_col = f"mbp{sfx}"
    gcs_col = f"gcs_total{sfx}"
    creat_col = f"creatinine{sfx}"
    # UO-Spaltenname konsistent mit get_urine_output_for_window
    uo_col = "uo_ml_t_star" if end_hours_col is not None else f"uo_ml_{int(window_hours)}h"

    # --- Respiration (PaO2/FiO2) ---
    if pao2_col in df.columns and fio2_col in df.columns:
        fio2 = df[fio2_col].copy()
        fio2 = fio2.where(fio2 > 1, fio2 * 100)  # normalise to %
        pf = df[pao2_col] / (fio2 / 100)
        df[f"sofa_respiration{sfx}"] = pd.cut(
            pf,
            bins=[-np.inf, 100, 200, 300, 400, np.inf],
            labels=[4, 3, 2, 1, 0],
        ).astype(float)
    else:
        df[f"sofa_respiration{sfx}"] = np.nan

    # --- Coagulation (Platelets) ---
    if plat_col in df.columns:
        df[f"sofa_coagulation{sfx}"] = pd.cut(
            df[plat_col],
            bins=[-np.inf, 20, 50, 100, 150, np.inf],
            labels=[4, 3, 2, 1, 0],
        ).astype(float)
    else:
        df[f"sofa_coagulation{sfx}"] = np.nan

    # --- Liver (Bilirubin) ---
    if bili_col in df.columns:
        df[f"sofa_liver{sfx}"] = pd.cut(
            df[bili_col],
            bins=[-np.inf, 1.2, 2.0, 6.0, 12.0, np.inf],
            labels=[0, 1, 2, 3, 4],
        ).astype(float)
    else:
        df[f"sofa_liver{sfx}"] = np.nan

    # --- Cardiovascular (MAP + vasopressor/inotrope support) ---
    df = get_vasopressor_features_for_window(
        df,
        window_hours=window_hours,
        end_hours_col=end_hours_col,
    )
    cardio_col = f"sofa_cardiovascular{sfx}"

    map_val = pd.to_numeric(df.get(mbp_col), errors="coerce") if mbp_col in df.columns else pd.Series(np.nan, index=df.index)
    vaso_any = pd.to_numeric(df.get(f"vaso_any{sfx}"), errors="coerce").fillna(0)
    dop_any = pd.to_numeric(df.get(f"dopamine_any{sfx}"), errors="coerce").fillna(0)
    dobut_any = pd.to_numeric(df.get(f"dobutamine_any{sfx}"), errors="coerce").fillna(0)
    norepi_any = pd.to_numeric(df.get(f"norepinephrine_any{sfx}"), errors="coerce").fillna(0)
    epi_any = pd.to_numeric(df.get(f"epinephrine_any{sfx}"), errors="coerce").fillna(0)
    vasopressin_any = pd.to_numeric(df.get(f"vasopressin_any{sfx}"), errors="coerce").fillna(0)
    phenyl_any = pd.to_numeric(df.get(f"phenylephrine_any{sfx}"), errors="coerce").fillna(0)

    dop_rate = pd.to_numeric(df.get(f"dopamine_rate_mcgkgmin{sfx}"), errors="coerce")
    norepi_rate = pd.to_numeric(df.get(f"norepinephrine_rate_mcgkgmin{sfx}"), errors="coerce")
    epi_rate = pd.to_numeric(df.get(f"epinephrine_rate_mcgkgmin{sfx}"), errors="coerce")

    cardio = pd.Series(0.0, index=df.index)

    # Score 1: MAP < 70 mmHg
    cardio = cardio.where(~(map_val < 70), 1.0)

    # Score 2: any vasoactive/inotropic support without high-dose evidence
    score2 = (dop_any > 0) | (dobut_any > 0) | (norepi_any > 0) | (epi_any > 0) | (vasopressin_any > 0) | (phenyl_any > 0)
    cardio = cardio.where(~score2, 2.0)

    # Score 3: moderate catecholamine dose (when unit allows mcg/kg/min interpretation)
    score3 = (
        ((dop_rate > 5) & (dop_rate <= 15))
        | ((norepi_rate > 0) & (norepi_rate <= 0.1))
        | ((epi_rate > 0) & (epi_rate <= 0.1))
    )
    cardio = cardio.where(~score3, 3.0)

    # Score 4: high catecholamine dose
    score4 = (dop_rate > 15) | (norepi_rate > 0.1) | (epi_rate > 0.1)
    cardio = cardio.where(~score4, 4.0)

    # If neither MAP nor vaso information exists, keep missing.
    has_cardio_data = map_val.notna() | (vaso_any > 0)
    cardio = cardio.where(has_cardio_data, np.nan)

    df[cardio_col] = cardio

    # --- CNS (GCS) ---
    if gcs_col in df.columns:
        df[f"sofa_cns{sfx}"] = pd.cut(
            df[gcs_col],
            bins=[-np.inf, 6, 9, 12, 14, np.inf],
            labels=[4, 3, 2, 1, 0],
        ).astype(float)
    else:
        df[f"sofa_cns{sfx}"] = np.nan

    # --- Renal (Creatinine + Urine output) ---
    renal = pd.Series(0.0, index=df.index)
    if creat_col in df.columns:
        cr = df[creat_col]
        renal = pd.cut(
            cr,
            bins=[-np.inf, 1.2, 2.0, 3.5, 5.0, np.inf],
            labels=[0, 1, 2, 3, 4],
        ).astype(float)
    if uo_col in df.columns:
        uo_per_day = df[uo_col] * (24 / max(window_hours, 1))
        uo_score = pd.Series(0.0, index=df.index)
        uo_score = uo_score.where(uo_per_day >= 500, 3)
        uo_score = uo_score.where(uo_per_day >= 200, 4)
        renal = pd.concat([renal, uo_score], axis=1).max(axis=1)
    df[f"sofa_renal{sfx}"] = renal

    # --- Total ---
    component_cols = [
        f"sofa_respiration{sfx}",
        f"sofa_coagulation{sfx}",
        f"sofa_liver{sfx}",
        f"sofa_cardiovascular{sfx}",
        f"sofa_cns{sfx}",
        f"sofa_renal{sfx}",
    ]
    df[f"sofa_total{sfx}"] = df[component_cols].sum(axis=1, min_count=1)

    return df


def add_sofa_at_intervention(
    df: pd.DataFrame,
    t_star_col: str,
) -> pd.DataFrame:
    """
    Berechnet SOFA-Scores zum patientenspezifischen Interventionszeitpunkt t*.

    Für jeden Patienten wird das Fenster [intime, intime + t*] verwendet,
    wobei t* aus t_star_col (Stunden nach ICU-Aufnahme) entnommen wird.
    Patienten ohne Intervention (NaN in t_star_col) erhalten NaN-Spalten.

    Hinweis: Die UO-Normierung auf 24h erfolgt über den Standardwert
    window_hours=24 in compute_sofa_from_raw. Bei sehr kurzem t* (<6h) ist
    die Normierung eine Näherung.

    Parameter
    ----------
    df : DataFrame
        Muss icustay_id, hadm_id, subject_id, intime sowie t_star_col enthalten.
    t_star_col : str
        Spaltenname mit dem patientenspezifischen Interventionszeitpunkt in Stunden
        nach ICU-Aufnahme (z.B. 'first_vaso_hours').

    Rückgabe
    --------
    DataFrame mit neuen Spalten sofa_*_t_star und sofa_total_t_star.
    Patienten ohne Intervention erhalten NaN in allen t_star-Spalten.
    """
    if t_star_col not in df.columns:
        raise ValueError(
            f"Spalte '{t_star_col}' fehlt in df. "
            f"Sie sollte den Interventionszeitpunkt in Stunden nach ICU-Aufnahme enthalten "
            f"(z.B. aus add_vasopressor_flags() oder einer Timing-Berechnung)."
        )

    df = df.copy()

    # Erwartete t*-SOFA-Ausgabespalten
    _t_star_sofa_cols = [
        "sofa_respiration_t_star",
        "sofa_coagulation_t_star",
        "sofa_liver_t_star",
        "sofa_cardiovascular_t_star",
        "sofa_cns_t_star",
        "sofa_renal_t_star",
        "sofa_total_t_star",
    ]

    # Nur Patienten mit gültigem t* verarbeiten
    mask = df[t_star_col].notna()

    if not mask.any():
        # Keine behandelten Patienten → alle Spalten auf NaN setzen
        for col in _t_star_sofa_cols:
            df[col] = np.nan
        return df

    # SOFA-Berechnung für behandelte Patienten mit patientenspezifischem Fenster
    df_treated = df[mask].copy()
    df_treated = compute_sofa_from_raw(df_treated, window_hours=24.0, end_hours_col=t_star_col)

    # Nur die neuen t*-Spalten zurückmergen (verhindert Dopplung anderer Spalten)
    new_cols = [c for c in df_treated.columns if c.endswith("_t_star")]
    df = df.merge(df_treated[["icustay_id"] + new_cols], on="icustay_id", how="left")

    return df
