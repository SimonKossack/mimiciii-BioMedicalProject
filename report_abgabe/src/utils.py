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

    merged = raw.merge(
        df_cohort[["hadm_id", "icustay_id", "intime"]].drop_duplicates(),
        on="hadm_id",
        how="inner",
    )
    merged["charttime"] = pd.to_datetime(merged["charttime"])
    merged["intime"] = pd.to_datetime(merged["intime"])
    merged["hours"] = (merged["charttime"] - merged["intime"]).dt.total_seconds() / 3600
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
        .apply(_agg_fn, include_groups=False)
        .unstack("lab")
    )

    sfx = f"_{int(window_hours)}h"
    result.columns = [c + sfx for c in result.columns]
    result = result.reset_index()

    return df_cohort.merge(result, on="icustay_id", how="left")


def get_vitals_for_window(
    df_cohort: pd.DataFrame,
    window_hours: float = 24.0,
    agg: str = "worst",
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

    merged = raw.merge(
        df_cohort[["icustay_id", "intime"]].drop_duplicates(),
        on="icustay_id",
        how="inner",
    )
    merged["charttime"] = pd.to_datetime(merged["charttime"])
    merged["intime"] = pd.to_datetime(merged["intime"])
    merged["hours"] = (merged["charttime"] - merged["intime"]).dt.total_seconds() / 3600
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
        .apply(_agg_fn, include_groups=False)
        .unstack("vital")
    )

    sfx = f"_{int(window_hours)}h"
    result.columns = [c + sfx for c in result.columns]
    result = result.reset_index()

    return df_cohort.merge(result, on="icustay_id", how="left")


def get_urine_output_for_window(
    df_cohort: pd.DataFrame,
    window_hours: float = 24.0,
) -> pd.DataFrame:
    """
    Total urine output (mL) from ``outputevents`` within
    ``[intime, intime + window_hours]``.

    Returns df with new column ``uo_ml_<window_hours>h``.
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

    if raw.empty:
        df_out = df_cohort.copy()
        df_out[f"uo_ml_{int(window_hours)}h"] = np.nan
        return df_out

    merged = raw.merge(
        df_cohort[["icustay_id", "intime"]].drop_duplicates(),
        on="icustay_id",
        how="inner",
    )
    merged["charttime"] = pd.to_datetime(merged["charttime"])
    merged["intime"] = pd.to_datetime(merged["intime"])
    merged["hours"] = (merged["charttime"] - merged["intime"]).dt.total_seconds() / 3600
    merged = merged[(merged["hours"] >= 0) & (merged["hours"] <= window_hours)]

    uo_total = merged.groupby("icustay_id")["value"].sum().reset_index()
    uo_total = uo_total.rename(columns={"value": f"uo_ml_{int(window_hours)}h"})

    return df_cohort.merge(uo_total, on="icustay_id", how="left")


def compute_sofa_from_raw(
    df_cohort: pd.DataFrame,
    window_hours: float = 24.0,
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
    sfx = f"_{int(window_hours)}h"

    df = get_labs_for_window(df, window_hours=window_hours, agg="worst")
    df = get_vitals_for_window(df, window_hours=window_hours, agg="worst")
    df = get_urine_output_for_window(df, window_hours=window_hours)

    pao2_col = f"pao2{sfx}"
    fio2_col = f"fio2_lab{sfx}"
    plat_col = f"platelets{sfx}"
    bili_col = f"bilirubin{sfx}"
    mbp_col = f"mbp{sfx}"
    gcs_col = f"gcs_total{sfx}"
    creat_col = f"creatinine{sfx}"
    uo_col = f"uo_ml_{int(window_hours)}h"

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

    # --- Cardiovascular (MAP; vasopressors not included for simplicity) ---
    if mbp_col in df.columns:
        df[f"sofa_cardiovascular{sfx}"] = np.where(
            df[mbp_col] < 70, 1, 0
        )
        df.loc[df[mbp_col].isna(), f"sofa_cardiovascular{sfx}"] = np.nan
    else:
        df[f"sofa_cardiovascular{sfx}"] = np.nan

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
