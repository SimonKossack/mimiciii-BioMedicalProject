# -*- coding: utf-8 -*-
"""Behält nur Zellen, die zu den Präsentationsgrafiken beitragen."""
import json
import sys

KEEP_SIGNATURES = [
    "AKI-Kohorte",
    "Setup",
    "Kohorte laden",
    "load_aki_cohort",
    "add_sofa_score",
    "add_sapsii_score",
    "df_aki",
    "intervention_summary",
    "Interventionen-Übersicht",
    "add_early_fluid_flag",
    "add_early_diuretic_flag",
    "plot_interventions",
    "Interventions in AKI ICU Cohort",
    "Hospital Mortality by Intervention Status",
    "plot_mortality_by_intervention",
    "first_intervention_timing",
    "VASO_PATTERNS",
    "FLUID_PATTERNS",
    "DIURETIC_PATTERNS",
    "df3 = ",
    "mortality_by_timing",
    "mort_vaso",
    "mort_fluid",
    "mort_diur",
    "plot_combined",
    "Going even deeper",
    "Persistent RRT at ICU Discharge",
    "rrt_persistent",
    "Within SOFA groups",
    "Within SAPS II groups",
    "Within SOFA renal",
    "mortality by vasopressor timing",
    "SOFA 5–8",
    "SOFA 8–22",
    "Severity scores by vasopressor timing",
    "sofa_renal_median",
    "sofa_total_median",
    "sapsii_median",
    "chi2_contingency",
    "Early vs Late",
    "Never vs Late",
    "test_late_vs_early",
    "test_never_vs_late",
    "SOFA score bin",
    "SOFA renal score bin",
    "smf.logit",
    "vaso_late",
    "Adjusted mortality",
    "hospital_mortality ~ vaso_late",
    "Early (≤24 h)",
    "Late (>24 h)",
    "Never",
    "sofa_bin",
    "vaso_timing",
    "score_group_timing_stats",
    "plot_mortality_by_timing_within_score_bins",
    "test_late_vs_early_by_score_bin",
    "test_never_vs_late_by_score_bin",
    "df_test",
    "df_evle",
    "df_simple",
    "df_late_never",
]

def should_keep(cell):
    src = "".join(cell.get("source", []))
    if not src.strip() or "Optional: Einmal ausführen" in src:
        return False
    return any(s in src for s in KEEP_SIGNATURES)

def main():
    path = "07_saps2.ipynb"
    with open(path, "r", encoding="utf-8") as f:
        nb = json.load(f)
    kept = [c for c in nb["cells"] if should_keep(c)]
    nb["cells"] = kept
    out_path = "07_saps2_presentation_only.ipynb"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(nb, f, indent=2, ensure_ascii=False)
    print(f"Kept {len(kept)} cells, wrote {out_path}")

if __name__ == "__main__":
    main()
