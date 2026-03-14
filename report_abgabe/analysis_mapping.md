## Übersicht: Präsentation „Critical Decisions in the ICU“ ↔ Code

Diese Datei verknüpft die **Präsentationsinhalte** (Grafiken, Tabellen) mit dem Code in diesem Abgabe-Ordner.

---

### Präsentationsstruktur → Code

| Präsentationsteil | Inhalt | Code / Notebook |
|-------------------|--------|------------------|
| **Methodik** | MIMIC-III, AKI-Kohorte (Studienkohorte 61.532 → 10.485) | `nieren/07_saps2.ipynb` (load_aki_cohort), `src/cohort.py` |
| **Interventionen & Ergebnisse** | Interventionen in AKI-Kohorte (Flüssigkeiten, Diuretika, Vasopressoren, Dialyse) | `nieren/07_saps2.ipynb` (plot_interventions, intervention_summary) |
| | Hospital Mortality by Intervention Status | `nieren/07_saps2.ipynb` (plot_mortality_by_intervention) |
| **Korrekturfaktoren** | SOFA, SAPS II (Value of SAPS II, Schweregrad-Vergleich) | `notebooks/t_03_saps-ii.ipynb`; SOFA/SAPS in `07_saps2` (add_sofa_score, add_sapsii_score aus `src/utils.py`) |
| **Vertiefende Analyse** | Timing of First Use (Vasopressoren, Flüssigkeiten, Diuretika, Persistent RRT) | `nieren/07_saps2.ipynb` (Timing-Plots, RRT bei Entlassung) |
| | Mortality by Vasopressor Timing (nach SOFA-, SAPS II-, SOFA-Renal-Gruppen) | `nieren/07_saps2.ipynb` (Liniendiagramme nach Schweregrad) |
| | Severity scores by vasopressor timing (SOFA/SAPS II Median) | `nieren/07_saps2.ipynb` (Boxplots/Balken) |
| | Chi-Quadrat-Tabellen (SOFA-Bins, Early/Late n, p-Werte) | `nieren/07_saps2.ipynb` |
| | Adjusted mortality (logistische Regression, Early vs. Late, Never vs. Late über SOFA) | `nieren/07_saps2.ipynb` (Regressionsgrafiken mit Konfidenzintervallen) |

**Hinweis:** Folien zu „Top 20 Diagnosen“, „Studienkohorte“-Flussdiagramm und „External Validation“ können aus anderen Auswertungen oder manuell erstellt sein; die zentralen Analysen und Figuren stammen aus `07_saps2.ipynb` und `t_03_saps-ii.ipynb`.

---

### 1. SAPS-II / Schweregrad & Mortalität

- **Notebook:** `notebooks/t_03_saps-ii.ipynb`
- **Inhalt:** SAPS-II-Gruppen, Mortalität pro Gruppe, ggf. nach Beatmung. Unterstützt die Folie „Value of SAPS II“ und Korrekturfaktoren.
- **Abhängigkeit:** `get_severity_stats.sql` (Schema z. B. auf `mimiciii_derived.sapsii` anpassen) oder Kohorte + `src.utils.add_sapsii_score()`.

---

### 2. AKI-Kohorte: Interventionen, Timing, SOFA/SAPS II, Chi², logistische Regression

- **Notebook:** `nieren/07_saps2.ipynb`
- **Daten:** `load_aki_cohort()` → View `derived.mv_aki_icu_first_cohort` (10.485 Patienten nach Filtern).
- **Erweiterungen:** `src.utils`: add_sofa_score, add_sapsii_score, add_vasopressor_flags, add_early_fluid_flag, add_early_diuretic_flag, add_early_late_dialysis_flags, add_rrt_persistence_near_discharge usw.
- **Figuren/Ergebnisse aus diesem Notebook:**
  - Interventionen in AKI ICU Kohorte (Balken)
  - Hospital Mortality by Intervention Status
  - Timing of First Use (Vasopressoren, Flüssigkeiten, Diuretika, Persistent RRT)
  - Mortality by Vasopressor Timing (stratifiziert nach SOFA/SAPS II/SOFA-Renal)
  - Severity scores by vasopressor timing
  - Chi-Quadrat-Tabellen (SOFA-Bins)
  - Adjusted mortality (logistische Regression über SOFA-Score)

---

### 3. Utilities & SQL

- **src/db.py:** Engine, `q(sql)`.
- **src/db_connect.py:** `get_engine()`, `load_sql()` für t_03_saps-ii.
- **src/utils.py:** add_sofa_score, add_sapsii_score (first 24h), Dialyse-Flags, Vasopressor-/Fluid-/Diuretika-Flags.
- **src/cohort.py:** `load_aki_cohort()` (AKI-Kohorte).
- **sql/:** build_7_views.sql, concepts_postgres (firstday, durations, severityscores sofa/sapsii, echo_data, postgres-functions), t_create_cohort_respiratory.sql.

---

### 4. Scores und Zeitfenster (für Methodik/Bericht)

- **SOFA** und **SAPS II** in diesem Projekt: **first 24h** (mimiciii_derived, first-day-Definition).
- In allen Figuren/Tabellen, in denen SOFA oder SAPS II vorkommen: als „SOFA (first 24h)“ bzw. „SAPS II (first 24h)“ bezeichnen.
- **AKI-Kohorte:** Fokus auf AKI; SOFA renal und SOFA total für Schweregrad-Vergleich und bereinigte Analysen.

---

Diese Mapping-Datei dient als Navigationshilfe: Zu jeder genannten Grafik oder Tabelle der Präsentation ist das zugehörige Notebook und ggf. die Funktion angegeben.
