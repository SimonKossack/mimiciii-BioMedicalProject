## Übersicht: Analysen ↔ Code (aktueller Stand)

Diese Datei verknüpft die in eurer Präsentation/Arbeit zentralen Themen mit den jeweiligen Notebooks und (soweit sinnvoll) Code-Blöcken/Funktionen im aktuellen Repo.

---

### 1. PEEP / ARDSnet-Adhärenz und Mortalität

- **Fragestellung**: Wie gut folgt die klinische Praxis der ARDSnet Low-PEEP-Tabelle in Bezug auf FiO₂, und wie hängen Unter-/Über-PEEP mit der Mortalität zusammen?
- **Notebook**: `notebooks/t_05_peep.ipynb`
- **Wesentliche Schritte**:
  - **PEEP–FiO₂-Extraktion**:
    - SQL-Query `query_peep_fio2` auf `chartevents` (FiO₂-ItemIDs `223835, 3420` und PEEP-ItemIDs `220339, 507`) → DataFrame `df_vent`.
  - **Berechnung Soll-PEEP & Delta**:
    - Funktion `get_ards_soll_peep(fio2)` (ARDSnet Low-PEEP-Tabelle).
    - Spalten `peep_soll` und `peep_delta = peep - peep_soll`.
    - Deskriptive Statistik (`describe()`, Anteile Unter‑/Über‑PEEP).
  - **Patienten-Level & Mortalität**:
    - SQL-Query `query_vent_mort` (JOIN `chartevents` + `admissions`) → `df_peep_study`.
    - Erneute Soll-PEEP-Berechnung (`get_ards_soll`) und `delta`.
    - Aggregation auf Patientenebene (`groupby('hadm_id')`) mit mittlerem Delta und `hospital_expire_flag`.
    - Kategorisierung in 3 Gruppen:
      - `1. Unter-PEEP (Gefahr: Atelektase)`
      - `2. Leitliniengerecht`
      - `3. Über-PEEP (Gefahr: Barotrauma)`
    - Mortalitätsraten und Patientenzahlen pro Gruppe → Tabelle, die direkt Grundlage für eine Figure ist.

---

### 2. Vitalparameter / Kreatinin / AKI‑relevante Items

- **Fragestellung**: Welche ItemIDs und Messungen sind für Kreatinin, Urinmenge und weitere AKI‑relevante Parameter wichtig, und wie sehen deren Verteilungen aus?
- **Notebook**: `notebooks/check_viatl_parameter.ipynb`
- **Wesentliche Schritte**:
  - **DB-Anbindung & Helper**:
    - Nutzung von `.env` und `sqlalchemy.create_engine` (lokale `engine`).
    - Helper `q(sql: str)` analog `src.db.q`, um SQL → DataFrames zu laden.
  - **Identifikation relevanter Items**:
    - Kreatinin-Items:
      - `SELECT itemid, label FROM d_labitems WHERE LOWER(label) LIKE '%creatinine%'`.
    - Urin-Items (z. B. `WHERE LOWER(label) LIKE '%urine%'` in `d_labitems` / `d_items`).
  - **Explorative Auswertungen**:
    - Anzeigen der gefundenen Items, evtl. erste Verteilungen/Verläufe (für Verständnis und spätere AKI-Definitionen).

Diese Explorations-Notebooks liefern Hintergrundwissen / Entscheidungsgrundlagen, gehen aber nicht direkt in eine einzelne „Final-Figure“ ein.

---

### 3. SAPS-II / Krankheitsschwere und Mortalität

- **Fragestellung**: Wie hängt der SAPS-II-Score mit der Mortalität zusammen, insbesondere im Kontext Beatmung/Intervention?
- **Notebook**: `notebooks/t_03_saps-ii.ipynb`
- **Wesentliche Schritte**:
  - **Setup**:
    - Pfad-Setup + `from src.db_connect import load_sql, get_engine`.
    - `check_tables`-Query auf `information_schema.tables` (zeigt u. a. `cohort_aki`, `cohort_respiratory`).
  - **Severity-DataFrame (aktuell mit Fehler)**:
    - `df_severity = load_sql('../sql/get_severity_stats.sql')`.
    - Fehler, weil im aktuellen Setup `mimic_derived.sapsii` nicht existiert (stattdessen `mimiciii_derived.sapsii` via `src/utils.py`).
  - **Geplante Auswertung**:
    - Bildung von `saps_group` via `pd.cut(df_severity['sapsii'], ...)`.
    - Balkenplot der Mortalität (`outcome_death`) pro SAPS-II-Gruppe, ggf. nach Beatmung (`intervention_ventilation`) gestratifiziert.

**Hinweis**: Für eine lauffähige Version sollte `get_severity_stats.sql` an `mimiciii_derived.sapsii` angepasst oder `src.utils.add_sapsii_score` verwendet werden.

---

### 4. „Target Trial Light“: Vasopressoren-Timing, AKI, IPTW (Propensity)

- **Fragestellung**: Wie beeinflusst der Zeitpunkt der hämodynamischen Stabilisierung (Vasopressor-Beginn) das Outcome (Mortalität, ggf. RRT) bei AKI‑Patient:innen?
- **Notebook**: `notebooks/Target-Trial-Light-vasso.ipynb`
- **Kernidee** (laut Notebook-Kommentaren):
  - Kohorte: ICU-Stays mit AKI (ICD‑9 `584*`), Landmark bei 24 h nach ICU‑Intime.
  - Exposition: `early_vaso` (frühe Vasopressor-Gabe) vs. `no early vaso`.
  - Outcome: Hospital-Mortalität (`hospital_mortality`), optional nach AKI‑Stadium (`aki_stage_24h`).
- **Wesentliche Schritte**:
  - **DB & Helper**:
    - Laden von `.env` und Bau der `engine` mit `sqlalchemy.create_engine`.
    - Funktion `q(sql: str)` als Wrapper um `pd.read_sql`.
  - **Kohortenbau & Features**:
    - Erstellung einer AKI-Kohorte (SQL-CTEs: AKI per ICD‑Codes, ICU-Stays mit `intime`, `t6`, `t24` etc.).
    - Baseline-Covariates: Alter, Geschlecht, ICU‑Typ, Admission‑Type, Charlson‑Index, Kreatinin (baseline/peak 24 h), frühe Beatmung, frühe Fluids usw.
    - Definition von `aki_stage_24h` via `stage_kdigo_24h(baseline, peak)`:
      - Gibt AKI‑Stadium 0–3 zurück; Ausgabe `df['aki_stage_24h']`.
  - **Propensity-Score & IPTW**:
    - Konstruktion von Feature-Matrix `X` (numerische + kategoriale Variablen) und Treatment-Variable `y_treat = early_vaso`.
    - `ColumnTransformer` + `OneHotEncoder` für Präprozessierung.
    - `ps_model = Pipeline([... , LogisticRegression(max_iter=2000, solver="lbfgs")])`.
    - Fit: `ps_model.fit(X, y_treat)` → `ps = predict_proba(X)[:, 1]`.
    - Berechnung **stabilisierter IPTW**:
      - `p_treat = y_treat.mean()`.
      - `w = np.where(y_treat == 1, p_treat/ps, (1-p_treat)/(1-ps))`.
      - Trimming: `w_trunc = np.clip(w, q01, q99)`.
      - Speichern in `df["ps"]`, `df["w"]`.
  - **Outcome-Analysen**:
    - Funktion `weighted_mean(a, w)` und Berechnung:
      - `risk_t`, `risk_c`, `rd`, `rr` (treated vs. control) global.
    - **Subgruppen-Analysen**:
      - `subgroup_effect(df_sub)` mit obigem Schema.
      - Aufruf z. B. für:
        - Geschlecht (`res_sex` für `female` vs. `male`),
        - AKI‑Stadium (`res_stage = df.groupby('aki_stage_24h').apply(subgroup_effect)`),
        - weitere Subgruppen nach Bedarf.
    - Ergebnis sind Tabellen je Subgruppe mit:
      - `n`, `risk_t`, `risk_c`, `rd`, `rr`.
  - **Korrelation & Visualisierung**:
    - `df_corr = df[["hospital_mortality","early_vaso",...,"aki_stage_24h","ps","w"]].corr(...)`.
    - Heatmap der Korrelationen (`plt.imshow` + Achsenbeschriftung).

Diese Notebook liefert den Großteil der Propensity‑/IPTW‑Logik, an die ihr für Dialyse‑Timing oder erweiterte Interventionsanalysen direkt anschließen könnt.

---

### 5. Nieren-/AKI‑spezifische Notebooks

- **Ordner**: 
  - `AkiNotebooks/` → `00_start_aki.ipynb`, `01_first_analyze.ipynb`, `02_interventions.ipynb`
  - `NierenNotebooks/` → `00_build_views.ipynb`, `01_start_analyse.ipynb`, `02.ipynb`, `03_dopamin.ipynb`, `04_test.ipynb`, `05_more_analyse.ipynb`, `06.ipynb`, `07_saps2.ipynb`
- **Rolle im Projekt**:
  - Aufbau und Analyse von Nierenkohorten, AKI‑Stadien, Dialyse-Analysen, zusätzliche Score-Auswertungen (z. B. SAPS2).
  - Teile daraus sind thematisch verwandt mit `src/utils.py` (Dialysefunktionen, SOFA/SAPS) und mit dem „Target Trial Light“-Notebook.
  - Für die finale Abgabe/Präsi sind diese Notebooks eher **Unterbau**; die zentralen „Story‑Notebooks“ sind aktuell:
    - `t_05_peep.ipynb` (PEEP/ARDSnet),
    - `t_03_saps-ii.ipynb` (Schweregrad),
    - `Target-Trial-Light-vasso.ipynb` (Propensity/IPTW),
    - plus 1–2 AKI-/Dialyse-Notebooks je nach Fragestellung.

---

### 6. Utilities & SQL als gemeinsame Basis

- **Datei**: `src/db.py`
  - Zentrale `engine` auf Basis `.env` und Helper `q(sql: str) -> pd.DataFrame`.
  - Empfohlen, statt in jedem Notebook eigene Engines zu bauen.

- **Datei**: `src/utils.py`
  - **Scores**:
    - `add_sofa_score(df_aki)` → Gesamt- und Komponenten‑SOFA aus `mimiciii_derived.sofa`.
    - `add_sapsii_score(df_aki)` → SAPS II & Komponenten aus `mimiciii_derived.sapsii`.
  - **Dialyse & RRT**:
    - `add_dialysis_flag`, `add_early_late_dialysis_flags`, `extract_dialysis_timing`,
      `add_dialysis_near_icu_discharge_flag`, `add_rrt_persistence_near_discharge`,
      `add_dialysis_dependent_at_discharge_flag`.
  - **Interventionen**:
    - `add_vasopressor_flags` (Norepi/Epi/Phenyleph/Vasopressin),
    - `add_early_dopamine_flag`,
    - `add_mechanical_ventilation_flag`,
    - `add_early_fluid_flag`, `add_early_diuretic_flag`.
  - **Sonstiges**:
    - `add_icu_los_days` (ICU‑Verweildauer),
    - `recode_ethnicity` (Aggregation von Ethnie in sinnvolle Gruppen).

- **Ordner**: `sql/`
  - Enthält Kohorten- und Konzept-Skripte (`t_create_cohort_respiratory.sql`, `concepts_postgres/severityscores/*.sql`, `.../organfailure/*.sql` usw.), auf die mehrere Notebooks aufbauen.

---

### 7. Scores und Zeitfenster

#### Welcher Score wofür?

| Krankheitsbild / Fokus | Sinnvolle Scores | Begründung |
|------------------------|------------------|------------|
| **AKI (Nierenersatz, Dialyse-Timing)** | KDIGO-Stadium (`aki_stage`), SOFA renal, SOFA total | KDIGO ist Standard fuer AKI-Schwere; SOFA renal organbezogen; SOFA total fuer Case-Mix/Propensity. |
| **Respiratorische Versorgung (PEEP, Beatmung)** | SOFA respiration, PaO2/FiO2, SOFA total | SOFA respiration und PaO2/FiO2 leitlinienrelevant; SOFA total fuer Schweregrad-Adjustierung. |
| **Allgemeine ICU-Schwere (Mortalitaet)** | SAPS II, SOFA total | SAPS II etabliert fuer Mortalitaetsvorhersage; SOFA fuer Organversagen. |

#### Zeitfenster der vorhandenen Scores

- **SOFA** (`mimiciii_derived.sofa` / `add_sofa_score`): berechnet ueber die **ersten 24 Stunden** des ICU-Aufenthalts (first-day-Definition; nutzt `*_first_day`-Views).
- **SAPS II** (`mimiciii_derived.sapsii` / `add_sapsii_score`): ebenfalls **first 24h**.
- **aki_stage_24h** (in `Target-Trial-Light-vasso.ipynb`): KDIGO-aehnlich, basierend auf Baseline-Kreatinin und Peak-Kreatinin in den **ersten 24h**.

#### Flexible Zeitfenster aus Rohdaten

Fuer andere Zeitfenster (z. B. erste 6h, 12h oder 48h) stehen Hilfsfunktionen in `src/utils.py` zur Verfuegung, die direkt auf den MIMIC-III-Rohdaten (`labevents`, `chartevents`, `outputevents`, `inputevents_mv`) arbeiten:

- `get_labs_for_window(window_hours)` -- Laborwerte (Krea, Bili, Thrombo, BUN, WBC usw.) fuer ein beliebiges Zeitfenster.
- `get_vitals_for_window(window_hours)` -- Vitalparameter (MAP, HR, Temp, SpO2, GCS) fuer ein beliebiges Zeitfenster.
- `get_urine_output_for_window(window_hours)` -- Urinmenge fuer ein beliebiges Zeitfenster.
- `compute_sofa_from_raw(window_hours)` -- Berechnet SOFA-Komponenten und Gesamtscore aus Rohdaten fuer ein beliebiges Zeitfenster.

Damit koennt ihr z. B. einen **Baseline-SOFA (first 6h)** oder **48h-SOFA** erzeugen, ohne neue DB-Tabellen anlegen zu muessen.

#### Limitation

Wenn SOFA/SAPS II (first 24h) als Baseline-Covariate in Propensity-Modellen genutzt wird und die Intervention innerhalb der ersten 24h beginnt, enthaelt der Score bereits Werte aus dem Interventionszeitraum. In der Methodik sollte das als Limitation genannt oder durch ein kuerzeres Fenster (z. B. first 6h) adressiert werden.

---

Diese Mapping-Datei ist als Navigationshilfe gedacht:

- Wenn du eine Figure oder Kennzahl aus der Präsentation suchst, kannst du von hier aus direkt sehen:
  - **Welches Notebook** sie erzeugt,
  - **welche Zellen/Blöcke** relevant sind,
  - und **welche Hilfsfunktionen** (`src/utils.py`) dahinter stecken.

