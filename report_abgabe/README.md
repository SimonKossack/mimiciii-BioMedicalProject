# Code-Abgabe: „Critical Decisions in the ICU“

Dieser Ordner enthält nur den Code, der zur **Präsentation „Critical Decisions in the ICU“** (Kritische Entscheidungen auf der Intensivstation) gehört: Auswirkungen des **Timings von Interventionen** (Flüssigkeiten, Diuretika, Vasopressoren, Dialyse) auf die **Krankenhausmortalität** in der **AKI-Kohorte** (MIMIC-III), inkl. Schweregrad-Scores (SOFA, SAPS II) und vertiefender Analysen (Chi-Quadrat, logistische Regression, Subgruppen).

## Struktur

```
report_abgabe/
├── README.md                 (diese Datei)
├── analysis_mapping.md       (Präsentation ↔ Code, Scores & Zeitfenster)
├── task.md                   (Aufgabenstellung)
├── .env.example              (DB-Variablen ohne geheime Werte)
├── notebooks/
│   └── t_03_saps-ii.ipynb    SAPS-II, Schweregrad & Mortalität (Korrekturfaktoren)
├── nieren/
│   └── 07_saps2.ipynb        AKI-Kohorte: Interventionen, Mortalität, Timing, SOFA/SAPS II, Chi², log. Regression
├── src/
│   ├── db.py                 DB-Engine & q(sql)
│   ├── db_connect.py         get_engine(), load_sql() für t_03_saps-ii
│   ├── utils.py              SOFA/SAPS, Dialyse-, Interventions-Flags
│   └── cohort.py             load_aki_cohort() (benötigt derived.mv_aki_icu_first_cohort)
└── sql/
    ├── build_7_views.sql     First-day-Views (Urin, Vitals, GCS, Labs, Blood Gas, Ventilation)
    ├── t_create_cohort_respiratory.sql  Kohorte respiratorisch (für t_03 optional)
    └── concepts_postgres/    Nur für Abgabe: firstday (6), durations (2), severityscores (sofa, sapsii), echo_data, postgres-functions
```

## Voraussetzungen

- **Python:** z. B. 3.9+ mit pandas, numpy, matplotlib, seaborn, scikit-learn, sqlalchemy, psycopg2-binary, python-dotenv
- **PostgreSQL** mit MIMIC-III und ggf. erstellten Schemas/Tabellen: `mimiciii_derived` (SOFA, SAPS II), `derived.mv_aki_icu_first_cohort` (AKI-Kohorte), optional `cohort_respiratory`

## Einrichtung

1. `.env.example` nach `.env` kopieren (im Ordner `report_abgabe`) und eintragen: `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`, `DB_NAME`
2. **Arbeitsverzeichnis:** Kernel/CWD so setzen, dass `src` importierbar ist (z. B. CWD = `report_abgabe`).
3. **t_03_saps-ii** nutzt `from src.db_connect import get_engine, load_sql`; **07_saps2** nutzt `from src.cohort import load_aki_cohort` und `from src.utils import ...`.

## Ausführung der Notebooks

- **notebooks/t_03_saps-ii.ipynb:** SAPS-II und Mortalität (Korrekturfaktoren). Nutzt `src.db_connect`, ggf. `get_severity_stats.sql` (Schema auf `mimiciii_derived.sapsii` anpassen) oder Kohorte + `add_sapsii_score()` aus `src.utils`.
- **nieren/07_saps2.ipynb:** Kern der Präsentation: AKI-Kohorte (10.485), Interventionen (Flüssigkeiten, Diuretika, Vasopressoren, Dialyse), Mortalität nach Intervention und nach Timing, SOFA/SAPS II nach Timing, Chi-Quadrat-Tabellen, bereinigte Mortalität (logistische Regression). Erwartet View `derived.mv_aki_icu_first_cohort`.

## SQL

- **build_7_views.sql** aus dem Ordner **report_abgabe** ausführen:
  ```bash
  cd report_abgabe
  psql -U postgres -d mimic -f sql/build_7_views.sql
  ```
- Optional: SOFA/SAPS-II aus `sql/concepts_postgres/severityscores/` (sofa.sql, sapsii.sql), falls noch nicht vorhanden.

## Referenz

- **analysis_mapping.md:** Zuordnung der Präsentationsinhalte (Grafiken, Tabellen) zu den Notebooks und zu Scores/Zeitfenstern.
