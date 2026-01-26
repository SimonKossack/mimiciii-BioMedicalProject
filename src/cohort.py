# src/cohort.py
import pandas as pd
from src.db import q

def load_aki_cohort():
    return q("""
        SELECT *
        FROM derived.mv_aki_icu_first_cohort
        WHERE age BETWEEN 18 AND 90
    """)
