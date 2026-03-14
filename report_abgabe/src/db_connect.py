# db_connect: get_engine() und load_sql() für Notebooks (t_03_saps-ii, t_05_peep)
from pathlib import Path
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# .env im übergeordneten Ordner (report_abgabe oder Projektwurzel)
_env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(_env_path)

def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME')}"
    )

engine = get_engine()

def q(sql: str):
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)

def load_sql(sql_path, params=None):
    """Liest eine SQL-Datei und führt sie aus; gibt Ergebnis als DataFrame zurück."""
    path = Path(sql_path)
    if not path.is_absolute():
        # Relativ zum aktuellen Arbeitsverzeichnis oder zum Ordner des Aufrufers
        path = Path.cwd() / path
    if not path.exists():
        path = Path(__file__).resolve().parents[1] / sql_path
    stmt = path.read_text(encoding="utf-8", errors="replace")
    if params is None:
        params = {}
    with engine.connect() as conn:
        return pd.read_sql(text(stmt), conn, params=params)
