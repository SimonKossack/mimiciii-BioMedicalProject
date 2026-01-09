import os
import pandas as pd
from sqlalchemy import create_engine, text  # <--- WICHTIG: "text" importieren
from dotenv import load_dotenv

load_dotenv()

def get_engine():
    """Erstellt die Verbindung zur Datenbank."""
    user = os.getenv("DB_USER")
    pw   = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db   = os.getenv("DB_NAME")

    if not user or not pw:
        raise ValueError("Fehler: .env Datei nicht gefunden oder leer!")

    conn_str = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"
    return create_engine(conn_str)

def load_sql(sql_path, params=None):
    """Liest SQL-Datei und gibt DataFrame zurück (robust für SQLAlchemy 2.x)."""
    engine = get_engine()

    with open(sql_path, "r", encoding="utf-8") as f:
        query_str = f.read()

    stmt = text(query_str)

    # params IMMER als normales dict, nie als SQLAlchemy-Objekt
    if params is None:
        params = {}

    with engine.connect() as conn:
        return pd.read_sql_query(stmt, conn, params=params)
