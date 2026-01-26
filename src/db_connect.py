import os
import pandas as pd
from sqlalchemy import create_engine, text  # <--- WICHTIG: "text" importieren
from dotenv import load_dotenv

# --- DIESER TEIL MUSS GEÄNDERT WERDEN ---
# Wir suchen die .env Datei relativ zu dieser Datei (db_connect.py)
# os.path.dirname(__file__) ist der 'src' Ordner
# '..' geht eine Ebene hoch zum Hauptprojektordner
current_dir = os.path.dirname(__file__)
dotenv_path = os.path.join(current_dir, '..', '.env')

load_dotenv(dotenv_path)
# ---------------------------------------

def get_engine():
    """Erstellt die Verbindung zur Datenbank."""
    user = os.getenv("DB_USER")
    pw   = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db   = os.getenv("DB_NAME")

    # WICHTIG: Wir entfernen 'not pw' aus der Fehlermeldung, 
    # damit ein leeres Passwort erlaubt ist!
    if not user:
        raise ValueError(f"Fehler: DB_USER wurde in .env nicht gefunden!")

    # Falls pw None ist (weil leer), machen wir einen leeren String daraus
    if pw is None:
        pw = ""

    if pw:
        conn_str = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"
    else:
        # Verbindung ohne Passwort
        conn_str = f"postgresql+psycopg2://{user}@{host}:{port}/{db}"
        
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
