from pathlib import Path
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

_env_dir = Path(__file__).resolve().parents[1]
load_dotenv(_env_dir / ".env")
if not os.getenv("DB_HOST"):
    raise RuntimeError(
        "DB_HOST (und ggf. DB_USER, DB_PASSWORD, DB_NAME) nicht gesetzt. "
        "Kopiere report_abgabe/.env.example nach report_abgabe/.env und trage die Zugangsdaten ein."
    )

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT','5432')}/{os.getenv('DB_NAME')}"
)

def q(sql: str) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)

