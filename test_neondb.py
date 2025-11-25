import psycopg2
from psycopg2 import OperationalError
from pathlib import Path
from dotenv import load_dotenv
import os

env_path = Path(__file__).resolve().parents[0] / ".env"
load_dotenv(dotenv_path=env_path, override=True)

DATABASE_URL = os.getenv("POSTGRES_DATABASE")

try:
    conn = psycopg2.connect(DATABASE_URL)
    print("✅ Connexion réussie à NeonDB")
    conn.close()

except OperationalError as e:
    print("❌ Connexion échouée")
    #print("Détail :", e)