# app_real/main.py
import os
from typing import List, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

#from pathlib import Path
from dotenv import load_dotenv

#env_path = Path(__file__).resolve().parents[1] / ".env"
#load_dotenv(dotenv_path=env_path, override=True)
# Charge le .env (utile en local ; en Docker on passera --env-file)
load_dotenv()

POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
if not POSTGRES_DATABASE:
    raise RuntimeError("POSTGRES_DATABASE environment variable is not set")

# Connexion SQLAlchemy
engine = create_engine(POSTGRES_DATABASE)

app = FastAPI(
    title="Housing Real Prices Export API",
    description="API qui expose le contenu de housing_prices_real en JSON pour l'entraînement ML",
    version="1.0.0",
)


class HousingPriceReal(BaseModel):
    id: int
    square_feet: float
    num_bedrooms: int
    num_bathrooms: int
    num_floors: int
    year_built: int
    has_garden: int
    has_pool: int
    garage_size: int
    location_score: float
    distance_to_center: float
    price: float
    price_predict: Optional[float] = None
    date_creation: Optional[datetime] = None


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.get("/housing_real", response_model=List[HousingPriceReal])
def get_housing_real():
    """
    Retourne toutes les lignes de housing_prices_real sous forme de JSON.
    Ce JSON sera utilisé par Airflow pour générer un CSV et le pousser sur S3.
    """
    query = text("""
        SELECT
            id,
            square_feet,
            num_bedrooms,
            num_bathrooms,
            num_floors,
            year_built,
            has_garden,
            has_pool,
            garage_size,
            location_score,
            distance_to_center,
            price,
            price_predict,
            date_creation
        FROM housing_prices_real
        WHERE date_creation >= NOW() - INTERVAL '7 days'
        ORDER BY date_creation DESC
    """)

    try:
        with engine.connect() as conn:
            result = conn.execute(query)
            rows = result.mappings().all()  # renvoie une liste de dict-like
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    # On laisse Pydantic faire la sérialisation (notamment des timestamps)
    return [HousingPriceReal(**row) for row in rows]