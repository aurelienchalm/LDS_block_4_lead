from fastapi import FastAPI, Query
from typing import Optional
from sqlalchemy import text
from app.database import engine
from app.model_predict import predict_dataframe
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pandas as pd
from datetime import datetime, timezone, timedelta
import math

app = FastAPI()


# 🔹 Modèle d'entrée pour l'insertion
class HousingInput(BaseModel):
    square_feet: float
    num_bedrooms: int
    num_bathrooms: int
    num_floors: int
    year_built: int
    has_garden: bool
    has_pool: bool
    garage_size: int
    location_score: float
    distance_to_center: float


# 🔹 Endpoint pour insérer un bien (sans gérer l'id côté app)
@app.post("/insert")
def insert_housing(data: HousingInput):
    payload = data.dict()
    payload["has_garden"] = int(payload["has_garden"])
    payload["has_pool"] = int(payload["has_pool"])

    insert_stmt = text("""
        INSERT INTO housing_prices (
            square_feet, num_bedrooms, num_bathrooms, num_floors,
            year_built, has_garden, has_pool, garage_size,
            location_score, distance_to_center
        ) VALUES (
            :square_feet, :num_bedrooms, :num_bathrooms, :num_floors,
            :year_built, :has_garden, :has_pool, :garage_size,
            :location_score, :distance_to_center
        )
        RETURNING id, date_creation
    """)

    # engine.begin() = transaction context manager (commit auto si OK)
    with engine.begin() as conn:
        result = conn.execute(insert_stmt, payload)
        row = result.fetchone()
        new_id = int(row[0])
        created_at = row[1]

    return {
        "message": f"Bien inséré avec ID {new_id}",
        "id": new_id,
        "date_creation": created_at.isoformat() if created_at else None
    }


# 🔹 Endpoint pour prédire un bien non encore prédit
@app.post("/predict")
def predict_and_update(id: Optional[int] = Query(default=None)):
    with engine.connect() as conn:
        if id is None:
            query = text("""
                SELECT * FROM housing_prices
                WHERE price IS NULL AND price_predict IS NULL
                ORDER BY id DESC
                LIMIT 1
            """)
            result = conn.execute(query)
        else:
            result = conn.execute(text("SELECT * FROM housing_prices WHERE id=:id"), {"id": id})

        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        if df.empty:
            return JSONResponse(content={"message": "Aucune ligne à prédire."})

        df_pred = predict_dataframe(df)
        predicted_price = float(df_pred.iloc[0]["price_predict"])
        row_id = int(df_pred.iloc[0]["id"])

        update_stmt = text("""
            UPDATE housing_prices
            SET price_predict = :pred
            WHERE id = :id
        """)
        conn.execute(update_stmt, {"pred": predicted_price, "id": row_id})
        conn.commit()

        return JSONResponse(content={
            "id": row_id,
            "predicted_price": predicted_price
        })


# 🔹 Endpoint pour récupérer les dernières prédictions
@app.get("/predictions")
def get_recent_predictions(limit: int = Query(5, ge=1, le=100)):
    with engine.connect() as conn:
        query = text("""
            SELECT id, square_feet, num_bedrooms, num_bathrooms, num_floors,
                year_built, has_garden, has_pool, garage_size,
                location_score, distance_to_center, price_predict
            FROM housing_prices
            WHERE price_predict IS NOT NULL
            AND price_predict::text <> 'NaN'
            ORDER BY id DESC
            LIMIT :limit
        """)
        result = conn.execute(query, {"limit": limit})
        rows_raw = [dict(zip(result.keys(), row)) for row in result.fetchall()]

    def sanitize_value(v):
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return v

    rows = [
        {k: sanitize_value(v) for k, v in row.items()}
        for row in rows_raw
    ]

    return rows
    

# 🔹 Endpoint pour récupérer les lignes depuis la db pour l'entrainement du modèle   
@app.get("/train")
def get_training_data(years: int = 2):
    # point de départ de la fenêtre glissante (UTC)
    start_date = datetime.now(timezone.utc) - timedelta(days=365 * years)

    with engine.connect() as conn:
        query = text("""
            SELECT *
            FROM housing_prices
            WHERE price IS NOT NULL AND price_predict IS NULL
            AND date_creation >= :start_date
            ORDER BY date_creation DESC
        """)
        print(f"Start date utilisée : {start_date}")
        result = conn.execute(query, {"start_date": start_date})
        rows = [dict(zip(result.keys(), row)) for row in result.fetchall()]
        return rows