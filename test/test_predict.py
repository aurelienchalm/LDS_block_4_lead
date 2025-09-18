import pandas as pd
from app.database import engine
from app.model_predict import predict_dataframe
from fastapi.testclient import TestClient
from app.main import app
from sqlalchemy import text

client = TestClient(app)

def test_db_connection():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1

def test_model_predict_dataframe():
    df_sample = pd.DataFrame([{
        "id": 99999,
        "square_feet": 100.0,
        "num_bedrooms": 2,
        "num_bathrooms": 1,
        "num_floors": 1,
        "year_built": 2000,
        "has_garden": 1,
        "has_pool": 0,
        "garage_size": 10,
        "location_score": 3.5,
        "distance_to_center": 5.0,
    }])
    df_result = predict_dataframe(df_sample)
    assert "price_predict" in df_result.columns
    assert isinstance(df_result["price_predict"].iloc[0], float)

def test_predict_endpoint():
    payload = {
        "area": 60,
        "property_type": "apartment",
        "rooms_number": 3,
        "zip_code": 75015,
        "land_area": 0,
        "garden": False,
        "garden_area": 0,
        "equipped_kitchen": True,
        "full_address": "Rue de Vaugirard, Paris",
        "building_state": "good"
    }

    response = client.post("/predict", json=payload)

    assert response.status_code == 200
    assert "prediction" in response.json() or "message" in response.json()