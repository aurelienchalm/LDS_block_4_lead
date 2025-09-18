import pytest
import pandas as pd
from sqlalchemy import inspect, text, MetaData
import boto3

import sys
import os

#pour ne pas avoir ModuleNotFoundError.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))


from load_to_db import (
    get_housing_table,
    create_table_if_not_exists,
    fetch_csv_from_s3,
    insert_data
)


def test_postgres_connection(db_engine):
    """Test simple de la connexion PostgreSQL"""
    with db_engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1


def test_table_created(db_engine):
    metadata = MetaData()
    table = get_housing_table(metadata)
    create_table_if_not_exists(db_engine, metadata, table)
    inspector = inspect(db_engine)
    assert table.name in inspector.get_table_names()


def test_table_schema(db_engine):
    """Valide que le schéma de la table est correct"""
    expected_columns = [
        "id", "square_feet", "num_bedrooms", "num_bathrooms",
        "num_floors", "year_built", "has_garden", "has_pool",
        "garage_size", "location_score", "distance_to_center",
        "price", "price_predict", "date_creation"
    ]
    inspector = inspect(db_engine)
    columns = [col["name"] for col in inspector.get_columns("housing_prices")]
    assert columns == expected_columns


def test_s3_fetch(monkeypatch):
    """Mocke l'accès S3 pour tester le parsing du CSV"""
    
    class MockS3Client:
        def get_object(self, Bucket, Key):
            return {
                'Body': open("test/test_data.csv", "rb")
            }

    
    monkeypatch.setattr(boto3, "client", lambda *args, **kwargs: MockS3Client())

    df = fetch_csv_from_s3(bucket_name="fake", key="fake.csv")
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "square_feet" in df.columns




def test_insert_data_on_test_table(db_engine):
    test_table_name = "housing_prices_test"
    metadata = MetaData()
    table = get_housing_table(metadata, name=test_table_name)

    # Créer la table dans une transaction explicite (commitée)
    with db_engine.begin() as conn:
        table.create(bind=conn)

    # Préparer un DataFrame de test
    df = pd.DataFrame([{
        "square_feet": 100.0,
        "num_bedrooms": 3,
        "num_bathrooms": 1,
        "num_floors": 1,
        "year_built": 2000,
        "has_garden": 1,
        "has_pool": 0,
        "garage_size": 1,
        "location_score": 7.5,
        "distance_to_center": 2.5,
        "price": 200000,
        "price_predict": 210000
    }])

    # Insertion dans la table avec la fonction
    insert_data(df, table, db_engine)

    # Vérifier qu'il y a bien une ligne
    with db_engine.connect() as conn:
        count = conn.execute(text(f"SELECT COUNT(*) FROM {test_table_name}")).scalar()
        assert count >= 1

    # Supprimer la table à la fin
    with db_engine.begin() as conn:
        table.drop(bind=conn)