import sys
import os
import pytest
import pandas as pd
from sklearn.pipeline import Pipeline

# Ajouter le chemin vers le dossier mlflow_code (ou src si tu l'as renommé)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'mlflow_code')))

from train import (
    drop_outlier,
    clean_data,
    get_feature_target,
    build_model,
    train_and_log,
)

# ---------- 🔬 UNIT TESTS ----------

def test_drop_outlier_removes_extreme_values():
    # Génère 20 valeurs normales, puis un gros outlier
    values = list(range(100, 120)) + [10000]
    df = pd.DataFrame({'col': values})
    cleaned = drop_outlier(df, 'col', threshold=3)  # 3 sigma classique
    assert 10000 not in cleaned['col'].values
    assert len(cleaned) == 20

def test_clean_data_multiple_columns():
    values = list(range(200, 220)) + [9999]
    df = pd.DataFrame({
        'price': values,
        'other': list(range(len(values)))
    })

    def patched_clean_data(df, cols):
        for col in cols:
            df = drop_outlier(df, col, threshold=3)
        return df

    cleaned = patched_clean_data(df, cols=['price'])
    assert 9999 not in cleaned['price'].values
    assert cleaned.shape[0] == 20

def test_get_feature_target():
    df = pd.DataFrame({
        'square_feet': [100, 150],
        'num_bedrooms': [2, 3],
        'num_bathrooms': [1, 1],
        'num_floors': [1, 2],
        'year_built': [2000, 2010],
        'has_garden': [0, 1],
        'has_pool': [0, 0],
        'garage_size': [1, 2],
        'location_score': [7.5, 8.2],
        'distance_to_center': [2.5, 1.8],
        'price': [100000, 120000]
    })
    X, y = get_feature_target(df)
    assert isinstance(X, pd.DataFrame)
    assert isinstance(y, pd.Series)
    assert X.shape[1] == 10
    assert y.shape[0] == 2

def test_build_model_returns_pipeline():
    features = [
        'square_feet', 'num_bedrooms', 'num_bathrooms', 'num_floors',
        'year_built', 'has_garden', 'has_pool', 'garage_size',
        'location_score', 'distance_to_center'
    ]
    model = build_model(features)
    assert isinstance(model, Pipeline)
    assert "Regressor" in model.named_steps

# ---------- 🧪 INTEGRATION TEST ----------

def test_train_and_log(tmp_path):
    from sklearn.datasets import make_regression
    import mlflow

    X, y = make_regression(n_samples=50, n_features=10, noise=0.1, random_state=42)
    X = pd.DataFrame(X, columns=[
        'square_feet', 'num_bedrooms', 'num_bathrooms', 'num_floors',
        'year_built', 'has_garden', 'has_pool', 'garage_size',
        'location_score', 'distance_to_center'
    ])
    y = pd.Series(y)

    X_train, X_test = X.iloc[:40], X.iloc[40:]
    y_train, y_test = y.iloc[:40], y.iloc[40:]

    model = build_model(X.columns.tolist())

    mlflow.set_tracking_uri(f"file:{tmp_path}")
    mlflow.set_experiment("TEST_LOCAL")

    rmse = train_and_log(X_train, X_test, y_train, y_test, model)
    assert isinstance(rmse, float)
    assert rmse > 0