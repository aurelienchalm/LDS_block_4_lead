import argparse
import os
import time
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine, text
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_squared_error

import mlflow
from mlflow.models import infer_signature
import requests

# Chargement des variables d’environnement
env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=env_path, override=True)

# -------------------------------
# Fonctions testables
# -------------------------------

def load_data_from_api(api_url):
    response = requests.get(f"{api_url}/train")
    response.raise_for_status()
    data = response.json()
    return pd.DataFrame(data)

def drop_outlier(dataset, col, threshold=3):
    mean = dataset[col].mean()
    std = dataset[col].std()
    upper = mean + threshold * std
    lower = mean - threshold * std
    return dataset[(dataset[col] <= upper) & (dataset[col] >= lower)]

def clean_data(df, cols=["price"]):
    cleaned_df = df.copy()
    for col in cols:
        cleaned_df = drop_outlier(cleaned_df, col)
    return cleaned_df

def get_feature_target(df):
    features = [
        'square_feet', 'num_bedrooms', 'num_bathrooms', 'num_floors',
        'year_built', 'has_garden', 'has_pool', 'garage_size',
        'location_score', 'distance_to_center'
    ]
    target = "price"
    X = df[features]
    y = df[target]
    return X, y

def build_model(numeric_features):
    numerical_transformer = Pipeline(steps=[
        ('scaler', StandardScaler())
    ])
    preprocessor = ColumnTransformer(transformers=[
        ('num', numerical_transformer, numeric_features)
    ])
    model = Pipeline(steps=[
        ("Preprocessing", preprocessor),
        ("Regressor", LinearRegression())
    ])
    return model

def train_and_log(X_train, X_test, y_train, y_test, model):
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))
    mlflow.set_experiment("Housing_prices_estimator_from_DB")

    with mlflow.start_run():
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        # Log des métriques
        rmse = mean_squared_error(y_test, y_pred)
        mlflow.log_metric("rmse_test", rmse)
    
        # Log du modèle
        signature = infer_signature(X_train, y_pred)
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="Housing_prices_estimator_from_DB",
            registered_model_name="Housing_prices_estimator_LR_from_DB",
            signature=signature,
            input_example=X_train[:5],
        )
    return rmse

# -------------------------------
# Exécution principale
# -------------------------------

def main():
    print("🏗️ Training model...")
    start_time = time.time()

    api_url = os.getenv("API_HOST") 
    raw_df = load_data_from_api(api_url)
    df = clean_data(raw_df)
    X, y = get_feature_target(df)

    # Split train/test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Forcer conversion en float64 si int
    X_train = X_train.astype({col: "float64" for col in X_train.select_dtypes("int").columns})

    model = build_model(numeric_features=X.columns.tolist())
    rmse = train_and_log(X_train, X_test, y_train, y_test, model)

    print(f"Training complete. RMSE: {rmse:.2f}")
    print(f"⏱ Duration: {round(time.time() - start_time, 2)} seconds")
    
if __name__ == "__main__":
    main()
    
