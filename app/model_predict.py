"""
model_predict.py — Version taguée

But : charger un modèle depuis le Model Registry MLflow en se basant sur un TAG
(posé par Jenkins après tests), au lieu de toujours prendre la dernière version.

ENV attendues :
- MLFLOW_TRACKING_URI : URI du tracking serveur MLflow
- MODEL_NAME          : Nom du modèle enregistré dans le Model Registry
- MODEL_TAG           : Le tag à rechercher. Deux formats acceptés :
    * "champion"  (équivaut à key="champion", value="true")
    * "role=champion" (key/value explicites)
- FALLBACK_TO_LATEST  : optionnel ("true"/"false"), par défaut "false". Si vrai et
    qu'aucune version taguée n'est trouvée, on tombe sur la dernière version.

Exemple côté Jenkins pour poser le tag si les tests passent :

from mlflow.tracking import MlflowClient
client = MlflowClient()
client.set_model_version_tag(
    name=os.environ["MODEL_NAME"],
    version=str(passed_version),
    key="champion", value="true"
)

"""

import os
from pathlib import Path
from typing import Tuple

import mlflow
import pandas as pd
from dotenv import load_dotenv
from mlflow.tracking import MlflowClient

# ──────────────────────────────────────────────────────────────────────────────
# Chargement des variables d'environnement (fichier .env à la racine du repo)
# ──────────────────────────────────────────────────────────────────────────────
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH, override=True)

# Variables d'env requises
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI")
MODEL_NAME = os.getenv("MODEL_NAME")
MODEL_TAG = os.getenv("MODEL_TAG", "champion")  # ex: "champion" ou "role=champion"
FALLBACK_TO_LATEST = os.getenv("FALLBACK_TO_LATEST", "false").lower() == "true"

if not MLFLOW_TRACKING_URI:
    raise RuntimeError("MLFLOW_TRACKING_URI manquant dans l'environnement")
if not MODEL_NAME:
    raise RuntimeError("MODEL_NAME manquant dans l'environnement")

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
print("✅ MLFLOW_TRACKING_URI:", MLFLOW_TRACKING_URI)
print("✅ MODEL_NAME:", MODEL_NAME)
print("✅ MODEL_TAG:", MODEL_TAG)
print("✅ FALLBACK_TO_LATEST:", FALLBACK_TO_LATEST)

# ──────────────────────────────────────────────────────────────────────────────
# Sélection du modèle par TAG dans le Model Registry
# ──────────────────────────────────────────────────────────────────────────────

def _parse_tag(tag_expr: str) -> Tuple[str, str]:
    """Retourne (key, value) à partir de "champion" ou "role=champion"."""
    if "=" in tag_expr:
        key, value = tag_expr.split("=", 1)
        return key.strip(), value.strip()
    # format clé seule → value=true
    return tag_expr.strip(), "true"


def _get_tagged_model_uri(model_name: str, tag_expr: str, fallback_to_latest: bool = False) -> tuple:
    client = MlflowClient()
    tag_key, tag_value = _parse_tag(tag_expr)

    versions = client.search_model_versions(f"name='{model_name}'")
    if not versions:
        raise RuntimeError(f"Aucune version trouvée pour le modèle '{model_name}'.")

    tagged = []
    for v in versions:
        v_tags = getattr(v, "tags", None) or {}
        if v_tags.get(tag_key) == tag_value:
            tagged.append(v)

    def _version_num(mv):
        try:
            return int(getattr(mv, "version", "0"))
        except Exception:
            return 0

    if tagged:
        best = sorted(tagged, key=_version_num, reverse=True)[0]
        print(f"🎯 Version taguée trouvée: {best.version} (tag {tag_key}={tag_value})")
        return f"models:/{model_name}/{best.version}", best.version

    if fallback_to_latest:
        best = sorted(versions, key=_version_num, reverse=True)[0]
        print(
            f"⚠️ Aucun modèle tagué {tag_key}={tag_value} trouvé. "
            f"Bascule sur la dernière version: {best.version}"
        )
        return f"models:/{model_name}/{best.version}", best.version

    raise RuntimeError(
        f"Aucune version taguée '{tag_key}={tag_value}' trouvée pour '{model_name}'. "
        f"(Active FALLBACK_TO_LATEST=true pour utiliser la dernière version en secours)"
    )


MODEL_URI, MODEL_VERSION = _get_tagged_model_uri(MODEL_NAME, MODEL_TAG, FALLBACK_TO_LATEST)
print("✅ MODEL_URI:", MODEL_URI)
print("✅ MODEL_VERSION:", MODEL_VERSION)

# ──────────────────────────────────────────────────────────────────────────────
# Chargement du modèle
# ──────────────────────────────────────────────────────────────────────────────
loaded_model = mlflow.pyfunc.load_model(MODEL_URI)

# ──────────────────────────────────────────────────────────────────────────────
# Schéma d'entrée
# ──────────────────────────────────────────────────────────────────────────────
FEATURES = [
    "square_feet", "num_bedrooms", "num_bathrooms", "num_floors",
    "year_built", "has_garden", "has_pool", "garage_size",
    "location_score", "distance_to_center",
]


def predict_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Retourne un DataFrame avec la colonne 'price_predict'."""
    print("MODEL_URI:", MODEL_URI)
    print("MODEL_VERSION:", MODEL_VERSION)
    df = df.copy()
    df[FEATURES] = df[FEATURES].astype("float64")  # alignement types modèle
    df["price_predict"] = loaded_model.predict(df[FEATURES])
    return df
