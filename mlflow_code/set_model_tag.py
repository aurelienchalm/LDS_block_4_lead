"""
set_model_tag.py
──────────────────────────────────────────────
But : Positionner automatiquement un tag sur la dernière version d'un modèle MLflow.

Ce script sera appelé à la fin du pipeline Jenkins, une fois que
la dernière version du modèle a été validée par les tests.

Variables d'environnement requises :
- MLFLOW_TRACKING_URI : URI de ton serveur MLflow
- MODEL_NAME          : Nom du modèle enregistré dans le Model Registry
- TAG_KEY             : Clé du tag (par défaut : "champion")
- TAG_VALUE           : Valeur du tag (par défaut : "true")

Exécution en CLI :
$ python mlflow_code/set_model_tag.py
"""

import os
from mlflow.tracking import MlflowClient
from dotenv import load_dotenv
from pathlib import Path

# ───────────────────────────────────────────────────────────────
# Chargement des variables d'environnement
# ───────────────────────────────────────────────────────────────
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH, override=True)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI")
MODEL_NAME = os.getenv("MODEL_NAME")
TAG_KEY = os.getenv("TAG_KEY", "champion")
TAG_VALUE = os.getenv("TAG_VALUE", "true")

if not MLFLOW_TRACKING_URI:
    raise RuntimeError("❌ MLFLOW_TRACKING_URI manquant dans l'environnement.")
if not MODEL_NAME:
    raise RuntimeError("❌ MODEL_NAME manquant dans l'environnement.")

# Initialisation du client MLflow
client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)

print(f"🔗 MLflow URI : {MLFLOW_TRACKING_URI}")
print(f"📦 Model name : {MODEL_NAME}")
print(f"🏷️ Tag à appliquer : {TAG_KEY}={TAG_VALUE}")

# ───────────────────────────────────────────────────────────────
# Récupération de la dernière version du modèle
# ───────────────────────────────────────────────────────────────
versions = client.search_model_versions(f"name='{MODEL_NAME}'")

if not versions:
    raise RuntimeError(f"❌ Aucun modèle trouvé pour '{MODEL_NAME}'")

# Trier par numéro de version décroissant pour trouver la dernière
latest_version = sorted(versions, key=lambda v: int(v.version), reverse=True)[0]

print(f"✅ Dernière version trouvée : {latest_version.version}")

# ───────────────────────────────────────────────────────────────
# Application du tag "unique" : on supprime le tag des autres versions,
# puis on le pose sur la dernière version validée.
# ───────────────────────────────────────────────────────────────
# 1) Retirer le tag des autres versions si présent
cleared = []
for mv in versions:
    if mv.version != latest_version.version:
        mv_tags = getattr(mv, "tags", {}) or {}
        if mv_tags.get(TAG_KEY) == TAG_VALUE:
            client.delete_model_version_tag(
                name=MODEL_NAME,
                version=str(mv.version),
                key=TAG_KEY,
            )
            cleared.append(mv.version)

if cleared:
    print(f"🧹 Tag {TAG_KEY}={TAG_VALUE} retiré des versions : {sorted(cleared)}")

# 2) Poser le tag sur la dernière version
client.set_model_version_tag(
    name=MODEL_NAME,
    version=str(latest_version.version),
    key=TAG_KEY,
    value=TAG_VALUE
)
print(f"🏷️ Tag appliqué avec succès sur la version {latest_version.version} !")