import os
import json
from io import BytesIO
import pandas as pd
import pytest
import sys
import pathlib

# Pour ne pas avoir ModuleNotFoundError: on ajoute le dossier 'evidently' au PYTHONPATH.
ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "evidently"))

import evidently_dashboard as ed


# =========================
# Mocks / Fixtures S3
# =========================

class _MockS3Client:
    """
    Mock très simple de boto3.client('s3') pour get_object et upload_file.
    - objects: dict[(bucket, key) -> str_csv]
    - uploads: dict[(bucket, key) -> {"filename": <local>, "extra": ExtraArgs}]
    """
    def __init__(self, objects):
        self.objects = objects
        self.uploads = {}

    def get_object(self, Bucket, Key):
        key = (Bucket, Key)
        if key not in self.objects:
            raise RuntimeError(f"Objet S3 manquant dans le mock: s3://{Bucket}/{Key}")
        return {"Body": BytesIO(self.objects[key].encode("utf-8"))}

    def upload_file(self, Filename, Bucket, Key, ExtraArgs=None):
        self.uploads[(Bucket, Key)] = {
            "filename": Filename,
            "extra": ExtraArgs or {},
        }


@pytest.fixture
def patch_boto3_client(monkeypatch):
    """
    Fixture qui patch boto3.client pour retourner notre client mocké.
    Utilisation:
        state = patch_boto3_client({ (bucket, key): "csv,...\n" })
        # state["client"].uploads contiendra les uploads effectués
    """
    state = {"mapping": {}, "client": None}

    def _factory(*args, **kwargs):
        state["client"] = _MockS3Client(state["mapping"])
        return state["client"]

    def setter(mapping: dict):
        state["mapping"] = mapping
        return state

    monkeypatch.setattr("boto3.client", _factory)
    return setter


# =========================
# Tests unitaires simples
# =========================

def test_normalize_df_columns():
    df = pd.DataFrame(columns=["Square Feet", "Num-Bedrooms", "Price", "Date-Creation", "Has Pool"])
    out = ed.normalize_df_columns(df)
    assert set(out.columns) == {"square_feet", "num_bedrooms", "price", "date_creation", "has_pool"}


def test_build_column_mapping_excludes_id_and_date():
    df = pd.DataFrame({
        "id": [1, 2],
        "date_creation": ["2025-09-03", "2025-09-03"],
        "square_feet": [50.0, 60.0],
        "num_bedrooms": [2, 3],
        "price": [100000, 120000],
        "price_predict": [None, None],
        "city": ["paris", "lyon"],  # catégorie fictive
    })
    cm = ed.build_column_mapping(df)
    # Exclusions
    assert "id" not in (cm.numerical_features or [])
    assert "date_creation" not in (cm.numerical_features or [])
    assert "id" not in (cm.categorical_features or [])
    assert "date_creation" not in (cm.categorical_features or [])
    # Cible / prédiction
    assert cm.target == "price"
    assert cm.prediction == "price_predict"


def test_get_s3_prefix_for_outputs():
    # Racine
    assert ed.get_s3_prefix_for_outputs("/current_data/current_data.csv","real_estate_dataset.csv") == ""
    # Sous-dossier
    #assert ed.get_s3_prefix_for_outputs("ref/train.csv", "prod/batches/new_batch.csv") == "prod/batches/"


# =========================
# Tests S3
# =========================

def test_read_csv_s3_with_mock(patch_boto3_client):
    bucket = "housing-prices-aurelien"
    key = "real_estate_dataset.csv"
    csv_data = "Square_Feet,Price\n60,100000\n80,150000\n"

    state = patch_boto3_client({(bucket, key): csv_data})
    df = ed.read_csv_s3(bucket, key)

    # Normalisation appliquée
    assert list(df.columns) == ["square_feet", "price"]
    assert len(df) == 2
    assert state["client"] is not None  # le mock a bien été utilisé


def test_load_ref_cur_via_constants_and_s3_mock(monkeypatch, patch_boto3_client):
    """
    On utilise BUCKET_NAME + REF_KEY/CUR_KEY présents dans le module,
    et on vérifie que read_csv_s3 charge correctement.
    """
    bucket = "housing-prices-aurelien"
    monkeypatch.setenv("BUCKET_NAME", bucket)  # même si le module lit déjà BUCKET_NAME au chargement

    # Récupère les clés valeurs par défaut exposées par le module
    ref_key = getattr(ed, "REF_KEY", "current_data/current_data.csv")
    cur_key = getattr(ed, "CUR_KEY", "real_estate_dataset.csv")
    

    ref_csv = "Square_Feet,Num_Bedrooms,Price\n60,2,100000\n80,3,150000\n"
    cur_csv = "Square_Feet,Num_Bedrooms,Price\n120,4,200000\n140,5,250000\n"

    patch_boto3_client({
        (bucket, ref_key): ref_csv,
        (bucket, cur_key): cur_csv,
    })

    ref_df = ed.read_csv_s3(bucket, ref_key)
    cur_df = ed.read_csv_s3(bucket, cur_key)

    assert set(ref_df.columns) == {"square_feet", "num_bedrooms", "price"}
    assert set(cur_df.columns) == {"square_feet", "num_bedrooms", "price"}
    assert len(ref_df) == 2 and len(cur_df) == 2


# =========================
# End-to-end (sans upload réel)
# =========================

def test_run_and_save_outputs_end_to_end(tmp_path, monkeypatch):
    # Mini-datasets ref / cur (cur = drifté)
    ref = pd.DataFrame({
        "square_feet": [60, 80, 100, 75],
        "num_bedrooms": [2, 3, 3, 2],
        "price": [100000, 150000, 180000, 120000],
    })
    cur = pd.DataFrame({
        "square_feet": [120, 140, 160, 180],
        "num_bedrooms": [4, 5, 4, 6],
        "price": [210000, 260000, 300000, 320000],
    })

    cm = ed.build_column_mapping(ref)
    tests, report = ed.run_tests_and_report(ref, cur, cm)

    out_tests = tmp_path / "evidently_tests.html"
    out_drift = tmp_path / "evidently_report.html"
    out_json  = tmp_path / "drift_summary.json"

    # Monkeypatch l'upload S3 pour ne pas toucher au réseau
    uploaded = {}

    def _fake_upload(local_path, bucket, key, content_type=None, retries=3, backoff=1.5):
        uploaded[(bucket, key)] = {"local": str(local_path), "content_type": content_type}

    monkeypatch.setenv("BUCKET_NAME", "dummy-bucket")
    monkeypatch.setattr(ed, "upload_file_s3", _fake_upload)

    # Préfixe arbitraire pour simuler un “dossier” de sortie
    s3_prefix = ""

    ed.save_outputs_local_and_s3(
        tests, report,
        out_tests, out_drift, out_json,
        bucket="dummy-bucket", s3_prefix=s3_prefix
    )

    # Vérifs locales
    assert out_tests.exists() and out_tests.stat().st_size > 0
    assert out_drift.exists() and out_drift.stat().st_size > 0
    data = json.loads(out_json.read_text())
    assert "dataset_drift" in data

    # Vérifs des clés cibles d'upload
    assert ("dummy-bucket", "evidently/evidently_tests.html") in uploaded
    assert ("dummy-bucket", "evidently/evidently_report.html") in uploaded
    assert ("dummy-bucket", "evidently/drift_summary.json") in uploaded