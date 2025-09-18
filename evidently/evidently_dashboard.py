#!/usr/bin/env python3
import logging
import json
import os
import re
import time
from pathlib import Path
from typing import Tuple

import pandas as pd
import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError

from evidently.report import Report
from evidently.metric_preset import DataDriftPreset
from evidently.test_suite import TestSuite
from evidently.test_preset import DataStabilityTestPreset  # Evidently 0.6
from evidently import ColumnMapping
from dotenv import load_dotenv

# ========= Logs =========
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("evidently_dashboard")

# ========= Paths =========
ROOT_DIR = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT_DIR / "evidently"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_DRIFT = OUT_DIR / "evidently_report.html"
OUT_TESTS = OUT_DIR / "evidently_tests.html"
OUT_JSON  = OUT_DIR / "drift_summary.json"

# ========= .env =========
env_path = ROOT_DIR / ".env"
if env_path.exists():
    load_dotenv(env_path, override=True)

AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION    = os.getenv("AWS_DEFAULT_REGION")
BUCKET_NAME           = os.getenv("BUCKET_NAME")  # ex: housing-prices-aurelien

# Clés par défaut dans le bucket (tu peux les changer ici si tu mets des sous-dossiers)

REF_KEY = "current_data/current_data.csv"
CUR_KEY = "real_estate_dataset.csv"

# ========= Normalisation colonnes =========
NORMALIZATION_MAP = {
    "id": "id",
    "square_feet": "square_feet",
    "num_bedrooms": "num_bedrooms",
    "num_bathrooms": "num_bathrooms",
    "num_floors": "num_floors",
    "year_built": "year_built",
    "has_garden": "has_garden",
    "has_pool": "has_pool",
    "garage_size": "garage_size",
    "location_score": "location_score",
    "distance_to_center": "distance_to_center",
    "price": "price",
    "price_predict": "price_predict",
    "date_creation": "date_creation",
}

def normalize_col(c: str) -> str:
    import re as _re
    x = _re.sub(r"[ \-]+", "_", c.strip()).lower()
    return NORMALIZATION_MAP.get(x, x)

def normalize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [normalize_col(c) for c in df.columns]
    return df

# ========= I/O S3 =========
def s3_client():
    cfg = BotoConfig(retries={"max_attempts": 2})
    if AWS_DEFAULT_REGION:
        return boto3.client("s3", region_name=AWS_DEFAULT_REGION, config=cfg)
    return boto3.client("s3", config=cfg)

def read_csv_s3(bucket: str, key: str, retries: int = 3, backoff: float = 1.5) -> pd.DataFrame:
    if not bucket or not key:
        raise ValueError("Bucket/key S3 manquant.")
    logger.info(f"Téléchargement S3: s3://{bucket}/{key}")
    s3 = s3_client()
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            df = pd.read_csv(obj["Body"])
            return normalize_df_columns(df)
        except (ClientError, BotoCoreError) as e:
            last_err = e
            logger.warning(f"[S3] tentative {attempt}/{retries} échouée: {e}")
            if attempt < retries:
                time.sleep(backoff ** attempt)
    raise RuntimeError(f"Échec téléchargement S3: s3://{bucket}/{key}") from last_err

def upload_file_s3(local_path: Path, bucket: str, key: str, content_type: str | None = None, retries: int = 3, backoff: float = 1.5):
    logger.info(f"Upload S3: {local_path} → s3://{bucket}/{key}")
    s3 = s3_client()
    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            s3.upload_file(
                Filename=str(local_path),
                Bucket=bucket,
                Key=key,
                ExtraArgs=extra_args or None
            )
            return
        except (ClientError, BotoCoreError) as e:
            last_err = e
            logger.warning(f"[S3 upload] tentative {attempt}/{retries} échouée: {e}")
            if attempt < retries:
                time.sleep(backoff ** attempt)
    raise RuntimeError(f"Échec upload S3: {local_path} → s3://{bucket}/{key}") from last_err

def get_s3_prefix_for_outputs(ref_key: str, cur_key: str) -> str:
    """
    On place les outputs dans le même 'dossier' que le CSV courant (cur_key).
    Si cur_key = 'proj/cur/new_batch.csv' → préfixe 'proj/cur/'.
    Si cur_key = 'new_batch.csv' → préfixe '' (racine du bucket).
    """
    prefix = os.path.dirname(cur_key)
    return (prefix + "/") if prefix else ""

# ========= Evidently helpers =========
def build_column_mapping(df: pd.DataFrame) -> ColumnMapping:
    target = "price" if "price" in df.columns else None
    prediction = "price_predict" if "price_predict" in df.columns else None
    exclude = {"id", "date_creation", target, prediction}
    num_cols = [c for c in df.select_dtypes(include=["number"]).columns if c not in exclude]
    cat_cols = [c for c in df.select_dtypes(exclude=["number"]).columns if c not in exclude]
    return ColumnMapping(
        target=target,
        prediction=prediction,
        numerical_features=num_cols,
        categorical_features=cat_cols,
    )

def run_tests_and_report(ref: pd.DataFrame, cur: pd.DataFrame, cm: ColumnMapping):
    tests = TestSuite(tests=[DataStabilityTestPreset()])
    tests.run(reference_data=ref, current_data=cur, column_mapping=cm)

    report = Report(metrics=[DataDriftPreset()])
    report.run(reference_data=ref, current_data=cur, column_mapping=cm)
    return tests, report

def save_outputs_local_and_s3(tests: TestSuite, report: Report, out_tests: Path, out_drift: Path, out_json: Path,
                            bucket: str, s3_prefix: str):
    # Sauvegarde locale (toujours utile pour debug et CI artefacts)
    tests.save_html(out_tests.as_posix())
    logger.info(f"Tests Evidently sauvegardés → {out_tests}")
    report.save_html(out_drift.as_posix())
    logger.info(f"Rapport Data Drift sauvegardé → {out_drift}")

    # Résumé JSON
    try:
        d = report.as_dict()
        dataset_result = None
        try:
            dataset_result = d["metrics"][0]["result"]["dataset_drift"]
        except Exception:
            pass
        out_json.write_text(json.dumps({"dataset_drift": dataset_result}), encoding="utf-8")
        logger.info(f"Résumé drift JSON → {out_json} | dataset_drift={dataset_result}")
    except Exception:
        logger.info("Résumé drift JSON non disponible, consulte le HTML.")

    # Upload S3 au même endroit que le CSV courant
    upload_file_s3(out_tests, bucket, f"{s3_prefix}evidently/evidently_tests.html", content_type="text/html")
    upload_file_s3(out_drift, bucket, f"{s3_prefix}evidently/evidently_report.html", content_type="text/html")
    upload_file_s3(out_json,  bucket, f"{s3_prefix}evidently/drift_summary.json", content_type="application/json")

# ========= Main =========
def main():
    logger.info("Démarrage Evidently (S3 via .env)")
    if not BUCKET_NAME:
        raise EnvironmentError("BUCKET_NAME non défini dans .env")

    # Lecture des CSV S3
    ref = read_csv_s3(BUCKET_NAME, REF_KEY)
    cur = read_csv_s3(BUCKET_NAME, CUR_KEY)

    # Colonnes communes après normalisation
    common_cols = sorted(set(ref.columns) & set(cur.columns))
    if not common_cols:
        raise ValueError("Aucune colonne commune entre ref et cur.")
    ref = ref[common_cols]
    cur = cur[common_cols]
    logger.info(f"Colonnes communes: {len(common_cols)} → {common_cols}")

    cm = build_column_mapping(ref)
    tests, report = run_tests_and_report(ref, cur, cm)

    # Préfixe S3 de sortie aligné sur l'emplacement du CSV courant
    s3_prefix = get_s3_prefix_for_outputs(REF_KEY, CUR_KEY)
    save_outputs_local_and_s3(tests, report, OUT_TESTS, OUT_DRIFT, OUT_JSON, BUCKET_NAME, s3_prefix)

if __name__ == "__main__":
    main()