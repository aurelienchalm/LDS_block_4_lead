from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from botocore.exceptions import ClientError
from airflow.utils.trigger_rule import TriggerRule 

from datetime import datetime, timedelta
from io import StringIO
import requests
import time
import pandas as pd


# === Config ===
AWS_CONN_ID = "aws_default"
POSTGRES_CONN_ID = "neondb"

# Airflow Variables (already set in your env, per your message)
TRAINING_CSV_BUCKET = Variable.get("TRAINING_CSV_BUCKET")
POSTGRES_TABLE = Variable.get("POSTGRES_TABLE", default_var="housing_prices")
DATE_COLUMN = Variable.get("DATE_COLUMN", default_var="date_creation")
REAL_ESTATE_API_URL = Variable.get("REAL_ESTATE_API_URL")

S3_SRC_KEY = Variable.get("TRAINING_CSV_SRC_KEY", default_var="real_estate_dataset.csv")


default_args = {
    "owner": "aurelien",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

def send_failure_email(context):
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = task_instance.task_id
    log_url = task_instance.log_url

    subject = f"❌ ECHEC du DAG {dag_id} - tâche {task_id}"
    body = f"""
    Le DAG <b>{dag_id}</b> a échoué sur la tâche <b>{task_id}</b>.<br>
    <a href="{log_url}">Voir les logs Airflow</a>
    """
    send_email = EmailOperator(
        task_id='send_email_on_failure_inner',
        to=Variable.get("ALERT_RECIPIENTS", default_var="aurelien.chalm@gmail.com"),
        subject=subject,
        html_content=body
    )
    return send_email.execute(context=context)

def move_previous_real_estate_dataset(**context):
    """
    Déplace le real_estate_dataset.csv actuel vers processed/ en début de DAG,
    s'il existe déjà.
    """
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3 = hook.get_client_type("s3")

    bucket = TRAINING_CSV_BUCKET
    src_key = S3_SRC_KEY

    # On fabrique une clé destination avec timestamp
    ts = context["execution_date"].strftime("%Y%m%d%H%M%S")
    base = src_key.split("/")[-1]  # real_estate_dataset.csv
    name, ext = (base.rsplit(".", 1) + [""])[:2]
    ext = f".{ext}" if ext else ""
    dest_key = f"processed/previous_{name}_{ts}{ext}"

    try:
        # Vérifie si le fichier existe
        s3.head_object(Bucket=bucket, Key=src_key)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
            print(f"[INFO] Aucun {src_key} à archiver en début de DAG (premier run ?).")
            return
        raise

    print(f"[INFO] Archive l'ancien dataset: s3://{bucket}/{src_key} → s3://{bucket}/{dest_key}")
    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": src_key},
        Key=dest_key,
        MetadataDirective="COPY",
    )
    s3.delete_object(Bucket=bucket, Key=src_key)
    print(f"[INFO] Ancien fichier supprimé à la racine: s3://{bucket}/{src_key}")

def generate_current_data_csv(**context):
    """
    Query NeonDB for the last 2 years and upload current_data.csv to S3:
    s3://{TRAINING_CSV_BUCKET}/current_data/current_data.csv
    """
    sql = f"""
        SELECT *
        FROM {POSTGRES_TABLE}
        WHERE {DATE_COLUMN} >= (CURRENT_DATE - INTERVAL '2 years') and price IS NOT NULL AND price_predict IS NULL
    """
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    df = pg.get_pandas_df(sql)

    if df.empty:
        raise Exception("Aucune donnée renvoyée pour les 2 dernières années. Vérifie POSTGRES_TABLE/DATE_COLUMN.")

    # Convert to CSV string
    csv_buf = StringIO()
    df.to_csv(csv_buf, index=False)
    csv_str = csv_buf.getvalue()

    # Upload to S3
    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
    key = "current_data/current_data.csv"
    s3.load_string(string_data=csv_str, key=key, bucket_name=TRAINING_CSV_BUCKET, replace=True)

    s3_uri = f"s3://{TRAINING_CSV_BUCKET}/{key}"
    context["ti"].xcom_push(key="current_data_s3_uri", value=s3_uri)
    print(f"✅ current_data.csv uploadé: {s3_uri}")
    
def generate_real_estate_dataset_csv(**context):
    max_attempts = 3
    last_exc = None

    for attempt in range(1, max_attempts + 1):
        try:
            print(f"[DEBUG] Tentative {attempt}/{max_attempts} appel API {REAL_ESTATE_API_URL}")
            resp = requests.get(REAL_ESTATE_API_URL, timeout=30)
            print(f"[DEBUG] Status code API = {resp.status_code}")
            print(f"[DEBUG] Response text (début) = {resp.text[:500]}")
            resp.raise_for_status()
            break  # ✅ on sort de la boucle si tout va bien
        except Exception as e:
            last_exc = e
            print(f"[WARN] Erreur appel API (tentative {attempt}): {e}")
            if attempt < max_attempts:
                time.sleep(5)  # on attend 5s avant de réessayer
            else:
                raise Exception(
                    f"❌ Erreur lors de l'appel à l'API des ventes réelles après {max_attempts} tentatives : {last_exc}"
                )

    data = resp.json()
    if not data:
        raise Exception("❌ Aucune donnée renvoyée par l'API des ventes réelles.")

    # data = liste de dict → DataFrame
    df = pd.DataFrame(data)

    # Si tu veux t'assurer de l'ordre des colonnes, tu peux expliciter :
    expected_cols = [
        "id",
        "square_feet",
        "num_bedrooms",
        "num_bathrooms",
        "num_floors",
        "year_built",
        "has_garden",
        "has_pool",
        "garage_size",
        "location_score",
        "distance_to_center",
        "price",
        "price_predict",
        "date_creation",
    ]
    # On garde uniquement celles qui existent, dans cet ordre
    df = df[[c for c in expected_cols if c in df.columns]]

    # Conversion en CSV
    csv_buf = StringIO()
    df.to_csv(csv_buf, index=False)
    csv_str = csv_buf.getvalue()

    # Upload sur S3
    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
    key = "real_estate_dataset.csv"
    s3.load_string(
        string_data=csv_str,
        key=key,
        bucket_name=TRAINING_CSV_BUCKET,
        replace=True,
    )

    s3_uri = f"s3://{TRAINING_CSV_BUCKET}/{key}"
    context["ti"].xcom_push(key="real_estate_dataset_s3_uri", value=s3_uri)
    print(f"✅ real_estate_dataset.csv uploadé: {s3_uri}")

def _get_jenkins_crumb(host, auth):
    try:
        resp = requests.get(f"{host}/crumbIssuer/api/json", auth=auth, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return {data["crumbRequestField"]: data["crumb"]}
    except Exception:
        pass
    return {}

def trigger_jenkins_job(**context):
    conn = BaseHook.get_connection("jenkins_api")
    username = conn.login
    password = conn.password

    # Obtenir le crumb pour les headers
    crumb_resp = requests.get(
        f"{conn.host}/crumbIssuer/api/json",
        auth=(username, password)
    )
    crumb_resp.raise_for_status()
    crumb_data = crumb_resp.json()
    headers = {
        crumb_data["crumbRequestField"]: crumb_data["crumb"],
        "Content-Type": "application/json",
    }

    # Lancer le job Jenkins
    build_resp = requests.post(
        f"{conn.host}/job/test_evidently_dashboard/build",
        auth=(username, password),
        headers=headers
    )

    if build_resp.status_code != 201:
        raise Exception(f"❌ Erreur lors du déclenchement du job : {build_resp.status_code}")

    queue_url = build_resp.headers.get("Location")
    if not queue_url:
        raise Exception("❌ Impossible de récupérer l’URL de queue Jenkins")

    # Attendre que Jenkins attribue un numéro de build
    build_number = None
    for _ in range(30):  # 30 x 2s = 60s max
        queue_resp = requests.get(f"{queue_url}api/json", auth=(username, password))
        queue_data = queue_resp.json()
        if 'executable' in queue_data and 'number' in queue_data['executable']:
            build_number = queue_data['executable']['number']
            break
        time.sleep(3)

    if build_number is None:
        raise Exception("❌ Timeout : le job Jenkins n'a pas démarré")

    print(f"🔄 Build #{build_number} en cours...")

    # Polling du statut du build
    for _ in range(60):  # 60 x 5s = 5 min max
        build_info_resp = requests.get(
            f"{conn.host}/job/test_evidently_dashboard/{build_number}/api/json",
            auth=(username, password)
        )
        build_info = build_info_resp.json()
        if not build_info["building"]:
            result = build_info["result"]
            jenkins_url = f"{conn.host}/job/test_evidently_dashboard/{build_number}/"

            print(f"✅ Résultat du build Jenkins : {result}")
            print(f"🔗 Voir le build sur Jenkins : {jenkins_url}")

            if result != "SUCCESS":
                raise Exception(f"❌ Le job Jenkins a échoué : {result}")
            break
        time.sleep(5)

    return f"✔️ Build #{build_number} terminé avec succès"


with DAG(
    dag_id="housing_evidently_dashboard",
    default_args=default_args,
    start_date=datetime(2025, 7, 29),
    schedule_interval=None,
    catchup=False,
    tags=["jenkins", "trigger", "evidently", "neondb", "s3"],
    on_failure_callback=send_failure_email,
) as dag:

    move_previous_dataset = PythonOperator(
        task_id="archive_previous_real_estate_dataset",
        python_callable=move_previous_real_estate_dataset,
        provide_context=True,
    )

    generate_current_data = PythonOperator(
        task_id="generate_current_data_csv",
        python_callable=generate_current_data_csv,
    )
    
    generate_real_estate_dataset = PythonOperator(
        task_id="generate_real_estate_dataset_csv",
        python_callable=generate_real_estate_dataset_csv,
    )

    trigger_jenkins_build = PythonOperator(
        task_id="trigger_jenkins_evidently_dashboard_build",
        python_callable=trigger_jenkins_job,
    )

    # Orchestration :
    # 1) on archive l'ancien dataset
    # 2) on génère les 2 CSV
    # 3) on lance Evidently
    move_previous_dataset >> [generate_current_data, generate_real_estate_dataset] >> trigger_jenkins_build
