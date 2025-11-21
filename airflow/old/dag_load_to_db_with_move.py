from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import requests
import time

from airflow.operators.email import EmailOperator

from botocore.exceptions import ClientError
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# --- End added imports ---

AWS_CONN_ID = "aws_default"

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

    email_task = EmailOperator(
        task_id='send_failure_email',
        to='aurelien.chalm@gmail.com',
        subject=subject,
        html_content=body
    )
    email_task.execute(context=context)

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
        f"{conn.host}/job/test_load_to_db/build",
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
        time.sleep(2)

    if build_number is None:
        raise Exception("❌ Timeout : le job Jenkins n'a pas démarré")

    print(f"🔄 Build #{build_number} en cours...")

    # Polling du statut du build
    for _ in range(60):  # 60 x 5s = 5 min max
        build_info_resp = requests.get(
            f"{conn.host}/job/test_load_to_db/{build_number}/api/json",
            auth=(username, password)
        )
        build_info = build_info_resp.json()
        if not build_info["building"]:
            result = build_info["result"]
            jenkins_url = f"{conn.host}/job/test_load_to_db/{build_number}/"

            print(f"✅ Résultat du build Jenkins : {result}")
            print(f"🔗 Voir le build sur Jenkins : {jenkins_url}")

            if result != "SUCCESS":
                raise Exception(f"❌ Le job Jenkins a échoué : {result}")
            break
        time.sleep(5)

    return f"✔️ Build #{build_number} terminé avec succès"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# --- Added S3 move helpers (non-intrusive) ---
def move_s3_object(bucket: str, src_key: str, dest_key: str):
    """S3 move via copy → head → delete en utilisant la Connection Airflow (aws_default)."""
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3 = hook.get_client_type("s3")  # boto3 client configuré avec ta Connection

    # 1) copy
    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": src_key},
        Key=dest_key,
        MetadataDirective="COPY",
    )
    # 2) vérif
    s3.head_object(Bucket=bucket, Key=dest_key)
    # 3) delete
    s3.delete_object(Bucket=bucket, Key=src_key)

# Config read via Airflow Variables to avoid hardcoding.
S3_BUCKET = Variable.get("TRAINING_CSV_BUCKET", default_var="housing-prices-aurelien")
S3_SRC_KEY = Variable.get("TRAINING_CSV_SRC_KEY", default_var="real_estate_dataset.csv")
PROCESSED_PREFIX = Variable.get("TRAINING_CSV_PROCESSED_PREFIX", default_var="processed/")
FAILED_PREFIX = Variable.get("TRAINING_CSV_FAILED_PREFIX", default_var="failed/")

def _dest_ok_key(execution_date) -> str:
    ts = execution_date.strftime("%Y%m%d%H%M%S")
    base = S3_SRC_KEY.split("/")[-1]
    name, ext = (base.rsplit(".", 1) + [""])[:2]
    ext = f".{ext}" if ext else ""
    return f"{PROCESSED_PREFIX}{name}_{ts}{ext}"

def _dest_ko_key(execution_date) -> str:
    ts = execution_date.strftime("%Y%m%d%H%M%S")
    base = S3_SRC_KEY.split("/")[-1]
    name, ext = (base.rsplit(".", 1) + [""])[:2]
    ext = f".{ext}" if ext else ""
    return f"{FAILED_PREFIX}{name}_{ts}{ext}"

def move_ok(**context):
    dest = _dest_ok_key(context["execution_date"])
    move_s3_object(S3_BUCKET, S3_SRC_KEY, dest)

def move_ko(**context):
    try:
        dest = _dest_ko_key(context["execution_date"])
        move_s3_object(S3_BUCKET, S3_SRC_KEY, dest)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
            # Source already missing (re-run) -> ignore gracefully
            return
        raise
# --- End helpers ---

with DAG(
    dag_id="trigger_jenkins_load_to_db_job_move",
    default_args=default_args,
    start_date=datetime(2025, 7, 29),
    schedule_interval=None,
    catchup=False,
    tags=["jenkins", "trigger"],
    on_failure_callback=send_failure_email,
) as dag:

    trigger_jenkins_build = PythonOperator(
        task_id="trigger_jenkins_load_to_db_build",
        python_callable=trigger_jenkins_job,
    )
    # --- Added tasks to move the training CSV on S3 ---
    move_to_processed = PythonOperator(
        task_id="move_training_csv_to_processed",
        python_callable=move_ok,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    move_to_failed = PythonOperator(
        task_id="move_training_csv_to_failed",
        python_callable=move_ko,
        provide_context=True,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    trigger_jenkins_build >> [move_to_processed, move_to_failed]
    # --- End added tasks ---
