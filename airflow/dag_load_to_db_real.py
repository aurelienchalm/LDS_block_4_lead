from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import requests
import time

AWS_CONN_ID = "aws_default"

# ---------------- EMAIL FAILURE ----------------
def send_failure_email(context):
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = task_instance.task_id
    log_url = task_instance.log_url

    email_task = EmailOperator(
        task_id='send_failure_email',
        to='aurelien.chalm@gmail.com',
        subject=f"❌ ECHEC du DAG {dag_id} - tâche {task_id}",
        html_content=f"""
        Le DAG <b>{dag_id}</b> a échoué sur la tâche <b>{task_id}</b>.<br>
        👉 <a href="{log_url}">Voir les logs Airflow</a>
        """
    )
    email_task.execute(context=context)


# ---------------- LOAD DATA JOB ----------------
def trigger_jenkins_load_job(**context):
    conn = BaseHook.get_connection("jenkins_api")
    username = conn.login
    password = conn.password

    # 1. Obtenir le crumb
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

    # 2. Déclencher le job Jenkins test_load_to_db
    build_resp = requests.post(
        f"{conn.host}/job/test_load_to_db/build",
        auth=(username, password),
        headers=headers
    )

    if build_resp.status_code != 201:
        raise Exception(f"❌ Erreur lors du déclenchement du job Jenkins load_to_db : {build_resp.status_code}")

    queue_url = build_resp.headers.get("Location")
    if not queue_url:
        raise Exception("❌ Impossible de récupérer l’URL de queue Jenkins pour test_load_to_db")

    print(f"📩 Job 'test_load_to_db' envoyé en queue : {queue_url}")

    # 3. Attente du build number
    build_number = None
    for _ in range(30):  # 30 x 2s = 60s max
        queue_resp = requests.get(f"{queue_url}api/json", auth=(username, password))
        queue_data = queue_resp.json()
        if 'executable' in queue_data and 'number' in queue_data['executable']:
            build_number = queue_data['executable']['number']
            break
        time.sleep(2)

    if build_number is None:
        raise Exception("❌ Timeout : Jenkins n’a pas attribué de numéro de build pour test_load_to_db")

    print(f"⏳ Build Jenkins test_load_to_db #{build_number} en cours...")

    # 4. Polling du statut du build
    for _ in range(60):  # 60 x 5s = 5 min max
        build_info_resp = requests.get(
            f"{conn.host}/job/test_load_to_db/{build_number}/api/json",
            auth=(username, password)
        )
        build_info = build_info_resp.json()

        if not build_info["building"]:
            result = build_info["result"]
            jenkins_url = f"{conn.host}/job/test_load_to_db/{build_number}/"
            print(f"✅ Résultat du build Jenkins load_to_db : {result}")
            print(f"🔗 Voir le build sur Jenkins : {jenkins_url}")

            if result != "SUCCESS":
                raise Exception(f"❌ Le job Jenkins load_to_db a échoué : {result}")
            break

        time.sleep(5)

    return f"✔️ Build test_load_to_db #{build_number} terminé avec succès"


# ------------------ S3 MOVE ------------------
S3_BUCKET = Variable.get("TRAINING_CSV_BUCKET", default_var="housing-prices-aurelien")
S3_SRC_KEY = Variable.get("TRAINING_CSV_SRC_KEY", default_var="real_estate_dataset.csv")
PROCESSED_PREFIX = Variable.get("TRAINING_CSV_PROCESSED_PREFIX", default_var="processed/")
FAILED_PREFIX = Variable.get("TRAINING_CSV_FAILED_PREFIX", default_var="failed/")

def move_s3_object(bucket, src_key, dest_key):
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3 = hook.get_client_type("s3")
    s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": src_key}, Key=dest_key)
    s3.delete_object(Bucket=bucket, Key=src_key)

def move_ok(**context):
    ts = context["execution_date"].strftime("%Y%m%d%H%M%S")
    move_s3_object(S3_BUCKET, S3_SRC_KEY, f"{PROCESSED_PREFIX}dataset_{ts}.csv")

def move_ko(**context):
    ts = context["execution_date"].strftime("%Y%m%d%H%M%S")
    move_s3_object(S3_BUCKET, S3_SRC_KEY, f"{FAILED_PREFIX}dataset_{ts}.csv")


# ---------------- DAG LOAD ----------------
with DAG(
    dag_id="housing_load_to_db",
    start_date=datetime(2025, 7, 30),
    schedule_interval=None,
    catchup=False,
    tags=["load", "jenkins"],
    on_failure_callback=send_failure_email,
) as dag:

    load_task = PythonOperator(
        task_id="trigger_jenkins_load",
        python_callable=trigger_jenkins_load_job,
    )
    #Pour l’instant, on désactive le move S3 pour le debug
    # move_to_processed = PythonOperator(
    #     task_id="move_csv_to_processed",
    #     python_callable=move_ok,
    #     trigger_rule=TriggerRule.ALL_SUCCESS,
    # )
    #
    # move_to_failed = PythonOperator(
    #     task_id="move_csv_to_failed",
    #     python_callable=move_ko,
    #     trigger_rule=TriggerRule.ONE_FAILED,
    # )
    #
    # load_task >> move_to_processed
    # load_task >> move_to_failed