from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.hooks.base import BaseHook
from airflow.operators.email import EmailOperator
from datetime import datetime
import requests
import time

def send_failure_email(context):
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = task_instance.task_id
    log_url = task_instance.log_url

    subject = f"❌ ECHEC du DAG {dag_id} - tâche {task_id}"
    body = f"""
    Le DAG <b>{dag_id}</b> a échoué sur la tâche <b>{task_id}</b>.<br>
    👉 <a href="{log_url}">Voir les logs Airflow</a>
    """

    email_task = EmailOperator(
        task_id='send_failure_email',
        to='aurelien.chalm@gmail.com',
        subject=subject,
        html_content=body
    )
    email_task.execute(context=context)

def trigger_jenkins_train_job(**context):
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

    # 2. Déclencher le job train
    build_resp = requests.post(
        f"{conn.host}/job/test_train/build",
        auth=(username, password),
        headers=headers
    )

    if build_resp.status_code != 201:
        raise Exception(f"❌ Erreur lors du déclenchement du job Jenkins : {build_resp.status_code}")

    queue_url = build_resp.headers.get("Location")
    if not queue_url:
        raise Exception("❌ Impossible de récupérer l’URL de queue Jenkins")

    # 3. Attente du build number
    build_number = None
    for _ in range(30):
        queue_resp = requests.get(f"{queue_url}api/json", auth=(username, password))
        queue_data = queue_resp.json()
        if 'executable' in queue_data and 'number' in queue_data['executable']:
            build_number = queue_data['executable']['number']
            break
        time.sleep(2)

    if build_number is None:
        raise Exception("❌ Timeout : Jenkins n’a pas attribué de numéro de build")

    print(f"⏳ Build #{build_number} en cours...")

    # 4. Polling du statut du build
    for _ in range(60):
        build_info = requests.get(
            f"{conn.host}/job/test_train/{build_number}/api/json",
            auth=(username, password)
        ).json()

        if not build_info["building"]:
            result = build_info["result"]
            jenkins_url = f"{conn.host}/job/test_train/{build_number}/"
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

with DAG(
    dag_id="trigger_jenkins_train_job",
    default_args=default_args,
    start_date=datetime(2025, 7, 30),
    schedule_interval=None,
    catchup=False,
    tags=["jenkins", "train"],
    on_failure_callback=send_failure_email,
) as dag:

    trigger_train = PythonOperator(
        task_id="trigger_jenkins_train_build",
        python_callable=trigger_jenkins_train_job,
    )