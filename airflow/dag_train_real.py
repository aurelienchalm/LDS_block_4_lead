from airflow import DAG
from airflow.operators.python import PythonOperator
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


def trigger_jenkins_train_job(**context):
    conn = BaseHook.get_connection("jenkins_api")
    username = conn.login
    password = conn.password

    # 1. Obtenir le crumb
    crumb_resp = requests.get(
        f"{conn.host}/crumbIssuer/api/json",
        auth=(username, password),
        timeout=10,
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
        headers=headers,
        timeout=10,
    )

    if build_resp.status_code != 201:
        raise Exception(f"❌ Erreur lors du déclenchement du job Jenkins train : {build_resp.status_code}")

    queue_url = build_resp.headers.get("Location")
    if not queue_url:
        raise Exception("❌ Impossible de récupérer l’URL de queue Jenkins (header Location manquant)")

    print(f"🚀 Job Jenkins train en file d'attente : {queue_url}")

    # 3. Attente du build number
    build_number = None
    for _ in range(30):  # 30 x 2s = ~60s max
        queue_resp = requests.get(f"{queue_url}api/json", auth=(username, password), timeout=10)
        queue_data = queue_resp.json()
        if "executable" in queue_data and "number" in queue_data["executable"]:
            build_number = queue_data["executable"]["number"]
            break
        time.sleep(2)

    if build_number is None:
        raise Exception("❌ Timeout : Jenkins n’a pas attribué de numéro de build pour test_train")

    print(f"⏳ Build Jenkins test_train #{build_number} démarré...")

    # 4. Polling du statut du build
    for _ in range(60):  # 60 x 5s = 5 minutes max
        build_info_resp = requests.get(
            f"{conn.host}/job/test_train/{build_number}/api/json",
            auth=(username, password),
            timeout=10,
        )
        build_info = build_info_resp.json()

        if not build_info.get("building", False):
            result = build_info.get("result")
            jenkins_url = f"{conn.host}/job/test_train/{build_number}/"
            print(f"✅ Build Jenkins terminé : {result}")
            print(f"🔗 Voir le build sur Jenkins : {jenkins_url}")

            if result != "SUCCESS":
                raise Exception(f"❌ Le job Jenkins test_train a échoué : {result}")
            break

        time.sleep(5)

    return f"✔️ Build test_train #{build_number} terminé avec succès"


with DAG(
    dag_id="housing_train_model",
    start_date=datetime(2025, 7, 30),
    schedule_interval=None,
    catchup=False,
    tags=["train", "jenkins"],
    on_failure_callback=send_failure_email,
) as dag:

    train_task = PythonOperator(
        task_id="trigger_jenkins_train",
        python_callable=trigger_jenkins_train_job,
    )