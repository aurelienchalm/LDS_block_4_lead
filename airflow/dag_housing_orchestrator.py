from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from datetime import datetime, timedelta


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
    👉 <a href="{log_url}">Voir les logs Airflow</a>
    """
    email_task = EmailOperator(
        task_id='send_failure_email',
        to=Variable.get("ALERT_RECIPIENTS", default_var="aurelien.chalm@gmail.com"),
        subject=subject,
        html_content=body
    )
    email_task.execute(context=context)


with DAG(
    dag_id="housing_orchestrator",
    default_args=default_args,
    start_date=datetime(2025, 7, 30),
    schedule_interval="0 8 * * 0",  # ✅ Tous les dimanches à 08h
    catchup=False,
    tags=["orchestrator", "housing"],
    on_failure_callback=send_failure_email,
) as dag:

    # 1) Build datasets S3 (NeonDB + API → current_data.csv + real_estate_dataset.csv)
    run_build_datasets = TriggerDagRunOperator(
        task_id="run_housing_build_datasets",
        trigger_dag_id="housing_build_datasets",   # DAG 1
        reset_dag_run=True,
        wait_for_completion=True,                  # ✅ on attend qu'il finisse
    )

    # 2) Lancer Evidently via Jenkins (rapports drift)
    run_evidently_reports = TriggerDagRunOperator(
        task_id="run_housing_evidently_reports",
        trigger_dag_id="housing_evidently_reports",  # DAG 2
        reset_dag_run=True,
        wait_for_completion=True,                    # ✅ on attend aussi
    )

    # 3) Chargement dans NeonDB via Jenkins (load_to_db)
    run_load_to_db = TriggerDagRunOperator(
        task_id="run_housing_load_to_db",
        trigger_dag_id="housing_load_to_db",       # DAG 3
        reset_dag_run=True,
        wait_for_completion=True,                  # ✅ on attend la fin
    )

    run_build_datasets >> run_evidently_reports >> run_load_to_db