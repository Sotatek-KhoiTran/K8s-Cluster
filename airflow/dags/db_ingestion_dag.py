import smtplib
import logging
import yaml
import os

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.operators.python import PythonOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def send_email_on_failure(context):
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url
    error = context.get("exception")
    
    sender_email = os.getenv("SENDER_EMAIL")
    receiver_email = os.getenv("RECEIVER_EMAIL")
    password = os.getenv("AIRFLOW__SMTP__SMTP_PASSWORD")

    msg = MIMEMultipart()
    msg['From'] = f"Airflow Alerts <{sender_email}>"
    msg['To'] = receiver_email
    msg['Subject'] = f"Airflow Alert: Task [{task_id}] failed in DAG [{dag_id}]"

    body = f"""
    Airflow Task Failure Alert
    --------------------------
    DAG: {dag_id}
    Task: {task_id}
    Execution Date: {execution_date}
    
    Error:
    {error}
    
    View Logs here: {log_url}
    """
    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, msg.as_string())
        server.quit()
        logger.info("Custom email alert sent successfully!")
    except Exception as e:
        logger.error(f"Failed to send custom email: {e}")

default_args = {
    "owner": "khoi",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": ["khoi.tran2@sotatek.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": send_email_on_failure,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="db_ingestion_dag",
    default_args=default_args,
    description="Ingest data from DB to GCS",
    schedule=timedelta(days=1),
    catchup=False,
    tags=["spark", "ingestion", "db_to_gcs"],
    max_active_runs=1,
    max_active_tasks=1
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "configs/db-ingestion-config.yaml")

with open(CONFIG_PATH, "r") as f:
    spark_config = yaml.safe_load(f)

for config in spark_config:
    spark_ingestion_task = SparkKubernetesOperator(
        task_id=f"spark_db_to_gcs_{config['table_name']}",
        namespace="spark",
        application_file="db-ingestion-app.yaml",
        params={
            "config": config,
        },
        dag=dag,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        delete_on_termination=False
    )

    spark_dq_task = SparkKubernetesOperator(
        task_id=f"spark_dq_checks_{config['table_name']}",
        namespace="spark",
        application_file="db-ingestion-dq.yaml",
        params={
            "config": config,
        },
        dag=dag,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        delete_on_termination=False
    )

    spark_ingestion_task >> spark_dq_task
    
    