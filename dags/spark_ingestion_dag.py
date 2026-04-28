from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

default_args = {
    "owner": "khoi",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="spark_ingestion_dag",
    default_args=default_args,
    description="Ingest data from DB to GCS",
    schedule=timedelta(days=1),
    catchup=False,
    tags=["spark", "ingestion", "db_to_gcs"],
)

spark_ingestion_task = SparkKubernetesOperator(
    task_id="spark_db_to_gcs",
    namespace="spark",
    application_file="spark-ingestion.yaml",
    dag=dag,
    kubernetes_conn_id="kubernetes_default",
    do_xcom_push=False,
    delete_on_termination=False
)

spark_ingestion_task