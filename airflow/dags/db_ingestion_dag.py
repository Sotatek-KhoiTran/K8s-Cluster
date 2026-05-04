import json

import yaml
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

default_args = {
    "owner": "khoi",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": ["khoi.tran2@sotatek.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
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
CONFIG_PATH = os.path.join(BASE_DIR, "../configs/db-ingestion-config.yaml")

with open(CONFIG_PATH, "r") as f:
    spark_config = yaml.safe_load(f)

for config in spark_config:
    spark_ingestion_task = SparkKubernetesOperator(
        task_id=f"spark_db_to_gcs_{config['table_name']}",
        namespace="spark",
        application_file="db-ingestion-app.yaml",
        params={
            "config": config,
            "data_interval_start": "{{ data_interval_start }}",    
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
            "data_interval_start": "{{ data_interval_start }}",
        },
        dag=dag,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        delete_on_termination=False
    )

    spark_ingestion_task >> spark_dq_task
    
    