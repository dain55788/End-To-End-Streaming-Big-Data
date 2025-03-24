# AIRFLOW DAGS for processing ELT pipeline from Bronze to Gold Layer
# Task dependencies: start_pipeline --> transform_data --> delta_convert --> end_pipeline

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator  # Dummy is replaced with EmptyOperator


# Default arguments for the DAG
default_args = {
    "owner": "nguyendai",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# MinIO configuration
MINIO_ENDPOINT = os.environ['MINIO_ENDPOINT']
MINIO_ACCESS_KEY = os.environ['MINIO_ACCESS_KEY']
MINIO_SECRET_KEY = os.environ['MINIO_SECRET_KEY']

with DAG("elt_pipeline", start_date=datetime(2025, 3, 16), schedule=None, default_args=default_args) as dag:
    start_pipeline = EmptyOperator(
        task_id="start_pipeline"
    )

    end_pipeline = EmptyOperator(
        task_id="end_pipeline"
    )

start_pipeline >> end_pipeline
