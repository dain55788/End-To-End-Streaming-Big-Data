# AIRFLOW DAGS for processing ELT pipeline from Bronze to Gold Layer
# Task dependencies: start_pipeline --> transform_data --> delta_convert --> end_pipeline

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator #Dummy is replaced with EmptyOperator



