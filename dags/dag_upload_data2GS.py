import os
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from customer_operators.upload_data_toGS import upload_data
from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator
import json
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator
)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "solar-idea-351402")
DATASET_NAME = os.environ.get("GCP_BIGQUERY_DATASET_NAME", "solar-idea-351402.dejon_dataset")
LOCATION = "asia-northeast1"

TABLE_1 = "WaterMeasuringList "

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 27),
    'email': ['baggi@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1000,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': '@hourly',
}
with DAG("upload_data2GS",
         catchup=False, default_args=default_args) as dag:


    Scraping_API = PythonOperator(
        task_id="upload_to_GS_09",
        python_callable=upload_data,
        op_kwargs= {
            "gcp_conn_id":"GS_Conn",
            "bucket_name":"dejon-data-bucket01",
            "year":"2022",
            "month":"03",
            "target": "3008A40"

        }
    )