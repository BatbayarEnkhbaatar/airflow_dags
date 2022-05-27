import os
from airflow import DAG
import datetime
from airflow.operators.python_operator import PythonOperator
from customer_operators.scraping_data import waterMeasuring
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryInsertJobOperator
)
from airflow.utils.dates import days_ago

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "solar-idea-351402")
DATASET_NAME = os.environ.get("GCP_BIGQUERY_DATASET_NAME", "solar-idea-351402.dejon_dataset")
LOCATION = "asia-northeast1"

TABLE_1 = "WaterMeasuringList "


with DAG("Ssraping from API", start_date=datetime(2022, 5, 24), schedule_interval = "@hourly", catchup=False) as dag:

    Scraping_API = PythonOperator(
        task_id="Scraping",
        python_callable=waterMeasuring,
        op_kwargs={
            'year': '2022',
            'month': '03',
            'target': '3008A40'
    }
    )