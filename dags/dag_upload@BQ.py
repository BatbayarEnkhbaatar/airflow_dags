import os
import json
from airflow import DAG
from airflow import models
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from customer_operators.scraping_data import waterMeasuring
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryInsertJobOperator)


PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "solar-idea-351402")
DATASET_NAME = os.environ.get("GCP_BIGQUERY_DATASET_NAME", "solar-idea-351402.dejon_dataset")
LOCATION = "asia-northeast1"

TABLE_1 = "WaterMeasuringList "

############################################
############################################

#############################################
#############################################



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
dag = models.DAG (
    'bigquery_insert_dag',
    schedule_interval=='@once',
    default_args=default_args
)
f = open("result/data_202205.json")
df = json.load(f)
rows = []
for key, value in df.items():
    rows.append(
        {'json': {
            key: value
        }

        }
    )
bigquery_insert_data = BigQueryInsertJobOperator(
    task_id="BigQuery_Insert",
    bigquery_conn_id='google_BQ_connection',
    dag=dag,

    ## big info
    project_id=PROJECT_ID,
    dataset_id=DATASET_NAME,
    table_id=TABLE_1,
    rows=rows

)