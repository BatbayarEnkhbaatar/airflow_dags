import os
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from customer_operators.scraping_data import waterMeasuring
from customer_operators.upload_data_toGS import upload_data
from customer_operators.fromGS2BQ import insert2BQ


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
with DAG("Data_from_API_to_GS",
         catchup=False, default_args=default_args) as dag:


    Scraping_API = PythonOperator(
        task_id="Scrapping_API",
        python_callable=waterMeasuring,
        op_kwargs=Variable.get("dejon_scrapping_data", deserialize_json=True)
    )
    Upload_GS = PythonOperator(
        task_id="Saving_to_Google_Storage",
        python_callable=upload_data,
        op_kwargs=Variable.get("gcs_input_params", deserialize_json=True)
    )
    Insert2BigQuery = PythonOperator(
        task_id="BigQuery_Insert",
        bigquery_conn_id='google_BQ_connection',
        dag=dag,
        ## big info
        python_callable=insert2BQ,
        op_kwargs=Variable.get("BQ_input_params", deserialize_json=True)

    )

    Scraping_API >>Upload_GS >> Insert2BigQuery