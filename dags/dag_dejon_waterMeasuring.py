from datetime import date
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from customer_operators.scraping_data import waterMeasuring

todays_date = date.today()
with DAG(
        'dejon_WaterMeasuring',
        # These args will get passed on to each operator
        # You can override them on a per-task basis during operator initialization
        default_args={
            'depends_on_past': False,
            'email': ['batbayar@northstar.kr.co'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 5,
            'retry_delay': timedelta(minutes=1),

        },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(minutes=1),
        start_date=datetime(2022, 5, 23),
        catchup=False,
        tags=['Data Pipe Line '],

) as dag:
    ScrapingData = PythonOperator(
        task_id="Scraping_WaterMeasuring_Data_10",
        python_callable=waterMeasuring,
        op_args={
            "year": todays_date.year,
            "month": todays_date.month,
            "gcp_conn_id": "airflow_gke_gcs_conn_id",
            "gcs_bucket": "dejon-data-bucket01"
        },
        dag=dag,
    )

    ScrapingData
