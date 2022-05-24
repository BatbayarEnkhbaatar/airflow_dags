from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import date
from customer_operators.scraping_data import  waterMeasuring
from customer_operators.upload_data_toGS import upload_data

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
        'retries': 2,
        'retry_delay': timedelta(hours=1),

    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2022, 5, 23),
    catchup=False,
    tags=['Data Pipe Line '],

) as dag:
    ScrapingData = PythonOperator(
    task_id="WaterMeasuring_data_to_Google_Storage",
    python_callable = waterMeasuring,
    op_kwargs={
        "year" : todays_date.year,
        "month" : todays_date.month
        }
    )
    UploadData = PythonOperator(
    task_id="Upload_Data_to_GS",
    python_callable = upload_data,
    op_kwargs={
        "connec_id" : "Dejon_data_Google_Storage",
        "bucket_name" : "dejon-data-bucket",
        "source_file_name" : "WaterMeasuringList.csv"
        }
    )

    ScrapingData >> UploadData # Tasks Sequence

