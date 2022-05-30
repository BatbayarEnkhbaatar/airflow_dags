import os
import datetime
from pathlib import Path

from airflow import DAG
from airflow.configuration import conf
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

comp_home_path = Path(conf.get("core", "dags_folder")).parent.absolute()
comp_bucket_path = "data/uploaded"  # <- if your file is within a folder
comp_local_path = os.path.join(comp_home_path, comp_bucket_path)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime.today(),
    'end_date': None,
    'email': ['batbayar@northstar.co.ke'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1)
}

sch_interval = None

dag = DAG(
    'to_save_GCP',
    default_args=default_args,
    tags=["example"],
    catchup=False,
    schedule_interval=sch_interval
)

mv_local_gcs = LocalFilesystemToGCSOperator(
    task_id="to_save_Google",
    src=comp_local_path + "customer_operators/WaterMeasuringList.csv",  # PATH_TO_UPLOAD_FILE
    dst="WaterMeasuringList.csv",  # BUCKET_FILE_LOCATION
    bucket="solar-idea-351402",  # using NO 'gs://' nor '/' at the end, only the project, folders, if any, in dst
    dag=dag
)
start = DummyOperator(task_id='Starting', dag=dag)

start >> mv_local_gcs
