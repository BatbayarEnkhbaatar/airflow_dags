from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import date
from customer_operators.upload_to_gcs import  waterMeasuring
todays_date = date.today()
with DAG(
    'Dejon_WaterMeasuring_data_to_Google_Storage_Dag_001',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['batbayar@northstar.kr.co'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(hours=1),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2022, 5, 23),
    catchup=False,
    tags=['Data Pipe Line '],

) as dag:

    PythonOperator(
    task_id="WaterMeasuring_data_to_Google_Storage",
    python_callable = waterMeasuring,
    op_kwargs={
        "year" : todays_date.year,
        "month" : todays_date.month
        }
    )


