import os
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from customer_operators.scraping_data import waterMeasuring
from customer_operators.upload_data_toGS import upload_data
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

GCS_CONN_ID = os.environ.get("GCS_CONN_ID", "GS_conn")
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
with DAG("WaterMeasuringList",
         catchup=False, default_args=default_args) as dag:


    Scraping_API = PythonOperator(
        task_id="Scrapping_Data",
        python_callable=waterMeasuring,
        op_kwargs=Variable.get("dejon_scrapping_data", deserialize_json=True)
    )
    Upload_GS = PythonOperator(
        task_id="Upload_2_Temporary_Saving_on_Google_Storage",
        python_callable=upload_data,
        op_kwargs=Variable.get("gcs_input_params", deserialize_json=True)
    )
    Insert2BQ = GCSToBigQueryOperator(
        task_id='Insert_2_Data_Warehouse_on_BigQuery',
        bucket='dejon-data-bucket01',
        source_objects=['data202205.csv'],
        destination_project_dataset_table="dejon_dataset.WaterMeasuringList_01",
        schema_fields=[
            {"name": "ROWNO", "type": "STRING", "mode": "NULLABLE"},
            {"name": "PT_NO", "type": "STRING", "mode": "NULLABLE"},
            {"name": "PT_NM", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ADDR", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ORG_NM", "type": "STRING", "mode": "NULLABLE"},
            {"name": "WMYR", "type": "STRING", "mode": "NULLABLE"},
            {"name": "WMOD", "type": "STRING", "mode": "NULLABLE"},
            {"name": "WMWK", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LON_DGR", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LON_MIN", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LON_SEC", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LAT_DGR", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LAT_MIN", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LAT_SEC", "type": "STRING", "mode": "NULLABLE"},
            {"name": "WMCYMD", "type": "STRING", "mode": "NULLABLE"},
            {"name": "WMDEP", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_LVL", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_AMNT", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_TEMP", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_PH", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_DOC", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_BOD", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_COD", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_SS", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_TCOLI", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_TN", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_TP", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_CD", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_CN", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_PB", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_CR6", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_AS", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_HG", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_CU", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_ABS", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_PCB", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_OP", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_MN", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_TRANS", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_CLOA", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_CL", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_ZN", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_CR", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_FE", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_PHENOL", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_NHEX", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_EC", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_TCE", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_PCE", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_NO3N", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_NH3N", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_ECOLI", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_POP", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_DTN", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_DTP", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_FL", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_COL", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_ALGOL", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_CCL4", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_DCETH", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_DCM", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_BENZENE", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_CHCL3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_TOC", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_DEHP", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_ANTIMON", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_DIOX", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_HCHO", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_HCB", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_NI", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_BA", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_SE", "type": "STRING", "mode": "NULLABLE"}
        ],
        write_disposition='WRITE_APPEND',
        # conn_id='GS_Conn'
    )

    Scraping_API >> Upload_GS
    Scraping_API >> Insert2BQ