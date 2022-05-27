import os
from datetime import datetime
import customer_operators.scraping_data as scraping
from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryExecuteQueryOperator,
    BigQueryGetDataOperator,
    BigQueryInsertJobOperator,
    BigQueryIntervalCheckOperator,
    BigQueryValueCheckOperator,
)
from airflow.utils.dates import days_ago
import pandas as pd
import requests
import time as tm
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "solar-idea-351402")
DATASET_NAME = os.environ.get("GCP_BIGQUERY_DATASET_NAME", "solar-idea-351402.dejon_dataset")
LOCATION = "asia-northeast1"

TABLE_1 = "WaterMeasuringList "
year = [2013, 2014,2015, 2016, 2017, 2018, 2019, 2020,2021]
month = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
target = ["3008A40", "2012F50"]

###DATA SCRAPING

def waterMeasuring(year, month, target):


    pageNo=1
    numOfRows=31

    resultType="JSON"
    ptNoList=target
    # gcp_conn_id = "Dejon_data_Google_Storage"
    # gcp_conn_id = gcp_conn_id
    #
    wmyrList=year
    wmodList=month


    base_url = "http://apis.data.go.kr/1480523/WaterQualityService"
    # function = "/getRealTimeWaterQualityList"
    function = "/getWaterMeasuringList"
    get_url =  base_url + function


    def access_api(function, params):
        # print("Getting results...")
        target = base_url + function
        r = requests.get(target, params).json()
        tm.sleep(0.1)
        res = r['getWaterMeasuringList']
        return res['item']

    data_file = []
    file_name = "data_serviWaterMeasuringList.csv"
    for item in range(0, len(ptNoList)):
        for i in range(0, len(wmyrList)):
            for j in range(0, len(wmodList)):

                Payload = {
                    "serviceKey": "/S1CuHzopeMWDtsc2q26Ezp5Vgpgf2XGBYzYZehUCBgBQpHaZ+GvLIbar8Q+MT7zAliK60Rzoj9kEDMZlIhI4Q==",
                    "pageNo": pageNo,
                    "numOfRows": numOfRows,
                    "resultType": resultType,
                    "ptNoList": ptNoList[item],
                    "wmyrList": wmyrList[i],
                    "wmodList": wmodList[j]
                }
                #  API ACCESS FUNCTIONS ###
                data = access_api(function, params=Payload)



                count = 0
                for row in data:
                    data_file.append(row)
                result = pd.DataFrame(data_file)
                print("TARGET:  ", ptNoList[item], "YEAR: ", wmyrList[i], "MONTH:  ", wmodList[j], "'s SCRAPPED ")
                print("ROW # =: ", len(result))# print(result)

        return result, result.columns

### END OF DATA SCRAPING
# [START howto_operator_bigquery_query]

s_data = waterMeasuring(year=[2013, 2014,2015, 2016, 2017, 2018, 2019, 2020,2021],
                                 month=["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"],
                                 target=target)[0]
s_columns = waterMeasuring(year=[2013, 2014,2015, 2016, 2017, 2018, 2019, 2020,2021],
                                 month=["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"],
                                 target=target)[1]
INSERT_ROWS_QUERY = (
    f"INSERT INTO {DATASET_NAME} ({s_columns}) VALUES({s_data})"
)

# [END howto_operator_bigquery_query]

dag_id = "dejon_WaterMeasuring"
with models.DAG(
        dag_id,
        schedule_interval=None,  # Override to match your needs
        start_date=days_ago(1),
        tags=["example"],
        user_defined_macros={"DATASET": DATASET_NAME, "TABLE": TABLE_1},
    ) as dag_with_locations:
    # [START howto_operator_bigquery_insert_job]
    insert_query_job = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration={
            "query": {
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": "False",
            }
        },
        location=LOCATION,
        bigquery_conn_id="google_BQ_connection"
    )
    # [END howto_operator_bigquery_insert_job]
    execute_insert_query = BigQueryExecuteQueryOperator(
        task_id="execute_insert_query", sql=INSERT_ROWS_QUERY, use_legacy_sql=False, location=LOCATION
    )
    insert_query_job