
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from datetime import date
import requests
import time as tm
import csv
# Operators; we need this to operate!
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# import getWaterMeasuringList as wml

def waterMeasuring():

    todays_date = date.today()
    current_year = todays_date.year
    current_month = todays_date.month
    pageNo=1
    numOfRows=1000
    resultType="JSON"
    ptNoList=["3008A40", "2012F50"]
    wmyrList=[current_year]
    wmodList=[current_month]
    # wmyrList=["2012", "2013", "2014", "2015", "2016", "2017","2018","2019", "2020", "2021", "2022"]
    # wmodList=["01","02","03", "04","05","06","07","08","09","10","11","12"]

    #### Parameters Values
    Payload = {
        "serviceKey" : "/S1CuHzopeMWDtsc2q26Ezp5Vgpgf2XGBYzYZehUCBgBQpHaZ+GvLIbar8Q+MT7zAliK60Rzoj9kEDMZlIhI4Q==",
        "pageNo" : pageNo,
        "numOfRows" : numOfRows,
        "resultType" :  resultType,
        "ptNoList" : ptNoList,
        "wmyrList": wmyrList ,
        "wmodList" : wmodList
    }

    base_url = "http://apis.data.go.kr/1480523/WaterQualityService"
    # function = "/getRealTimeWaterQualityList"
    function = "/getWaterMeasuringList"
    get_url =  base_url + function


    def access_api(function, params):
        print("Getting results...")
        target = base_url + function
        r = requests.get(target, params).json()
        tm.sleep(2)
        res = r['getWaterMeasuringList']
        return res['item']


    for item in range(0, len(ptNoList)):
        for i in range(0, len(wmyrList)):
            for j in range(0, len(wmodList)):
                print("TARGET:  ", ptNoList[item], "YEAR: = ", wmyrList[i], "MONTH:  ", wmodList[j])
                data_file = open("WaterMeasuringList.csv", 'a')
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

                csv_writer = csv.writer(data_file)
                count = 0

                for row in data:
                    if count == 0:
                        # header = row.keys()
                        # csv_writer.writerow(header)
                        count += 1
                    csv_writer.writerow(row.values())

                data_file.close()
    ### Main program
    return "Done"



with DAG(
    'tutorial',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['batbayar@northstar.kr.co'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
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
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 23),
    catchup=False,
    tags=['Data Pipe Line '],
) as dag:

    PythonOperator(
        task_id=f"Dejon_dataPipeline_{datetime.today()}",
        python_callable=waterMeasuring()
    )


