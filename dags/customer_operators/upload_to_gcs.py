
from datetime import date
from airflow.providers.google.cloud.hooks.gcs import GCSHook
todays_date = date.today()
year = todays_date.year
month = todays_date.month

import requests
import time as tm
import csv

from datetime import date

todays_date = date.today()
year = todays_date.year
month = todays_date.month


### Upload csv file to My bucket on GCP

def upload_data(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    try:
        gcp_conn_id = "Dejon_data_Google_Storage"

        # storage_client = storage.Client()
        # bucket = storage_client.bucket(bucket_name)
        # blob = bucket.blob(destination_blob_name)
        # blob.upload_from_filename(f"{source_file_name}.csv")
        d_file = f"{source_file_name}.csv"
        gcs_hook = GCSHook(gcp_conn_id)
        gcs_hook.upload(
            bucket_name= bucket_name,
            object_name= d_file,
            filename=source_file_name
        )
        return True
    except Exception as e:
        return e


def waterMeasuring(year, month):
    current_year = year
    current_month = month
    pageNo=1
    numOfRows=31
    resultType="JSON"
    ptNoList=["3008A40", "2012F50"]
    wmyrList=[current_year]
    wmodList=[current_month]
    # wmyrList=["2012", "2013", "2014", "2015", "2016", "2017","2018","2019", "2020", "2021", "2022"]
    # wmodList=["01","02","03", "04","05","06","07","08","09","10","11","12"]

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

    file_name = "WaterMeasuringList.csv"
    for item in range(0, len(ptNoList)):
        for i in range(0, len(wmyrList)):
            for j in range(0, len(wmodList)):
                print("TARGET:  ", ptNoList[item], "YEAR: ", wmyrList[i], "MONTH:  ", wmodList[j])

                data_file = open(file_name, 'w')
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
                        header = row.keys()
                        csv_writer.writerow(header)
                        count += 1
                    csv_writer.writerow(row.values())

                data_file.close()
    source_file_name = file_name
    bucket_name = "Dejon-data-bucket"
    current = date.today()
    uploaded_date = current.strftime("%Y-%m")
    destination_blob_name = source_file_name + "_" + uploaded_date
    upload_data(bucket_name=bucket_name, source_file_name=source_file_name, destination_blob_name=destination_blob_name)
    return "Done"

