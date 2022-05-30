import datetime
from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import pandas as pd
import requests

# # TODO(developer): Set table_id to the ID of the table to create.s

def waterMeasuring(year, month, target, gcp_conn_id, bucket_name):


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
        # tm.sleep(0.1)
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
                t_date = datetime.date.today().strftime("%Y%m")
                print("TARGET:  ", ptNoList[item], "YEAR: ", wmyrList[i], "MONTH:  ", wmodList[j], "'s SCRAPPED ")
                print("ROW # =: ", len(result))# print(result)
                # result.to_csv(f"data_{t_date}.csv")
                result.to_json(f'result/data_{t_date}.json')
                fn = "data_"+ t_date + ".json"

                gcs_hook = GCSHook(gcp_conn_id)
                gcs_hook.upload(
                    bucket_name=bucket_name,
                    object_name=result.to_json(f'data_{t_date}.json'),
                    filename=fn )
        return "success"
# year = [2021]
# month = [ "11", "10", "09", "08", "07", "06"]
# target = ["3008A40", "2012F50"]
# waterMeasuring (year= year, month=month, target=target)
# data = json.loads(data)
# print(pd.DataFrame(data))
# print(data.columns)