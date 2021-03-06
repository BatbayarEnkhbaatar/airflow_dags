from datetime import date
import datetime
import pandas as pd
import requests
from airflow.providers.google.cloud.hooks.gcs import GCSHook



todays_date = date.today()
year = todays_date.year
month = todays_date.month

# # TODO(developer): Set table_id to the ID of the table to create.s

def upload_data(year, month, target, gcp_conn_id, bucket_name):


    pageNo=1
    numOfRows=31

    resultType="JSON"
    ptNoList=[target]
    # gcp_conn_id = "Dejon_data_Google_Storage"
    # gcp_conn_id = gcp_conn_id
    #
    wmyrList=[year]
    wmodList=[month]


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
                print("ROW # =: ", len(result))  # print(result)
                result.to_csv(f"result/data_{t_date}.csv", index=False)
                # result.to_json(f'result/data_{t_date}.json')
                # fn = "data_"+ t_date + ".json"
                # print("DATA is exportd to JSON format", fn)

                gcs_hook = GCSHook(gcp_conn_id)
                gcs_hook.upload(
                    bucket_name=bucket_name,
                    object_name="data_"+t_date+".csv",
                    filename=f"result/data_{t_date}.csv"
                )
        return "success"