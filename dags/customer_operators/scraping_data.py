import pandas as pd
import requests
import time as tm
# from google.cloud import bigquery
import os

# # Construct a BigQuery client object.
# client = bigquery.Client()
# credentials_path = "customer_operators/solar-idea-BQ.json"
#
# # print(credentials_path)
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
# print(credentials_path)
# # TODO(developer): Set table_id to the ID of the table to create.s
# table_id = "solar-idea-351402.dejon_dataset.WaterMeasuringList"
# table = client.get_table(table_id)
# generated_schema = [{'name':i.name, 'type':i.field_type} for i in table.schema]
def waterMeasuring(year, month, target):


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
                print("TARGET:  ", ptNoList[item], "YEAR: ", wmyrList[i], "MONTH:  ", wmodList[j], "'s SCRAPPED ")
                print("ROW # =: ", len(result))# print(result)

        return result, result.columns
# year = [2021]
# month = [ "12"]
# target = ["3008A40", "2012F50"]
# data = waterMeasuring (year= year, month=month, target=target)
# print("DATAIS SS= ",data[1])
# print(data.columns)