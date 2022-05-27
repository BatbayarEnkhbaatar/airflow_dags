import pandas as pd
import requests
import time as tm
from google.cloud import bigquery
import os

# Construct a BigQuery client object.
client = bigquery.Client()
credentials_path ="solar-idea-BQ.json"

# print(credentials_path)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
print(credentials_path)
# TODO(developer): Set table_id to the ID of the table to create.s
table_id = "solar-idea-351402.dejon_dataset.WaterMeasuringList"
table = client.get_table(table_id)
generated_schema = [{'name':i.name, 'type':i.field_type} for i in table.schema]
def waterMeasuring(year, month):


    pageNo=1
    numOfRows=31

    resultType="JSON"
    ptNoList=["3008A40", "2012F50"]
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
        print("Getting results...")
        target = base_url + function
        r = requests.get(target, params).json()
        tm.sleep(0.1)
        res = r['getWaterMeasuringList']
        return res['item']

    file_name = "data_serviWaterMeasuringList.csv"
    for item in range(0, len(ptNoList)):
        for i in range(0, len(wmyrList)):
            for j in range(0, len(wmodList)):
                try:

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

                    data_file = []

                    count = 0
                    for row in data:
                        data_file.append(row)
                    result = pd.DataFrame(data_file)
                    result.columns = [i.name for i in table.schema]
                    if len(result) > 0:
                        # print(result)
                        result.to_gbq(destination_table="dejon_dataset.WaterMeasuringList", project_id="solar-idea-351402", table_schema=generated_schema, progress_bar=True, if_exists="append")
                        print("TARGET:  ", ptNoList[item], "YEAR: ", wmyrList[i], "MONTH:  ", wmodList[j], "'s DATA INSERTED INTO BigQueryDB ")
                    else:
                        print("There is no data available in", wmyrList[i], wmodList[j], "for ", ptNoList[item] )

                except:
                    print("There is no data available in ", wmyrList[i], wmodList[j], "for ", ptNoList[item] )



    return "success"
# year = [2012]
# month = ["01"]
