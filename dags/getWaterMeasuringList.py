import pandas as pd
import requests
import time as tm
import csv
from google.cloud import storage
client = storage.Client
from datetime import date
### Working with GOOGLE
# Retrieve an existing bucket
# https://console.cloud.google.com/storage/browser/dejon-data-bucket
bucket = client.get_bucket('bucket-id')
# Then do other things...
blob = bucket.get_blob('remote/path/to/file.txt')
print(blob.download_as_bytes())
blob.upload_from_string('New contents!')


###
todays_date = date.today()
year = date.year
month = date.month
def waterMeasuring(year = 2013, month = "06"):
    current_year = year
    current_month = month
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


waterMeasuring()
