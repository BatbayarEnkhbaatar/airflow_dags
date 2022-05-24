import requests
import time as tm
import csv
import os
from datetime import date

# todays_date = date.today()
# year = todays_date.year
# month = todays_date.month

def waterMeasuring(year, month):


    pageNo=1
    numOfRows=31

    resultType="JSON"
    ptNoList=["3008A40", "2012F50"]
    # gcp_conn_id = "Dejon_data_Google_Storage"
    # gcp_conn_id = gcp_conn_id
    #
    wmyrList=[year]
    wmodList=[month]
    # wmyrList=["2012", "2013", "2014", "2015", "2016", "2017","2018","2019", "2020", "2021", "2022", {year}]
    # wmodList=["01","02","03", "04","05","06","07","08","09","10","11","12", {month}]

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
                # file_path = os.path.join("${AIRFLOW_HOME}/tmp/", file_name)
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

    return "Done"
# waterMeasuring(2013, "08")
