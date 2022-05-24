import os
from google.cloud import storage
from datetime import date


# my_bucket = storage_client.get_bucket("dejon_pipline_bucket01")

### Upload csv file to My bucket on GCP

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)

        print(
            f"File {source_file_name} uploaded to {destination_blob_name}"
        )
        return True
    except Exception as e:
        return e
file_name = "WaterMeasuringList.csv"
bucket_name ="dejon-data-bucket"
current = date.today()
uploaded_date = current.strftime("%Y-%m")
destination_blob_name = "WaterMeasuringList_" + uploaded_date

result_upload = upload_blob(bucket_name=bucket_name, source_file_name=file_name, destination_blob_name = destination_blob_name)

if result_upload ==True:
    print(result_upload)
else:
    print("RESULT_ERROR:  ", result_upload)

