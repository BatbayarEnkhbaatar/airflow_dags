
from datetime import date
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import os
todays_date = date.today()
year = todays_date.year
month = todays_date.month



### Upload csv file to My bucket on GCP
def upload_data(connec_id, bucket_name, source_file_name):
    """Uploads a file to the bucket."""
    # file_name = "WaterMeasuringList.csv"
    try:
        # storage_client = storage.Client()
        # bucket = storage_client.bucket(bucket_name)
        # blob = bucket.blob(destination_blob_name)
        # blob.upload_from_filename(f"{source_file_name}.csv")
        d_file = f"{source_file_name}".csv
        gcs_hook = GCSHook(connec_id)
        gcs_hook.upload(
            bucket_name= bucket_name,
            object_name= d_file,
            filename=d_file
        )
        return True
    except Exception as e:
        return e

# upload_data(connec_id="Dejon_data_Google_Storage", bucket_name="dejon-data-bucket", source_file_name="WaterMeasuringList.csv")