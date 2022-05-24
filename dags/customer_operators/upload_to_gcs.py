from google.cloud import storage
from datetime import date
from airflow.providers.google.cloud.hooks.gcs import GCSHook
todays_date = date.today()
year = todays_date.year
month = todays_date.month

### Upload csv file to My bucket on GCP

def upload_blob(bucket_name, source_file_name, destination_blob_name):
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
        print(
            f"File {source_file_name} uploaded to {destination_blob_name}"
        )
        return True
    except Exception as e:
        return e
