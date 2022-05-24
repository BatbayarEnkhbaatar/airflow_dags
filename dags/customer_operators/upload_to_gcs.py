from google.cloud import storage
from datetime import date

todays_date = date.today()
year = todays_date.year
month = todays_date.month

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
