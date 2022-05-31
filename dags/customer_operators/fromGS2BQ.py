from google.oauth2 import service_account
from google.cloud import bigquery


def insert2BQ (credentials, project_id, table_id, file_to_upload):
    # Create Authentication Credentials
    project_id = project_id
    table_id = table_id
    gcp_credentials = service_account.Credentials.from_service_account_file(
        {
              "type": "service_account",
              "project_id": "solar-idea-351402",
              "private_key_id": "ddbf9ad6503ddb2a40f3b7042cda7ca38471f981",
              "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCsAmpSx2RKl9ua\nsCQnC/9rv604MxqWv1OC1hajwk+AXvGjbHbZMLb1EUzdz29j6TTffYhviu+Z1ZWq\nvIYpPWgsn83fCxu8V8pT6e2a21k7nCpZAuwCSVJM5EsW4+OUzBzxBYSkTB7aotKZ\n9wodMMCCbuM9Y3avcxOVeWJpHEYM6h37OGqxrjoTwkSmlfOL49j2HbXw5wf/tin2\neBmrUaW71/DIAc8bRXjpw9/SvRgqRouzTcCRSM21/W9Gv0o54kjoy8CJBSY7rOqB\nBqCeFLbKLHbGcGa8TJeC/1l2UuWEUl6yUJ1sHfjzu7ZPZ12wzQsirppg80bNmbwe\n7XZwHTNdAgMBAAECggEAARt8ij8ONPxlyvORLq7Wgd/THRPCeyUiWUBeFxyi4jlv\nlk+riInrqke70N4V7Oon2jwmvaBtcEuDohICeMT/BPHcGNsWM7VKpZV8X10ZUGIu\nE/J0yQNQ/ug7AzEhOl3Bs5vcvFRgys5gteI1LottYy7qEm+FEyuV58Yx3T8z7z+B\njBbYjvTUh1vkZX6dK3MyhntCCuWTlD4JLF683jL4TGvboxELzwul3gOyOk1vFRQB\nNE5IJSRUwryyuiJwE6Xi7dAJH5cMWUGhgaa9HdWBJQboQlq6XAbL88fB8eozRpRd\nDd+aHT/gWdBjDhVqdicpVDL53PK7y0zxoURr4kDt4QKBgQDtdL+FkIICS7N1eLwb\nzzlLAk0V4+/L3w6wsGfp1mSQqZH92JT8CfcGsPhQxW9UZEAvULoqXY5kR/uiYvQq\nENQdZ654dXjbr/u27o0wgWWeMbPc//p51SQ6YbVGl/O0EZtkx7dunJQMFHovK9XI\nBSs7QLcCGH7fjJ0LRRvqZM+ooQKBgQC5cUAF9reHGd21nMeXR/JReMZyjR798al/\nMjIFS8v3DWzlyLwH840Yk5FQqHv/ofijfYjyoHPviB9Zzx1DZrbAI78FQc9f84Kg\nD4T0l+IGoG21LFpKFnC7bwdlbzJuNOSanP2btlBF8xWEOWEET5VicrIIekBHEzwj\njRVcgorlPQKBgDMKYEyOZlOl//olLJHxtgf4g6dL6nRxCtBR6tQpoE40FMxyrHuM\njMukaIu8FJUYQ2+oM4v9tii6DE24ZXFRUrbLXYZJAeR+7GVC25hsh/I8DsRXRXjA\nptvhurxk1x6ClltQTo55G52JHUZcbsRi0AsZevz5VPQqXmtYvIpihvxhAoGAUK1U\njUOo1L4MFtb+fnJumcNRksSSlyhr9UqBOIyhT/onRlDufQcYe3i3379tMunkojwb\nOHNV/P/bo9bXhFtLZowmrsLS1Yu7aKdX0JHDY96n3ApPvavFn8XOEfMunYOlcR6b\ntwK6KbR3x+6uppF5DDuZ+NY24LUdlNnsUTL5+oECgYBkbdyK/tiSsHpDlNzFlqf4\nXZ3ZHzXiG5Ti9oJf8JWyhlbos72nBd5GYhC9hTzwL2JiGC7nV+jNrw825I4ULjv2\nk3MrEOPlwhOdPYHzTdRf97zZQwQms04OlY0E2yxGbcVqfzm4fbZVR+WL7ohUQR5t\nZ51YPA5rxZSeNrRsKEVjxA==\n-----END PRIVATE KEY-----\n",
              "client_email": "airflow-gke@solar-idea-351402.iam.gserviceaccount.com",
              "client_id": "110973729110907272207",
              "auth_uri": "https://accounts.google.com/o/oauth2/auth",
              "token_uri": "https://oauth2.googleapis.com/token",
              "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
              "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/airflow-gke%40solar-idea-351402.iam.gserviceaccount.com"
            }
    )

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = "solar-idea-351402.dejon_dataset.WaterMeasuringList"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("ROWNO", "INTEGER"),
            bigquery.SchemaField("PT_NO", "STRING"),
            bigquery.SchemaField("PT_NM", "STRING"),
            bigquery.SchemaField("ADDR", "STRING"),
            bigquery.SchemaField("ORG_NM", "STRING"),
            bigquery.SchemaField("WMYR", "STRING"),
            bigquery.SchemaField("WMOD", "STRING"),
            bigquery.SchemaField("WMWK", "STRING"),
            bigquery.SchemaField("LON_DGR", "STRING"),
            bigquery.SchemaField("LON_MIN", "STRING"),
            bigquery.SchemaField("LON_SEC", "STRING"),
            bigquery.SchemaField("LAT_DGR", "STRING"),
            bigquery.SchemaField("LAT_MIN", "STRING"),
            bigquery.SchemaField("LAT_SEC", "STRING"),
            bigquery.SchemaField("WMCYMD", "STRING"),
            bigquery.SchemaField("WMDEP", "STRING"),
            bigquery.SchemaField("ITEM_LVL", "STRING"),
            bigquery.SchemaField("ITEM_AMNT", "STRING"),
            bigquery.SchemaField("ITEM_TEMP", "STRING"),
            bigquery.SchemaField("ITEM_PH", "STRING"),
            bigquery.SchemaField("ITEM_DOC", "STRING"),
            bigquery.SchemaField("ITEM_BOD", "STRING"),
            bigquery.SchemaField("ITEM_COD", "STRING"),
            bigquery.SchemaField("ITEM_SS", "STRING"),
            bigquery.SchemaField("ITEM_TCOLI", "STRING"),
            bigquery.SchemaField("ITEM_TN", "STRING"),
            bigquery.SchemaField("ITEM_TP", "STRING"),
            bigquery.SchemaField("ITEM_CD", "STRING"),
            bigquery.SchemaField("ITEM_CN", "STRING"),
            bigquery.SchemaField("ITEM_PB", "STRING"),
            bigquery.SchemaField("ITEM_CR6", "STRING"),
            bigquery.SchemaField("ITEM_AS", "STRING"),
            bigquery.SchemaField("ITEM_HG", "STRING"),
            bigquery.SchemaField("ITEM_CU", "STRING"),
            bigquery.SchemaField("ITEM_ABS", "STRING"),
            bigquery.SchemaField("ITEM_PCB", "STRING"),
            bigquery.SchemaField("ITEM_OP", "STRING"),
            bigquery.SchemaField("ITEM_MN", "STRING"),
            bigquery.SchemaField("ITEM_TRANS", "STRING"),
            bigquery.SchemaField("ITEM_CLOA", "STRING"),
            bigquery.SchemaField("ITEM_CL", "STRING"),
            bigquery.SchemaField("ITEM_ZN", "STRING"),
            bigquery.SchemaField("ITEM_CR", "STRING"),
            bigquery.SchemaField("ITEM_FE", "STRING"),
            bigquery.SchemaField("ITEM_PHENOL", "STRING"),
            bigquery.SchemaField("ITEM_NHEX", "STRING"),
            bigquery.SchemaField("ITEM_EC", "STRING"),
            bigquery.SchemaField("ITEM_TCE", "STRING"),
            bigquery.SchemaField("ITEM_PCE", "STRING"),
            bigquery.SchemaField("ITEM_NO3N", "STRING"),
            bigquery.SchemaField("ITEM_NH3N", "STRING"),
            bigquery.SchemaField("ITEM_ECOLI", "STRING"),
            bigquery.SchemaField("ITEM_POP", "STRING"),
            bigquery.SchemaField("ITEM_DTN", "STRING"),
            bigquery.SchemaField("ITEM_DTP", "STRING"),
            bigquery.SchemaField("ITEM_FL", "STRING"),
            bigquery.SchemaField("ITEM_COL", "STRING"),
            bigquery.SchemaField("ITEM_ALGOL", "STRING"),
            bigquery.SchemaField("ITEM_CCL4", "STRING"),
            bigquery.SchemaField("ITEM_DCETH", "STRING"),
            bigquery.SchemaField("ITEM_DCM", "STRING"),
            bigquery.SchemaField("ITEM_BENZENE", "STRING"),
            bigquery.SchemaField("ITEM_CHCL3", "STRING"),
            bigquery.SchemaField("ITEM_TOC", "STRING"),
            bigquery.SchemaField("ITEM_DEHP", "STRING"),
            bigquery.SchemaField("ITEM_ANTIMON", "STRING"),
            bigquery.SchemaField("ITEM_DIOX", "STRING"),
            bigquery.SchemaField("ITEM_HCHO", "STRING"),
            bigquery.SchemaField("ITEM_HCB", "STRING"),
            bigquery.SchemaField("ITEM_NI", "STRING"),
            bigquery.SchemaField("ITEM_BA", "STRING"),
            bigquery.SchemaField("ITEM_SE", "STRING"),

        ],
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
    )

    # CSV File Location (Cloud Storage Bucket)
    # uri = "https://storage.cloud.google.com/dejon-data-bucket01/data202205.csv"
    uri = file_to_upload

    # Create the Job
    csv_load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config,
    )

    csv_load_job.result()
    print("Successfully done")
    return "success"
