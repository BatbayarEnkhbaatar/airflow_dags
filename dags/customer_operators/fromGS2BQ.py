from google.oauth2 import service_account
from google.cloud import bigquery


def insert2BQ (credentials, project_id, table_id, file_to_upload):
    # Create Authentication Credentials
    project_id = project_id
    table_id = table_id
    gcp_credentials = service_account.Credentials.from_service_account_file(credentials)

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
        uri, table_id, job_config=job_config
    )

    csv_load_job.result()
    print("Successfully done")
    return "success"
