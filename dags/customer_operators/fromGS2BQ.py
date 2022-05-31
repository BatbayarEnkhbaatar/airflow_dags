from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.
table_id = "solar-idea-351402.dejon_dataset.WaterMeasuringList"

job_config = bigquery.LoadJobConfig(
    schema=[
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
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)
uri = "gs://cloud-samples-data/bigquery/us-states/us-states.json"

load_job = client.load_table_from_uri(
    uri,
    table_id,
    location="US",  # Must match the destination dataset location.
    job_config=job_config,
)  # Make an API request.

load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)
print("Loaded {} rows.".format(destination_table.num_rows))

