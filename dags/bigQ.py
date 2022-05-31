# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Example Airflow DAG that shows how to use SalesforceToGcsOperator.
"""
import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.salesforce_to_gcs import SalesforceToGcsOperator

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "airflow-salesforce-bucket")
DATASET_NAME = os.environ.get("DATASET_NAME", "solar-idea-351402")
TABLE_NAME = os.environ.get("TABLE_NAME", "WaterMeasuringList ")
GCS_OBJ_PATH = os.environ.get("GCS_OBJ_PATH", " data202205.csv")
QUERY = "SELECT Id, Name, Company, Phone, Email, CreatedDate, LastModifiedDate, IsDeleted FROM Lead"
GCS_CONN_ID = os.environ.get("GCS_CONN_ID", "google_BQ_connection")
# SALESFORCE_CONN_ID = os.environ.get("SALESFORCE_CONN_ID", "salesforce_default")


with models.DAG(
    "GCSToBigQueryOperator",
    schedule_interval='@once',  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    # [START howto_operator_salesforce_to_gcs]

    # [END howto_operator_salesforce_to_gcs]


    load_csv = GCSToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket=GCS_BUCKET,
        source_objects=[GCS_OBJ_PATH],
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        write_disposition='WRITE_APPEND',
    )

