# -*- coding: utf-8 -*-
#
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

from typing import Any

import airflow
from airflow import models
from airflow.operators import bash_operator

gcs_to_bq = None  # type: Any
try:
    from airflow.contrib.operators import gcs_to_bq
except ImportError:
    pass


if gcs_to_bq is not None:
    args = {
        'owner': 'airflow',
        'start_date': airflow.utils.dates.days_ago(2)
    }

    dag = models.DAG(
        dag_id='gcs_to_bq_operator', default_args=args,
        schedule_interval=None)

    # [START howto_operator_gcs_to_bq]
    saveTo_BQ = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bQ',
        bucket='dejon-data-bucket01',
        source_objects=['dejon-data-bucket01/data202205.csv'],
        destination_project_dataset_table='solar-idea-351402.dejon_dataset',
        schema_fields=[
            {"name": "ITEM_SS", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_TCOLI", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_TN", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_TP", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_CD", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_CN", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_PB", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_CR6", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_AS", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_HG", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_CU", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_ABS", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_PCB", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_OP", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_MN", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_TRANS", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_CLOA", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_CL", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_ZN", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_CR", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_FE", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_PHENOL", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_NHEX",   "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_EC", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_TCE", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_PCE", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_NO3N", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_NH3N", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_ECOLI",        "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_POP", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_DTN",  "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_DTP", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_FL",  "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_COL", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_ALGOL",        "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_CCL4", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_DCETH",        "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_DCM", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_BENZENE",        "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_CHCL3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_TOC",        "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_DEHP", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_ANTIMON", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_DIOX", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_HCHO", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_HCB", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_NI", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_BA", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ITEM_SE", "type": "STRING", "mode": "NULLABLE"}
    ],
        write_disposition='WRITE_TRUNCATE',
        dag=dag)
    # [END howto_operator_gcs_to_bq]

    saveTo_BQ