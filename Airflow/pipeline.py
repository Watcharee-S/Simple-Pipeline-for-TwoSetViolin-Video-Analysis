from airflow.models import DAG
from pathlib import Path
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import requests

REGION = "us-central1"
ZONE = "us-central1-a"
CLUSTER_NAME = "pyspark-simple-de"
PROJECT_ID = Variable.get("project_id")
RAW_BUCKET_NAME = "raw-youtube-twoset"
PYSPARK_FILE = "transform_data_to_parquet.py"
PROCESSED_BUCKET_NAME = "processed-youtube-twoset"
DATASET_NAME = "youtube_data"
TABLE_NAME = "video_data_final"

def handle_response(response):
    if response.status_code == 200:
        print("Received 200 OK")
        return True
    else:
        print("Error")
        return False

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances" : 1,
        "machine_type_uri" : "n1-standard-2",
        "disk_config" : {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    },
    "worker_config": {
        "num_instances" : 2,
        "machine_type_uri" : "n1-standard-2",
        "disk_config" : {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    },
}

PYSPARK_JOB = {
    "reference" : {"project_id" : PROJECT_ID},
    "placement" : {"cluster_name" : CLUSTER_NAME},
    "pyspark_job" : {"main_python_file_uri" : f"gs://{RAW_BUCKET_NAME}/{PYSPARK_FILE}"}
}

with DAG(
    "simple-de-project",
    start_date = days_ago(1),
    schedule_interval = "@once",
    tags = ["de-project", "TwoSetViolin"]
) as dag:

    trigger_http = SimpleHttpOperator(
        task_id = "trigger_http",
        method = "GET",
        http_conn_id = "http_trigger",
        endpoint = Variable.get("http_trigger_endpoint"),
        response_filter = lambda response: handle_response(response),
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id = "create_cluster",
        project_id = PROJECT_ID,
        cluster_config = CLUSTER_CONFIG,
        region = REGION,
        cluster_name = CLUSTER_NAME,
    )

    pyspark_task = DataprocSubmitJobOperator(
        task_id = "pyspark_task",
        job = PYSPARK_JOB,
        region = REGION,
        project_id = PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id = "delete_cluster",
        project_id = PROJECT_ID,
        cluster_name = CLUSTER_NAME,
        region = REGION,
        trigger_rule = TriggerRule.ALL_DONE,
    )

    load_parquet = GCSToBigQueryOperator(
        task_id = "load_parquet",
        bucket = PROCESSED_BUCKET_NAME,
        source_objects = ["/result_youtube_parquet.parquet"],
        destination_project_dataset_table = f"{DATASET_NAME}.{TABLE_NAME}",
        write_disposition = "WRITE_APPEND",
    )

    trigger_http >> create_cluster >> pyspark_task >> load_parquet >> delete_cluster


