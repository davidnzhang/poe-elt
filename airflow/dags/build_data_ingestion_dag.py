import os
import logging
import requests

from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage

import json

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

def download_dataset(input_url, output_path):
    request = requests.get(input_url)
    output = request.json()
    with open(output_path, 'w') as f:
        json.dump(output, f)

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(0),
    "depends_on_past": False,
    "retries": 1,
}

def download_upload_dag(
    dag,
    url_template,
    local_json_path_template,
    gcs_path_template
):
    with dag:    
        download_dataset_task = PythonOperator(
            task_id="download_dataset_task",
            python_callable=download_dataset,
            op_kwargs={
                "input_url": url_template,
                "output_path": local_json_path_template
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template,
                "local_file": local_json_path_template,
            },
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {local_json_path_template}"
        )

        download_dataset_task >> local_to_gcs_task >> rm_task

build_url = "https://poe.ninja/api/data/0/getbuildoverview?overview=crucible&type=exp&language=en"
build_json_file_template = path_to_local_home + "/build_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
build_gcs_path_template = "build/build_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

build_dag = DAG(
    dag_id="build",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['build']
)

download_upload_dag(
    dag=build_dag,
    url_template=build_url,
    local_json_path_template=build_json_file_template,
    gcs_path_template=build_gcs_path_template
)