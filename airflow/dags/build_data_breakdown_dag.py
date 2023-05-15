import os
import logging
import requests

from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from google.cloud import storage

import json

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
GCS_SOURCE_BUILD_PATH = "build/build_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

keys_to_keep = ['names','accounts','classes','levels','life','energyShield','delveSolo']

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

def download_from_gcs(bucket_name, object_name, destination_file):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name) # object name is the build_yyyy-mm-dd.json file downloaded in the build data ingestion dag
    blob.download_to_filename(destination_file)

def split_json(downloaded_file, table_name, split_file_path):
    json_file = open(downloaded_file)
    with open(split_file_path, 'w') as f:
        output = json.load(json_file)[table_name]
        json.dump(output, f)

def upload_to_gcs(bucket_name, object_name, local_file):
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
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def extract_by_keys(downloaded_file, split_file_path):
    json_file = open(downloaded_file)
    output = json.load(json_file)
    dict_to_keep = {key: output[key] for key in keys_to_keep}
    with open(split_file_path, 'w') as f:
        json.dump(dict_to_keep, f)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023,5,6),
    "depends_on_past": False,
    "retries": 1,
}

def download_upload_dag(
    dag,
    downloaded_json_path,
    json_key,
    transformed_json_path,
    gcs_destination_path
):
    with dag:
        sensor = ExternalTaskSensor(
            task_id="sensor",
            external_dag_id="build"
        )

        download_dataset_task = PythonOperator(
            task_id="download_dataset_task",
            python_callable=download_from_gcs,
            op_kwargs={
                "bucket_name": BUCKET,
                "object_name": GCS_SOURCE_BUILD_PATH,
                "destination_file": downloaded_json_path
            },
        )

        split_dataset_task = PythonOperator(
            task_id="split_dataset_task",
            python_callable=split_json,
            op_kwargs={
                "downloaded_file": downloaded_json_path,
                "table_name": json_key,
                "split_file_path": transformed_json_path
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket_name": BUCKET,
                "object_name": gcs_destination_path,
                "local_file": transformed_json_path,
            },
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {downloaded_json_path} {transformed_json_path}"
        )

        sensor >> download_dataset_task >> split_dataset_task >> local_to_gcs_task >> rm_task

def download_upload_dims_dag(
    dag,
    downloaded_json_path,
    transformed_json_path,
    gcs_destination_path
):
    with dag:
        sensor = ExternalTaskSensor(
            task_id="sensor",
            external_dag_id="build"
        )

        download_dataset_task = PythonOperator(
            task_id="download_dataset_task",
            python_callable=download_from_gcs,
            op_kwargs={
                "bucket_name": BUCKET,
                "object_name": GCS_SOURCE_BUILD_PATH,
                "destination_file": downloaded_json_path
            },
        )

        extract_by_keys_task = PythonOperator(
            task_id="extract_by_keys_task",
            python_callable=extract_by_keys,
            op_kwargs={
                "downloaded_file": downloaded_json_path,
                "split_file_path": transformed_json_path
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket_name": BUCKET,
                "object_name": gcs_destination_path,
                "local_file": transformed_json_path,
            },
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {downloaded_json_path} {transformed_json_path}"
        )

        sensor >> download_dataset_task >> extract_by_keys_task >> local_to_gcs_task >> rm_task

uniqueitemuse_downloaded_json_path = path_to_local_home + "/uniqueitemuse_d_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
uniqueitemuse_transformed_json_path = path_to_local_home + "/uniqueitemuse_t_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
uniqueitemuse_gcs_destination_path = "build/uniqueitemuse/uniqueitemuse_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

uniqueitemuse_dag = DAG(
    dag_id="build_unique_item_use",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['build']
)

download_upload_dag(
    dag=uniqueitemuse_dag,
    downloaded_json_path=uniqueitemuse_downloaded_json_path,
    json_key='uniqueItemUse',
    transformed_json_path=uniqueitemuse_transformed_json_path,
    gcs_destination_path=uniqueitemuse_gcs_destination_path
)

uniqueitems_downloaded_json_path = path_to_local_home + "/uniqueitems_d_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
uniqueitems_transformed_json_path = path_to_local_home + "/uniqueitems_t_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
uniqueitems_gcs_destination_path = "build/uniqueitems/uniqueitems_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

uniqueitems_dag = DAG(
    dag_id="build_unique_items",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['build']
)

download_upload_dag(
    dag=uniqueitems_dag,
    downloaded_json_path=uniqueitems_downloaded_json_path,
    json_key='uniqueItems',
    transformed_json_path=uniqueitems_transformed_json_path,
    gcs_destination_path=uniqueitems_gcs_destination_path
)

activeskilluse_downloaded_json_path = path_to_local_home + "/activeskilluse_d_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
activeskilluse_transformed_json_path = path_to_local_home + "/activeskilluse_t_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
activeskilluse_gcs_destination_path = "build/activeskilluse/activeskilluse_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

activeskilluse_dag = DAG(
    dag_id="build_active_skill_use",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['build']
)

download_upload_dag(
    dag=activeskilluse_dag,
    downloaded_json_path=activeskilluse_downloaded_json_path,
    json_key='activeSkillUse',
    transformed_json_path=activeskilluse_transformed_json_path,
    gcs_destination_path=activeskilluse_gcs_destination_path
)


activeskills_downloaded_json_path = path_to_local_home + "/activeskills_d_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
activeskills_transformed_json_path = path_to_local_home + "/activeskills_t_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
activeskills_gcs_destination_path = "build/activeskills/activeskills_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

activeskills_dag = DAG(
    dag_id="build_active_skills",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['build']
)

download_upload_dag(
    dag=activeskills_dag,
    downloaded_json_path=activeskills_downloaded_json_path,
    json_key='activeSkills',
    transformed_json_path=activeskills_transformed_json_path,
    gcs_destination_path=activeskills_gcs_destination_path
)

allskilluse_downloaded_json_path = path_to_local_home + "/allskilluse_d_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
allskilluse_transformed_json_path = path_to_local_home + "/allskilluse_t_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
allskilluse_gcs_destination_path = "build/allskilluse/allskilluse_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

allskilluse_dag = DAG(
    dag_id="build_all_skill_use",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['build']
)

download_upload_dag(
    dag=allskilluse_dag,
    downloaded_json_path=allskilluse_downloaded_json_path,
    json_key='allSkillUse',
    transformed_json_path=allskilluse_transformed_json_path,
    gcs_destination_path=allskilluse_gcs_destination_path
)

allskills_downloaded_json_path = path_to_local_home + "/allskills_d_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
allskills_transformed_json_path = path_to_local_home + "/allskills_t_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
allskills_gcs_destination_path = "build/allskills/allskills_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

allskills_dag = DAG(
    dag_id="build_all_skills",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['build']
)

download_upload_dag(
    dag=allskills_dag,
    downloaded_json_path=allskills_downloaded_json_path,
    json_key='allSkills',
    transformed_json_path=allskills_transformed_json_path,
    gcs_destination_path=allskills_gcs_destination_path
)

keystones_downloaded_json_path = path_to_local_home + "/keystones_d_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
keystones_transformed_json_path = path_to_local_home + "/keystones_t_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
keystones_gcs_destination_path = "build/keystones/keystones_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

keystones_dag = DAG(
    dag_id="build_keystones",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['build']
)

download_upload_dag(
    dag=keystones_dag,
    downloaded_json_path=keystones_downloaded_json_path,
    json_key='keystones',
    transformed_json_path=keystones_transformed_json_path,
    gcs_destination_path=keystones_gcs_destination_path
)

keystoneuse_downloaded_json_path = path_to_local_home + "/keystoneuse_d_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
keystoneuse_transformed_json_path = path_to_local_home + "/keystoneuse_t_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
keystoneuse_gcs_destination_path = "build/keystoneuse/keystoneuse_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

keystoneuse_dag = DAG(
    dag_id="build_keystone_use",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['build']
)

download_upload_dag(
    dag=keystoneuse_dag,
    downloaded_json_path=keystoneuse_downloaded_json_path,
    json_key='keystoneUse',
    transformed_json_path=keystoneuse_transformed_json_path,
    gcs_destination_path=keystoneuse_gcs_destination_path
)

masteries_downloaded_json_path = path_to_local_home + "/masteries_d_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
masteries_transformed_json_path = path_to_local_home + "/masteries_t_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
masteries_gcs_destination_path = "build/masteries/masteries_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

masteries_dag = DAG(
    dag_id="build_masteries",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['build']
)

download_upload_dag(
    dag=masteries_dag,
    downloaded_json_path=masteries_downloaded_json_path,
    json_key='masteries',
    transformed_json_path=masteries_transformed_json_path,
    gcs_destination_path=masteries_gcs_destination_path
)

masteryuse_downloaded_json_path = path_to_local_home + "/masteryuse_d_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
masteryuse_transformed_json_path = path_to_local_home + "/masteryuse_t_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
masteryuse_gcs_destination_path = "build/masteryuse/masteryuse_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

masteryuse_dag = DAG(
    dag_id="build_mastery_use",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['build']
)

download_upload_dag(
    dag=masteryuse_dag,
    downloaded_json_path=masteryuse_downloaded_json_path,
    json_key='masteryUse',
    transformed_json_path=masteryuse_transformed_json_path,
    gcs_destination_path=masteryuse_gcs_destination_path
)

classnames_downloaded_json_path = path_to_local_home + "/classnames_d_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
classnames_transformed_json_path = path_to_local_home + "/classnames_t_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
classnames_gcs_destination_path = "build/classnames/classnames_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

classnames_dag = DAG(
    dag_id="build_class_names",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['build']
)

download_upload_dag(
    dag=classnames_dag,
    downloaded_json_path=classnames_downloaded_json_path,
    json_key='classNames',
    transformed_json_path=classnames_transformed_json_path,
    gcs_destination_path=classnames_gcs_destination_path
)

skilldetails_downloaded_json_path = path_to_local_home + "/skilldetails_d_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
skilldetails_transformed_json_path = path_to_local_home + "/skilldetails_t_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
skilldetails_gcs_destination_path = "build/skilldetails/skilldetails_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

skilldetails_dag = DAG(
    dag_id="build_skill_details",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['build']
)

download_upload_dag(
    dag=skilldetails_dag,
    downloaded_json_path=skilldetails_downloaded_json_path,
    json_key='skillDetails',
    transformed_json_path=skilldetails_transformed_json_path,
    gcs_destination_path=skilldetails_gcs_destination_path
)

build_dims_downloaded_json_path = path_to_local_home + "/build_dims_d_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
build_dims_transformed_json_path = path_to_local_home + "/build_dims_t_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
build_dims_gcs_destination_path = "build/build_dims/build_dims_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

build_dims_dag = DAG(
    dag_id="build_dims",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['build']
)

download_upload_dims_dag(
    dag=build_dims_dag,
    downloaded_json_path=build_dims_downloaded_json_path,
    transformed_json_path=build_dims_transformed_json_path,
    gcs_destination_path=build_dims_gcs_destination_path
)