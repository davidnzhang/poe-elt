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
import pyarrow.json as pj
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

def download_dataset(input_url, output_path):
    request = requests.get(input_url)
    output = request.json()['lines']
    with open(output_path, 'w') as f:
        json.dump(output, f, ensure_ascii=False)


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
    "start_date": datetime(2023,5,5),
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

item_url_prefix = "https://poe.ninja/api/data/itemoverview?league=Crucible&type="
currency_url_prefix = "https://poe.ninja/api/data/currencyoverview?league=Crucible&type="

currency_url_suffix = "Currency"
fragment_url_suffix = "Fragment"

unique_weapon_url_suffix = "UniqueWeapon"
unique_armour_url_suffix = "UniqueArmour"
unique_accessory_url_suffix = "UniqueAccessory"
unique_flask_url_suffix = "UniqueFlask"
unique_jewel_url_suffix = "UniqueJewel"

skill_gem_url_suffix = "SkillGem"

cluster_jewel_url_suffix = "ClusterJewel"

divination_url_suffix = "DivinationCard"
artifact_url_suffix = "Artifact"
oil_url_suffix = "Oil"
incubator_url_suffix = "Incubator"

map_url_suffix = "Map"
blighted_map_url_suffix = "BlightedMap"
blight_ravaged_map_url_suffix = "BlightRavagedMap"
unique_map_url_suffix = "UniqueMap"

delirium_orb_url_suffix = "DeliriumOrb"
invitation_url_suffix = "Invitation"
scarab_url_suffix = "Scarab"

base_type_url_suffix = "BaseType"
fossil_url_suffix = "Fossil"
resonator_url_suffix = "Resonator"
beast_url_suffix = "Beast"
essence_url_suffix = "Essence"
vial_url_suffix = "Vial"

helmet_enchant_url_suffix = "HelmetEnchant"

unique_weapon_url = item_url_prefix + unique_weapon_url_suffix
unique_weapon_json_file_template = path_to_local_home + "/unique_weapon_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
unique_weapon_gcs_path_template = "economy/unique_weapon/unique_weapon_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

unique_weapon_dag = DAG(
    dag_id="unique_weapon",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=unique_weapon_dag,
    url_template=unique_weapon_url,
    local_json_path_template=unique_weapon_json_file_template,
    gcs_path_template=unique_weapon_gcs_path_template
)

unique_armour_url = item_url_prefix + unique_armour_url_suffix
unique_armour_json_file_template = path_to_local_home + "/unique_armour_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
unique_armour_gcs_path_template = "economy/unique_armour/unique_armour_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

unique_armour_dag = DAG(
    dag_id="unique_armour",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=unique_armour_dag,
    url_template=unique_armour_url,
    local_json_path_template=unique_armour_json_file_template,
    gcs_path_template=unique_armour_gcs_path_template
)

unique_accessory_url = item_url_prefix + unique_accessory_url_suffix
unique_accessory_json_file_template = path_to_local_home + "/unique_accessory_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
unique_accessory_gcs_path_template = "economy/unique_accessory/unique_accessory_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

unique_accessory_dag = DAG(
    dag_id="unique_accessory",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=unique_accessory_dag,
    url_template=unique_accessory_url,
    local_json_path_template=unique_accessory_json_file_template,
    gcs_path_template=unique_accessory_gcs_path_template
)

unique_flask_url = item_url_prefix + unique_flask_url_suffix
unique_flask_json_file_template = path_to_local_home + "/unique_flask_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
unique_flask_gcs_path_template = "economy/unique_flask/unique_flask_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

unique_flask_dag = DAG(
    dag_id="unique_flask",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=unique_flask_dag,
    url_template=unique_flask_url,
    local_json_path_template=unique_flask_json_file_template,
    gcs_path_template=unique_flask_gcs_path_template
)

unique_jewel_url = item_url_prefix + unique_jewel_url_suffix
unique_jewel_json_file_template = path_to_local_home + "/unique_jewel_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
unique_jewel_gcs_path_template = "economy/unique_jewel/unique_jewel_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

unique_jewel_dag = DAG(
    dag_id="unique_jewel",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=unique_jewel_dag,
    url_template=unique_jewel_url,
    local_json_path_template=unique_jewel_json_file_template,
    gcs_path_template=unique_jewel_gcs_path_template
)

skill_gem_url = item_url_prefix + skill_gem_url_suffix
skill_gem_json_file_template = path_to_local_home + "/skill_gem_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
skill_gem_gcs_path_template = "economy/skill_gem/skill_gem_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

skill_gem_dag = DAG(
    dag_id="skill_gem",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=skill_gem_dag,
    url_template=skill_gem_url,
    local_json_path_template=skill_gem_json_file_template,
    gcs_path_template=skill_gem_gcs_path_template
)

cluster_jewel_url = item_url_prefix + cluster_jewel_url_suffix
cluster_jewel_json_file_template = path_to_local_home + "/cluster_jewel_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
cluster_jewel_gcs_path_template = "economy/cluster_jewel/cluster_jewel_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

cluster_jewel_dag = DAG(
    dag_id="cluster_jewel",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=cluster_jewel_dag,
    url_template=cluster_jewel_url,
    local_json_path_template=cluster_jewel_json_file_template,
    gcs_path_template=cluster_jewel_gcs_path_template
)

divination_url = item_url_prefix + divination_url_suffix
divination_json_file_template = path_to_local_home + "/divination_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
divination_gcs_path_template = "economy/divination/divination_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

divination_dag = DAG(
    dag_id="divination",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=divination_dag,
    url_template=divination_url,
    local_json_path_template=divination_json_file_template,
    gcs_path_template=divination_gcs_path_template
)

artifact_url = item_url_prefix + artifact_url_suffix
artifact_json_file_template = path_to_local_home + "/artifact_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
artifact_gcs_path_template = "economy/artifact/artifact_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

artifact_dag = DAG(
    dag_id="artifact",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=artifact_dag,
    url_template=artifact_url,
    local_json_path_template=artifact_json_file_template,
    gcs_path_template=artifact_gcs_path_template
)

oil_url = item_url_prefix + oil_url_suffix
oil_json_file_template = path_to_local_home + "/oil_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
oil_gcs_path_template = "economy/oil/oil_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

oil_dag = DAG(
    dag_id="oil",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=oil_dag,
    url_template=oil_url,
    local_json_path_template=oil_json_file_template,
    gcs_path_template=oil_gcs_path_template
)

incubator_url = item_url_prefix + incubator_url_suffix
incubator_json_file_template = path_to_local_home + "/incubator_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
incubator_gcs_path_template = "economy/incubator/incubator_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

incubator_dag = DAG(
    dag_id="incubator",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=incubator_dag,
    url_template=incubator_url,
    local_json_path_template=incubator_json_file_template,
    gcs_path_template=incubator_gcs_path_template
)

map_url = item_url_prefix + map_url_suffix
map_json_file_template = path_to_local_home + "/map_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
map_gcs_path_template = "economy/map/map_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

map_dag = DAG(
    dag_id="map",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=map_dag,
    url_template=map_url,
    local_json_path_template=map_json_file_template,
    gcs_path_template=map_gcs_path_template
)

blighted_map_url = item_url_prefix + blighted_map_url_suffix
blighted_map_json_file_template = path_to_local_home + "/blighted_map_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
blighted_map_gcs_path_template = "economy/blighted_blighted_map/blighted_map_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

blighted_map_dag = DAG(
    dag_id="blighted_map",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=blighted_map_dag,
    url_template=blighted_map_url,
    local_json_path_template=blighted_map_json_file_template,
    gcs_path_template=blighted_map_gcs_path_template
)

blight_ravaged_map_url = item_url_prefix + blight_ravaged_map_url_suffix
blight_ravaged_map_json_file_template = path_to_local_home + "/blight_ravaged_map_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
blight_ravaged_map_gcs_path_template = "economy/blight_ravaged_map/blight_ravaged_map_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

blight_ravaged_map_dag = DAG(
    dag_id="blight_ravaged_map",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=blight_ravaged_map_dag,
    url_template=blight_ravaged_map_url,
    local_json_path_template=blight_ravaged_map_json_file_template,
    gcs_path_template=blight_ravaged_map_gcs_path_template
)

unique_map_url = item_url_prefix + unique_map_url_suffix
unique_map_json_file_template = path_to_local_home + "/unique_map_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
unique_map_gcs_path_template = "economy/unique_map/unique_map_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

unique_map_dag = DAG(
    dag_id="unique_map",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=unique_map_dag,
    url_template=unique_map_url,
    local_json_path_template=unique_map_json_file_template,
    gcs_path_template=unique_map_gcs_path_template
)

delirium_orb_url = item_url_prefix + delirium_orb_url_suffix
delirium_orb_json_file_template = path_to_local_home + "/delirium_orb_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
delirium_orb_gcs_path_template = "economy/blighted_delirium_orb/delirium_orb_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

delirium_orb_dag = DAG(
    dag_id="delirium_orb",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=delirium_orb_dag,
    url_template=delirium_orb_url,
    local_json_path_template=delirium_orb_json_file_template,
    gcs_path_template=delirium_orb_gcs_path_template
)

invitation_url = item_url_prefix + invitation_url_suffix
invitation_json_file_template = path_to_local_home + "/invitation_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
invitation_gcs_path_template = "economy/invitation/invitation_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

invitation_dag = DAG(
    dag_id="invitation",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=invitation_dag,
    url_template=invitation_url,
    local_json_path_template=invitation_json_file_template,
    gcs_path_template=invitation_gcs_path_template
)

scarab_url = item_url_prefix + scarab_url_suffix
scarab_json_file_template = path_to_local_home + "/scarab_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
scarab_gcs_path_template = "economy/scarab/scarab_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

scarab_dag = DAG(
    dag_id="scarab",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=scarab_dag,
    url_template=scarab_url,
    local_json_path_template=scarab_json_file_template,
    gcs_path_template=scarab_gcs_path_template
)

base_type_url = item_url_prefix + base_type_url_suffix
base_type_json_file_template = path_to_local_home + "/base_type_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
base_type_gcs_path_template = "economy/base_type/base_type_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

base_type_dag = DAG(
    dag_id="base_type",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=base_type_dag,
    url_template=base_type_url,
    local_json_path_template=base_type_json_file_template,
    gcs_path_template=base_type_gcs_path_template
)

fossil_url = item_url_prefix + fossil_url_suffix
fossil_json_file_template = path_to_local_home + "/fossil_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
fossil_gcs_path_template = "economy/fossil/fossil_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

fossil_dag = DAG(
    dag_id="fossil",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=fossil_dag,
    url_template=fossil_url,
    local_json_path_template=fossil_json_file_template,
    gcs_path_template=fossil_gcs_path_template
)

resonator_url = item_url_prefix + resonator_url_suffix
resonator_json_file_template = path_to_local_home + "/resonator_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
resonator_gcs_path_template = "economy/resonator/resonator_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

resonator_dag = DAG(
    dag_id="resonator",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=resonator_dag,
    url_template=resonator_url,
    local_json_path_template=resonator_json_file_template,
    gcs_path_template=resonator_gcs_path_template
)

beast_url = item_url_prefix + beast_url_suffix
beast_json_file_template = path_to_local_home + "/beast_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
beast_gcs_path_template = "economy/beast/beast_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

beast_dag = DAG(
    dag_id="beast",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=beast_dag,
    url_template=beast_url,
    local_json_path_template=beast_json_file_template,
    gcs_path_template=beast_gcs_path_template
)

essence_url = item_url_prefix + essence_url_suffix
essence_json_file_template = path_to_local_home + "/essence_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
essence_gcs_path_template = "economy/essence/essence_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

essence_dag = DAG(
    dag_id="essence",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=essence_dag,
    url_template=essence_url,
    local_json_path_template=essence_json_file_template,
    gcs_path_template=essence_gcs_path_template
)

vial_url = item_url_prefix + vial_url_suffix
vial_json_file_template = path_to_local_home + "/vial_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
vial_gcs_path_template = "economy/vial/vial_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

vial_dag = DAG(
    dag_id="vial",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=vial_dag,
    url_template=vial_url,
    local_json_path_template=vial_json_file_template,
    gcs_path_template=vial_gcs_path_template
)

helmet_enchant_url = item_url_prefix + helmet_enchant_url_suffix
helmet_enchant_json_file_template = path_to_local_home + "/helmet_enchant_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
helmet_enchant_gcs_path_template = "economy/helmet_enchant/helmet_enchant_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

helmet_enchant_dag = DAG(
    dag_id="helmet_enchant",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=helmet_enchant_dag,
    url_template=helmet_enchant_url,
    local_json_path_template=helmet_enchant_json_file_template,
    gcs_path_template=helmet_enchant_gcs_path_template
)

currency_url = currency_url_prefix + currency_url_suffix
currency_json_file_template = path_to_local_home + "/currency_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
currency_gcs_path_template = "economy/currency/currency_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

currency_dag = DAG(
    dag_id="currency",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=currency_dag,
    url_template=currency_url,
    local_json_path_template=currency_json_file_template,
    gcs_path_template=currency_gcs_path_template
)

currency_url = currency_url_prefix + currency_url_suffix
currency_json_file_template = path_to_local_home + "/currency_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
currency_gcs_path_template = "economy/currency/currency_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

currency_dag = DAG(
    dag_id="currency",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=currency_dag,
    url_template=currency_url,
    local_json_path_template=currency_json_file_template,
    gcs_path_template=currency_gcs_path_template
)

fragment_url = currency_url_prefix + fragment_url_suffix
fragment_json_file_template = path_to_local_home + "/fragment_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"
fragment_gcs_path_template = "economy/fragment/fragment_{{ execution_date.strftime(\'%Y-%m-%d\') }}.json"

fragment_dag = DAG(
    dag_id="fragment",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['economy','items']
)

download_upload_dag(
    dag=fragment_dag,
    url_template=fragment_url,
    local_json_path_template=fragment_json_file_template,
    gcs_path_template=fragment_gcs_path_template
)