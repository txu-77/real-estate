import json
import logging
from datetime import timedelta
from pathlib import Path
from typing import Dict

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from schema.honestdoor_raw import HONESTDOOR_LOADJOBCONFIGS
from src.HonestDoor import HonestDoor

GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_CONNECT_ID = Variable.get("GCP_CONNECT_ID")
BUCKET = "txu-raw"
BLOB_PREFIX = "real_estate/honestdoor"

DAG_ID = Path(__file__).stem
AIRFLOW_DEFAULT_ARGS = {
    "owner": "txu",
    "depends_on_past": False,
    "start_date": pendulum.today("UTC").add(days=-1),
    "retries": 6,
    "retry_delay": timedelta(minutes=15),
}


def get_property_listings(city_config: Dict[str, str], timestamp: str) -> str:
    """
    Gets all property listings / city and saves into GCS

    Args:
        timestamp (str): YYYY-MM-DD
    """
    honestdoor = HonestDoor(city=city_config["city"], province=city_config["province"])

    properties = honestdoor.get_all_properties()

    # Save into GCS
    gcs_client = GCSHook(gcp_conn_id=GCP_CONNECT_ID).get_conn()
    gcs_bucket = gcs_client.get_bucket(BUCKET)
    blob = gcs_bucket.blob(
        f"{BLOB_PREFIX}/{timestamp.replace('-', '/')}/{city_config['city'].lower()}.json"
    )
    blob.upload_from_string(
        data="\n".join(json.dumps(dict(p, **dict(dt=timestamp))) for p in properties),
        content_type="application/json",
    )
    return f"gs://{BUCKET}/{blob.name}"


@task
def load_property_listings(
    city_config: Dict[str, str], gcs_uri: str, timestamp: str
) -> str:
    """
    Loads properety listings into BQ
    """
    bq_client = BigQueryHook(gcp_conn_id=GCP_CONNECT_ID).get_client(GCP_PROJECT)

    destination_table = f"{GCP_PROJECT}.honestdoor.{city_config['city'].lower()}"
    try:
        bq_client.get_table(destination_table)
    except NotFound:
        des_table = bigquery.Table(
            destination_table,
            schema=HONESTDOOR_LOADJOBCONFIGS["properties"]["schema"],
        )
        des_table.clustering_fields = ["dt"]
        des_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="dt",
            require_partition_filter=True,
        )
        bq_client.create_table(des_table)
        logging.info(f"Successfully created {destination_table}!")

    job_config = bigquery.LoadJobConfig(**HONESTDOOR_LOADJOBCONFIGS["properties"])
    bq_client.load_table_from_uri(
        source_uris=gcs_uri,
        destination=f"{destination_table}${timestamp}",
        job_config=job_config,
    )
    return destination_table


@task
def get_property_details(
    city_config: Dict[str, str], properties_table: str, timestamp: str
):
    """
    Gets property details for each property
    """
    honestdoor = HonestDoor(city=city_config["city"], province=city_config["province"])

    bq_client = BigQueryHook(gcp_conn_id=GCP_CONNECT_ID).get_client(GCP_PROJECT)
    sql = f"""
    SELECT DISTINCT meta.id
    FROM `{properties_table}`
    WHERE dt = "{timestamp}";
    """
    property_details = []

    results = bq_client.query(sql)
    total_rows = results.result().total_rows
    for i, result in enumerate(results):
        logging.info(f"Getting {i}/{total_rows} properties...")
        result = dict(result)
        property_dict = honestdoor.get_property_id(id_=result["id"])
        if property_dict:
            property_detail = honestdoor.get_property(property_dict=property_dict)
            property_details.append(property_detail)
        else:
            logging.info(
                f"Property {result['id']} has no details available, skipping..."
            )
            continue

    # Save into GCS
    gcs_client = GCSHook(gcp_conn_id=GCP_CONNECT_ID).get_conn()
    gcs_bucket = gcs_client.get_bucket(BUCKET)
    blob = gcs_bucket.blob(
        f"{BLOB_PREFIX}/{timestamp.replace('-', '/')}/{city_config['city'].lower()}_property_details.json"
    )
    blob.upload_from_string(
        data="\n".join(
            json.dumps(dict(p, **dict(dt=timestamp))) for p in property_details
        ),
        content_type="application/json",
    )
    return f"gs://{BUCKET}/{blob.name}"


@task
def load_property_details(
    city_config: Dict[str, str], gcs_uri: str, timestamp: str
) -> str:
    """
    Loads properety listings into BQ
    """
    bq_client = BigQueryHook(gcp_conn_id=GCP_CONNECT_ID).get_client(GCP_PROJECT)

    destination_table = (
        f"{GCP_PROJECT}.honestdoor.{city_config['city'].lower()}_details"
    )
    try:
        bq_client.get_table(destination_table)
    except NotFound:
        des_table = bigquery.Table(
            destination_table,
            schema=HONESTDOOR_LOADJOBCONFIGS["property"]["schema"],
        )
        des_table.clustering_fields = ["dt"]
        des_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="dt",
            require_partition_filter=True,
        )
        bq_client.create_table(des_table)
        logging.info(f"Successfully created {destination_table}!")

    job_config = bigquery.LoadJobConfig(**HONESTDOOR_LOADJOBCONFIGS["property"])
    bq_client.load_table_from_uri(
        source_uris=gcs_uri,
        destination=f"{destination_table}${timestamp}",
        job_config=job_config,
    )


@dag(
    dag_id=DAG_ID,
    default_args=AIRFLOW_DEFAULT_ARGS,
    schedule_interval="0 12 * * 7",
    tags=["real_estate", "buy", "raw"],
)
def workflow():
    start = EmptyOperator(task_id="start")
    for city_config in [
        {"city": "Vancouver", "province": "British Columbia"},
        {"city": "Burnaby", "province": "British Columbia"},
        {"city": "Richmond", "province": "British Columbia"},
        {"city": "Calgary", "province": "Alberta"},
    ]:
        properties_to_gcs = task(
            task_id=f"get_properties_{city_config['city'].lower()}"
        )(get_property_listings)(
            city_config=city_config,
            timestamp="{{ ds }}",
        )
        properties_to_bq = load_property_listings(
            city_config=city_config,
            gcs_uri=properties_to_gcs,
            timestamp="{{ ds_nodash }}",
        )
        details_to_gcs = get_property_details(
            city_config=city_config,
            properties_table=properties_to_bq,
            timestamp="{{ ds }}",
        )
        load_property_details(
            city_config=city_config, gcs_uri=details_to_gcs, timestamp="{{ ds_nodash }}"
        )
        start >> properties_to_gcs


dag = workflow()
