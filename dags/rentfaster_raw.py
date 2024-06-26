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

from schema.rentfaster_raw import RENTFASTER_LOADJOBCONFIGS
from src.RentFaster import RentFaster

GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_CONNECT_ID = Variable.get("GCP_CONNECT_ID")
BUCKET = "txu-raw"
BLOB_PREFIX = "real_estate/rentals"

DAG_ID = Path(__file__).stem
AIRFLOW_DEFAULT_ARGS = {
    "owner": "txu",
    "depends_on_past": False,
    "start_date": pendulum.today("UTC").add(days=-7),
    "retries": 6,
    "retry_delay": timedelta(minutes=15),
}


def get_features(city_id: str):
    """
    Gets city features including neighorhood and type
    """
    rentfaster = RentFaster(city_id=city_id)
    features = rentfaster.get_city_features()
    return json.dumps(features)


@task
def get_rentals(city_id: str, features: Dict, timestamp: str):
    """
    Gets rentals / neighborhood and unit_type
    """
    rentals = []

    rentfaster = RentFaster(city_id=city_id)

    features = json.loads(features)
    unit_types = features["type"]
    neighborhoods = features["neighborhood"]
    if not neighborhoods:
        logging.info(f"Found no neighborhoods for {city_id}.")
        neighborhoods = [""]

    for unit_type in unit_types:
        for neighborhood in neighborhoods:
            logging.info(
                f"Collecting rentals for city {city_id}, neighborhood "
                f"{neighborhood}, unit_type {unit_type}..."
            )
            resp = rentfaster.get_city_rentals(
                unit_type=unit_type,
                neighborhood=neighborhood,
            )

            # Add timestamp
            for r in resp:
                r["dt"] = timestamp

            rentals.extend(resp)

    logging.info(f"Collected {len(rentals)} for city {city_id}.")

    # Save into GCS
    gcs_client = GCSHook(gcp_conn_id=GCP_CONNECT_ID).get_conn()
    gcs_bucket = gcs_client.get_bucket(BUCKET)
    blob = gcs_bucket.blob(
        f"{BLOB_PREFIX}/{timestamp.replace('-', '/')}/{city_id.split('/')[-1]}.json"
    )
    blob.upload_from_string(
        data="\n".join(json.dumps(r) for r in rentals),
        content_type="application/json",
    )

    return f"gs://{BUCKET}/{blob.name}"


@task
def load_rentals(city_id: str, gcs_uri: str, timestamp: str):
    """
    Loads rentals from cloud storage into BQ
    """
    bq_client = BigQueryHook(gcp_conn_id=GCP_CONNECT_ID).get_client(GCP_PROJECT)

    destination_table = f"{GCP_PROJECT}.rentals.{city_id.split('/')[-1]}"
    try:
        bq_client.get_table(destination_table)
    except NotFound:
        des_table = bigquery.Table(
            destination_table,
            schema=RENTFASTER_LOADJOBCONFIGS["rentals"]["schema"],
        )
        des_table.clustering_fields = ["dt"]
        des_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="dt",
            require_partition_filter=True,
        )
        bq_client.create_table(des_table)
        logging.info(f"Successfully created {destination_table}!")

    job_config = bigquery.LoadJobConfig(**RENTFASTER_LOADJOBCONFIGS["rentals"])
    bq_client.load_table_from_uri(
        source_uris=gcs_uri,
        destination=f"{destination_table}${timestamp}",
        job_config=job_config,
    )
    return destination_table


@task
def get_rentals_scores(city_id: str, rentals_table: str, timestamp: str):
    """
    Gets rental scores / listing / day
    """
    rentfaster = RentFaster(city_id=city_id)

    sql = f"""
    SELECT DISTINCT
        ref_id, latitude, longitude
    FROM `{rentals_table}`
    WHERE dt = "{timestamp}"
    """
    scores = []
    bq_client = BigQueryHook(gcp_conn_id=GCP_CONNECT_ID).get_client(GCP_PROJECT)
    for r in bq_client.query(sql):
        r = dict(r)
        score = rentfaster.get_scores(latitude=r["latitude"], longitude=r["longitude"])
        score["ref_id"] = r["ref_id"]
        score["dt"] = timestamp
        scores.append(score)

    # Save into GCS
    gcs_client = GCSHook(gcp_conn_id=GCP_CONNECT_ID).get_conn()
    gcs_bucket = gcs_client.get_bucket(BUCKET)
    blob = gcs_bucket.blob(
        f"{BLOB_PREFIX}/{timestamp.replace('-', '/')}/{city_id.split('/')[-1]}_scores.json"
    )
    blob.upload_from_string(
        data="\n".join(json.dumps(score) for score in scores),
        content_type="application/json",
    )
    return f"gs://{BUCKET}/{blob.name}"


@task
def load_scores(city_id: str, gcs_uri: str, timestamp: str):
    """
    Loads scores into BQ
    """
    bq_client = BigQueryHook(gcp_conn_id=GCP_CONNECT_ID).get_client(GCP_PROJECT)

    destination_table = f"{GCP_PROJECT}.rentals.{city_id.split('/')[-1]}_scores"
    try:
        bq_client.get_table(destination_table)
    except NotFound:
        des_table = bigquery.Table(
            destination_table,
            schema=RENTFASTER_LOADJOBCONFIGS["rentals_scores"]["schema"],
        )
        des_table.clustering_fields = ["dt"]
        des_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="dt",
            require_partition_filter=True,
        )
        bq_client.create_table(des_table)
        logging.info(f"Successfully created {destination_table}!")

    job_config = bigquery.LoadJobConfig(**RENTFASTER_LOADJOBCONFIGS["rentals_scores"])
    bq_client.load_table_from_uri(
        source_uris=gcs_uri,
        destination=f"{destination_table}${timestamp}",
        job_config=job_config,
    )
    return destination_table


@dag(
    dag_id=DAG_ID,
    default_args=AIRFLOW_DEFAULT_ARGS,
    schedule_interval="0 9 * * 7",
    tags=["real_estate", "rent", "raw"],
)
def workflow():
    start = EmptyOperator(task_id="start")
    for city_id in [
        "ab/calgary",
        "bc/burnaby",
        "bc/richmond",
        "bc/vancouver",
    ]:
        features = task(task_id=f"get_features_{city_id.split('/')[-1]}")(get_features)(
            city_id=city_id
        )
        rentals_to_gcs = get_rentals(
            city_id=city_id, features=features, timestamp="{{ ds }}"
        )
        rentals_to_bq = load_rentals(
            city_id=city_id,
            gcs_uri=rentals_to_gcs,
            timestamp="{{ ds_nodash }}",
        )
        scores_to_gcs = get_rentals_scores(
            city_id=city_id,
            rentals_table=rentals_to_bq,
            timestamp="{{ ds }}",
        )
        load_scores(city_id=city_id, gcs_uri=scores_to_gcs, timestamp="{{ ds_nodash }}")
        start >> features


dag = workflow()
