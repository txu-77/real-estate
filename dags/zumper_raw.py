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

from schema.zumper_raw import ZUMPER_LOADJOBCONFIGS
from src.Zumper import Zumper

GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_CONNECT_ID = Variable.get("GCP_CONNECT_ID")
BUCKET = "txu-raw"
BLOB_PREFIX = "real_estate/zumper"

DAG_ID = Path(__file__).stem
AIRFLOW_DEFAULT_ARGS = {
    "owner": "txu",
    "depends_on_past": False,
    "start_date": pendulum.today("UTC").add(days=-1),
    "retries": 6,
    "retry_delay": timedelta(minutes=15),
}


def get_listables(city: str, timestamp: str) -> str:
    """
    Gets all listables land saves into GCS

    Args:
        timestamp (str): YYYY-MM-DD
    """
    zumper = Zumper(city=city)

    listables = zumper.get_listables()

    # Save into GCS
    gcs_client = GCSHook(gcp_conn_id=GCP_CONNECT_ID).get_conn()
    gcs_bucket = gcs_client.get_bucket(BUCKET)
    blob = gcs_bucket.blob(f"{BLOB_PREFIX}/{timestamp.replace('-', '/')}/{city}.json")
    blob.upload_from_string(
        data="\n".join(json.dumps(dict(l, **dict(dt=timestamp))) for l in listables),
        content_type="application/json",
    )
    return f"gs://{BUCKET}/{blob.name}"


@task
def load_listables(city: str, gcs_uri: str, timestamp: str) -> str:
    """
    Loads listings into BQ
    """
    bq_client = BigQueryHook(gcp_conn_id=GCP_CONNECT_ID).get_client(GCP_PROJECT)

    destination_table = f"{GCP_PROJECT}.zumper.{city.split('-')[0]}"
    try:
        bq_client.get_table(destination_table)
    except NotFound:
        des_table = bigquery.Table(
            destination_table,
            schema=ZUMPER_LOADJOBCONFIGS["listables"]["schema"],
        )
        des_table.clustering_fields = ["dt"]
        des_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="dt",
            require_partition_filter=True,
        )
        bq_client.create_table(des_table)
        logging.info(f"Successfully created {destination_table}!")

    job_config = bigquery.LoadJobConfig(**ZUMPER_LOADJOBCONFIGS["listables"])
    bq_client.load_table_from_uri(
        source_uris=gcs_uri,
        destination=f"{destination_table}${timestamp}",
        job_config=job_config,
    )
    return destination_table


@task
def get_listable_location_scores(
    city: str, listable_table: str, timestamp: str
) -> Dict:
    """
    Gets location scores for each listable
    """
    zumper = Zumper(city=city)
    bq_client = BigQueryHook(gcp_conn_id=GCP_CONNECT_ID).get_client(GCP_PROJECT)
    sql = f"""
    SELECT
        listing_id,
        group_id,
        lat,
        lng
    FROM `{listable_table}`
    WHERE dt = "{timestamp}";
    """
    scores = []
    results = bq_client.query(sql)
    total_rows = results.result().total_rows
    for idx, listing in enumerate(results, 1):
        logging.info(f"Collecting {idx} / {total_rows} listable location scores.")
        listing = dict(listing)
        score = zumper.get_location_scores(
            group_id=listing["group_id"],
            lat=listing["lat"],
            lng=listing["lng"],
        )
        if not score:
            continue
        score["listing_id"] = listing["listing_id"]
        score["dt"] = timestamp
        scores.append(score)

    # Save into GCS
    gcs_client = GCSHook(gcp_conn_id=GCP_CONNECT_ID).get_conn()
    gcs_bucket = gcs_client.get_bucket(BUCKET)
    blob = gcs_bucket.blob(
        f"{BLOB_PREFIX}/{timestamp.replace('-', '/')}/" f"{city}_location_scores.json"
    )
    blob.upload_from_string(
        data="\n".join(json.dumps(score) for score in scores),
        content_type="application/json",
    )
    return {
        "gcs_uri": f"gs://{BUCKET}/{blob.name}",
        "destination_table": f"{GCP_PROJECT}.zumper.{city.split('-')[0]}_location_scores",
    }


@task
def load_location_scores(configs: Dict[str, str], timestamp: str):
    """
    Ingests detailed reports into BQ
    """
    bq_client = BigQueryHook(gcp_conn_id=GCP_CONNECT_ID).get_client(GCP_PROJECT)
    destination_table = configs["destination_table"]
    try:
        bq_client.get_table(destination_table)
    except NotFound:
        des_table = bigquery.Table(
            destination_table,
            schema=ZUMPER_LOADJOBCONFIGS["location_scores"]["schema"],
        )
        des_table.clustering_fields = ["dt"]
        des_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="dt",
            require_partition_filter=True,
        )
        bq_client.create_table(des_table)
        logging.info(f"Successfully created {destination_table}!")

    job_config = bigquery.LoadJobConfig(**ZUMPER_LOADJOBCONFIGS["location_scores"])
    bq_client.load_table_from_uri(
        source_uris=configs["gcs_uri"],
        destination=f"{destination_table}${timestamp}",
        job_config=job_config,
    )


@dag(
    dag_id=DAG_ID,
    default_args=AIRFLOW_DEFAULT_ARGS,
    schedule_interval="0 10 * * 7",
    tags=["real_estate", "rent", "raw"],
)
def workflow():
    start = EmptyOperator(task_id="start")
    for city in [
        "calgary-ab",
        "burnaby-bc",
        "richmond-bc",
        "vancouver-bc",
    ]:
        listables_to_gcs = task(task_id=f"get_listables_{city.replace('-', '_')}")(
            get_listables
        )(
            city=city,
            timestamp="{{ ds }}",
        )
        listables_to_bq = load_listables(
            city=city,
            gcs_uri=listables_to_gcs,
            timestamp="{{ ds_nodash }}",
        )
        location_scores_to_gcs = get_listable_location_scores(
            city=city,
            listable_table=listables_to_bq,
            timestamp="{{ ds }}",
        )
        load_location_scores(
            configs=location_scores_to_gcs, timestamp="{{ ds_nodash }}"
        )
        start >> listables_to_gcs


dag = workflow()
