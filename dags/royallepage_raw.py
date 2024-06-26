import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from schema.royallepage_raw import ROYALLEPAGE_LOADJOBCONFIGS
from src.RoyalLePage import RoyalLePage

GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_CONNECT_ID = Variable.get("GCP_CONNECT_ID")
BUCKET = "txu-raw"
BLOB_PREFIX = "real_estate/royallepage"

DAG_ID = Path(__file__).stem
AIRFLOW_DEFAULT_ARGS = {
    "owner": "txu",
    "depends_on_past": False,
    "start_date": pendulum.today("UTC").add(days=-1),
    "retries": 6,
    "retry_delay": timedelta(minutes=15),
}


def get_listings(search_str: str, timestamp: str):
    """
    Gets overall listings per price_range and property_type, then
    saves into GCS

    Args:
        timestamp (str): YYYY-MM-DD
    """
    realtor = RoyalLePage(search_str)

    listings = []
    for price_ranges in realtor.price_ranges:
        for property_type in realtor.property_types.values():
            resp = realtor.get_listings(
                property_type=property_type,
                min_price=price_ranges["min"],
                max_price=price_ranges["max"],
            )

            # Add dt for partitioning
            for r in resp:
                r["dt"] = timestamp

            listings.extend(resp)

    logging.info(f"Collected {len(listings)} listings for {realtor.city}.")

    # Save into GCS
    gcs_client = GCSHook(gcp_conn_id=GCP_CONNECT_ID).get_conn()
    gcs_bucket = gcs_client.get_bucket(BUCKET)
    blob = gcs_bucket.blob(
        f"{BLOB_PREFIX}/{timestamp.replace('-', '/')}/{realtor.city.lower()}.json"
    )
    blob.upload_from_string(
        data="\n".join(json.dumps(l) for l in listings),
        content_type="application/json",
    )
    return f"gs://{BUCKET}/{blob.name}"


@task
def load_listings(gcs_uri: str, timestamp: str):
    """
    Loads listings into BQ
    """
    bq_client = BigQueryHook(gcp_conn_id=GCP_CONNECT_ID).get_client(GCP_PROJECT)

    city = gcs_uri.split("/")[-1].replace(".json", "")
    destination_table = f"{GCP_PROJECT}.royallepage.{city}_listings"
    try:
        bq_client.get_table(destination_table)
    except NotFound:
        des_table = bigquery.Table(
            destination_table,
            schema=ROYALLEPAGE_LOADJOBCONFIGS["listings"]["schema"],
        )
        des_table.clustering_fields = ["dt"]
        des_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="dt",
            require_partition_filter=True,
        )
        bq_client.create_table(des_table)
        logging.info(f"Successfully created {destination_table}!")

    job_config = bigquery.LoadJobConfig(**ROYALLEPAGE_LOADJOBCONFIGS["listings"])
    bq_client.load_table_from_uri(
        source_uris=gcs_uri,
        destination=f"{destination_table}${timestamp}",
        job_config=job_config,
    )

    # Remove duplicates (from: https://stackoverflow.com/questions/57900777/deduplicate-rows-in-a-bigquery-partition/57900778#57900778)
    timestamp_dash = datetime.strptime(timestamp, "%Y%m%d").strftime("%Y-%m-%d")
    remove_duplicates_sql = f"""
        MERGE `{destination_table}` t
        USING (
            SELECT a.*
            FROM (
                SELECT ANY_VALUE(a) a
                FROM `{destination_table}` a
                WHERE dt = "{timestamp_dash}"
                GROUP BY dt, detailsPath
            )
        )
        ON FALSE
        WHEN NOT MATCHED BY SOURCE AND dt = "{timestamp_dash}"
        # delete the duplicates
        THEN DELETE
        WHEN NOT MATCHED BY TARGET THEN INSERT ROW
        """
    bq_client.query(remove_duplicates_sql)

    return destination_table


@task
def get_lifestyle_scores(search_str: str, listings_table: str, timestamp: str):
    """
    Gets lifestyle scores per listing
    """
    realtor = RoyalLePage(search_str=search_str)
    bq_client = BigQueryHook(gcp_conn_id=GCP_CONNECT_ID).get_client(GCP_PROJECT)
    sql = f"""
        SELECT DISTINCT detailsPath, lat, lng
        FROM `{listings_table}`
        WHERE dt = "{timestamp}"
        """

    scores = []
    results = bq_client.query(sql)
    total_rows = results.result().total_rows
    for idx, listing in enumerate(results, 1):
        logging.info(f"Collecting {idx} / {total_rows} listing lifestyle scores.")
        listing = dict(listing)
        score = realtor.get_listing_lifestyle_scores(
            detailsPath=listing["detailsPath"],
            lat=listing["lat"],
            lng=listing["lng"],
        )
        if not score:
            continue
        score["detailsPath"] = listing["detailsPath"]
        score["dt"] = timestamp
        scores.append(score)

    # Save into GCS
    gcs_client = GCSHook(gcp_conn_id=GCP_CONNECT_ID).get_conn()
    gcs_bucket = gcs_client.get_bucket(BUCKET)
    blob = gcs_bucket.blob(
        f"{BLOB_PREFIX}/{timestamp.replace('-', '/')}/"
        f"{realtor.city.lower()}_lifestyle_scores.json"
    )
    blob.upload_from_string(
        data="\n".join(json.dumps(score) for score in scores),
        content_type="application/json",
    )
    return {
        "gcs_uri": f"gs://{BUCKET}/{blob.name}",
        "destination_table": f"{GCP_PROJECT}.royallepage.{realtor.city.lower()}_lifestyle_scores",
        "report_type": "lifestyle_scores",
    }


@task
def get_demographics(search_str: str, listings_table: str, timestamp: str):
    """
    Gets demographics per listing
    """
    realtor = RoyalLePage(search_str=search_str)
    bq_client = BigQueryHook(gcp_conn_id=GCP_CONNECT_ID).get_client(GCP_PROJECT)
    sql = f"""
        SELECT DISTINCT detailsPath
        FROM `{listings_table}`
        WHERE dt = "{timestamp}"
        """

    demographics = []
    results = bq_client.query(sql)
    total_rows = results.result().total_rows
    for idx, listing in enumerate(results, 1):
        logging.info(f"Collecting {idx} / {total_rows} listing demographics.")
        listing = dict(listing)
        demographic = realtor.get_listing_demographic_information(
            detailsPath=listing["detailsPath"],
        )
        if not demographic:
            continue
        demographic["detailsPath"] = listing["detailsPath"]
        demographic["dt"] = timestamp
        demographics.append(demographic)

    # Save into GCS
    gcs_client = GCSHook(gcp_conn_id=GCP_CONNECT_ID).get_conn()
    gcs_bucket = gcs_client.get_bucket(BUCKET)
    blob = gcs_bucket.blob(
        f"{BLOB_PREFIX}/{timestamp.replace('-', '/')}/"
        f"{realtor.city.lower()}_demographics.json"
    )
    blob.upload_from_string(
        data="\n".join(json.dumps(d) for d in demographics),
        content_type="application/json",
    )
    return {
        "gcs_uri": f"gs://{BUCKET}/{blob.name}",
        "destination_table": f"{GCP_PROJECT}.royallepage.{realtor.city.lower()}_demographics",
        "report_type": "demographics",
    }


@task
def get_listing_details(search_str: str, listings_table: str, timestamp: str):
    """
    Gets listing details / listing
    """
    realtor = RoyalLePage(search_str=search_str)
    bq_client = BigQueryHook(gcp_conn_id=GCP_CONNECT_ID).get_client(GCP_PROJECT)
    sql = f"""
        SELECT DISTINCT
            detailsPath,
            extraInfo.listingTypeKey_en
        FROM `{listings_table}`
        WHERE dt = "{timestamp}"
        """
    details = defaultdict(list)
    results = bq_client.query(sql)
    total_rows = results.result().total_rows
    for idx, row in enumerate(results, 1):
        logging.info(f"Collecting {idx} / {total_rows} listing details.")
        row = dict(row)
        detail = realtor.get_listing_details(row["detailsPath"])
        detail["listingTypeKey_en"] = row["listingTypeKey_en"]
        detail["detailsPath"] = row["detailsPath"]
        detail["dt"] = timestamp
        for k in detail.keys():
            if k.startswith("\n"):
                raise Exception(f"Check keys for listing @ {row['detailsPath']}")
        details[row["listingTypeKey_en"].lower().replace(" ", "_")].append(detail)

    # Save into GCS
    types = []
    for key, values in details.items():
        logging.info(f"Uploading {key} to GCS...")

        gcs_client = GCSHook(gcp_conn_id=GCP_CONNECT_ID).get_conn()
        gcs_bucket = gcs_client.get_bucket(BUCKET)
        blob = gcs_bucket.blob(
            f"{BLOB_PREFIX}/{timestamp.replace('-', '/')}/"
            f"{realtor.city.lower()}_{key}_details.json"
        )
        blob.upload_from_string(
            data="\n".join(json.dumps(v) for v in values),
            content_type="application/json",
        )
        types.append(key)

    return types


@task
def ingest_demographics_lifestyle_scores(configs: Dict[str, str], timestamp: str):
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
            schema=ROYALLEPAGE_LOADJOBCONFIGS[configs["report_type"]]["schema"],
        )
        des_table.clustering_fields = ["dt"]
        des_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="dt",
            require_partition_filter=True,
        )
        bq_client.create_table(des_table)
        logging.info(f"Successfully created {destination_table}!")

    job_config = bigquery.LoadJobConfig(
        **ROYALLEPAGE_LOADJOBCONFIGS[configs["report_type"]]
    )
    bq_client.load_table_from_uri(
        source_uris=configs["gcs_uri"],
        destination=f"{destination_table}${timestamp}",
        job_config=job_config,
    )


@task
def ingest_listing_details(search_str: str, type_: str, timestamp: str):
    """
    Ingests listing details / building type
    """
    bq_client = BigQueryHook(gcp_conn_id=GCP_CONNECT_ID).get_client(GCP_PROJECT)

    city = search_str.split(",")[0].lower()
    destination_table = f"{GCP_PROJECT}.royallepage.{city}_{type_}_details"
    try:
        bq_client.get_table(destination_table)
    except NotFound:
        des_table = bigquery.Table(
            destination_table,
            schema=ROYALLEPAGE_LOADJOBCONFIGS["listing_details"][type_]["schema"],
        )
        des_table.clustering_fields = ["dt"]
        des_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="dt",
            require_partition_filter=True,
        )
        bq_client.create_table(des_table)
        logging.info(f"Successfully created {destination_table}!")

    job_config = bigquery.LoadJobConfig(
        **ROYALLEPAGE_LOADJOBCONFIGS["listing_details"][type_]
    )
    bq_client.load_table_from_uri(
        source_uris=(
            f"gs://{BUCKET}/{BLOB_PREFIX}/{timestamp.replace('-', '/')}/{city}_{type_}_details.json"
        ),
        destination=f"{destination_table}${timestamp.replace('-', '')}",
        job_config=job_config,
    )


@task_group
def get_all_details(search_str: str, listings_table: str, timestamp: str):
    lifestyle = get_lifestyle_scores(search_str, listings_table, timestamp)
    demographics = get_demographics(search_str, listings_table, timestamp)
    details = get_listing_details(search_str, listings_table, timestamp)

    ingest_demographics_lifestyle_scores(lifestyle, timestamp="{{ ds_nodash }}")
    ingest_demographics_lifestyle_scores(demographics, timestamp="{{ ds_nodash }}")
    ingest_listing_details.partial(search_str=search_str, timestamp="{{ ds }}").expand(
        type_=details
    )


@dag(
    dag_id=DAG_ID,
    default_args=AIRFLOW_DEFAULT_ARGS,
    schedule_interval="0 11 * * 7",
    tags=["real_estate", "buy", "raw"],
)
def workflow():
    start = EmptyOperator(task_id="start")
    for search_str in [
        "Calgary, AB, CAN",
        "Vancouver, BC, CAN",
        "Burnaby, BC, CAN",
        "Richmond, BC, CAN",
    ]:
        listings_to_gcs = task(
            task_id=f"get_listings_{search_str.split(',')[0].lower()}"
        )(get_listings)(
            search_str=search_str,
            timestamp="{{ ds }}",
        )
        listings_to_bq = load_listings(
            gcs_uri=listings_to_gcs,
            timestamp="{{ ds_nodash }}",
        )
        get_all_details(
            search_str=search_str,
            listings_table=listings_to_bq,
            timestamp="{{ ds }}",
        )
        start >> listings_to_gcs


dag = workflow()
