import os

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from web.operators.socrata.socrata_operator import SocrataToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    "email": [os.getenv("ALERT_EMAIL", "")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
DESTINATION_BUCKET = os.environ.get("GCP_GCS_BUCKET")
ENDPOINT = "https://data.sfgov.org/resource/5cei-gny5.json?"
ITEMS_PER_PAGE = 150000
TOTAL_ROWS = 401382
NUM_PAGES = TOTAL_ROWS // ITEMS_PER_PAGE + 1
OBJECT_NAME = "eviction_data"
DATASET="airflow_socrata_dataset"

columns = ["eviction_id","address","city","state","zip","file_date","non_payment","breach","nuisance","illegal_use","failure_to_sign_renewal","access_denial","unapproved_subtenant","owner_move_in","demolition","capital_improvement","substantial_rehab","ellis_act_withdrawal","condo_conversion","roommate_same_unit","other_cause","late_payments","lead_remediation","development","good_samaritan_ends","supervisor_district","neighborhood","client_location","shape"]

with DAG(
    dag_id="Assignment3-Socrata-To-GCS-To-BQ",
    description="Download data from Socrata API and upload to GCS bucket",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 2 * *",
    max_active_runs=1,
    catchup=True,
    tags=["Socrata-to-GCS-Bucket-To-BigQuery"],
) as dag:
    start = EmptyOperator(task_id="start")


    socrata_to_gcs = SocrataToGCSOperator(
        task_id=f"socrata_to_gcs",
        endpoint=ENDPOINT,
        items_per_page=ITEMS_PER_PAGE,
        destination_bucket=DESTINATION_BUCKET,
        num_pages=NUM_PAGES,
        destination_path=OBJECT_NAME,
    )
        
    gcs_to_bgquery =  GCSToBigQueryOperator(
        task_id = f"gcs_to_bgquery",
        bucket=f"{DESTINATION_BUCKET}",
        source_objects=[f"{OBJECT_NAME}.csv"],
        destination_project_dataset_table=f"{DATASET}.eviction_notices",
        autodetect=True, #DETECT SCHEMA : the columns and the type of data in each columns of the CSV file
        write_disposition="WRITE_APPEND", # command to update table from the  latest (or last row) row number upon every job run or task run
    )

    end = EmptyOperator(task_id="end")

    start >> socrata_to_gcs >> gcs_to_bgquery >> end