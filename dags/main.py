from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum
from datetime import datetime, timedelta
from api.video_stats import (
    get_playlist_Id,
    get_video_ids,
    extract_video_data,
    save_to_json,
)
from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality

local_tz = pendulum.timezone("Europe/Belgrade")

default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "test@test.com",
    # "retries" : 1,
    # "retry_delay" : timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2026, 4, 4, tzinfo=local_tz),
    # "end_date" : datetime(2026,12,31, tzinfo=local_tz),
}

with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce json file with raw data from YT API",
    schedule="0 14 * * *",
    catchup=False,
) as dag_produce:
    # define tasks
    playlist_id = get_playlist_Id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)
    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db", trigger_dag_id="update_database"
    )

    # define dependencies between tasks
    playlist_id >> video_ids >> extract_data >> save_to_json_task >> trigger_update_db


with DAG(
    dag_id="update_database",
    default_args=default_args,
    description="DAG to insert data into staging and core table in Postgres from json file from YT API",
    schedule=None,
    catchup=False,
) as dag_update:
    # define tasks
    update_staging = staging_table()
    update_core = core_table()
    trigger_data_quality_check = TriggerDagRunOperator(
        task_id="trigger_data_quality_check", trigger_dag_id="run_data_quality"
    )

    # define dependencies between tasks
    update_staging >> update_core >> trigger_data_quality_check


with DAG(
    dag_id="run_data_quality",
    default_args=default_args,
    description="DAG to perform data quality tests using soda",
    schedule=None,
    catchup=False,
) as dag_check_quality:
    # define tasks
    soda_validate_staging = yt_elt_data_quality(schema="staging")
    soda_validate_core = yt_elt_data_quality(schema="core")

    # define dependencies between tasks
    soda_validate_staging >> soda_validate_core
