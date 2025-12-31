from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import duckdb
import os
import requests

# --- Configuration ---
AWS_BUCKET_RAW = "amzn-s3-gharchive-raw-rslob"
AWS_BUCKET_PROCESSED = "amzn-s3-gharchive-processed-rslob"
AWS_REGION = 'eu-north-1'
PREFIX = "gharchive"
GHARCHIVE_URL = "http://data.gharchive.org/{date}-{hour}.json.gz"

# --- Define Ingest and Tansform ---

def ingest_and_transform(date_str: str, hour: int) -> str:
    """Downloads a GHArchive file for a given date and hour.
    Uploads it raw and processed as parquet to S3."""
    
    # 1. Download from GHArchive to /tmp
    url = GHARCHIVE_URL.format(date=date_str, hour=hour)
    local_json_gz = f"/tmp/gharchive_{date_str}_{hour}.json.gz"
    
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_json_gz, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    
    # 2. Upload raw to S3
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_key_raw = f"{PREFIX}/{date_str}/{date_str}-{hour}.json.gz"
    
    # Upload Raw
    s3_hook.load_file(filename=local_json_gz, 
                      key=s3_key_raw, 
                      bucket_name=AWS_BUCKET_RAW, 
                      replace=True)
    print(f"Sucessfully uploaded {s3_key_raw} to {AWS_BUCKET_RAW}.")
    
    # 3. Convert to Parquet using DuckDB
    local_parquet = f"/tmp/gharchive_{date_str}_{hour}.parquet"
    con = duckdb.connect(database=':memory:')
    
    con.execute(f"""
        COPY (
            SELECT 
                id, 
                type,
                public, 
                created_at,
                actor,
                repo,
                payload
            FROM read_json_auto('{local_json_gz}', sample_size = -1)
        ) TO '{local_parquet}' (FORMAT 'PARQUET', COMPRESSION 'SNAPPY');
    """)
    con.close()
    
    # 4. Upload processed to S3
    s3_key_processed = f"{PREFIX}/{date_str}/{date_str}-{hour}.parquet"
    
    s3_hook.load_file(filename=local_parquet, 
                      key=s3_key_processed, 
                      bucket_name=AWS_BUCKET_PROCESSED, 
                      replace=True)

    print(f"Sucessfully uploaded {s3_key_processed} to {AWS_BUCKET_PROCESSED}.")
    
    # 5. Clean up /tmp
    os.remove(local_json_gz)
    os.remove(local_parquet)

# --- DAG ---

@dag(
    dag_id="gharchive_through_s3_into_snowflake",
    schedule="@hourly",
    start_date=datetime(2025, 12, 1),
    template_searchpath="/opt/airflow/sql",
    catchup=False,
    tags=["gharchive", "snowflake", "elt"],
    max_active_tasks=3,
    description="Converts raw GHArchive JSON.gz files to Parquet format"
)
def github_to_snowflake():
    
    # 1. Define the Sensor
    wait_for_data_to_appear_on_gharchive = TimeDeltaSensor(
        task_id="wait_for_gharchive_lag",
        delta=timedelta(hours=3),
        mode="reschedule" # This is important so it doesn't hog a worker
    )
            
    @task
    def ingest_and_transform_GHArchive_data(data_interval_start=None):
        
        date_str = data_interval_start.strftime("%Y-%m-%d")
        hour = data_interval_start.hour
        
        ingest_and_transform(date_str, hour)
    
    # DDL Tasks
    init_stg_table = SQLExecuteQueryOperator(
        task_id='init_snowflake_stg_table',
        conn_id='snowflake_default',
        sql='ddl/create_snowflake_stg_table.sql'
    )
    
    init_core_table = SQLExecuteQueryOperator(
        task_id='init_snowflake_core_table',
        conn_id='snowflake_default',
        sql='ddl/create_snowflake_core_table.sql'
    )
    
    # Move from S3 Parquet to STG
    load_to_stg = SQLExecuteQueryOperator(
        task_id='load_to_stg_snowflake',
        conn_id='snowflake_default',
        sql='staging/copy_into_stg.sql'
    )

    # Logic to move from STG to CORE (using MERGE)
    transform_stg_to_core = SQLExecuteQueryOperator(
        task_id='merge_to_core_snowflake',
        conn_id='snowflake_default',
        sql='transformations/stg_to_core.sql'
    )

    # 1. Instantiate ingest_and_transform_task
    ingest_data = ingest_and_transform_GHArchive_data()

    # 2. Define dependencies
    wait_for_data_to_appear_on_gharchive >> [ingest_data, init_stg_table, init_core_table]
    [ingest_data, init_stg_table, init_core_table] >> load_to_stg >> transform_stg_to_core
    
dag_github_to_snowflake = github_to_snowflake()