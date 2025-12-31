from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

@dag(
    'snowflake_view_refresh',
    start_date=datetime(2025, 1, 1),
    schedule=None, # Run after your hourly load
    template_searchpath='/opt/airflow/sql', # Tells Airflow where to find .sql files
    catchup=False,
    tags=["gharchive", "snowflake", "views"]
)
def create_snowflake_views():
    # Run these in parallel as they don't depend on each other
    t1 = SQLExecuteQueryOperator(task_id='dim_actors',         conn_id='snowflake_default', sql='dims/create_dim_actors.sql')
    t2 = SQLExecuteQueryOperator(task_id='dim_repos',          conn_id='snowflake_default', sql='dims/create_dim_repositories.sql')
    t3 = SQLExecuteQueryOperator(task_id='view_daily_trends',  conn_id='snowflake_default', sql='marts/create_view_daily_trends.sql')
    t4 = SQLExecuteQueryOperator(task_id='view_hourly_trends', conn_id='snowflake_default', sql='marts/create_view_hourly_trends.sql')
    t5 = SQLExecuteQueryOperator(task_id='view_pop',           conn_id='snowflake_default', sql='marts/create_view_repo_popularity.sql')

    # run in parallel
    [t1, t2, t3, t4, t5]
    
dag_create_snowflake_views = create_snowflake_views()