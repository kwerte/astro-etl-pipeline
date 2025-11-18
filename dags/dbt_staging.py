from airflow.sdk import dag, chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping
from pendulum import datetime
import os

# Get the path to the dbt project
# This points to the folder that contains your dbt_project.yml
dbt_project_path = f"{os.environ['AIRFLOW_HOME']}/dags/dbt_project"

DB_NAME = 'main'
SCHEMA_NAME = 'public'
MODEL_TO_QUERY = 'stg_voter_data.sql'
# Define the profile configuration
# Cosmos will use this to generate a profiles.yml file
# and populate it with the details from our Airflow Connection
profile_config = ProfileConfig(
    profile_name="goodparty", # Must match dbt_project.yml
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="dbt_postgres_connection", # Must match Airflow Connection
        profile_args={"schema": "public"}, # dbt will write to this schema
    ),
)

@dag(
    dag_id="dag_to_run_dbt",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
)
def dbt_pipeline():
    
    transform_data = DbtTaskGroup(
        group_id="staging",
        project_config=ProjectConfig(
            dbt_project_path=dbt_project_path,
        ),
        profile_config=profile_config,
        default_args={"retries": 2}
    )

# This is what makes the DAG "run"
dbt_pipeline()