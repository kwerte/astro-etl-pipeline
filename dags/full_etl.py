from airflow.sdk import dag
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from pendulum import datetime
import pandas as pd
import os

# --- Configuration (Project Constants) ---
# Connection and Path Config
POSTGRES_CONNECTION_ID = "dbt_postgres_connection"
CSV_FILE_PATH = os.path.join(os.environ["AIRFLOW_HOME"], "data", "voter_data.csv")
PRODUCTION_TABLE_NAME = "raw_voter_data"

# dbt Config
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/dbt_project"
SCHEMA_NAME = "dbt_output" # Note: dbt best practice is to write final output to its own schema

# Define the profile configuration for Cosmos
profile_config = ProfileConfig(
    profile_name="my_dbt_profile", # Must match dbt_project.yml profile name
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=POSTGRES_CONNECTION_ID, # Use the defined connection ID
        profile_args={"schema": SCHEMA_NAME},
    ),
)
# --- End Configuration ---

@dag(
    dag_id="full_etl_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "dbt"]
)
def full_etl_pipeline():
    
    # 1. THE EXTRACT & LOAD TASK (Reads CSV, Replaces Raw Table)
    @task(task_id="load_raw_data_to_postgres")
    def load_csv_to_production():
        """Reads CSV, replaces the raw_voter_data table, and makes the data ready for dbt."""
        print(f"Starting CSV load from {CSV_FILE_PATH}...")
        
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)
        engine = hook.get_sqlalchemy_engine()
        
        # Read with header detection
        df = pd.read_csv(CSV_FILE_PATH)
        
        print(f"Read {len(df)} rows. Uploading to {PRODUCTION_TABLE_NAME}...")

        # Replaces the entire table, making the load idempotent.
        df.to_sql(
            name=PRODUCTION_TABLE_NAME,
            con=engine,
            if_exists="replace", 
            index=False,
        )
        
        print(f"Successfully loaded and replaced {PRODUCTION_TABLE_NAME}.")

    # 2. THE TRANSFORM TASK GROUP (Runs the entire dbt project)
    dbt_transform_tasks = DbtTaskGroup(
        group_id="dbt_transform_layer",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        operator_args={
            "install_deps": True, # Ensures dbt packages are ready
        },
    )

    # 3. SET THE DEPENDENCY (Chain the tasks)
    # The dbt tasks must wait for the load task to complete successfully.
    load_voter_data = load_csv_to_production()
    
    load_voter_data >> dbt_transform_tasks


# Instantiate the DAG
full_etl_pipeline()