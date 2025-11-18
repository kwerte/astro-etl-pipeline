from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
import pandas as pd
import os

# --- CONFIGURATION ---
# This is the Airflow Connection ID you created in the UI
POSTGRES_CONNECTION_ID = "dbt_postgres_connection"

# Define the location of your CSV file.
# We are telling Airflow to look for a 'data' folder
# in its "home" directory, which Astro mounts from your project root.
CSV_FILE_PATH = os.path.join(os.environ["AIRFLOW_HOME"], "data", "voter_data.csv")

# Define what you want the new table in Postgres to be called
TABLE_NAME = "raw_voter_data"
# --- END CONFIGURATION ---


@dag(
    dag_id="load_csv_to_postgres",
    start_date=datetime(2025, 1, 1),
    schedule=None, # You can set this to a schedule (e.g., "@daily")
    catchup=False,
)
def load_csv_pipeline():
    """
    A pipeline to load a CSV file into a Postgres "raw" table.
    """

    @task
    def load_csv():
        """
        This task uses pandas to read a CSV and SQLAlchemy
        (via the PostgresHook) to upload the DataFrame to Postgres.
        """
        print(f"Starting CSV load from {CSV_FILE_PATH}...")
        
        # 1. Get a SQLAlchemy engine from the Airflow connection
        # This is the "Airflow-native" way to connect
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)
        engine = hook.get_sqlalchemy_engine()
        
        # 2. Read your CSV into a pandas DataFrame
        df = pd.read_csv(CSV_FILE_PATH)
        
        print(f"Read {len(df)} rows from CSV. Uploading to {TABLE_NAME}...")

        df.to_sql(
            name=TABLE_NAME,
            con=engine,
            if_exists="replace", # Use "append" for incremental loads
            index=False,       # Don't include the pandas index as a column
        )
        
        print(f"Successfully loaded data into {TABLE_NAME}.")

    # Run the task
    load_csv()

# This is what makes the DAG "run"
load_csv_pipeline()