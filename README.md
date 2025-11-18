This project is an etl pipeline which ingests a csv file /data/voter_data.csv. The pipeline stores raw data, cleans and standardizes staged data, and includes an aggregate layer with some sample calculated metrics.

PREQUISITES:
Astro CLI installed https://www.astronomer.io/docs/astro/cli/install-cli
Docker desktop

SETUP:
Clone the project
 
docker compose -f docker-compose.psql.yaml up -d

This will start the postgres container. Note that astro uses a psql instance for metadata on port 5432, so we use 5433 here (can be configured in docker-compose.psql.yaml)

astro dev start

This will start the containers used by astro

Open Airflow UI at http://localhost:8080 with default credentials (user: admin/ pass: admin)

Navigate to Admin -> connections

Add a connection with these values
connection ID: dbt_postgres_connection
host: host.docker.internal
port: 5433
database: main
login: postgres
password: postgres

You can now navigate to DAGS and see the two jobs:

load_csv_to_postgres will load and replace data from /data/voter_data.csv into public.main.raw_voter_data

dag_to_run_dbt will incrementally load data from raw_voter_data to stage and mart layers, performing validation checks and writing rows that fail validation to stg_voter_data_errors.

The data can be inspected from our postgres container through the docker desktop application, or through terminal with

docker exec -it dbt_postgres bash

psql -U postgres -d main

select * from table;

Stopping containers:
To stop the airflow containers, simply astro dev stop

To stop the postgres container, docker-compose -f docker-compose.psql.yaml down

