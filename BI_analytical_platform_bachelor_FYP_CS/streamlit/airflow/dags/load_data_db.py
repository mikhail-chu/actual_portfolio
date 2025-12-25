from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

DATA_DIR = "/opt/airflow/data/input"

TABLE_MAPPING = {
    "retail.csv": "retail_sales",
    "marketplace.csv": "marketplace_sales",
    "advertising.csv": "advertising_campaigns",
}

POSTGRES_CONN_ID = "postgres_analytics"


@dag(
    dag_id="load_csv_datasets_to_postgres",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "csv", "postgres", "lit"],
)
def load_csv_datasets_to_postgres():
    """
    Loads prepared CSV datasets from the local input directory
    into PostgreSQL analytical tables.
    """

    @task()
    def load_single_dataset(file_name: str, table_name: str):
        """
        Reads a CSV file from the input directory and loads it
        into the corresponding PostgreSQL table.
        """

        file_path = os.path.join(DATA_DIR, file_name)

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Input file not found: {file_path}")

        logger.info(f"Reading dataset from {file_path}")
        df = pd.read_csv(file_path)

        logger.info(f"Loaded {len(df)} rows from {file_name}")

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE {table_name};")
                conn.commit()

        logger.info(f"Table {table_name} truncated")

        pg_hook.insert_rows(
            table=table_name,
            rows=df.itertuples(index=False, name=None),
            target_fields=list(df.columns),
            commit_every=1000,
        )

        logger.info(f"Inserted data into {table_name}")

    for csv_file, table in TABLE_MAPPING.items():
        load_single_dataset(csv_file, table)


#dag = load_csv_datasets_to_postgres()