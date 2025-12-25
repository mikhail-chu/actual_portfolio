from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os
import logging

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_analytics"

BASE_INPUT_PATH = "/opt/airflow/data/input"
SQL_TEMPLATES_BASE_PATH = "/opt/airflow/sql_templates"


@dag(
    dag_id="load_staging_from_csv",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["staging", "postgres", "csv"],
)
def load_staging_from_csv():
    """
    Creates staging tables and loads CSV datasets into PostgreSQL staging schema.
    """

    @task()
    def create_staging_tables():
        """
        Creates staging tables using SQL DDL templates.
        """
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        sql_files = [
            "staging_retail.sql",
            "staging_marketplace.sql",
            "staging_advertising.sql",
        ]

        for filename in sql_files:
            sql_path = os.path.join(SQL_TEMPLATES_BASE_PATH, filename)

            with open(sql_path, "r") as f:
                ddl_sql = f.read()

            pg.run(ddl_sql)
            logger.info(f"Executed staging DDL: {filename}")

    def copy_csv_to_staging(table_name: str, csv_filename: str):
        """
        Loads CSV file into a staging table using client-side COPY (STDIN).
        """
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        csv_path = os.path.join(BASE_INPUT_PATH, csv_filename)

        copy_sql = f"""
        COPY staging.{table_name}
        FROM STDIN
        WITH (
            FORMAT csv,
            HEADER true
        );
        """

        conn = pg.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute(f"TRUNCATE TABLE staging.{table_name};")

            with open(csv_path, "r") as file:
                cursor.copy_expert(copy_sql, file)

            conn.commit()
            logger.info(f"Loaded {csv_filename} into staging.{table_name}")

        finally:
            cursor.close()
            conn.close()

    @task()
    def load_retail():
        copy_csv_to_staging("retail", "retail.csv")

    @task()
    def load_marketplace():
        copy_csv_to_staging("marketplace", "marketplace.csv")

    @task()
    def load_advertising():
        copy_csv_to_staging("advertising", "advertising.csv")

    staging_tables = create_staging_tables()

    staging_tables >> load_retail()
    staging_tables >> load_marketplace()
    staging_tables >> load_advertising()


dag = load_staging_from_csv()