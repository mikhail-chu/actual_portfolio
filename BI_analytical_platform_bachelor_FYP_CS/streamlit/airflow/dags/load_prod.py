from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os
import logging

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_analytics"
SQL_TEMPLATES_BASE_PATH = "/opt/airflow/sql_templates"


@dag(
    dag_id="load_analytics_warehouse",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["analytics", "dimensions", "facts"],
)
def load_analytics_warehouse():
    """
    Loads dimension and fact tables into analytics schema.
    Currently includes only dimension loading for validation.
    """

    @task()
    def load_dimensions():
        """
        Loads all dimension tables from staging schema into analytics schema.
        """
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        dim_sql_files = [
            "dim_categories_load.sql",
            "dim_regions_load.sql",
            "dim_payment_methods_load.sql",
            "dim_products_load.sql",
            "dim_stores_load.sql",
            "dim_campaigns_load.sql",
        ]

        for filename in dim_sql_files:
            sql_path = os.path.join(SQL_TEMPLATES_BASE_PATH, filename)

            logger.info(f"Executing dimension load script: {sql_path}")

            with open(sql_path, "r") as file:
                sql_query = file.read()

            pg.run(sql_query)

            logger.info(f"Completed: {filename}")

    @task()
    def load_facts():
        """
        Loads all fact tables from staging into analytics schema.
        """
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        fact_sql_files = [
            "fact_marketplace_sales_load.sql",
            "fact_retail_sales_load.sql",
            "fact_advertising_campaigns_load.sql",
        ]

        for filename in fact_sql_files:
            sql_path = os.path.join(SQL_TEMPLATES_BASE_PATH, filename)
            logger.info(f"Executing FACT script: {sql_path}")

            with open(sql_path, "r") as file:
                sql_query = file.read()

            pg.run(sql_query)

            logger.info(f"Completed FACT load: {filename}")

    dims = load_dimensions()
    facts = load_facts()

    dims >> facts

dag = load_analytics_warehouse()