from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime
import pandas as pd
import os
import tempfile
import logging

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_analytics"
MINIO_CONN_ID = "minio_conn"

SQL_TEMPLATES_PATH = "/opt/airflow/sql_templates"
BUCKET_NAME = "airflow-temp-fs"
S3_PREFIX = "marts"


MARTS = {
    "sales_overview": "mart_sales_overview.sql",
    "sales_by_category_region": "mart_sales_by_category_region.sql",
    "inventory_vs_demand": "mart_inventory_vs_demand.sql",
    "campaign_effectiveness": "mart_campaign_effectiveness.sql",
}


@dag(
    dag_id="create_update_marts",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["marts", "postgres", "minio", "bi"],
)
def create_update_marts():
    """
    Extracts analytical marts from PostgreSQL and stores them in MinIO as Parquet files.
    """

    @task()
    def build_and_upload_mart(mart_name: str, sql_file: str):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()

        sql_path = os.path.join(SQL_TEMPLATES_PATH, sql_file)
        with open(sql_path) as f:
            query = f.read()

        logger.info(f"Executing mart query: {sql_file}")

        df = pd.read_sql(query, engine)
        logger.info(f"Mart {mart_name} extracted, rows: {len(df)}")

        tmp_dir = tempfile.gettempdir()
        file_name = f"{mart_name}.parquet"
        local_path = os.path.join(tmp_dir, file_name)

        table = pa.Table.from_pandas(df)
        pq.write_table(table, local_path)

        s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
        s3.load_file(
            filename=local_path,
            key=f"{S3_PREFIX}/{file_name}",
            bucket_name=BUCKET_NAME,
            replace=True,
        )

        logger.info(f"Mart {mart_name} uploaded to MinIO")

    for mart_name, sql_file in MARTS.items():
        build_and_upload_mart.override(task_id=f"build_{mart_name}")(
            mart_name=mart_name,
            sql_file=sql_file,
        )


dag = create_update_marts()