from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
import pandas as pd
import os
import tempfile
import logging

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)



BUCKET_NAME = "airflow-temp-fs"
S3_PREFIX = "x5"
MV_TABLE = "x5.mv_lit_sales_inventory_90d"


@dag(
    dag_id="load_lit_sales_inventory_to_minio",
    schedule_interval="0 3 * * *",  # каждый день в 03:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["lit", "x5", "minio", "etl"],
)
def load_lit_sales_inventory_to_minio():
    """
    DAG for daily extraction of X5 materialized view (LIT sales & inventory)
    from PostgreSQL and upload to MinIO as Parquet.
    """

    @task()
    def extract_from_postgres() -> str:
        """
        Потоково извлекает данные из материализованного представления
        батчами по 100k строк с использованием server-side cursor
        и пишет прямо в Parquet без промежуточных merges.
        """
        pg_hook = PostgresHook("analytics_new")
        engine = pg_hook.get_sqlalchemy_engine()

        tmp_dir = tempfile.gettempdir()
        file_name = f"lit_sales_inventory_{datetime.now():%Y%m%d}.parquet"
        local_path = os.path.join(tmp_dir, file_name)

        batch_size = 100_000
        total_rows = 0
        writer = None

        # создаём соединение с потоковой выдачей
        with engine.connect().execution_options(stream_results=True) as conn:
            query = f"SELECT * FROM {MV_TABLE};"

            result = conn.execution_options(yield_per=batch_size).execute(query)
            while True:
                rows = result.fetchmany(batch_size)
                if not rows:
                    break

                df = pd.DataFrame(rows, columns=result.keys())
                total_rows += len(df)
                logger.info(f"📦 Batch {total_rows // batch_size + 1}: {len(df)} rows")

                # пишем прямо в Parquet, не перечитывая файл
                table = pa.Table.from_pandas(df)
                if writer is None:
                    writer = pq.ParquetWriter(local_path, table.schema)
                writer.write_table(table)

            if writer:
                writer.close()

        logger.info(f"✅ Extracted total {total_rows} rows → {local_path}")
        return local_path

    @task()
    def upload_to_minio(local_path: str):
        """Upload the Parquet file to MinIO (S3-compatible)."""
        s3_hook = S3Hook(aws_conn_id="minio_conn")
        file_name = os.path.basename(local_path)

        s3_hook.load_file(
            filename=local_path,
            key=f"{S3_PREFIX}/{file_name}",
            bucket_name=BUCKET_NAME,
            replace=True,
        )

        logger.info(f"✅ Uploaded to MinIO: s3://{BUCKET_NAME}/{S3_PREFIX}/{file_name}")

    local_path = extract_from_postgres()
    upload_to_minio(local_path)


# dag = load_lit_sales_inventory_to_minio()