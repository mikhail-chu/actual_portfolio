import uuid
import pandas as pd

from typing import Any
from airflow.models.xcom import BaseXCom
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def serialize_pandas_dataframe(df: pd.DataFrame) -> str:
    hook = S3Hook("minio")
    key = f"{str(uuid.uuid4())}.pickle"
    filename = f"{key}.pickle"
    df.to_pickle(filename)

    hook.load_file(
        filename=filename, key=key, bucket_name=S3XComBackend.BUCKET_NAME, replace=True
    )

    return f"{S3XComBackend.PREFIX}://{S3XComBackend.BUCKET_NAME}/{key}"


def deserialize_pandas_dataframe(s3path: str) -> pd.DataFrame:
    hook = S3Hook("minio")
    key = s3path.replace(f"{S3XComBackend.PREFIX}://{S3XComBackend.BUCKET_NAME}/", "")
    filename = hook.download_file(
        key=key, bucket_name=S3XComBackend.BUCKET_NAME, local_path="/tmp"
    )

    return pd.read_pickle(filename)


class S3XComBackend(BaseXCom):
    PREFIX = "xcom_s3"
    BUCKET_NAME = "airflow-xcom-data"

    @staticmethod
    def serialize_value(value: Any):
        if isinstance(value, pd.DataFrame):
            value = serialize_pandas_dataframe(value)

        return BaseXCom.serialize_value(value)

    @staticmethod
    def deserialize_value(result) -> Any:
        result = BaseXCom.deserialize_value(result)

        if isinstance(result, str) and result.startswith(S3XComBackend.PREFIX):
            result = deserialize_pandas_dataframe(result)
        return result
