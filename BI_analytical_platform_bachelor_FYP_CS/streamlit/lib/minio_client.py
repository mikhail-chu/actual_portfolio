from __future__ import annotations

import io
import os

import boto3
import pandas as pd
import streamlit as st

# MinIO / S3 config
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET_NAME = os.getenv("MINIO_BUCKET", "airflow-temp-fs")
MART_PREFIX = "marts"


@st.cache_resource
def get_minio_client():
    """
    Create and cache S3-compatible client for MinIO.
    """
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


@st.cache_data(ttl=3600)
def load_mart(mart_key: str) -> pd.DataFrame:
    """
    Load a mart parquet from MinIO into a pandas DataFrame.

    The result is cached for one hour.
    """
    client = get_minio_client()
    obj_key = f"{MART_PREFIX}/{mart_key}"

    obj = client.get_object(Bucket=BUCKET_NAME, Key=obj_key)
    data = obj["Body"].read()

    df = pd.read_parquet(io.BytesIO(data))

    # Normalize datetime-like columns (best-effort based on column name)
    for col in df.columns:
        if "date" in col.lower() or "month" in col.lower():
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass

    return df
