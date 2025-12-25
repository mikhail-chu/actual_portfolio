from __future__ import annotations

import io
import os
from typing import Dict, List

import boto3
import pandas as pd
import streamlit as st
from pandas.api.types import is_datetime64_any_dtype, is_categorical_dtype, is_object_dtype

from lib.date_filters import render_date_filter
from lib.minio_client import load_mart

# Logical mart names -> parquet keys
MARTS: Dict[str, str] = {
    "Sales overview": "sales_overview.parquet",
    "Sales by category & region": "sales_by_category_region.parquet",
    "Inventory vs demand": "inventory_vs_demand.parquet",
    "Campaign effectiveness": "campaign_effectiveness.parquet",
}

def _detect_date_column(df: pd.DataFrame) -> str | None:
    """
    Try to detect a date-like column in the mart.
    """
    # Prefer common names first
    preferred = ["date", "day", "dt", "month"]
    for name in preferred:
        if name in df.columns and is_datetime64_any_dtype(df[name]):
            return name

    # Fallback: first datetime64 column
    for col in df.columns:
        if is_datetime64_any_dtype(df[col]):
            return col

    return None


def _dimension_columns(df: pd.DataFrame, date_col: str | None) -> List[str]:
    """
    Pick columns for dropdown filters:
    - object / category columns
    - excluding the date column
    """
    dims: List[str] = []

    for col in df.columns:
        if col == date_col:
            continue

        s = df[col]
        if is_object_dtype(s) or is_categorical_dtype(s):
            dims.append(col)

    return dims


def render_aggregation_block(df: pd.DataFrame) -> pd.DataFrame:
    """
    Render aggregation controls and return aggregated DataFrame.
    If aggregation is disabled, returns original df.
    """

    # Identify columns by data type for grouping and aggregation
    numeric_cols = df.select_dtypes(include=["int64", "float64", "int32", "float32"]).columns.tolist()
    date_cols = df.select_dtypes(include=["datetime64[ns]"]).columns.tolist()
    categorical_cols = df.select_dtypes(include=["object", "string", "category"]).columns.tolist()

    grouping_columns = categorical_cols + date_cols
    value_columns = numeric_cols

    if not value_columns:
        st.info("No numeric fields available for aggregation.")
        return df

    enable_agg = st.checkbox("Enable aggregation", value=False)

    if not enable_agg:
        return df

    col1, col2, col3 = st.columns(3)

    with col1:
        group_by_cols = st.multiselect(
            "Group by columns",
            options=grouping_columns,
            default=[],
        )

    with col2:
        value_col = st.selectbox(
            "Value column",
            options=value_columns,
        )

    with col3:
        agg_func = st.selectbox(
            "Aggregation method",
            ["sum", "avg", "count", "min", "max"],
        )

    # Compute aggregation dynamically based on current UI selections
    df2 = df.copy()

    if group_by_cols:
        grp = df2.groupby(group_by_cols)[value_col]
        if agg_func == "avg":
            df2 = grp.mean().reset_index()
        elif agg_func == "count":
            df2 = grp.count().reset_index()
        elif agg_func == "sum":
            df2 = grp.sum().reset_index()
        elif agg_func == "min":
            df2 = grp.min().reset_index()
        elif agg_func == "max":
            df2 = grp.max().reset_index()
    else:
        # No grouping: return a single-row aggregate
        if agg_func == "avg":
            df2 = pd.DataFrame({value_col: [df[value_col].mean()]})
        elif agg_func == "count":
            df2 = pd.DataFrame({value_col: [df[value_col].count()]})
        elif agg_func == "sum":
            df2 = pd.DataFrame({value_col: [df[value_col].sum()]})
        elif agg_func == "min":
            df2 = pd.DataFrame({value_col: [df[value_col].min()]})
        elif agg_func == "max":
            df2 = pd.DataFrame({value_col: [df[value_col].max()]})

    st.caption(f"Aggregation: {agg_func} of '{value_col}'" + (f" by {group_by_cols}" if group_by_cols else ""))
    st.dataframe(df2.head(50), use_container_width=True)

    return df2

def _reset_report_state() -> None:
    """
    Clear all filter/aggregation state and any intermediate DataFrame.

    Note: st.session_state["mart_select"] is intentionally preserved to avoid triggering
    a rerun loop via the selectbox on_change handler.
    """
    for key in list(st.session_state.keys()):
        if (
            key.startswith("filter_")
            or key.startswith("date_filter_")
            or key.startswith("agg_")
            or key.startswith("cfg_")
        ):
            st.session_state.pop(key, None)

def configurator_page():
    st.title("Report configurator")
    st.markdown(
        "Configure a mart, apply filters, optional aggregations and download the result as an Excel file."
    )

    # Global reset
    if st.button("Reset all filters and aggregations", key="reset_all"):
        _reset_report_state()
        first_mart = list(MARTS.keys())[0]
        st.session_state["mart_select"] = first_mart
        st.rerun()

    st.divider()

    # Report selection with on_change reset
    mart_label = st.selectbox(
        "Select report",
        options=list(MARTS.keys()),
        index=(
            0
            if "mart_select" not in st.session_state
            else list(MARTS.keys()).index(st.session_state["mart_select"])
        ),
        key="mart_select",
        on_change=_reset_report_state,
    )
    mart_key = MARTS[mart_label]

    # Session state initialization
    st.session_state.setdefault("cfg_filters_applied", False)
    st.session_state.setdefault("cfg_filtered_df", None)

    # Load mart
    with st.spinner("Loading mart from MinIO..."):
        df = load_mart(mart_key)

    st.caption(f"Loaded {len(df):,} rows, {len(df.columns)} columns.")

    # Date filter
    date_col = _detect_date_column(df)
    date_filter_result = None

    if date_col is not None:
        min_date = df[date_col].min().date()
        max_date = df[date_col].max().date()

        date_filter_result = render_date_filter(
            min_date=min_date,
            max_date=max_date,
            key=f"date_filter_{mart_key}",
        )
    else:
        st.info("No date column detected in this mart.")

    st.divider()

    # Dimension filters
    st.subheader("Filters")

    dim_cols = _dimension_columns(df, date_col)
    if dim_cols:
        cols = st.columns(3)
        selected_values: Dict[str, List[str]] = {}

        for idx, col_name in enumerate(dim_cols):
            col = cols[idx % 3]
            with col:
                unique_values = (
                    df[col_name]
                    .dropna()
                    .astype(str)
                    .sort_values()
                    .unique()
                    .tolist()
                )

                sel = st.multiselect(
                    label=col_name,
                    options=unique_values,
                    default=[],
                    help="Leave empty to ignore this field.",
                    key=f"filter_{mart_key}_{col_name}",
                )

                selected_values[col_name] = sel
    else:
        selected_values = {}
        st.info("No categorical columns available for dropdown filters.")

    st.divider()

    # Apply filters
    apply_btn = st.button("Apply filters", key=f"cfg_apply_filters_{mart_key}")

    if apply_btn:
        with st.spinner("Applying filters..."):
            df_filtered = df.copy()

            # Apply date range filter (if available)
            if date_col is not None and date_filter_result is not None:
                start = date_filter_result.start_date
                end = date_filter_result.end_date
                if start and end:
                    mask = (
                        (df_filtered[date_col].dt.date >= start)
                        & (df_filtered[date_col].dt.date <= end)
                    )
                    df_filtered = df_filtered[mask]

            # Apply categorical filters
            for col_name, values in selected_values.items():
                if values:
                    df_filtered = df_filtered[df_filtered[col_name].astype(str).isin(values)]

        st.session_state.cfg_filters_applied = True
        st.session_state.cfg_filtered_df = df_filtered

    # Stop early until filters are applied at least once
    if not st.session_state.cfg_filters_applied or st.session_state.cfg_filtered_df is None:
        return

    df_filtered = st.session_state.cfg_filtered_df
    row_count = len(df_filtered)
    st.caption(f"After filters: {row_count:,} rows.")

    if row_count == 0:
        st.warning("No data after filtering.")
        return

    # Aggregation
    st.divider()
    df_final = render_aggregation_block(df_filtered)

    # Get data
    st.divider()
    get_data_btn = st.button("Get data", key=f"cfg_get_data_{mart_key}")

    if not get_data_btn:
        return

    # Prepare Excel output
    df_excel = df_final.copy()
    for col in df_excel.columns:
        if pd.api.types.is_datetime64_any_dtype(df_excel[col]):
            df_excel[col] = df_excel[col].dt.tz_localize(None)

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df_excel.to_excel(writer, sheet_name="Report", index=False)

    buffer.seek(0)

    # Download Excel
    st.download_button(
        label="Download Excel file",
        data=buffer,
        file_name=f"{mart_key.replace('.parquet', '')}_report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )