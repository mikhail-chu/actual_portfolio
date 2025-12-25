# pages/plots_page.py

import os
import io
from typing import Optional

import boto3
import pandas as pd
import streamlit as st

from lib.minio_client import load_mart
from lib.date_filters import render_date_filter
from lib.plots_page_renders import (
    render_sales_overview_charts,
    render_category_region_charts,
    render_inventory_vs_demand_charts,
    render_campaign_effectiveness_charts,
)


SALES_OVERVIEW_KEY = "sales_overview.parquet"
CATEGORY_REGION_KEY = "sales_by_category_region.parquet"
INVENTORY_DEMAND_KEY = "inventory_vs_demand.parquet"
CAMPAIGNS_KEY = "campaign_effectiveness.parquet"


def _apply_date_filter(
    df: pd.DataFrame,
    start_date,
    end_date,
    date_col: str = "date",
) -> pd.DataFrame:
    """
    Filter a DataFrame by an inclusive date range using the specified datetime column.
    """
    if df is None or df.empty:
        return df
    if date_col not in df.columns or start_date is None or end_date is None:
        return df

    mask = (
        (df[date_col].dt.date >= start_date)
        & (df[date_col].dt.date <= end_date)
    )
    return df[mask]


def _compute_global_kpis(
    df_sales: pd.DataFrame,
    df_inventory: Optional[pd.DataFrame],
    start_date,
    end_date,
):
    """
    Compute top-level KPIs for the selected reporting period.

    Sales KPIs are computed over the selected period. Inventory KPIs are computed using
    the latest inventory snapshot on or before the period end date.
    """
    df_sales_period = _apply_date_filter(df_sales, start_date, end_date, "date")

    total_revenue = float(df_sales_period["revenue"].sum()) if "revenue" in df_sales_period.columns else 0.0
    total_units = float(df_sales_period["units_sold"].sum()) if "units_sold" in df_sales_period.columns else 0.0
    avg_price = total_revenue / total_units if total_units > 0 else 0.0

    total_inventory_units = None
    coverage_days = None
    itr = None
    snapshot_date = None

    if df_inventory is not None and not df_inventory.empty and "date" in df_inventory.columns:
        df_inv = df_inventory.copy()
        df_inv["date"] = pd.to_datetime(df_inv["date"])

        df_inv_period = df_inv[df_inv["date"].dt.date <= end_date]
        if not df_inv_period.empty and "inventory_level" in df_inv_period.columns:
            snapshot_date = df_inv_period["date"].max()
            df_inv_last = df_inv_period[df_inv_period["date"] == snapshot_date]

            total_inventory_units = float(df_inv_last["inventory_level"].sum())

            avg_daily_demand = None

            if "demand_forecast" in df_inv_period.columns:
                window_start = snapshot_date.date() - pd.Timedelta(days=30)
                df_window = df_inv_period[df_inv_period["date"].dt.date >= window_start]
                if not df_window.empty:
                    days_count = max(df_window["date"].dt.date.nunique(), 1)
                    total_forecast = float(df_window["demand_forecast"].sum())
                    avg_daily_demand = total_forecast / days_count if days_count > 0 else None

            if avg_daily_demand is None and total_units > 0 and start_date is not None and end_date is not None:
                days_in_period = max((end_date - start_date).days + 1, 1)
                avg_daily_demand = total_units / days_in_period

            if avg_daily_demand and avg_daily_demand > 0:
                coverage_days = total_inventory_units / avg_daily_demand

            if total_inventory_units and total_inventory_units > 0 and total_units > 0:
                itr = total_units / total_inventory_units

    return {
        "total_revenue": total_revenue,
        "total_units": total_units,
        "avg_price": avg_price,
        "total_inventory_units": total_inventory_units,
        "coverage_days": coverage_days,
        "itr": itr,
        "snapshot_date": snapshot_date,
    }


def plots_page():
    """
    Render the Sales & Inventory overview dashboard page.
    """
    st.title("Sales & Inventory Overview")

    # Load the primary sales mart (used to define the global date range)
    with st.spinner("Loading sales overview mart..."):
        df_sales = load_mart(SALES_OVERVIEW_KEY)

    if df_sales.empty or "date" not in df_sales.columns:
        st.error("Sales overview mart is empty or has no 'date' column.")
        return

    min_date = df_sales["date"].min().date()
    max_date = df_sales["date"].max().date()

    # Global date filter applied across all tabs
    date_filter = render_date_filter(
        min_date=min_date,
        max_date=max_date,
        key="plots_global",
    )

    start = date_filter.start_date
    end = date_filter.end_date

    if not (start and end):
        st.warning("Select a valid date period to display KPIs and charts.")
        return

    st.caption(f"Current period: {start} — {end}")

    # Load inventory mart (used for KPIs and the Inventory vs Demand tab)
    try:
        df_inventory_raw = load_mart(INVENTORY_DEMAND_KEY)
    except Exception:
        df_inventory_raw = None

    # Compute KPIs for the selected period
    kpis = _compute_global_kpis(df_sales, df_inventory_raw, start, end)

    total_revenue = kpis["total_revenue"]
    total_units = kpis["total_units"]
    avg_price = kpis["avg_price"]
    total_inventory_units = kpis["total_inventory_units"]
    coverage_days = kpis["coverage_days"]
    itr = kpis["itr"]
    snapshot_date = kpis["snapshot_date"]

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Revenue (period)", f"{total_revenue:,.0f}")
    col2.metric("Units sold (period)", f"{total_units:,.0f}")

    col3.metric(
        "Stock on hand (last snapshot, units)",
        "—" if total_inventory_units is None else f"{total_inventory_units:,.0f}",
    )

    col4.metric(
        "Inventory turnover (units / stock)",
        "—" if itr is None else f"{itr:.1f}x",
    )

    if snapshot_date is not None and coverage_days is not None:
        st.caption(
            f"Approximate coverage: ~{coverage_days:.1f} days of stock "
            f"based on demand up to {snapshot_date.date()}."
        )
    elif snapshot_date is not None:
        st.caption(f"Inventory snapshot date: {snapshot_date.date()}.")

    st.divider()

    # Apply the global date filter to marts used in tabs
    df_sales_filtered = _apply_date_filter(df_sales, start, end, "date")

    try:
        df_cat_region_raw = load_mart(CATEGORY_REGION_KEY)
    except Exception:
        df_cat_region_raw = None
    df_cat_region_filtered = (
        _apply_date_filter(df_cat_region_raw, start, end, "date")
        if df_cat_region_raw is not None
        else None
    )

    df_inventory_filtered = (
        _apply_date_filter(df_inventory_raw, start, end, "date")
        if df_inventory_raw is not None
        else None
    )

    try:
        df_campaigns_raw = load_mart(CAMPAIGNS_KEY)
    except Exception:
        df_campaigns_raw = None
    df_campaigns_filtered = (
        _apply_date_filter(df_campaigns_raw, start, end, "date")
        if df_campaigns_raw is not None
        else None
    )

    tab_sales, tab_cat, tab_inv, tab_campaigns = st.tabs(
        [
            "Sales overview",
            "Category & Region",
            "Inventory vs Demand",
            "Campaigns",
        ]
    )

    with tab_sales:
        st.subheader("Sales overview")
        render_sales_overview_charts(df_sales_filtered)

    with tab_cat:
        st.subheader("Category & Region")
        render_category_region_charts(df_cat_region_filtered)

    with tab_inv:
        st.subheader("Inventory vs Demand")
        render_inventory_vs_demand_charts(df_inventory_filtered)

    with tab_campaigns:
        st.subheader("Campaign effectiveness")
        render_campaign_effectiveness_charts(df_campaigns_filtered)