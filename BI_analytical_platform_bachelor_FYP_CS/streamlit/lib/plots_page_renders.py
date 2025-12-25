# lib/plots_page_renders.py

import pandas as pd
import streamlit as st
import plotly.express as px


def _ensure_datetime(df: pd.DataFrame, col: str = "date") -> pd.DataFrame:
    """
    Ensure that df[col] is a datetime64 dtype.
    """
    if col in df.columns and not pd.api.types.is_datetime64_any_dtype(df[col]):
        df = df.copy()
        df[col] = pd.to_datetime(df[col])
    return df


def render_sales_overview_charts(df: pd.DataFrame) -> None:
    """
    Render charts for the `sales_overview.parquet` mart.

    Expected columns:
        - date (datetime)
        - channel (str)
        - units_sold (numeric)
        - revenue (numeric)
    """
    if df.empty:
        st.warning("No data for selected period.")
        return

    required = {"date", "channel", "units_sold", "revenue"}
    missing = required - set(df.columns)
    if missing:
        st.warning(f"Sales overview mart is missing columns: {', '.join(missing)}")
        return

    df = _ensure_datetime(df, "date")

    # Channel filter within the tab scope
    channels = sorted(df["channel"].dropna().unique())
    selected_channels = st.multiselect(
        "Channel",
        options=channels,
        default=channels,
        help="Limit charts to selected sales channels.",
        key="sales_overview_channels",
    )

    if selected_channels:
        df = df[df["channel"].isin(selected_channels)]

    if df.empty:
        st.warning("No data after applying channel filter.")
        return

    # Time series: revenue and units sold
    daily = (
        df.groupby(["date", "channel"], as_index=False)
        .agg(
            revenue=("revenue", "sum"),
            units_sold=("units_sold", "sum"),
        )
        .sort_values("date")
    )

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Revenue over time")
        fig_rev = px.line(
            daily,
            x="date",
            y="revenue",
            color="channel",
            labels={"date": "Date", "revenue": "Revenue", "channel": "Channel"},
            template="plotly_white",
        )
        fig_rev.update_layout(legend_title_text="")
        st.plotly_chart(fig_rev, use_container_width=True)

    with col2:
        st.markdown("#### Units sold over time")
        fig_units = px.bar(
            daily,
            x="date",
            y="units_sold",
            color="channel",
            labels={"date": "Date", "units_sold": "Units sold", "channel": "Channel"},
            template="plotly_white",
        )
        fig_units.update_layout(legend_title_text="")
        st.plotly_chart(fig_units, use_container_width=True)

    # Revenue distribution by channel for the selected period
    st.markdown("#### Revenue split by channel (selected period)")

    by_channel = (
        df.groupby("channel", as_index=False)
        .agg(
            revenue=("revenue", "sum"),
            units_sold=("units_sold", "sum"),
        )
        .sort_values("revenue", ascending=False)
    )

    col1, col2 = st.columns(2)

    with col1:
        fig_bar = px.bar(
            by_channel,
            x="channel",
            y="revenue",
            labels={"channel": "Channel", "revenue": "Revenue"},
            template="plotly_white",
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    with col2:
        fig_pie = px.pie(
            by_channel,
            names="channel",
            values="revenue",
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    # Weekday pattern (average per day)
    st.markdown("#### Weekday pattern (average per day)")

    df_weekday = df.copy()
    df_weekday["weekday"] = df_weekday["date"].dt.day_name()

    weekday_order = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]

    by_weekday = (
        df_weekday.groupby("weekday", as_index=False)
        .agg(
            avg_revenue=("revenue", "mean"),
            avg_units=("units_sold", "mean"),
        )
    )

    by_weekday["weekday"] = pd.Categorical(
        by_weekday["weekday"], categories=weekday_order, ordered=True
    )
    by_weekday = by_weekday.sort_values("weekday")

    col1, col2 = st.columns(2)

    with col1:
        fig_w_rev = px.bar(
            by_weekday,
            x="weekday",
            y="avg_revenue",
            labels={"weekday": "Weekday", "avg_revenue": "Avg revenue per day"},
            template="plotly_white",
        )
        st.plotly_chart(fig_w_rev, use_container_width=True)

    with col2:
        fig_w_units = px.bar(
            by_weekday,
            x="weekday",
            y="avg_units",
            labels={"weekday": "Weekday", "avg_units": "Avg units per day"},
            template="plotly_white",
        )
        st.plotly_chart(fig_w_units, use_container_width=True)


def render_category_region_charts(df: pd.DataFrame) -> None:
    """
    Render charts for the `sales_by_category_region.parquet` mart.

    Expected columns:
        - date
        - category_name
        - region_name
        - units_sold
        - revenue
    """
    if df is None or df.empty:
        st.warning("No category/region data for selected period.")
        return

    required = {"date", "category_name", "region_name", "units_sold", "revenue"}
    missing = required - set(df.columns)
    if missing:
        st.warning(
            "Category & Region mart is missing columns: " + ", ".join(sorted(missing))
        )
        return

    df = _ensure_datetime(df, "date")

    # Filters
    cats = sorted(df["category_name"].dropna().unique())
    regs = sorted(df["region_name"].dropna().unique())

    col_filters = st.columns(2)
    with col_filters[0]:
        selected_cats = st.multiselect(
            "Category",
            options=cats,
            default=cats,
            key="catreg_categories",
        )
    with col_filters[1]:
        selected_regions = st.multiselect(
            "Region",
            options=regs,
            default=regs,
            key="catreg_regions",
        )

    if selected_cats:
        df = df[df["category_name"].isin(selected_cats)]
    if selected_regions:
        df = df[df["region_name"].isin(selected_regions)]

    if df.empty:
        st.warning("No data after applying category/region filters.")
        return

    # Revenue by category
    st.markdown("#### Revenue by category")

    by_cat = (
        df.groupby("category_name", as_index=False)
        .agg(
            revenue=("revenue", "sum"),
            units_sold=("units_sold", "sum"),
        )
        .sort_values("revenue", ascending=False)
    )

    fig_cat = px.bar(
        by_cat,
        x="category_name",
        y="revenue",
        labels={"category_name": "Category", "revenue": "Revenue"},
        template="plotly_white",
    )
    fig_cat.update_layout(xaxis_tickangle=-30)
    st.plotly_chart(fig_cat, use_container_width=True)

    # Revenue by region (stacked by category)
    st.markdown("#### Revenue by region (stacked by category)")

    by_region_cat = (
        df.groupby(["region_name", "category_name"], as_index=False)
        .agg(revenue=("revenue", "sum"))
    )

    fig_reg_cat = px.bar(
        by_region_cat,
        x="region_name",
        y="revenue",
        color="category_name",
        labels={
            "region_name": "Region",
            "revenue": "Revenue",
            "category_name": "Category",
        },
        template="plotly_white",
    )
    fig_reg_cat.update_layout(xaxis_tickangle=-30, legend_title_text="")
    st.plotly_chart(fig_reg_cat, use_container_width=True)

    # Heatmap: category × region revenue
    st.markdown("#### Revenue heatmap: Category × Region")

    heat = (
        df.groupby(["category_name", "region_name"], as_index=False)
        .agg(revenue=("revenue", "sum"))
    )

    pivot = heat.pivot(
        index="category_name",
        columns="region_name",
        values="revenue",
    )

    fig_heat = px.imshow(
        pivot,
        labels=dict(x="Region", y="Category", color="Revenue"),
        aspect="auto",
    )
    st.plotly_chart(fig_heat, use_container_width=True)


def render_inventory_vs_demand_charts(df: pd.DataFrame) -> None:
    """
    Render charts for the `inventory_vs_demand.parquet` mart.

    Expected columns (minimum):
        - date
        - inventory_level
        - demand_forecast (optional)
        - units_sold (optional)
    """
    if df is None or df.empty:
        st.warning("No inventory/demand data for selected period.")
        return

    df = _ensure_datetime(df, "date")

    if "inventory_level" not in df.columns:
        st.warning("Inventory mart has no 'inventory_level' column.")
        return

    # Optional breakdown dimensions (if present in the mart)
    dim_cols = [c for c in ["category", "region", "store_id"] if c in df.columns]

    if dim_cols:
        dim = st.selectbox(
            "Breakdown dimension (optional)",
            options=["(none)"] + dim_cols,
            index=0,
            key="inv_dim",
        )
    else:
        dim = "(none)"

    st.markdown("#### Inventory vs demand over time")

    group_cols = ["date"]
    if dim != "(none)":
        group_cols.append(dim)

    # Groupby aggregation specification for time-series rollups
    agg_dict = {"inventory_level": "sum"}
    if "demand_forecast" in df.columns:
        agg_dict["demand_forecast"] = "sum"
    if "units_sold" in df.columns:
        agg_dict["units_sold"] = "sum"

    daily = (
        df.groupby(group_cols, as_index=False)
        .agg(agg_dict)
        .sort_values("date")
    )

    if dim == "(none)":
        value_cols = [c for c in ["inventory_level", "demand_forecast", "units_sold"] if c in daily.columns]

        fig = px.line(
            daily,
            x="date",
            y=value_cols,
            labels={"value": "Value", "variable": "Metric", "date": "Date"},
            template="plotly_white",
        )
    else:
        fig = px.line(
            daily,
            x="date",
            y="inventory_level",
            color=dim,
            labels={"date": "Date", "inventory_level": "Inventory level", dim: dim},
            template="plotly_white",
        )

    st.plotly_chart(fig, use_container_width=True)

    # Scatter plot: inventory level vs sales volume (if available)
    if "units_sold" in daily.columns:
        st.markdown("#### Inventory vs sales (scatter)")

        fig_sc = px.scatter(
            daily,
            x="inventory_level",
            y="units_sold",
            color=dim if dim != "(none)" else None,
            labels={
                "inventory_level": "Inventory level",
                "units_sold": "Units sold",
            },
            template="plotly_white",
        )
        st.plotly_chart(fig_sc, use_container_width=True)


def render_campaign_effectiveness_charts(df: pd.DataFrame) -> None:
    """
    Render charts for the `campaign_effectiveness.parquet` mart.

    Expected structure (approximate):
        - campaign_id (int, optional)
        - campaign_name / campaign (str)
        - category_name (str, optional)
        - start_date, end_date (date/datetime)
        - duration_days (numeric)
        - total_budget (numeric)
        - revenue_during_campaign (numeric)
        - roi (numeric)
    """
    if df is None or df.empty:
        st.warning("No campaign data for selected period.")
        return

    # Resolve campaign name column
    name_col = None
    if "campaign_name" in df.columns:
        name_col = "campaign_name"
    elif "campaign" in df.columns:
        name_col = "campaign"

    if name_col is None:
        st.warning("Campaign mart must contain 'campaign_name' or 'campaign' column.")
        return

    # Campaign filter
    campaigns = sorted(df[name_col].dropna().unique())
    selected_campaigns = st.multiselect(
        "Campaigns",
        options=campaigns,
        default=campaigns[:10],
        key="campaigns_select",
    )

    if selected_campaigns:
        df = df[df[name_col].isin(selected_campaigns)]

    if df.empty:
        st.warning("No data after applying campaign filter.")
        return

    # Detect numeric metric columns, excluding identifier-like fields
    numeric_cols = df.select_dtypes(
        include=["int64", "float64", "Int64", "Float64"]
    ).columns.tolist()

    numeric_cols = [c for c in numeric_cols if not c.endswith("_id")]

    if not numeric_cols:
        st.info("No numeric metrics detected in campaign mart.")
        return

    # Prefer commonly used metrics when available
    default_metrics_order = [
        "revenue_during_campaign",
        "total_budget",
        "roi",
        "duration_days",
    ]
    default_metrics = [c for c in default_metrics_order if c in numeric_cols] or numeric_cols

    metrics_to_show = st.multiselect(
        "Metrics to display",
        options=numeric_cols,
        default=default_metrics,
        key="campaign_metrics",
        help="Select numeric metrics to summarise by campaign.",
    )

    if not metrics_to_show:
        st.warning("Select at least one metric to analyse.")
        return

    # Aggregate selected metrics by campaign
    agg_map = {m: "sum" for m in metrics_to_show}

    by_campaign = (
        df.groupby(name_col, as_index=False)
        .agg(agg_map)
    )

    # Recompute ROI where both revenue and budget are available
    if "revenue_during_campaign" in by_campaign.columns and "total_budget" in by_campaign.columns:
        by_campaign["roi_calc"] = (
            by_campaign["revenue_during_campaign"]
            / by_campaign["total_budget"].replace(0, pd.NA)
        )

    st.markdown("#### Campaign performance (summary)")
    st.dataframe(by_campaign, use_container_width=True)

    # Revenue vs budget (bar chart)
    if {"revenue_during_campaign", "total_budget"}.issubset(by_campaign.columns):
        st.markdown("#### Revenue vs budget by campaign")

        fig_rb = px.bar(
            by_campaign,
            x=name_col,
            y=["revenue_during_campaign", "total_budget"],
            barmode="group",
            labels={
                name_col: "Campaign",
                "value": "Amount",
                "variable": "Metric",
            },
            template="plotly_white",
        )
        fig_rb.update_layout(xaxis_tickangle=-30)
        st.plotly_chart(fig_rb, use_container_width=True)

    # Revenue vs budget (scatter)
    if {"revenue_during_campaign", "total_budget"}.issubset(by_campaign.columns):
        st.markdown("#### Revenue vs budget (scatter)")

        fig_sc = px.scatter(
            by_campaign,
            x="total_budget",
            y="revenue_during_campaign",
            text=name_col,
            labels={
                "total_budget": "Total budget",
                "revenue_during_campaign": "Revenue during campaign",
            },
            template="plotly_white",
        )
        fig_sc.update_traces(textposition="top center")
        st.plotly_chart(fig_sc, use_container_width=True)

    # ROI bar chart (calculated or source column)
    roi_col = None
    if "roi_calc" in by_campaign.columns:
        roi_col = "roi_calc"
    elif "roi" in by_campaign.columns:
        roi_col = "roi"

    if roi_col is not None:
        st.markdown("#### ROI by campaign")

        fig_roi = px.bar(
            by_campaign.sort_values(roi_col, ascending=False),
            x=name_col,
            y=roi_col,
            labels={name_col: "Campaign", roi_col: "ROI"},
            template="plotly_white",
        )
        fig_roi.update_layout(xaxis_tickangle=-30)
        st.plotly_chart(fig_roi, use_container_width=True)