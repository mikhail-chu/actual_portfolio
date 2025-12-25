from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import streamlit as st


@dataclass
class DateFilterResult:
    mode: str
    start_date: Optional[date]
    end_date: Optional[date]
    label: str


def render_date_filter(
    *,
    min_date: date,
    max_date: date,
    key: str = "date_filter",
) -> DateFilterResult:
    """
    Render a reusable date filter component.

    The component restricts all selections to [min_date; max_date]
    and supports:
      - full period
      - current month
      - last 7 / 30 / 90 days
      - custom period
    """
    st.subheader("Date filter")
    st.caption(f"Available period: {min_date} — {max_date}")

    mode = st.radio(
        "Selection mode",
        (
            "Full period",
            "Current month",
            "Last 7 days",
            "Last 30 days",
            "Last 90 days",
            "Custom period",
        ),
        key=f"{key}_mode",
        horizontal=True,
    )

    today = max_date
    start: Optional[date] = None
    end: Optional[date] = None
    label = ""

    if mode == "Full period":
        start = min_date
        end = max_date
        label = f"Full period: {start} — {end}"

    elif mode == "Current month":
        start = today.replace(day=1)
        end = today
        label = f"Current month: {start} — {end}"

    elif mode == "Last 7 days":
        start = max(min_date, today - timedelta(days=7))
        end = today
        label = f"Last 7 days: {start} — {end}"

    elif mode == "Last 30 days":
        start = max(min_date, today - timedelta(days=30))
        end = today
        label = f"Last 30 days: {start} — {end}"

    elif mode == "Last 90 days":
        start = max(min_date, today - timedelta(days=90))
        end = today
        label = f"Last 90 days: {start} — {end}"

    elif mode == "Custom period":
        start_default = max(min_date, today - timedelta(days=30))
        end_default = today

        start, end = st.date_input(
            "Select period",
            value=(start_default, end_default),
            min_value=min_date,
            max_value=max_date,
            key=f"{key}_range",
        )

        if isinstance(start, date) and isinstance(end, date):
            label = f"Custom period: {start} — {end}"
        else:
            start = end = None
            label = "No period selected"

    st.caption(label)

    return DateFilterResult(
        mode=mode,
        start_date=start,
        end_date=end,
        label=label,
    )