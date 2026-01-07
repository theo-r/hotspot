"""Altair chart factory functions for Hotspot."""

import altair as alt
import pandas as pd

from config import ROLLING_WINDOW_DAYS


def create_listens_per_day_chart(data: pd.DataFrame) -> alt.Chart:
    """Create a line chart showing average listens per day with rolling window."""
    return (
        alt.Chart(data)
        .mark_line(size=1)
        .transform_window(
            avg_listens="mean(listens)",
            frame=[-ROLLING_WINDOW_DAYS, 0],
            groupby=["user"],
        )
        .encode(
            alt.X("date:T"),
            alt.Y("avg_listens:Q").title(
                f"avg listens in last {ROLLING_WINDOW_DAYS // 7} weeks"
            ),
            alt.Color("user:N"),
        )
    )


def create_genres_chart(data: pd.DataFrame) -> alt.Chart:
    """Create a bar chart showing top genres by listen count."""
    return (
        alt.Chart(data)
        .mark_bar()
        .encode(
            alt.X("genre:N").sort("-y"),
            alt.Y("count:Q").title("total listens"),
        )
    )


def create_listen_distribution_pie_chart(data: pd.DataFrame) -> alt.Chart:
    """Create a pie chart showing listen distribution by user."""
    return (
        alt.Chart(data)
        .mark_arc()
        .transform_aggregate(
            sum="sum(listens)",
            groupby=["user"],
        )
        .encode(
            alt.Theta("sum:Q"),
            alt.Color("user:N"),
        )
    )


def create_monthly_distribution_chart(
    data: pd.DataFrame, month_years: list[str]
) -> alt.Chart:
    """Create a stacked bar chart showing monthly listen distribution by user."""
    return (
        alt.Chart(data)
        .mark_bar()
        .transform_aggregate(
            sum="sum(listens)",
            groupby=["user", "month_year"],
        )
        .encode(
            alt.X("month_year:O", title="month", sort=month_years),
            alt.Y("sum:Q", title="listens").stack("normalize"),
            alt.Color("user:N"),
        )
        .configure_legend(orient="bottom")
    )
