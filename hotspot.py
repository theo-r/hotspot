"""Hotspot - Spotify listening analytics dashboard."""

from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

from config import (
    DEFAULT_LOOKBACK_DAYS,
    PAGE_ICON,
    PAGE_TITLE,
    TOP_ITEMS_LIMIT,
    USERS,
)
from components.cards import render_top_items
from components.charts import (
    create_genres_chart,
    create_listen_distribution_pie_chart,
    create_listens_per_day_chart,
    create_monthly_distribution_chart,
)
from lib.utils import (
    get_all_tracks,
    get_listens_per_day,
    get_top_albums,
    get_top_artists,
    get_top_genres,
    get_top_tracks,
    load_data,
)

st.set_page_config(
    page_title=PAGE_TITLE,
    page_icon=PAGE_ICON,
    layout="wide",
)


def render_date_selector() -> tuple[datetime, datetime]:
    """Render date range selector and return start/end datetimes."""
    with st.container(horizontal=True):
        date_columns = st.columns(2, gap="medium", width=500)
        with date_columns[0]:
            start_date = st.date_input(
                "Select start date:",
                value=datetime.now() - timedelta(days=DEFAULT_LOOKBACK_DAYS),
                min_value=datetime.now() - timedelta(days=DEFAULT_LOOKBACK_DAYS),
                max_value=datetime.now(),
            )
        with date_columns[1]:
            end_date = st.date_input(
                "Select end date:",
                value=datetime.now(),
                min_value=start_date + timedelta(days=1),
                max_value=datetime.now(),
            )

    start = datetime.combine(start_date, datetime.min.time())
    end = datetime.combine(end_date, datetime.max.time()) + timedelta(days=1)
    return start, end


def render_user_selector() -> list[str] | None:
    """Render user selector and return selected users, or None if invalid."""
    user_names = st.pills(
        label="Select users:",
        options=USERS,
        selection_mode="multi",
        default=USERS,
    )

    if len(user_names) < 1:
        st.error("Please select at least 1 user")
        return None
    return user_names


def render_metrics(
    num_tracks: int, distinct_artists: int, distinct_albums: int, duration_hrs: float
) -> None:
    """Render the top metrics row."""
    with st.container(horizontal=True, gap="large", border=True):
        cols = st.columns(4)
        with cols[0]:
            st.metric("Tracks", num_tracks, width="content")
        with cols[1]:
            st.metric("Artists", distinct_artists, width="content")
        with cols[2]:
            st.metric("Albums", distinct_albums, width="content")
        with cols[3]:
            st.metric("Play Time", f"{round(duration_hrs, 1)}hrs", width="content")


def compute_date_indices(
    start: datetime, end: datetime
) -> tuple[pd.DataFrame, list[str]]:
    """Compute date index and month labels for the date range."""
    num_days = (end - start).days
    dates_index = pd.DataFrame(
        {"dates": [(start + timedelta(days=x)).date() for x in range(num_days - 1)]}
    )
    months_frame = pd.DataFrame(
        {
            "months": [pd.to_datetime(start)]
            + pd.date_range(start=start, end=end, freq="MS", inclusive="both").to_list()
        }
    )
    month_years = months_frame["months"].dt.strftime("%b %y").to_list()
    return dates_index, month_years


def main() -> None:
    st.title(PAGE_TITLE)

    start, end = render_date_selector()
    user_names = render_user_selector()

    if user_names is None:
        return

    dates_index, month_years = compute_date_indices(start, end)

    df = load_data()

    # Compute data
    top_artists = get_top_artists(df=df, user_names=user_names, start=start, end=end)
    top_tracks = get_top_tracks(df=df, user_names=user_names, start=start, end=end)
    top_albums = get_top_albums(df=df, user_names=user_names, start=start, end=end)
    top_genres = get_top_genres(df=df, user_names=user_names, start=start, end=end)
    all_tracks = get_all_tracks(df=df, user_names=user_names, start=start, end=end)
    listens_per_day = get_listens_per_day(
        df=df, user_names=user_names, start=start, end=end, dates_index=dates_index
    )

    # Compute metrics
    distinct_artists = len(set(top_artists["artist"].to_list()))
    distinct_albums = len(set(top_albums["album_name"].to_list()))
    num_tracks = all_tracks.shape[0]
    duration_hrs = all_tracks["duration_ms"].sum() / 3600000

    render_metrics(num_tracks, distinct_artists, distinct_albums, duration_hrs)

    # Charts row 1
    cols = st.columns(2)
    with cols[0].container(border=True, height="stretch"):
        st.text("Listens per day")
        st.altair_chart(create_listens_per_day_chart(listens_per_day))

    with cols[1].container(border=True, height="stretch"):
        st.text("Genres")
        st.altair_chart(create_genres_chart(top_genres))

    # Distribution charts (only for multiple users)
    if len(user_names) > 1:
        cols = st.columns(2)
        with cols[0].container(border=True, height="stretch"):
            st.text("Listen Distribution")
            st.altair_chart(create_listen_distribution_pie_chart(listens_per_day))

        with cols[1].container(border=True, height="stretch"):
            st.text("Monthly Listen Distribution")
            st.altair_chart(
                create_monthly_distribution_chart(listens_per_day, month_years)
            )

    # Top items section
    with st.container(horizontal=True, gap="large"):
        cols = st.columns(3, border=True)

        with cols[0]:
            render_top_items(
                title="Top Tracks",
                items=top_tracks[:TOP_ITEMS_LIMIT].to_dict(orient="records"),
                image_key="album_image",
                primary_text_key="artist_name",
                secondary_text_key="name",
                count_key="count",
            )

        with cols[1]:
            render_top_items(
                title="Top Artists",
                items=top_artists[:TOP_ITEMS_LIMIT].to_dict(orient="records"),
                image_key="artist_image",
                primary_text_key="artist",
                secondary_text_key=None,
                count_key="plays",
            )

        with cols[2]:
            render_top_items(
                title="Top Albums",
                items=top_albums[:TOP_ITEMS_LIMIT].to_dict(orient="records"),
                image_key="album_image",
                primary_text_key="artist_name",
                secondary_text_key="album_name",
                count_key="count",
            )


if __name__ == "__main__":
    main()
