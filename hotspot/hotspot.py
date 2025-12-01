import boto3
import pandas as pd
from datetime import datetime, timedelta
import streamlit as st
import requests

from lib.utils import *

boto3.setup_default_session(region_name="eu-west-1")

users = ["Dan", "Fred", "George", "Theo"]

st.set_page_config(
    page_title="Hotspot",
    page_icon="💿",
    layout="wide",
)


@st.cache_data(ttl=3600, show_spinner=False)
def load_data():
    res = requests.get(
        "https://ddhry4h9th.execute-api.eu-west-1.amazonaws.com/prod/past_month"
    )
    df = pd.DataFrame(res.json()["body"])
    df["played_at"] = [datetime.fromtimestamp(a / 1000) for a in df["played_at"]]
    df["date"] = df["played_at"].dt.date
    df["year"] = df["played_at"].dt.year
    df["month"] = df["played_at"].dt.month
    df["day"] = df["played_at"].dt.day
    df["hour"] = df["played_at"].dt.hour
    df["dayofweek"] = df["played_at"].dt.dayofweek
    return df


def main():
    user_name = st.sidebar.selectbox(
        label="Select a user",
        options=users + ["All"],
    )

    start_date = st.sidebar.date_input(
        "Start date:",
        value=datetime.now() + timedelta(days=-30),
        min_value=datetime.now() + timedelta(days=-30),
        max_value=datetime.now(),
    )

    end_date = st.sidebar.date_input(
        "End date:",
        value=datetime.now(),
        min_value=start_date + timedelta(days=1),
        max_value=datetime.now(),
    )

    start: datetime = datetime(start_date.year, start_date.month, start_date.day)
    end: datetime = datetime(end_date.year, end_date.month, end_date.day) + timedelta(
        days=1
    )

    num_days = (end - start).days
    dates_index = pd.DataFrame(
        {"dates": [(start + timedelta(days=x)).date() for x in range(num_days)]}
    )

    df: pd.DataFrame = load_data()
    render_page(
        df=df, user_name=user_name, start=start, end=end, dates_index=dates_index
    )


def render_page(
    df: pd.DataFrame,
    user_name: str,
    start: datetime,
    end: datetime,
    dates_index: pd.DataFrame,
):
    top_artists = get_top_artists(df=df, user_name=user_name, start=start, end=end)
    distinct_artists = len(set(top_artists["artist"].to_list()))
    top_tracks = get_top_tracks(df=df, user_name=user_name, start=start, end=end)
    top_albums = get_top_albums(df=df, user_name=user_name, start=start, end=end)
    top_genres = get_top_genres(df=df, user_name=user_name, start=start, end=end)
    top_album_images = get_top_album_images(
        df=df, user_name=user_name, start=start, end=end
    )
    top10_albums = top_album_images[:10]["album_image"].to_list()
    distinct_albums = len(set(top_albums["album"].to_list()))

    if distinct_albums < 1:
        st.write("Something went wrong: number of distinct albums less than 1")

    all_tracks = get_all_tracks(df=df, user_name=user_name, start=start, end=end)
    num_tracks = all_tracks.shape[0]
    duration = all_tracks["duration_ms"].sum() / 3600000
    listens_per_day = get_listens_per_day(
        df=df, user_name=user_name, start=start, end=end, dates_index=dates_index
    )
    listens_by_hour_of_day = get_listens_by_hour_of_day(
        df=df, user_name=user_name, start=start, end=end, num_tracks=num_tracks
    )
    latest_tracks = get_latest_tracks(df=df, user_name=user_name, start=start, end=end)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.image(top10_albums[0])
    c2.image(top10_albums[1])
    c3.image(top10_albums[2])
    c4.image(top10_albums[3])
    c5.image(top10_albums[4])

    with st.container(horizontal=True, gap="large"):
        cols = st.columns(2, gap="medium", width=300)

        with cols[0]:
            st.metric(
                "Tracks",
                num_tracks,
                width="content",
            )

        with cols[1]:
            st.metric(
                "Artists",
                distinct_artists,
                width="content",
            )

        cols = st.columns(2, gap="medium", width=300)

        with cols[0]:
            st.metric(
                "Albums",
                distinct_albums,
                width="content",
            )

        with cols[1]:
            st.metric(
                "Play Time",
                f"{round(duration, 1)}hrs",
                width="content",
            )

    col1, col2, col3 = st.columns(3)
    col1.header("Top tracks")
    col1.dataframe(top_tracks)
    col2.header("Top artists")
    col2.dataframe(top_artists)
    col3.header("Top albums")
    col3.dataframe(top_albums)

    cols = st.columns(2)

    with cols[0].container(border=True, height="stretch"):
        st.text("Listens per day")
        st.line_chart(listens_per_day)

    with cols[1].container(border=True, height="stretch"):
        st.text("Listens per hour (%)")
        st.bar_chart(listens_by_hour_of_day)

    cols = st.columns(2)
    with cols[0].container(border=True, height="stretch"):
        st.text("Genres")
        st.bar_chart(top_genres, use_container_width=True)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.image(top10_albums[5])
    c2.image(top10_albums[6])
    c3.image(top10_albums[7])
    c4.image(top10_albums[8])
    c5.image(top10_albums[9])


if __name__ == "__main__":
    main()
