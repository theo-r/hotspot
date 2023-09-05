import boto3
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import streamlit as st
import requests

boto3.setup_default_session(region_name="eu-west-1")

cols = ["name", "artist_name", "album_name", "played_at", "user_name"]
users = ["Dan", "Fred", "George", "Theo"]

st.set_page_config(layout="wide")


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


def get_top_artists(df: pd.DataFrame, user_name: str, start: datetime, end: datetime):
    if user_name == "All":
        return (
            df["artist_name"][df["played_at"] > start][df["played_at"] <= end]
            .value_counts()
            .to_frame()
            .reset_index(drop=False)
            .rename(columns={"artist_name": "artist", "count": "plays"})
        )
    else:
        return (
            df["artist_name"][df["user_name"] == user_name][df["played_at"] > start][
                df["played_at"] <= end
            ]
            .value_counts()
            .to_frame()
            .reset_index(drop=False)
            .rename(columns={"artist_name": "artist", "count": "plays"})
        )


def get_top_tracks(df: pd.DataFrame, user_name: str, start: datetime, end: datetime):
    if user_name == "All":
        return (
            df["name"][df["played_at"] > start][df["played_at"] <= end]
            .value_counts()
            .to_frame()
            .reset_index(drop=False)
            .rename(columns={"index": "track", "name": "plays"})
        )
    else:
        return (
            df["name"][df["user_name"] == user_name][df["played_at"] > start][
                df["played_at"] <= end
            ]
            .value_counts()
            .to_frame()
            .reset_index(drop=False)
            .rename(columns={"index": "track", "name": "plays"})
        )


def get_top_albums(df: pd.DataFrame, user_name: str, start: datetime, end: datetime):
    if user_name == "All":
        return (
            df["album_name"][df["played_at"] > start][df["played_at"] <= end]
            .value_counts()
            .to_frame()
            .reset_index(drop=False)
            .rename(columns={"album_name": "album", "count": "plays"})
        )
    else:
        return (
            df["album_name"][df["user_name"] == user_name][df["played_at"] > start][
                df["played_at"] <= end
            ]
            .value_counts()
            .to_frame()
            .reset_index(drop=False)
            .rename(columns={"album_name": "album", "count": "plays"})
        )


def get_top_genres(df: pd.DataFrame, user_name: str, start: datetime, end: datetime):
    if user_name == "All":
        genres_df = (
            df["genres"][df["played_at"] > start][df["played_at"] <= end]
            .replace("", np.nan)
            .dropna()
            .reset_index(drop=True)
        )
    else:
        genres_df = (
            df["genres"][df["user_name"] == user_name][df["played_at"] > start][
                df["played_at"] <= end
            ]
            .replace("", np.nan)
            .dropna()
            .reset_index(drop=True)
        )

    return (
        pd.Series([x for genres in genres_df for x in genres.split(";")])
        .value_counts()[:10]
        .to_frame()
        .rename(columns={"count": "genre"})
        .sort_values(by="genre")
    )


def get_top_album_images(
    df: pd.DataFrame, user_name: str, start: datetime, end: datetime
):
    if user_name == "All":
        return (
            df[["album_name", "album_image"]][df["played_at"] > start][
                df["played_at"] <= end
            ]
            .value_counts()
            .to_frame()
            .reset_index(drop=False)
        )
    else:
        return (
            df[["album_name", "album_image"]][df["user_name"] == user_name][
                df["played_at"] > start
            ][df["played_at"] <= end]
            .value_counts()
            .to_frame()
            .reset_index(drop=False)
        )


def get_all_tracks(df: pd.DataFrame, user_name: str, start: datetime, end: datetime):
    if user_name == "All":
        return df[["name", "duration_ms"]][df["played_at"] > start][
            df["played_at"] <= end
        ].reset_index(drop=False)
    else:
        return df[["name", "duration_ms"]][df["user_name"] == user_name][
            df["played_at"] > start
        ][df["played_at"] <= end].reset_index(drop=False)


def get_listens_per_day(
    df: pd.DataFrame,
    user_name: str,
    start: datetime,
    end: datetime,
    dates_index: pd.DataFrame,
):
    if user_name == "All":
        dfs = [
            df[df["user_name"] == user][df["played_at"] > start][df["played_at"] <= end]
            .groupby("date")
            .size()
            .replace(np.nan, 0)
            .to_frame()
            .rename(columns={0: user})
            for user in users
        ]
        result = pd.concat(dfs, axis=1)
    else:
        result = (
            df[df["user_name"] == user_name][df["played_at"] > start][
                df["played_at"] <= end
            ]
            .groupby("date")
            .size()
            .to_frame()
            .rename(columns={0: "listens"})
        )

    return (
        pd.merge(result, dates_index, how="outer", left_index=True, right_on="dates")
        .set_index("dates")
        .replace(np.nan, 0)
    )


def get_listens_by_hour_of_day(
    df: pd.DataFrame, user_name: str, start: datetime, end: datetime, num_tracks: int
):
    if user_name == "All":
        return (
            df[df["played_at"] > start][df["played_at"] <= end]
            .hour.value_counts()
            .sort_index()
            / num_tracks
            * 100
        )
    else:
        return (
            df[df["user_name"] == user_name][df["played_at"] > start][
                df["played_at"] <= end
            ]
            .hour.value_counts()
            .sort_index()
            / num_tracks
            * 100
        )


def get_latest_tracks(df: pd.DataFrame, user_name: str, start: datetime, end: datetime):
    if user_name == "All":
        return df.sort_values(by="played_at", ascending=False)[cols][
            df["played_at"] > start
        ][df["played_at"] <= end].reset_index(drop=True)
    else:
        return df.sort_values(by="played_at", ascending=False)[cols][
            df["user_name"] == user_name
        ][df["played_at"] > start][df["played_at"] <= end].reset_index(drop=True)


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

    st.title("Hotspot")

    column1, column2 = st.columns(2)
    column1.header(f"Number of tracks: {num_tracks}")
    column2.header(f"Number of unique artists: {distinct_artists}")
    column1.header(f"Number of unique albums: {distinct_albums}")
    column2.header(f"Hours of play time: {round(duration, 1)}")

    col1, col2, col3 = st.columns(3)
    col1.header("Top tracks")
    col1.dataframe(top_tracks)
    col2.header("Top artists")
    col2.dataframe(top_artists)
    col3.header("Top albums")
    col3.dataframe(top_albums)

    st.header("Listens per day")
    st.line_chart(listens_per_day, use_container_width=True)

    st.header("Listens by hour (%)")
    st.bar_chart(listens_by_hour_of_day, use_container_width=True)

    st.bar_chart(top_genres, use_container_width=True)

    st.header("Latest tracks")
    st.dataframe(latest_tracks)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.image(top10_albums[5])
    c2.image(top10_albums[6])
    c3.image(top10_albums[7])
    c4.image(top10_albums[8])
    c5.image(top10_albums[9])


if __name__ == "__main__":
    main()
