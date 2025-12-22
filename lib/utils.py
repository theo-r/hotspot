import pandas as pd
import numpy as np
from datetime import datetime
import streamlit as st
import requests

cols = ["name", "artist_name", "album_name", "played_at", "user_name", "album_image"]
users = ["Dan", "Fred", "George", "Theo"]


@st.cache_data(ttl=3600, show_spinner=False)
def load_data():
    res = requests.get(
        "https://ddhry4h9th.execute-api.eu-west-1.amazonaws.com/prod/past_year"
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


def get_top_artists(df: pd.DataFrame, user_names: list, start: datetime, end: datetime):
    return (
        df["artist_name"][df["user_name"].isin(user_names)][df["played_at"] > start][
            df["played_at"] <= end
        ]
        .value_counts()
        .to_frame()
        .reset_index(drop=False)
        .rename(columns={"artist_name": "artist", "count": "plays"})
    )


def get_top_tracks(df: pd.DataFrame, user_names: list, start: datetime, end: datetime):
    return (
        df[["name", "artist_name", "album_image", "user_name"]][
            df["user_name"].isin(user_names)
        ][df["played_at"] > start][df["played_at"] <= end]
        .value_counts()
        .to_frame()
        .reset_index(drop=False)
    )


def get_top_albums(df: pd.DataFrame, user_names: list, start: datetime, end: datetime):
    return (
        df[["artist_name", "album_name", "album_image", "user_name"]][
            df["user_name"].isin(user_names)
        ][df["played_at"] > start][df["played_at"] <= end]
        .value_counts()
        .to_frame()
        .reset_index(drop=False)
    )


def get_top_genres(df: pd.DataFrame, user_names: list, start: datetime, end: datetime):
    genres_df = (
        df["genres"][df["user_name"].isin(user_names)][df["played_at"] > start][
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
        .reset_index()
        .rename(columns={"index": "genre"})
    )


def get_all_tracks(df: pd.DataFrame, user_names: list, start: datetime, end: datetime):
    return df[["name", "duration_ms"]][df["user_name"].isin(user_names)][
        df["played_at"] > start
    ][df["played_at"] <= end].reset_index(drop=False)


def get_listens_per_day(
    df: pd.DataFrame,
    user_names: list,
    start: datetime,
    end: datetime,
    dates_index: pd.DataFrame,
):
    final_dfs = []
    for user in user_names:
        transformed = (
            df[df["user_name"] == user][df["played_at"] > start][df["played_at"] <= end]
            .groupby("date")
            .size()
            .to_frame()
            .rename(columns={0: "listens"})
            .astype({"listens": int})
        )
        merged = (
            pd.merge(
                transformed,
                dates_index,
                how="right",
                left_index=True,
                right_on="dates",
            )
            .set_index("dates")
            .replace(np.nan, 0)
            .assign(user=user)
        )
        final_dfs.append(merged)

    final_df = pd.concat(final_dfs, axis=0)

    final_df["date"] = pd.to_datetime(final_df.index)
    final_df["month_year"] = final_df["date"].dt.strftime("%b %y")
    return final_df


def get_listens_by_hour_of_day(
    df: pd.DataFrame, user_names: list, start: datetime, end: datetime, num_tracks: int
):
    return (
        df[df["user_name"].isin(user_names)][df["played_at"] > start][
            df["played_at"] <= end
        ]
        .hour.value_counts()
        .sort_index()
        / num_tracks
        * 100
    ).reset_index()


def get_latest_tracks(
    df: pd.DataFrame, user_names: list, start: datetime, end: datetime
):
    return df.sort_values(by="played_at", ascending=False)[cols][
        df["user_name"].isin(user_names)
    ][df["played_at"] > start][df["played_at"] <= end].reset_index(drop=True)
