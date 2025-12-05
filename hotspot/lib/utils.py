import pandas as pd
import numpy as np
from datetime import datetime

cols = ["name", "artist_name", "album_name", "played_at", "user_name"]
users = ["Dan", "Fred", "George", "Theo"]


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
        .reset_index()
        .rename(columns={"index": "genre"})
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
            .rename(columns={0: "listens"})
            .astype({"listens": int})
            .assign(user=user)
            for user in users
        ]
        result = pd.concat(dfs, axis=0)
    else:
        result = (
            df[df["user_name"] == user_name][df["played_at"] > start][
                df["played_at"] <= end
            ]
            .groupby("date")
            .size()
            .to_frame()
            .rename(columns={0: "listens"})
            .astype({"listens": int})
        )
    final_df = (
        pd.merge(result, dates_index, how="outer", left_index=True, right_on="dates")
        .set_index("dates")
        .replace(np.nan, 0)
    )
    final_df["date"] = pd.to_datetime(final_df.index)
    return final_df


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
        ).reset_index()
    else:
        return (
            df[df["user_name"] == user_name][df["played_at"] > start][
                df["played_at"] <= end
            ]
            .hour.value_counts()
            .sort_index()
            / num_tracks
            * 100
        ).reset_index()


def get_latest_tracks(df: pd.DataFrame, user_name: str, start: datetime, end: datetime):
    if user_name == "All":
        return df.sort_values(by="played_at", ascending=False)[cols][
            df["played_at"] > start
        ][df["played_at"] <= end].reset_index(drop=True)
    else:
        return df.sort_values(by="played_at", ascending=False)[cols][
            df["user_name"] == user_name
        ][df["played_at"] > start][df["played_at"] <= end].reset_index(drop=True)
