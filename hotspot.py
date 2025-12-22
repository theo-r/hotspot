import boto3
import pandas as pd
from datetime import datetime, timedelta
import streamlit as st
import requests

from lib.utils import *
import altair as alt

boto3.setup_default_session(region_name="eu-west-1")

users = ["Dan", "Fred", "George", "Theo"]
user_colours = {
    "Dan": "red",
    "Fred": "green",
    "George": "blue",
    "Theo": "orange",
}

st.set_page_config(
    page_title="Hotspot",
    page_icon="💿",
    layout="wide",
)


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


def main():
    user_name = st.sidebar.selectbox(
        label="Select a user",
        options=["All"] + users,
    )

    start_date = st.sidebar.date_input(
        "Start date:",
        value=datetime.now() + timedelta(days=-365),
        min_value=datetime.now() + timedelta(days=-365),
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
        {"dates": [(start + timedelta(days=x)).date() for x in range(num_days - 1)]}
    )
    months_frame = pd.DataFrame(
        {
            "months": [pd.to_datetime(start)]
            + pd.date_range(start=start, end=end, freq="MS", inclusive="both").to_list()
        }
    )
    month_years = months_frame["months"].dt.strftime("%b %y").to_list()

    df: pd.DataFrame = load_data()
    render_page(
        df=df,
        user_name=user_name,
        start=start,
        end=end,
        dates_index=dates_index,
        month_years=month_years,
    )


def render_page(
    df: pd.DataFrame,
    user_name: str,
    start: datetime,
    end: datetime,
    dates_index: pd.DataFrame,
    month_years: list[str],
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
    distinct_albums = len(set(top_albums["album_name"].to_list()))

    if distinct_albums < 1:
        st.write("Something went wrong: number of distinct albums less than 1")

    all_tracks = get_all_tracks(df=df, user_name=user_name, start=start, end=end)
    num_tracks = all_tracks.shape[0]
    duration = all_tracks["duration_ms"].sum() / 3600000
    listens_per_day = get_listens_per_day(
        df=df, user_name=user_name, start=start, end=end, dates_index=dates_index
    )
    # listens_by_hour_of_day = get_listens_by_hour_of_day(
    # df=df, user_name=user_name, start=start, end=end, num_tracks=num_tracks
    # )
    latest_tracks = get_latest_tracks(df=df, user_name=user_name, start=start, end=end)

    with st.container(horizontal=True, gap="large"):
        cols = st.columns(2, gap="medium", width=500)

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

        cols = st.columns(2, gap="medium", width=500)

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

    cols = st.columns(2)

    with cols[0].container(border=True, height="stretch"):
        st.text("Listens per day")
        if user_name == "All":
            st.altair_chart(
                alt.Chart(listens_per_day)
                .mark_line(size=1)
                .transform_window(
                    avg_listens="mean(listens)",
                    frame=[-14, 0],
                    groupby=["user"],
                )
                .encode(
                    alt.X("date:T"),
                    alt.Y("avg_listens:Q").title("avg listens in last 2 weeks"),
                    alt.Color("user:N"),
                )
            )
        else:
            st.altair_chart(
                alt.Chart(listens_per_day)
                .mark_line(size=1)
                .transform_window(
                    avg_listens="mean(listens)",
                    frame=[-14, 0],
                )
                .encode(
                    alt.X("date:T"),
                    alt.Y("avg_listens:Q").title("avg listens in last 2 weeks"),
                )
            )

    with cols[1].container(border=True, height="stretch"):
        st.text("Genres")
        st.altair_chart(
            alt.Chart(top_genres)
            .mark_bar()
            .encode(
                alt.X("genre:N").sort("-y"), alt.Y("count:Q").title("total listens")
            )
        )

    if user_name == "All":
        cols = st.columns(2)
        with cols[0].container(border=True, height="stretch"):
            st.text("Listen Distribution")
            st.altair_chart(
                alt.Chart(listens_per_day)
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

        with cols[1].container(border=True, height="stretch"):
            st.text("Monthly Listen Distribution")
            st.altair_chart(
                alt.Chart(listens_per_day)
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

    with st.container(horizontal=True, gap="large"):
        cols = st.columns(3, border=True)
        with cols[0]:
            tt = top_tracks[:5].to_dict(orient="records")
            st.text("Top Tracks")
            for i, t in enumerate(tt):
                inner_cols = st.columns(3, gap=None)
                with inner_cols[0]:
                    st.image(t["album_image"], width=100)
                with inner_cols[1]:
                    st.markdown(f"**{t["artist_name"]}**")
                    st.markdown(f"{t["name"]}")
                    if user_name == "All":
                        st.badge(t["user_name"], color=user_colours[t["user_name"]])
                with inner_cols[2]:
                    st.metric(label="plays", value=t["count"])

        with cols[1]:
            ta = top_albums[:5].to_dict(orient="records")
            st.text("Top Albums")
            for i, t in enumerate(ta):
                inner_cols = st.columns(3, gap=None)
                with inner_cols[0]:
                    st.image(t["album_image"], width=100)
                with inner_cols[1]:
                    st.markdown(f"**{t["artist_name"]}**")
                    st.markdown(f"{t["album_name"]}")
                    if user_name == "All":
                        st.badge(t["user_name"], color=user_colours[t["user_name"]])
                with inner_cols[2]:
                    st.metric(label="plays", value=t["count"])

        with cols[2]:
            lt = latest_tracks[:5].to_dict(orient="records")
            st.text("Latest Tracks")
            for i, t in enumerate(lt):
                inner_cols = st.columns(2, gap=None, width=300)
                with inner_cols[0]:
                    st.image(t["album_image"], width=100)
                with inner_cols[1]:
                    st.markdown(f"**{t["artist_name"]}**")
                    st.markdown(f"{t["name"]}")
                    if user_name == "All":
                        st.badge(t["user_name"], color=user_colours[t["user_name"]])


if __name__ == "__main__":
    main()
