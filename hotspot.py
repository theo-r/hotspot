import boto3
import pandas as pd
from datetime import datetime, timedelta
import streamlit as st

from lib.utils import *
import altair as alt

boto3.setup_default_session(region_name="eu-west-1")

st.set_page_config(
    page_title="Hotspot",
    page_icon="💿",
    layout="wide",
)

users = ["Dan", "Fred", "George", "Theo"]
user_colours = {
    "Dan": "red",
    "Fred": "green",
    "George": "blue",
    "Theo": "orange",
}


def main():
    st.title("Hotspot")

    with st.container(horizontal=True):
        date_columns = st.columns(2, gap="medium", width=500)
        with date_columns[0]:
            start_date = st.date_input(
                "Select start date:",
                value=datetime.now() + timedelta(days=-365),
                min_value=datetime.now() + timedelta(days=-365),
                max_value=datetime.now(),
            )
        with date_columns[1]:
            end_date = st.date_input(
                "Select end date:",
                value=datetime.now(),
                min_value=start_date + timedelta(days=1),
                max_value=datetime.now(),
            )

    user_names = st.pills(
        label="Select users:", options=users, selection_mode="multi", default=users
    )

    if len(user_names) < 1:
        st.error("Please select at least 1 user")
        return

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

    top_artists = get_top_artists(df=df, user_names=user_names, start=start, end=end)
    distinct_artists = len(set(top_artists["artist"].to_list()))
    top_tracks = get_top_tracks(df=df, user_names=user_names, start=start, end=end)
    top_albums = get_top_albums(df=df, user_names=user_names, start=start, end=end)
    top_genres = get_top_genres(df=df, user_names=user_names, start=start, end=end)
    distinct_albums = len(set(top_albums["album_name"].to_list()))
    all_tracks = get_all_tracks(df=df, user_names=user_names, start=start, end=end)
    num_tracks = all_tracks.shape[0]
    duration = all_tracks["duration_ms"].sum() / 3600000
    listens_per_day = get_listens_per_day(
        df=df, user_names=user_names, start=start, end=end, dates_index=dates_index
    )
    latest_tracks = get_latest_tracks(
        df=df, user_names=user_names, start=start, end=end
    )
    # listens_by_hour_of_day = get_listens_by_hour_of_day(
    # df=df, user_names=user_names, start=start, end=end, num_tracks=num_tracks
    # )

    with st.container(horizontal=True, gap="large", border=True):
        cols = st.columns(4)

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

        with cols[2]:
            st.metric(
                "Albums",
                distinct_albums,
                width="content",
            )

        with cols[3]:
            st.metric(
                "Play Time",
                f"{round(duration, 1)}hrs",
                width="content",
            )

    cols = st.columns(2)

    with cols[0].container(border=True, height="stretch"):
        st.text("Listens per day")
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

    with cols[1].container(border=True, height="stretch"):
        st.text("Genres")
        st.altair_chart(
            alt.Chart(top_genres)
            .mark_bar()
            .encode(
                alt.X("genre:N").sort("-y"), alt.Y("count:Q").title("total listens")
            )
        )

    if len(user_names) > 1:
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
            tt = top_tracks[:10].to_dict(orient="records")
            st.text("Top Tracks")
            for i, t in enumerate(tt):
                with st.container(horizontal=True, border=True):
                    inner_cols = st.columns(3, gap=None)
                    with inner_cols[0]:
                        st.image(t["album_image"], width=100)
                    with inner_cols[1]:
                        st.markdown(f"**{t["artist_name"]}**")
                        st.markdown(f"{t["name"]}")
                        st.badge(t["user_name"], color=user_colours[t["user_name"]])
                    with inner_cols[2]:
                        st.metric(label="plays", value=t["count"])

        with cols[2]:
            ta = top_albums[:10].to_dict(orient="records")
            st.text("Top Albums")
            for i, t in enumerate(ta):
                with st.container(horizontal=True, border=True):
                    inner_cols = st.columns(3, gap=None)
                    with inner_cols[0]:
                        st.image(t["album_image"], width=100)
                    with inner_cols[1]:
                        st.markdown(f"**{t["artist_name"]}**")
                        st.markdown(f"{t["album_name"]}")
                        st.badge(t["user_name"], color=user_colours[t["user_name"]])
                    with inner_cols[2]:
                        st.metric(label="plays", value=t["count"])

        with cols[1]:
            ta = top_artists[:10].to_dict(orient="records")
            st.text("Top Artists")
            for i, t in enumerate(ta):
                with st.container(
                    horizontal=True,
                    border=True,
                ):
                    inner_cols = st.columns(3, gap=None)
                    with inner_cols[0]:
                        st.image(t["artist_image"], width=100)
                    with inner_cols[1]:
                        st.markdown(f"**{t["artist"]}**")
                        st.badge(t["user_name"], color=user_colours[t["user_name"]])
                    with inner_cols[2]:
                        st.metric(label="plays", value=t["plays"])


if __name__ == "__main__":
    main()
