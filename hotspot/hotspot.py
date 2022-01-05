import logging
import boto3
import awswrangler as wr
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import streamlit as st
from typing import Tuple
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

boto3.setup_default_session(region_name="eu-west-1")

cols = [
    'name',
    'artist_name',
    'album_name',   
    'played_at'
]

users = ('Charley', 'Dan', 'Fred', 'George', 'Theo', 'All')

st.set_page_config(layout='wide')

bucket_name = os.environ.get("S3_BUCKET")
glue_db = os.environ.get("GLUE_DB")
glue_table = os.environ.get("GLUE_TABLE")
workgroup = os.environ.get("WORKGROUP")

@st.cache(persist=True, allow_output_mutation=True, ttl=3600)
def load_data(user_name: str, users: Tuple):
    if user_name == "All":
        df = wr.athena.read_sql_query(
            f"SELECT * FROM {glue_table}",
            database=glue_db,
            ctas_approach=True,
            s3_output=f"s3://{bucket_name}/athena-query-results/",
            workgroup=workgroup
        )
    else:
        df = wr.athena.read_sql_query(
            f"SELECT * FROM {glue_table} WHERE user_name = '{user_name}'",
            database=glue_db,
            ctas_approach=True,
            s3_output=f"s3://{bucket_name}/athena-query-results/",
            workgroup=workgroup
        )
    df = df.astype({'explicit': 'object'})
    return df

user_name = st.sidebar.selectbox(
    label='Select a user',
    options=users,
    index=4
)

df: pd.DataFrame = load_data(user_name=user_name, users=users)

start_date = st.sidebar.date_input(
    "Start date:",
    value=datetime.now() + timedelta(days=-7),
    min_value=datetime(2021, 1, 1),
    max_value=datetime.now()
)

end_date = st.sidebar.date_input(
    "End date:",
    value=datetime.now(),
    min_value=start_date + timedelta(days=1),
    max_value=datetime.now()
)

start = datetime(start_date.year, start_date.month, start_date.day)
end = datetime(end_date.year, end_date.month, end_date.day) + timedelta(days=1)

top_artists = (df['artist_name']
    [df['played_at'] > start]
    [df['played_at'] <= end]
    .value_counts()
    .to_frame()
    .reset_index(drop=False)
    .rename(columns={'index': 'artist', 'artist_name': 'plays'}))

distinct_artists = len(set(top_artists['artist'].to_list()))

top_tracks = (df['name']
    [df['played_at'] > start]
    [df['played_at'] <= end]
    .value_counts()
    .to_frame()
    .reset_index(drop=False)
    .rename(columns={'index': 'track', 'name': 'plays'}))

top_albums = (df['album_name']
    [df['played_at'] > start]
    [df['played_at'] <= end]
    .value_counts()
    .to_frame()
    .reset_index(drop=False)
    .rename(columns={'index': 'album', 'album_name': 'plays'}))

genres_df = (df['genres']
    [df['played_at'] > start]
    [df['played_at'] <= end]
    .replace("", np.nan)
    .dropna()
    .reset_index(drop=True))

top_genres = (pd.Series(
    [x for genres in genres_df for x in genres.split(';')])
    .value_counts()[:10]
    .to_frame()
    .rename(columns={0: 'genre'})
    .sort_values(by='genre'))

top_album_images = (df[['album_name', 'album_image']]
    [df['played_at'] > start]
    [df['played_at'] <= end]
    .value_counts()
    .to_frame()
    .reset_index(drop=False))

top10_albums = top_album_images[:10]['album_image'].to_list()
distinct_albums = len(set(top_albums['album'].to_list()))

if distinct_albums < 1:
    st.write("Something went wrong: number of distinct albums less than 1")

all_tracks = (df[['name', 'duration_ms']]
    [df['played_at'] > start]
    [df['played_at'] <= end]
    .reset_index(drop=False))

num_tracks = all_tracks.shape[0]
duration = all_tracks['duration_ms'].sum()/3600000

if user_name == "All":
    listens_per_day = (df[df['played_at'] > start]
                    [df['played_at'] <= end]
                    .groupby('date')
                    .size()
                    .to_frame()
                    .rename(columns={0: "listens"}))
    
else:
    listens_per_day = (df[df['played_at'] > start]
                        [df['played_at'] <= end]
                        .groupby('date')
                        .size()
                        .to_frame()
                        .rename(columns={0: "listens"}))

latest_tracks = (df.sort_values(by='played_at', ascending=False)[cols]
                   [df['played_at'] > start]
                   [df['played_at'] <= end]
                   .reset_index(drop=True))

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

st.bar_chart(top_genres, use_container_width=True)

st.header("Latest tracks")
st.dataframe(latest_tracks)

c1, c2, c3, c4, c5 = st.columns(5)
c1.image(top10_albums[5])
c2.image(top10_albums[6])
c3.image(top10_albums[7])
c4.image(top10_albums[8])
c5.image(top10_albums[9])