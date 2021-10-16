import logging
import json
import os
import boto3
import awswrangler as wr
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

boto3.setup_default_session(region_name="eu-west-1")
s3 = boto3.client('s3')

cols = [
    'duration_ms',
    'explicit',
    'id',
    'name',
    'popularity',
    'album.name',
    'album.release_date',
    'album.release_date_precision',
    'album.image',
    'artist.name',
    'played_at'
]

def lambda_handler(event, context):
    logger.info(event)
    bucket = event['Records'][0]['s3']['bucket']['name']
    glue_table_name = os.getenv("GLUE_TABLE_NAME")
    glue_db_name = os.getenv("GLUE_DB_NAME")
    key = event['Records'][0]['s3']['object']['key']
    logger.info(f"{bucket}/{key}")
    user_name = key.split("/")[1]
    spotipy_resp = s3.get_object(Bucket=bucket, Key=key)
    logger.info(spotipy_resp)
    rp_json = json.loads(spotipy_resp['Body'].read())['items']
    df = pd.DataFrame(rp_json)
    track = prep_data(df, user_name=user_name)
    partition_cols = ['user_name']
    res = push_data(track, partition_cols, glue_table_name, glue_db_name)
    logger.info(json.dumps(res, indent=2))
    return "200"


def push_data(df, partition_cols, table, database):
    return wr.s3.to_parquet(
        df=df,
        mode="append",
        dataset=True,
        partition_cols=partition_cols,
        database=database,
        table=table
    )


def prep_data(df, user_name: str):
    track = pd.json_normalize(df.track)
    played_at = df.played_at.copy()
    # only take primary artists
    artist_name = [artist[0]['name'] for artist in track.artists]
    album_image = [images[1]['url'] for images in track['album.images']]
    track['artist.name'] = artist_name
    track['album.image'] = album_image
    track['played_at'] = played_at
    track = track[cols]
    track['user_name'] = user_name
    track['played_at'] = pd.to_datetime(track.played_at).copy()
    track['year']= track['played_at'].dt.year
    track['month']= track['played_at'].dt.month
    track['day']= track['played_at'].dt.day
    track['hour']= track['played_at'].dt.hour
    track["date"] = track['played_at'].dt.date
    track["dayofweek"] = track['played_at'].dt.dayofweek
    return track
