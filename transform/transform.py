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

PARTITION_COLS = ['user_name']

COLS = [
    'duration_ms',
    'explicit',
    'id',
    'name',
    'popularity',
    'album_name',
    'album.release_date',
    'album.release_date_precision',
    'album_image',
    'artist_name',
    'genres',
    'played_at',
    'name_binary',
    'artist_name_binary',
    'album_name_binary',
    'user_name'
]

def lambda_handler(event, context):
    logger.info(event)
    transform_manager = TransformManager()
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    user_name = key.split("/")[1]
    logger.info(f"{bucket}/{key}")
    rp_json = transform_manager.read_spotify_response(bucket=bucket, key=key)
    items_df = pd.DataFrame(rp_json['items'])
    items_df['user_name'] = user_name

    if 'artists' in rp_json:
        artists = rp_json['artists']
        items_df['genres'] = [';'.join(artist['genres']) for artist in artists]
    else:
        items_df['genres'] = ""

    track = transform_manager.prep_data(items_df)
    res = transform_manager.push_data(track)
    logger.info(json.dumps(res, indent=2))
    return "200"

class TransformManager:
    def __init__(self):
        self.glue_table_name = os.getenv("GLUE_TABLE_NAME")
        self.glue_db_name = os.getenv("GLUE_DB_NAME")
        self.s3 = boto3.client("s3")

    def read_spotify_response(self, bucket: str, key: str):
        res = self.s3.get_object(Bucket=bucket, Key=key)
        return json.loads(res['Body'].read())

    def push_data(self, df: pd.DataFrame):
        return wr.s3.to_parquet(
            df=df,
            mode="append",
            dataset=True,
            partition_cols=PARTITION_COLS,
            database=self.glue_db_name,
            table=self.glue_table_name
        )

    @staticmethod
    def prep_data(items_df):
        track = pd.json_normalize(items_df.track)
        # only take primary artists
        artist_name = [artist[0]['name'] for artist in track.artists]
        album_image = [images[1]['url'] for images in track['album.images']]
        track['artist_name'] = artist_name
        track['album_image'] = album_image
        track['album_name'] = track['album.name']
        track['genres'] = items_df.genres.copy()
        track['user_name'] = items_df.user_name.copy()
        track['played_at'] = pd.to_datetime(items_df.played_at).copy()
        track['name_binary'] = track['name'].str.encode('utf-8')
        track['artist_name_binary'] = track['artist_name'].str.encode('utf-8')
        track['album_name_binary'] = track['album_name'].str.encode('utf-8')
        track = track[COLS]
        track['year'] = track['played_at'].dt.year
        track['month'] = track['played_at'].dt.month
        track['day'] = track['played_at'].dt.day
        track['hour'] = track['played_at'].dt.hour
        track["date"] = track['played_at'].dt.date
        track["dayofweek"] = track['played_at'].dt.dayofweek
        return track
