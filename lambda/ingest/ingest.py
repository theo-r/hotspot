import json
import logging
import os
import sys
from datetime import datetime
import boto3
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotipy.cache_handler import CacheHandler

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class MemoryCacheHandler(CacheHandler):
    """
    A cache handler that simply stores the token info in memory as an
    instance attribute of this class. The token info will be lost when this
    instance is freed.
    """

    def __init__(self, token_info=None):
        """
        Parameters:
            * token_info: The token info to store in memory. Can be None.
        """
        self.token_info = token_info

    def get_cached_token(self):
        return self.token_info

    def save_token_to_cache(self, token_info):
        self.token_info = token_info


class IngestManager:

    SCOPE = "user-read-recently-played"

    def __init__(self):
        self._s3 = boto3.client("s3")
        self._ddb = boto3.resource("dynamodb")
        self.watermark_table_name = os.getenv("WATERMARK_TABLE_NAME")
        self.cache_table_name = os.getenv("CACHE_TABLE_NAME")
        self.bucket_name = os.getenv("BUCKET_NAME")
        self.watermark_table = self._ddb.Table(self.watermark_table_name)
        self.cache_table = self._ddb.Table(self.cache_table_name)
        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(logging.INFO)

    def get_caches(self):
        return self.cache_table.scan()["Items"]

    def get_current_watermark_v2(self, id):
        response = self.watermark_table.get_item(Key={"id": id})
        return response["Item"]["watermark"]

    def update_watermark_v2(self, id, new_watermark: str):
        self._logger.info("Updating watermark")
        resp = self.watermark_table.update_item(
            Key={
                "id": id,
            },
            UpdateExpression="SET watermark = :val1",
            ExpressionAttributeValues={":val1": new_watermark},
        )
        self._logger.info(resp)

    def update_cache(self, id, token):
        self._logger.info(f"Updating cache for {id}")
        resp = self.cache_table.update_item(
            Key={
                "id": id,
            },
            UpdateExpression="SET access_token = :val1",
            ExpressionAttributeValues={":val1": token},
        )
        self._logger.info(resp)

    def upload_json(self, bucket, key, data):
        self._logger.info("Uploading json data")
        try:
            self._s3.put_object(
                Body=bytes(json.dumps(data).encode("UTF-8")),
                Bucket=bucket,
                Key=key,
                ContentType="application/json",
            )
        except Exception as e:
            self._logger.error(f"Error while uploading json: {e}")
            raise (Exception)


def lambda_handler(event, context):
    logger.info(f"Python version: {sys.version}")
    ingest_manager = IngestManager()
    # cached tokens for each user
    caches = ingest_manager.get_caches()

    for cache in caches:
        user_name = cache["id"]
        user_token_dict = cache["access_token"]
        watermark = ingest_manager.get_current_watermark_v2(id=user_name)
        cache_handler = MemoryCacheHandler(user_token_dict)
        auth_manager = SpotifyOAuth(
            cache_handler=cache_handler, scope=ingest_manager.SCOPE
        )
        sp = spotipy.Spotify(auth_manager=auth_manager)
        logger.info(f"API call for {user_name}")
        rp_json = sp.current_user_recently_played(after=watermark)

        if not rp_json["cursors"]:
            logger.info("No new tracks")
            continue

        artists = sp.artists(
            [item["track"]["artists"][0]["id"] for item in rp_json["items"]]
        )
        rp_json["artists"] = artists["artists"]

        new_watermark = rp_json["cursors"]["after"]
        new_tracks = len(rp_json["items"])
        logger.info(f"Found {new_tracks} new track(s)")
        fname = datetime.utcnow().strftime(f"landing/{user_name}/%Y/%m/%d/%H-%M.json")
        ingest_manager.upload_json(ingest_manager.bucket_name, fname, rp_json)
        new_token_info = auth_manager.get_cached_token()

        if watermark != new_watermark:
            ingest_manager.update_watermark_v2(
                id=user_name, new_watermark=new_watermark
            )

        if user_token_dict["access_token"] != new_token_info["access_token"]:
            ingest_manager.update_cache(id=user_name, token=new_token_info)

    return "200"
