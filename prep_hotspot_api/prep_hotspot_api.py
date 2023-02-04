import logging
import awswrangler as wr
import os
from datetime import datetime, timedelta
import json
import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

boto3.setup_default_session(region_name="eu-west-1")


def lambda_handler(event, context):
    logger.info(f"Received event: {event}")
    try:
        glue_db = os.getenv("GLUE_DB", "")
        glue_table = os.getenv("GLUE_TABLE", "")
        bucket_name = os.getenv("BUCKET_NAME", "")
        past_week_path = os.getenv("PAST_WEEK_PATH", "")
        past_month_path = os.getenv("PAST_MONTH_PATH", "")
        past_year_path = os.getenv("PAST_YEAR_PATH", "")
        full_df_path = os.getenv("FULL_DF_PATH", "")
        workgroup = os.getenv("WORKGROUP", "")

        week_ago_datetime = datetime.now() + timedelta(days=-7)
        week_ago = datetime(
            week_ago_datetime.year, week_ago_datetime.month, week_ago_datetime.day
        )
        year_ago_datetime = datetime.now() + timedelta(days=-365)
        year_ago = datetime(
            year_ago_datetime.year, year_ago_datetime.month, year_ago_datetime.day
        )
        month_ago_datetime = datetime.now() + timedelta(days=-30)
        month_ago = datetime(
            month_ago_datetime.year, month_ago_datetime.month, month_ago_datetime.day
        )

        past_week_query = f"""
        SELECT name, artist_name, album_name, album_image, genres, duration_ms, played_at, date, hour, user_name FROM {glue_table}
        """

        df = wr.athena.read_sql_query(
            sql=past_week_query,
            database=glue_db,
            ctas_approach=False,
            s3_output=f"s3://{bucket_name}/athena-query-results/",
            workgroup=workgroup,
        )

        past_week = df[df["played_at"] > week_ago]
        past_month = df[df["played_at"] > month_ago]
        past_year = df[df["played_at"] > year_ago]

        past_week_result = wr.s3.to_json(
            past_week, f"s3://{bucket_name}/{past_week_path}"
        )
        past_month_result = wr.s3.to_json(
            past_month, f"s3://{bucket_name}/{past_month_path}"
        )
        past_year_result = wr.s3.to_json(
            past_year, f"s3://{bucket_name}/{past_year_path}"
        )
        full_df_result = wr.s3.to_json(df, f"s3://{bucket_name}/{full_df_path}")
        logger.info(f"Past week write result: {past_week_result}")
        logger.info(f"Past month write result: {past_month_result}")
        logger.info(f"Past year write result: {past_year_result}")
        logger.info(f"Full df write result: {full_df_result}")
        return "200"
    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}")
        raise (e)
