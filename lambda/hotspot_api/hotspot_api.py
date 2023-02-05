import os
import requests
from requests import Response
import json

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.event_handler import APIGatewayRestResolver
from aws_lambda_powertools.utilities.typing import LambdaContext
import boto3

tracer = Tracer()
logger = Logger()
app = APIGatewayRestResolver()


@app.get("/past_month", compress=True)
@tracer.capture_method
def get_month():
    bucket_name = os.getenv("BUCKET_NAME", "")
    past_month_path = os.getenv("PAST_MONTH_PATH", "")
    s3 = boto3.client("s3")
    obj = json.loads(
        s3.get_object(Bucket=bucket_name, Key=past_month_path)["Body"].read()
    )
    return {"body": obj}


@app.get("/past_year", compress=True)
@tracer.capture_method
def get_year():
    bucket_name = os.getenv("BUCKET_NAME", "")
    past_year_path = os.getenv("PAST_YEAR_PATH", "")
    s3 = boto3.client("s3")
    obj = json.loads(
        s3.get_object(Bucket=bucket_name, Key=past_year_path)["Body"].read()
    )
    return {"body": obj}


@app.get("/todos", compress=True)
@tracer.capture_method
def get_todos():
    todos: Response = requests.get("https://jsonplaceholder.typicode.com/todos")
    todos.raise_for_status()

    # for brevity, we'll limit to the first 10 only
    return {"todos": todos.json()[:10]}


@app.get("/past_week", compress=True)
@tracer.capture_method
def get_week():
    bucket_name = os.getenv("BUCKET_NAME", "")
    past_week_path = os.getenv("PAST_WEEK_PATH", "")
    s3 = boto3.client("s3")
    obj = json.loads(
        s3.get_object(Bucket=bucket_name, Key=past_week_path)["Body"].read()
    )
    return {"body": obj}


@tracer.capture_lambda_handler
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    return app.resolve(event, context)
