from mock import patch
from transform.transform import lambda_handler
import json
import os
from unittest.mock import MagicMock


def read_spotify_response_helper(*args, **kwargs):
    with open("lambda/transform/test/data/res.json", "r") as f:
        res = json.load(f)
    return res


def _get_event():
    with open("lambda/transform/test/data/event.json", "r") as f:
        event = json.load(f)
    return event


@patch(
    "transform.transform.TransformManager.read_spotify_response",
    new=read_spotify_response_helper,
)
@patch(
    "transform.transform.TransformManager.push_data", new=MagicMock(return_value="200")
)
def test_lambda_handler():
    os.environ["GLUE_TABLE_NAME"] = ""
    os.environ["GLUE_DB_NAME"] = ""
    event = _get_event()
    res = lambda_handler(event, "")
    assert res == "200"
