#!/usr/bin/env python3
import sys
import aws_cdk as cdk
from hotspot_stack import HotspotStack
from api_stack import ApiStack

account = sys.argv[1]
region = sys.argv[2]

app = cdk.App()
hotspot_stack = HotspotStack(
    app, "HotspotStack", env=cdk.Environment(account=account, region=region)
)
api_stack = ApiStack(
    app,
    "ApiStack",
    env=cdk.Environment(account=account, region=region),
    hotspot_stack=hotspot_stack,
)
app.synth()
