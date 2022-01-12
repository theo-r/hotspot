#!/usr/bin/env python3
import os
import aws_cdk as cdk
from hotspot_stack import HotspotStack

account = os.environ.get("AWS_ACCOUNT")
region = os.environ.get("AWS_DEFAULT_REGION")
app = cdk.App()
HotspotStack(app, "HotspotStack", env=cdk.Environment(account=account, region=region))
app.synth()
