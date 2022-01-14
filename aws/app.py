#!/usr/bin/env python3
import sys
import aws_cdk as cdk
from hotspot_stack import HotspotStack

account = sys.argv[1]
region = sys.argv[2]

app = cdk.App()
HotspotStack(app, "HotspotStack", env=cdk.Environment(account=account, region=region))
app.synth()
