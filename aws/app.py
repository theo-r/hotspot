#!/usr/bin/env python3
import aws_cdk as cdk
from hotspot_stack import HotspotStack


app = cdk.App()
HotspotStack(app, "HotspotStack")
app.synth()
