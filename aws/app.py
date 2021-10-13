#!/usr/bin/env python3
import os
from aws_cdk import core as cdk
from hotspot_stack import HotspotStack


app = cdk.App()
HotspotStack(app, "HotspotStack")
app.synth()
