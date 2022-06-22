#!/usr/bin/env python3
import os

import aws_cdk as cdk

from data_mesh_producer.data_mesh_producer_stack import ProducerAccountStack


app = cdk.App()
ProducerAccountStack(app, "ProducerAccountStack")

app.synth()
