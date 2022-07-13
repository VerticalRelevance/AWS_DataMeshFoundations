#!/usr/bin/env python3
import os

import aws_cdk as cdk

from data_mesh_central_catalog.data_mesh_central_catalog_stack import CentralCatalogStack


app = cdk.App()
CentralCatalogStack(app, "CentralCatalogStack")

app.synth()
