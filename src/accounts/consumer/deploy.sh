#!/bin/bash

# Run deployment
echo "Begin Consumer deployment.."
cdk bootstrap --profile consumer
echo "Consumer deployment complete."