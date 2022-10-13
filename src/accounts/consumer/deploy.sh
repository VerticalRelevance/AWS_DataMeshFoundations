#!/bin/bash

# Run deployment
echo "Begin Consumer deployment.."
cdk bootstrap --profile consumer
cdk deploy --profile consumer
echo "Consumer deployment complete."
