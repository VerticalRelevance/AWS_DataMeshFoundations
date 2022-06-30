#!/bin/bash

# Run deployment
echo "Begin Central deployment.."
cdk bootstrap --profile central
cdk deploy --profile central

echo "Central deployment complete."