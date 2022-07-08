#!/bin/bash

# Run deployment
echo "Begin Central deployment.."
cdk bootstrap --profile central
cdk deploy --profile central --require-approval never --force true
echo "Central deployment complete."
