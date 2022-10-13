#!/bin/bash

# Run deployment
echo "Begin Producer deployment.."
cdk bootstrap --profile producer
cdk deploy --profile producer --require-approval never --force true
echo "Producer deployment complete."
