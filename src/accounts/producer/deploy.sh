#!/bin/bash


# Run deployment
echo "Begin Producer deployment.."
cdk bootstrap --profile producer
cdk deploy --profile producer
echo "Producer deployment complete."