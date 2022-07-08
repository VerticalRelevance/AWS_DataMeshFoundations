#!/bin/bash

# Run deployment
echo "Begin Central deployment.."
cdk bootstrap --profile central
echo ""
echo "!!CONFIGURE LAKE FORMATION DATABASE CREATOR STEP!!"
read -p "Ensure the CDK bootstrap Cloudformation role is configured as a Lake Formation Database Creator and press Enter to continue."
cdk deploy --profile central --require-approval never --force true
echo "Central deployment complete."
