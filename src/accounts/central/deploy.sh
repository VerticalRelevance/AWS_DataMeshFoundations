#!/bin/bash

# Run deployment
echo "Begin Central deployment.."
cdk bootstrap --profile central --cloudformation-execution-policies arn:aws:iam::aws:policy/AdministratorAccess,arn:aws:iam::480025846069:policy/LFTagAdmin
#echo ""
#echo "!!CONFIGURE LAKE FORMATION DATABASE CREATOR STEP!!"
#read -p "Ensure the CDK bootstrap Cloudformation role is configured as a Lake Formation Database Creator and press Enter to continue."
cdk deploy --profile central --require-approval never --force true --cloudformation-execution-policies arn:aws:iam::aws:policy/AdministratorAccess,arn:aws:iam::480025846069:policy/LFTagAdmin
echo "Central deployment complete."
