#!/bin/bash


# Deploy Producer Account
cd ../accounts/producer/
. ./deploy.sh

# Deploy Central Account
cd ../accounts/central/
. ./deploy.sh

# Deploy Consumer Account
cd ../accounts/consumer/
. ./deploy.sh 

cd ../../ops/