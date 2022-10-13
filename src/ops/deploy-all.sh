#!/bin/bash

# Deploy Producer Account
cd ../accounts/producer/
. ./deploy.sh
cd ../../ops/

# Deploy Central Account
cd ../accounts/central/
. ./deploy.sh
cd ../../ops/

# Deploy Consumer Account
cd ../accounts/consumer/
. ./deploy.sh
cd ../../ops/ 
