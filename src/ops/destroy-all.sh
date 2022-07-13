#!/bin/bash


# Destroy Consumer Account
cd ../accounts/consumer/
. ./destroy.sh 
cd ../../ops/

# Destroy Central Account
cd ../accounts/central/
. ./destroy.sh
cd ../../ops/

# Destroy Producer Account
cd ../accounts/producer/
. ./destroy.sh
cd ../../ops/
