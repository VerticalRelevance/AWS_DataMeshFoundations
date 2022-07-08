#!/bin/bash

# Run deployment
echo "Begin Central teardown.."
cdk destroy --profile central --force true
echo "Central teardown complete."