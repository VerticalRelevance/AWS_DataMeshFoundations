#!/bin/bash

# Run teardown
echo "Begin Consumer teardown.."
cdk destroy --profile consumer --force true
echo "Consumer teardown complete."
