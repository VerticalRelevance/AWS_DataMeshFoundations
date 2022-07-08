#!/bin/bash

# Run teardown
echo "Begin Producer teardown.."
cdk destroy --profile producer --force true
echo "Producer teardown complete."