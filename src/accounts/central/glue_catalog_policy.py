#!/usr/bin/python3
import boto3
import json

session = boto3.Session(profile_name = 'central')
client = session.client('glue')

policy = {
  "Version" : "2012-10-17",
  "Statement" : [ {
    "Effect" : "Allow",
    "Principal" : {
      "AWS" : ["arn:aws:iam::262869217819:root", "arn:aws:iam::204319279320:root", "arn:aws:iam::024571141324:root", "arn:aws:iam::480025846069:user/central-catalog-cdk"]
    },
    "Action" : "glue:*",
    "Resource" : ["arn:aws:glue:us-east-1:480025846069:table/*", "arn:aws:glue:us-east-1:480025846069:database/*", "arn:aws:glue:us-east-1:480025846069:catalog"],
    "Condition" : {
      "Bool" : {
        "glue:EvaluatedByLakeFormationTags" : "true"
      }
    }
  } ]
}

policy_string = str(json.dumps(policy))

glue_resource_policy = client.put_resource_policy(PolicyInJson = policy_string)

