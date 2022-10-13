import boto3
session = boto3.session.Session(profile_name = "central")
print(session.client("sts").get_caller_identity())
