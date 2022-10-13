import boto3


client = boto3.client('lakeformation')


def lambda_handler(event, context):
    """
    expected event schema
    {
        "tag_key" : "source",
        "tag_values" : ["producer-a"],
        "account_id" : 1234568910,
        "account_type" : "producer" OR "consumer"
    }
    """
    tagkey = event.get("tag_key")
    tagvalues = event.get("tag_values")
    if not isinstance(tagvalues, list):
        tagvalues = [tagvalues]
    account_id = event.get("account_to_grant")
    account_type = event.get("account_type")

    if account_type == "producer":
        permissions = ["DESCRIBE"]

    if account_type == "consumer:
        permissions = ["DESCRIBE", "READ"]

    grant = client.grant_permissions(
        Principal={
            'DataLakePrincipalIdentifier': account_id #producer account
        },
        Resource={
            'LFTag': {
                'TagKey':tagkey,
                'TagValues': tagvalues
            }
        },
        Permissions=[
            permissions
        ],
        PermissionsWithGrantOption=[
            permissions
        ]
    )

