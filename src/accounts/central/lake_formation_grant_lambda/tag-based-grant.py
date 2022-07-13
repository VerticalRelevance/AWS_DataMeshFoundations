import boto3


client = boto3.client('lakeformation')


def lambda_handler(event, context):
    grant = client.grant_permissions(
        Principal={
            'DataLakePrincipalIdentifier': '024571141324' #producer account
        },
        Resource={
            'LFTag': {
                'TagKey': 'source',
                'TagValues': [
                    'producer-a',
                ]
            }
        },
        Permissions=[
            'DESCRIBE'
        ],
        PermissionsWithGrantOption=[
            'DESCRIBE'
        ]
    )

