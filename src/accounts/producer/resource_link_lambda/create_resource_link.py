import boto3

client = boto3.client('glue')


def lambda_handler(event, context):
    response = client.create_table(
        DatabaseName='',
        TableInput={
            'Name': '',
            'TargetTable': {
                'CatalogId': '480025846069',
                'DatabaseName': 'producer-data',
                'Name': 'data_mesh_refined'
            }
        },
    )