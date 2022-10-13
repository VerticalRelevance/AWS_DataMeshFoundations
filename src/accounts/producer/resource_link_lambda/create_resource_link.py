import boto3

client = boto3.client('glue')


def lambda_handler(event, context):
    response = client.create_table(
        DatabaseName='joe_test_db',
        TableInput={
            'Name': 'joe_testcustomers',
            'TargetTable': {
                'CatalogId': '480025846069',
                'DatabaseName': 'joe_central_catalog_db',
                'Name': 'data_mesh_refined'
            }
        },
    )