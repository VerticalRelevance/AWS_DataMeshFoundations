import boto3

client = boto3.client('glue')


def lambda_handler(event, context):
    """
    {
        "table_name" : "my_demo_table",
        "catalog_account_id" :  01234678910,
        "catalog_database_name" : "demo_db_name",
        "source_table_name" : "my_demo_table"
    """
    table_name = event.get("table_name")
    catalog_id = event.get("catalog_account_id")
    database_name = event.get("catalog_database_name")
    target_table_name = event.get("source_table_name")
    response = client.create_table(
        DatabaseName='',
        TableInput={
            'Name': table_name,
            'TargetTable': {
                'CatalogId': catalog_id,
                'DatabaseName': database_name,
                'Name': target_table_name
            }
        },
    )