import boto3

session = boto3.Session(profile_name = 'central')
client = session.client('glue')

client.create_database(
    CatalogId='480025846069',
    DatabaseInput={
        'Name': 'producer-data'
        }
)