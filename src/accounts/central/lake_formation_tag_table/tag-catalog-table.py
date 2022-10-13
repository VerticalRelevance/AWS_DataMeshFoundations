import boto3


client = boto3.client('lakeformation')


def lambda_handler(event, context):
    """
    {
        "database_name": "test_catalog_db",
        "table_name" : "test_demo_table"
    }
    """
    dbname = event.get("database_name")
    tablename = event.get("table_name")
    tagkey = event.get("tag_key")
    tagvalues = event.get("tag_values")
    if not isinstance(tagvalues, list):
        tagvalues = [tagvalues]

    return client.add_lf_tags_to_resource(
        Resource = {"Table" : {"DatabaseName":dbname, 
                               "Name":tablename
                                }
                    },
        LFTags = [{
            "TagKey" :  tagkey,
            "TagValues": tagvalues
        }]
        )