from argparse import ArgumentParser
import time
import boto3
import json

class DataMeshClient:
    PROFILES = [("producer", "us-east-1"), ("consumer", "us-east-1"), ("central", "us-east-1")]
    VARIABLES = "../accounts/variables.json"

    def __init__(self):
        self.region = "us-east-1"
        self.sessions = self.initialize_sessions()
        self.acct_id_map = self.build_account_id_map(self.sessions)
        self.variables = self.load_variables()

    def initialize_sessions(self):
        return {profile : boto3.session.Session(profile_name = profile, region_name = self.region)
                for profile, region in self.PROFILES}

    def get_client(self, session_name, client_name):
        return self.sessions[session_name].client(client_name)

    def grant_tag_resource(self, lakeformation, principal_id, tag_key, tag_values, permissions):
        return lakeformation.grant_permissions(
                                            Principal={
                                                'DataLakePrincipalIdentifier': principal_id
                                            },
                                            Resource = {"LFTag" : {'TagKey': tag_key, 'TagValues': tag_values}
                                            },
                                            Permissions=tag_permissions,
                                            PermissionsWithGrantOption=tag_permissions
                                            )

    def grant_tag_policy(self, lakeformation, principal_id, tag_key, tag_values, permissions):
        return lakeformation.grant_permissions(
                                            Principal={
                                                'DataLakePrincipalIdentifier': principal_id
                                            },
                                            Resource = {"LFTagPolicy" :{
                                                "ResourceType": "TABLE",
                                                "Expression": [{'TagKey': tag_key, 'TagValues': tag_values}]
                                            }},
                                            Permissions=permissions,
                                            PermissionsWithGrantOption=permissions
                                            )

    def grant_tbac_to_account(self, lakeformation, account_type):
        """
        'ALL'|'SELECT'|'ALTER'|'DROP'|'DELETE'|'INSERT'|'DESCRIBE'|'CREATE_DATABASE'|'CREATE_TABLE'|
        'DATA_LOCATION_ACCESS'|'CREATE_TAG'|'ASSOCIATE',

        'ALL'|'SELECT'|'ALTER'|'DROP'|'DELETE'|'INSERT'|'DESCRIBE'|'CREATE_DATABASE'|'CREATE_TABLE'|'DATA_LOCATION_ACCESS',

        """
        tag_key = self.variables.get("central_tag_key")
        tag_values = self.variables.get("central_tag_values")
        if account_type  == "producer":
            data_permissions = ["DESCRIBE", "SELECT", "INSERT", "ALTER"]
        elif account_type == "consumer":
            data_permissions = [ "DESCRIBE", "SELECT"]
        else:
            raise Exception(" invalid account type: {account_type}, can be 'producer' or 'consumer'")
        tag_permissions = ["DESCRIBE"]
        principal_id = self.acct_id_map[account_type]
        results = []
        results.append(self.grant_tag_policy(lakeformation, principal_id, tag_key, tag_values, data_permissions))
        results.append(self.grant_tag_resource(lakeformation, principal_id, tag_key, tag_values, tag_permissions))
        return results

    def grant_tbac_to_consumer_user(self, lakeformation, user_arn, tag_key, tag_values, permissions = ["DESCRIBE", "SELECT"]):
        principal_id = self.variables.get("consumer_analyst_arn")
        return self.grant_tag_policy(lakeformation,
                                        principal_id = user_arn,
                                        tag_key = tag_key,
                                        tag_values = tag_values,
                                        permissions = permissions)

    def run_central_glue_crawler(self, glue):
        crawler_name = self.variables.get("central_crawler_name")
        return glue.start_crawler(Name=crawler_name)

    def catalog_table_names(self, glue):
        """
        keys each table keeps:
        'Name', 'DatabaseName', 'Owner', 'CreateTime', 'UpdateTime', 'LastAccessTime', 'Retention',
        'StorageDescriptor', 'PartitionKeys', 'TableType', 'Parameters', 'CreatedBy',
        'IsRegisteredWithLakeFormation', 'CatalogId', 'VersionId'
        """
        dbname = self.variables.get("central_database_name")
        return [x["Name"] for x in glue.get_tables(DatabaseName = dbname)["TableList"]]

    def get_central_glue_crawler(self, glue):
        crawler_name = self.variables.get("central_crawler_name")
        return glue.get_crawler(Name=crawler_name)

    def get_central_glue_crawler_state(self, glue):
        return self.get_central_glue_crawler(glue)['Crawler']['State']

    def wait_for_crawler_state(self, glue, state = "READY", wait_for = 40):
        i = 0
        while i < wait_for:
            current_state = self.get_central_glue_crawler_state(glue)
            if current_state == state:
                return True
            time.sleep(5)
            i += 1
        return False

    @classmethod
    def load_variables(cls):
        with open(cls.VARIABLES, "r" ) as f:
            return json.loads(f.read())

    @classmethod
    def build_account_id_map(cls, sessions):
        acct_id_map = {}
        for profile, _ in cls.PROFILES:
            session = sessions.get(profile)
            acct_id_map[profile] = session.client("sts").get_caller_identity()["Account"]
        return acct_id_map
    
    def update_lf_tag(self, lakeformation):
        tag_key = self.variables.get("central_tag_key")
        tag_values = self.variables.get("central_tag_values")
        actual_tagvalues = lakeformation.get_lf_tag(TagKey=tag_key)["TagValues"]
        tagvalues_to_add = [x for x in tag_values if x not in actual_tagvalues]
        if tagvalues_to_add:
            return lakeformation.update_lf_tag(TagKey=tag_key, TagValuesToAdd = tagvalues_to_add)
        return "Tag and values already exist"

    def create_lf_tag(self, lakeformation):
        tag_key = self.variables.get("central_tag_key")
        tag_values = self.variables.get("central_tag_values")
        try:
            return lakeformation.create_lf_tag(TagKey = tag_key, TagValues = tag_values)
        except lakeformation.exceptions.InvalidInputException as e:
            if e.response["Error"]["Message"] == "Tag key already exists":
                return self.update_lf_tag(lakeformation)
            raise e

    def tag_table(self, lakeformation, table_name):
        tag_key = self.variables.get("central_tag_key")
        tag_values = self.variables.get("central_tag_values")

        db_name = self.variables.get("central_database_name")
        return lakeformation.add_lf_tags_to_resource(
                                                Resource = {"Table" :
                                                                {"DatabaseName":db_name, 
                                                                "Name":table_name
                                                                }},
                                                LFTags = [{
                                                            "TagKey" :  tag_key,
                                                            "TagValues": tag_values
                                                        }])

    def central_glue_tasks(self):
        glue = self.get_client("central", "glue")
        #self.wait_for_crawler_state(glue, "READY")
        #client.run_central_glue_crawler(glue)
        #self.wait_for_crawler_state(glue, "STOPPING")
        lakeformation = self.get_client("central", "lakeformation")
        #self.create_lf_tag(lakeformation)
        #for tablename in self.catalog_table_names(glue):
        #    self.tag_table(lakeformation, tablename)

        print(self.grant_tbac_to_account(lakeformation, "producer"))
        print(self.grant_tbac_to_account(lakeformation, "consumer"))

        for table_name in self.catalog_table_names(glue):
            self.create_consumer_resource_link(table_name)
            #self.create_producer_resource_link(table_name)

    def create_consumer_resource_link(self, table_name):
        glue = self.get_client("consumer", "glue")
        target_db = self.variables.get("consumer_database_name")
        target_table = f"consumer_{table_name}"
        source_acct = self.acct_id_map["central"]
        source_db = self.variables.get("central_database_name")
        return self.create_resource_link(glue,
                                    target_db=target_db,
                                    target_table_name = target_table,
                                    source_acct = source_acct,
                                    source_db = source_db,
                                    source_table = table_name)

    def create_resource_link(self, glue, target_db, target_table_name, source_acct, source_db, source_table):
          response = glue.create_table(
                            DatabaseName=target_db,
                            TableInput={
                                'Name': target_table_name,
                                'TargetTable': {
                                    'CatalogId': source_acct,
                                    'DatabaseName': source_db,
                                    'Name': source_table
                                }
                            },
                        )

    def parse():
        parser = ArgumentParser()
        parser.add_argument("-p", "--profile", type=str, choices = ["producer","consumer","central"], dest = "profile")
        return vars(parser.parse_args())


if __name__ == "__main__":
    client = DataMeshClient()
    client.central_glue_tasks()
    print(client.acct_id_map)
