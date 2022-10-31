from argparse import ArgumentParser
import time
import boto3
import json
import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)


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
                                            Permissions=permissions,
                                            PermissionsWithGrantOption=permissions
                                            )

    def grant_tag_policy(self, lakeformation, principal_id, tag_key, tag_values, resource_type, permissions):
        logging.debug(f"tag key in policy grant {tag_key} for permissions {permissions} on resource type {resource_type} ")
        try:
            return lakeformation.grant_permissions(
                                                Principal={
                                                    'DataLakePrincipalIdentifier': principal_id
                                                },
                                                Resource = {"LFTagPolicy" :{
                                                    "ResourceType": resource_type,
                                                    "Expression": [{'TagKey': tag_key, 'TagValues': tag_values}]
                                                }},
                                                Permissions=permissions,
                                                PermissionsWithGrantOption=permissions
                                                )
        except lakeformation.exceptions.EntityNotFoundException as e:
            if e.response['Error']['Message'].lower() == "tag key does not exist":
                logging.debug(f" tag_key does not exist {tag_key} ?")
                available_tags = lakeformation.list_lf_tags(ResourceShareType= "FOREIGN")['LFTags']
                #available_tags = [{"TagKey":x["TagKey"], "TagValues": x["TagValues"]} for x in lakeformation.list_lf_tags(ResourceShareType= "FOREIGN")['LFTags']]
                tag_keys = set(x["TagKey"] for x in available_tags)
                if tag_key in tag_keys:
                    logging.debug(f"wtf the key is in {tag_keys}")
                else:
                    logging.debug(f" tag key {tag_key} not found in {tag_keys} ")
            return False

    def grant_tbac_to_user(self, lakeformation, user_arn, tag_key, tag_values):
        self.grant_tag_policy(lakeformation,
                                principal_id = user_arn,
                                tag_key = tag_key,
                                tag_values = tag_values,
                                resource_type = "TABLE")

    def grant_tbac_to_account(self, lakeformation, account_id, account_type, tag_key, tag_values):
        """
        'ALL'|'SELECT'|'ALTER'|'DROP'|'DELETE'|'INSERT'|'DESCRIBE'|'CREATE_DATABASE'|'CREATE_TABLE'|
        'DATA_LOCATION_ACCESS'|'CREATE_TAG'|'ASSOCIATE',

        'ALL'|'SELECT'|'ALTER'|'DROP'|'DELETE'|'INSERT'|'DESCRIBE'|'CREATE_DATABASE'|'CREATE_TABLE'|'DATA_LOCATION_ACCESS',

        """
        # set data permission string based on account type and grant it
        if account_type  == "producer":
            data_permissions = ["DESCRIBE", "SELECT", "INSERT", "ALTER"]
            db_permissions = ["DESCRIBE", "CREATE_TABLE"]
        elif account_type == "consumer":
            data_permissions = [ "DESCRIBE", "SELECT"]
            db_permissions =["DESCRIBE", "CREATE_TABLE"]
        else:
            raise Exception(" invalid account type: {account_type}, can be 'producer' or 'consumer'")
        logging.debug(f" Granting tag policy {data_permissions} to {account_id} on {tag_key} : {tag_values}" )
        resp = self.grant_tag_policy(lakeformation,
                                principal_id=account_id,
                                tag_key=tag_key,
                                tag_values=tag_values,
                                resource_type="TABLE",
                                permissions=data_permissions)

        logging.debug(f"tag policy response status for table policy : {resp['ResponseMetadata']['HTTPStatusCode']}" )
        
        resp = self.grant_tag_policy(lakeformation,
                                principal_id=account_id,
                                tag_key=tag_key,
                                tag_values=tag_values,
                                resource_type="DATABASE",
                                permissions=db_permissions)

        logging.debug(f"tag policy response status for database policy : {resp['ResponseMetadata']['HTTPStatusCode']}" )


        # set tag level permissions for the account. should be able to describe the tag and associate it with resources.
        tag_permissions = ["DESCRIBE", "ASSOCIATE"]
        logging.debug(F" Granting tag resource permissions {tag_permissions} to {account_id} on tag_key : {tag_key}, tag_values : {tag_values}")
        self.grant_tag_resource(lakeformation,
                                    principal_id=account_id,
                                    tag_key=tag_key,
                                    tag_values=tag_values,
                                    permissions=tag_permissions)
        logging.debug(f"tag resource response status : {resp['ResponseMetadata']['HTTPStatusCode']}" )

    def rwun_glue_crawler(self, glue, crawler_name):
        logging.debug(f" running central glue crawler {crawler_name} ")
        return glue.start_crawler(Name=crawler_name)

    def catalog_table_names(self, glue, dbname):
        """
        keys each table keeps:
        'Name', 'DatabaseName', 'Owner', 'CreateTime', 'UpdateTime', 'LastAccessTime', 'Retention',
        'StorageDescriptor', 'PartitionKeys', 'TableType', 'Parameters', 'CreatedBy',
        'IsRegisteredWithLakeFormation', 'CatalogId', 'VersionId'
        """
        try:
            return [x["Name"] for x in glue.get_tables(DatabaseName = dbname)["TableList"]]
        except glue.exceptions.EntityNotFoundException:
            logging.error(f" Glue Database not found. Try running deploy-all.sh before this script ")
            return []

    def get_glue_crawler(self, glue, crawler_name):
        return glue.get_crawler(Name=crawler_name)

    def get_glue_crawler_state(self, glue, crawler_name):
        return self.get_glue_crawler(glue, crawler_name)['Crawler']['State']

    def wait_for_crawler_state(self, glue, crawler_name, state = "READY", wait_for = 40):
        logging.debug(f" waiting for crawler state {state} ")
        i = 0
        while i < wait_for:
            current_state = self.get_glue_crawler_state(glue, crawler_name)
            logging.debug(f"crawler current state {current_state} T = {i * 5} ")
            if current_state == state:
                logging.debug(f" found crawler state : {state} ")
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
    
    def update_lf_tag(self, lakeformation, tag_key, tag_values):
        actual_tagvalues = lakeformation.get_lf_tag(TagKey=tag_key)["TagValues"]
        tagvalues_to_add = [x for x in tag_values if x not in actual_tagvalues]
        if tagvalues_to_add:
            return lakeformation.update_lf_tag(TagKey=tag_key, TagValuesToAdd = tagvalues_to_add)
        return "Tag and values already exist"

    def create_lf_tag(self, lakeformation, tag_key, tag_values):
        try:
            return lakeformation.create_lf_tag(TagKey = tag_key, TagValues = tag_values)
        except lakeformation.exceptions.InvalidInputException as e:
            if e.response["Error"]["Message"] == "Tag key already exists":
                return self.update_lf_tag(lakeformation, tag_key, tag_values)
            raise e

    @staticmethod
    def tag_database(lakeformation, database_name, tag_key, tag_values):
        logging.debug(f" Tagging database {database_name} with key {tag_key} values {tag_values} ")
        resp = lakeformation.add_lf_tags_to_resource(
                                                Resource = {"Database" :
                                                                { "Name": database_name}},
                                                LFTags = [{
                                                            "TagKey" :  tag_key,
                                                            "TagValues": tag_values
                                                        }])
        logging.debug(f" tag database response status code : {resp['ResponseMetadata']['HTTPStatusCode']}" )
        return resp

    @staticmethod
    def tag_table(lakeformation, table_name, database_name, tag_key, tag_values):
        logging.debug(f" Tagging rable {database_name} with key {tag_key} values {tag_values} ")
        resp = lakeformation.add_lf_tags_to_resource(
                                                Resource = {"Table" :
                                                                {"DatabaseName":database_name, 
                                                                "Name":table_name
                                                                }},
                                                LFTags = [{
                                                            "TagKey" :  tag_key,
                                                            "TagValues": tag_values
                                                        }])
        logging.debug(f" tag database response status code : {resp['ResponseMetadata']['HTTPStatusCode']}" )
        return resp

    def central_glue_tasks(self):
        glue = self.get_client("central", "glue")

        # Make sure crawler is ready to be run, run it, then wait for it to stop
        #central_crawler_name = self.variables.get("central_crawler_name")
        #self.wait_for_crawler_state(glue, central_crawler_name, "READY")
        #client.run_glue_crawler(glue, central_crawler_name)
        #self.wait_for_crawler_state(glue, central_crawler_name, "STOPPING")

        # setup lakeformation tags in central account
        lakeformation = self.get_client("central", "lakeformation")
        tag_key = self.variables.get("central_tag_key")
        tag_values = self.variables.get("central_tag_values")
        self.create_lf_tag(lakeformation, tag_key, tag_values)
        central_db_name = self.variables.get("central_database_name")
        for tablename in self.catalog_table_names(glue, central_db_name):
            logging.debug(f" tagging table {tablename} in central account")
            self.tag_table(lakeformation,
                                table_name=tablename,
                                database_name=central_db_name, 
                                tag_key=tag_key,
                                tag_values=tag_values)

        # Grant tag-level lakeformation permissions to producer and consumer accounts
        producer_account_id = self.acct_id_map["producer"]
        self.grant_tbac_to_account(lakeformation,
                                        account_id=producer_account_id,
                                        account_type="producer",
                                        tag_key=tag_key,
                                        tag_values=tag_values)
        consumer_account_id = self.acct_id_map["consumer"]
        self.grant_tbac_to_account(lakeformation,
                                        account_id=consumer_account_id,
                                        account_type="consumer",
                                        tag_key=tag_key,
                                        tag_values=tag_values)

        # Create resource links and tag the linked tables
        consumer_lakeformation = self.get_client("consumer", "lakeformation")
        consumer_db_name = self.variables.get("consumer_database_name")
        consumer_glue = self.get_client("consumer", "glue")
        source_acct = self.acct_id_map["central"]

        self.tag_database(consumer_lakeformation,
                            database_name = consumer_db_name,
                            tag_key = tag_key,
                            tag_values = tag_values)
        for central_table_name in self.catalog_table_names(glue, central_db_name):
            consumer_table_name = f"consumer_{central_table_name}"
            logging.debug(f" creating consumer resource link for {central_table_name} to {consumer_table_name} ")
            self.create_resource_link(consumer_glue,
                                        create_in_database=consumer_db_name,
                                        new_table_name=consumer_table_name,
                                        source_acct=source_acct,
                                        source_db=central_db_name,
                                        source_table=central_table_name)

            logging.debug(f" tagging table {central_table_name} in consumer account with {tag_key} : {tag_values} ")
            self.tag_table(consumer_lakeformation,
                            table_name=consumer_table_name,
                            database_name=consumer_db_name,
                            tag_key=tag_key,
                            tag_values=tag_values)



    def create_resource_link(self, glue, create_in_database, new_table_name, source_acct, source_db, source_table):
        try:
            return glue.create_table(
                                DatabaseName=create_in_database,
                                TableInput={
                                    'Name': new_table_name,
                                    'TargetTable': {
                                        'CatalogId': source_acct,
                                        'DatabaseName': source_db,
                                        'Name': source_table
                                    }
                                },
                            )
        except glue.exceptions.AlreadyExistsException:
            logging.debug(f"table {new_table_name} already exists")
            return False
        

    def parse():
        parser = ArgumentParser()
        parser.add_argument("-p", "--profile", type=str, choices = ["producer","consumer","central"], dest = "profile")
        return vars(parser.parse_args())

if __name__ == "__main__":
    client = DataMeshClient()
    client.central_glue_tasks()
    logging.debug(client.acct_id_map)
    #lfn = client.get_client("consumer", "lakeformation")
    #client.grant_tbac_to_consumer_user(lfn)
    #print(lfn.list_lf_tags(ResourceShareType= "FOREIGN"))

    #principal_id = client.variables.get("consumer_analyst_arn")
    #tag_key = client.variables.get("central_tag_key")
    #tag_values = client.variables.get("central_tag_values")
    #consumer_lakeformation = client.get_client("consumer", "lakeformation")
    #logging.debug(f" granting tbac to consumer user {principal_id} for tag k : {tag_key} v : {tag_values}" )
    #resp = client.grant_tag_policy(consumer_lakeformation, #                                        principal_id = principal_id,
    #                                        tag_key = tag_key,
    #                                        tag_values = tag_values,
    #                                        permissions = ["DESCRIBE","SELECT"])
    #logging.debug(f" grant tag policy response {resp}")
    #logging.debug(f" {client.acct_id_map}")