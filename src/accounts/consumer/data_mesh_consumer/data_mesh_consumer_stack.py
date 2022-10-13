import json
import boto3

from aws_cdk import (
    # Duration,
    Stack,
    aws_glue_alpha as glue_alpha,
    aws_athena as athena,
    aws_lambda as lambda_,
    aws_iam as iam
)
from constructs import Construct

class DataMeshConsumerStack(Stack):

    OTHER_PROFILES = ["central"]

    @classmethod
    def build_account_id_map(cls):
        acct_id_map = {}
        for profile in cls.OTHER_PROFILES:
            session = cls.init_session(profile)
            acct_id_map[profile] = session.client("sts").get_caller_identity()["Account"]
        return acct_id_map

    @staticmethod
    def init_session(profile_name):
        return boto3.session.Session(profile_name = profile_name)

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        with open("../variables.json", "r") as f:
            self.variables = json.loads(f.read())

        db_name = self.variables.get("consumer_database_name")
        wg_name = self.variables.get("consumer_athena_wg_name")
        acct_ids = self.build_account_id_map()
        central_account_id = acct_ids["central"]

        database = glue_alpha.Database(self, id=db_name, database_name=db_name)

        #CREATING WORKGROUPS
        consumer_workgroup = self.create_athena_workgroup(workgroup_name = wg_name)

        
        #LAMBDA FUNCTION FOR RESOURCE LINK
        fn = lambda_.Function(self, "lf-resource-link-joe-test",
                              runtime = lambda_.Runtime.PYTHON_3_8,
                              handler = "create_resource_link.lambda_handler",
                              code = lambda_.Code.from_asset("resource_link_lambda"))
        
        fn.role.add_managed_policy(policy = iam.ManagedPolicy.from_managed_policy_arn(self, "Lambda Glue Access", 
                                                                                      managed_policy_arn = "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess"))

        


    
    #FUNCTION TO CREATE WORK GROUP

    def create_athena_workgroup(self, workgroup_name: str):
    
        workgroup = athena.CfnWorkGroup(self, workgroup_name,
                                        name = workgroup_name,
                                        state = "ENABLED")

        return workgroup

