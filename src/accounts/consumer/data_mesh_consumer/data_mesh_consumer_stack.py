import json
import boto3

from aws_cdk import (
    # Duration,
    Stack,
    aws_lakeformation as lakeformation,
    aws_glue_alpha as glue_alpha,
    aws_athena as athena,
    aws_lambda as lambda_,
    aws_iam as iam,
    SecretValue
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

        #SSO Role should have database permissions
        admin = "arn:aws:iam::901228589034:role/aws-reserved/sso.amazonaws.com/AWSReservedSSO_AdministratorAccess_a045a31cc2cdc3a5"

        cfn_permissions = lakeformation.CfnPermissions(self, "admin access to db", 
            data_lake_principal = lakeformation.CfnPermissions.DataLakePrincipalProperty(data_lake_principal_identifier = admin),
            resource = lakeformation.CfnPermissions.ResourceProperty(
                database_resource = lakeformation.CfnPermissions.DatabaseResourceProperty(name = database.database_name)
            ),
            permissions = ["ALL"],
            permissions_with_grant_option = ["ALL"]
        )

        cross_account_manager = iam.ManagedPolicy.from_aws_managed_policy_name(managed_policy_name = "AWSLakeFormationCrossAccountManager")
        data_admin =  iam.ManagedPolicy.from_aws_managed_policy_name(managed_policy_name = "AWSLakeFormationDataAdmin")

        admin_role = iam.Role.from_role_arn(self, "AdminRole", admin)
        #admin_role.add_managed_policy(cross_account_manager)
        #admin_role.add_managed_policy(data_admin)



    def create_athena_workgroup(self, workgroup_name: str):
    
        workgroup = athena.CfnWorkGroup(self, workgroup_name,
                                        name = workgroup_name,
                                        state = "ENABLED")

        return workgroup



#        lakeformation_admin = iam.Policy(self,
#                                        "lftagpolicy",
#                                        document = iam.PolicyDocument(
#                                        statements = [
#                                            iam.PolicyStatement(actions = [
#                                                #"lakeformation:AddLFTagsToResource",
#                                                #"lakeformation:RemoveLFTagsFromResource",
#                                                #"lakeformation:GetResourceLFTags",
#                                                #"lakeformation:ListLFTags",
#                                                #"lakeformation:CreateLFTag",
#                                                #"lakeformation:GetLFTag",
#                                                #"lakeformation:UpdateLFTag",
#                                                #"lakeformation:DeleteLFTag",
#                                                #"lakeformation:SearchTablesByLFTags",
#                                                #"lakeformation:SearchDatabasesByLFTags",
#                                                "lakeformation:*"],
#                                            resources =["*"],
#                                            effect = iam.Effect.ALLOW,
#                                            )],
#                                        users = [admin_user]
#                                            ))
