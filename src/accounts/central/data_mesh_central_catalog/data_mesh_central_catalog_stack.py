from constructs import Construct
import boto3
import json
from aws_cdk import (
    Duration,
    Stack,
    aws_glue as glue,
    aws_glue_alpha as glue_alpha,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_lakeformation as lakeformation
)


class CentralCatalogStack(Stack):
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

        bucket_name = self.variables.get('producer_bucket_name')
        db_name = self.variables.get("central_database_name")
        table_prefix = self.variables.get("central_table_prefix")
        crawler_name = self.variables.get("central_crawler_name")
        tag_key = self.variables.get("central_tag_key")
        tag_values = self.variables.get("central_tag_values")

        acct_ids = self.build_account_id_map()
        central_account_id = acct_ids["central"]

        #Create databse
        database = glue_alpha.Database(self, id=db_name, database_name=db_name)

        lakeformation_broad_policy = iam.PolicyDocument(
            statements = [iam.PolicyStatement(
                actions = ["lakeformation:*",
                            "glue:*",
                            "s3:*",
                            "logs:*",
                            "lambda:*"],
                resources =["*"],
                effect = iam.Effect.ALLOW,
            ),
                iam.PolicyStatement(
                    effect = iam. Effect.ALLOW,
                    actions = ["s3:*"],
                    resources = ["arn:aws:s3:::",
                                    f"arn:aws:s3:::{bucket_name}/*"]
                )

            ]
        )

        glue_role = iam.Role(
            self,
            'producer-glue-test-role',
            assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole'),
            iam.ManagedPolicy.from_aws_managed_policy_name('AmazonS3FullAccess'),
            iam.ManagedPolicy.from_aws_managed_policy_name('AWSLakeFormationCrossAccountManager'),
            iam.ManagedPolicy.from_aws_managed_policy_name('AWSGlueConsoleFullAccess'),
            iam.ManagedPolicy.from_aws_managed_policy_name('AWSLakeFormationDataAdmin')
            ],
            inline_policies = {"lf_policy" : lakeformation_broad_policy} 
        )

        crawler = glue.CfnCrawler(
            self, 
            crawler_name, 
            role=glue_role.role_arn,
            name = crawler_name,
            database_name=database.database_name,
            targets={
                's3Targets': [{"path": f"s3://{bucket_name}/customers/"}],
            },
            table_prefix = table_prefix
        )

        #CfnOutput(self, "CentralProducerCrawlerName", value=crawler.)


        cfn_permissions = lakeformation.CfnPermissions(self, "grant crawler permissions", 
            data_lake_principal = lakeformation.CfnPermissions.DataLakePrincipalProperty(data_lake_principal_identifier = glue_role.role_arn),
            resource = lakeformation.CfnPermissions.ResourceProperty(
                database_resource = lakeformation.CfnPermissions.DatabaseResourceProperty(name = database.database_name)
            ),
            permissions = ["CREATE_TABLE"]
        )

        #cfn_tag = lakeformation.CfnTag(self, "Demo Tag", tag_key = tag_key, tag_values = tag_values)

        #Creating Lambda function to grant tag-based lakeformation permissions
        gt_fn = lambda_.Function(self, "lakeformation-grant-tag-based",
                              runtime = lambda_.Runtime.PYTHON_3_8,
                              handler = "tag-based-grant.lambda_handler",
                              code = lambda_.Code.from_asset("lake_formation_grant_lambda"))

        ##Adding LakeFormation Permissions to the Lambda IAM Role
        gt_fn.role.add_managed_policy(policy = iam.ManagedPolicy.from_managed_policy_arn(self, "LF Lambda Access", 
                                                                                      managed_policy_arn = "arn:aws:iam::aws:policy/AWSLakeFormationDataAdmin"))


        

        ##Creating Lambda function to grant tag-based lakeformation permissions
        #tt_fn = lambda_.Function(self, "lakeformation-tag-table",
        #                      runtime = lambda_.Runtime.PYTHON_3_8,
        #                      handler = "tag-catalog-table.lambda_handler",
        #                      code = lambda_.Code.from_asset("lake_formation_tag_table"))


        ##Adding LakeFormation Permissions to the Lambda IAM Role
        #tt_fn.role.add_managed_policy(policy = iam.ManagedPolicy.from_managed_policy_arn(self, "LF Lambda Access 2 ", 
        #                                                                              managed_policy_arn = "arn:aws:iam::aws:policy/AWSLakeFormationDataAdmin"))

        #cfn_permissions = lakeformation.CfnPermissions(self, "tag associate lambda permissions", 
        #    data_lake_principal = lakeformation.CfnPermissions.DataLakePrincipalProperty(data_lake_principal_identifier = tt_fn.role.role_arn),
        #    resource = lakeformation.CfnPermissions.ResourceProperty(
        #        database_resource = lakeformation.CfnPermissions.DatabaseResourceProperty(name = database.database_name)
        #    ),
        #    permissions = ["ASSOCIATE"]
        #)

        #lakeformation_tag_policy = iam.Policy(self, "lftagpolicy", document = iam.PolicyDocument(
        #    statements = [iam.PolicyStatement(
        #        actions = [
        #            "lakeformation:AddLFTagsToResource",
        #            "lakeformation:RemoveLFTagsFromResource",
        #            "lakeformation:GetResourceLFTags",
        #            "lakeformation:ListLFTags",
        #            "lakeformation:CreateLFTag",
        #            "lakeformation:GetLFTag",
        #            "lakeformation:UpdateLFTag",
        #            "lakeformation:DeleteLFTag",
        #            "lakeformation:SearchTablesByLFTags",
        #            "lakeformation:SearchDatabasesByLFTags"],
        #        resources =["*"],
        #        effect = iam.Effect.ALLOW,
        #    )]))

        #tt_fn.role.attach_inline_policy(lakeformation_tag_policy)

        #lambda_lfn_permissions = lakeformation.CfnPrincipalPermissions(self,
        #                                    "letlambdaassociate",
        #                                    permissions=["ASSOCIATE"],
        #                                    permissions_with_grant_option=["ASSOCIATE"],
        #                                    principal=lakeformation.CfnPrincipalPermissions.DataLakePrincipalProperty(
        #                                                    data_lake_principal_identifier=tt_fn.role.role_arn
        #                                                ),
        #                                    resource=lakeformation.CfnPrincipalPermissions.ResourceProperty(
        #                                            lf_tag=lakeformation.CfnPrincipalPermissions.LFTagKeyResourceProperty(
        #                                                catalog_id = central_account_id,
        #                                                tag_key="source",
        #                                                tag_values=["provider-a"]
        #                                            )))

        #lambda_lfn_permissions_2 = lakeformation.CfnPrincipalPermissions(self,
        #                                    "letlambdaassociate2",
        #                                    permissions=["ASSOCIATE"],
        #                                    permissions_with_grant_option=["ASSOCIATE"],
        #                                    principal=lakeformation.CfnPrincipalPermissions.DataLakePrincipalProperty(
        #                                                    data_lake_principal_identifier=tt_fn.role.role_arn
        #                                                ),
        #                                    resource=lakeformation.CfnPrincipalPermissions.ResourceProperty(
        #                                   table=lakeformation.CfnPrincipalPermissions.TableResourceProperty(
        #                                        catalog_id = central_account_id,
        #                                        database_name=db_name,
        #                                        table_wildcard={}
        #                                    )))


        #Creating Schema
        #schema = glue_alpha.Table(self, "MyTable",
        #                    database= database,
        #                    table_name="us_customers-a",
        #                    columns=[glue_alpha.Column(name="first_name",type=glue_alpha.Schema.STRING),
        #                             glue_alpha.Column(name="last_name",type=glue_alpha.Schema.STRING),
        #                             glue_alpha.Column(name="company_name",type=glue_alpha.Schema.STRING),
        #                             glue_alpha.Column(name="address",type=glue_alpha.Schema.STRING),
        #                             glue_alpha.Column(name="city",type=glue_alpha.Schema.STRING),
        #                             glue_alpha.Column(name="county",type=glue_alpha.Schema.STRING),
        #                             glue_alpha.Column(name="state",type=glue_alpha.Schema.STRING),
        #                             glue_alpha.Column(name="zip",type=glue_alpha.Schema.STRING),
        #                             glue_alpha.Column(name="phone1",type=glue_alpha.Schema.STRING),
        #                             glue_alpha.Column(name="phone2",type=glue_alpha.Schema.STRING),
        #                             glue_alpha.Column(name="email",type=glue_alpha.Schema.STRING),
        #                             glue_alpha.Column(name="web",type=glue_alpha.Schema.STRING)],
        #                    partition_keys=[glue_alpha.Column(name="year", type=glue_alpha.Schema.SMALL_INT), 
        #                                    glue_alpha.Column(name="month",type=glue_alpha.Schema.SMALL_INT)],
        #                    data_format=glue_alpha.DataFormat.JSON
        #                )


