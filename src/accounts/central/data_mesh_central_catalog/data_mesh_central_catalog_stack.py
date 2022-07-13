from constructs import Construct
from aws_cdk import (
    Duration,
    Stack,
    aws_glue as glue,
    aws_glue_alpha as glue_alpha,
    aws_iam as iam,
    aws_lambda as lambda_
)


class CentralCatalogStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)


        #Create databse
        database = glue_alpha.Database(self, id='my_database_id',
                                database_name='producer-a-db'
        )

        #Creating Schema
        schema = glue_alpha.Table(self, "MyTable",
                            database= database,
                            table_name="us_customers-a",
                            columns=[glue_alpha.Column(name="first_name",type=glue_alpha.Schema.STRING),
                                     glue_alpha.Column(name="last_name",type=glue_alpha.Schema.STRING),
                                     glue_alpha.Column(name="company_name",type=glue_alpha.Schema.STRING),
                                     glue_alpha.Column(name="address",type=glue_alpha.Schema.STRING),
                                     glue_alpha.Column(name="city",type=glue_alpha.Schema.STRING),
                                     glue_alpha.Column(name="county",type=glue_alpha.Schema.STRING),
                                     glue_alpha.Column(name="state",type=glue_alpha.Schema.STRING),
                                     glue_alpha.Column(name="zip",type=glue_alpha.Schema.STRING),
                                     glue_alpha.Column(name="phone1",type=glue_alpha.Schema.STRING),
                                     glue_alpha.Column(name="phone2",type=glue_alpha.Schema.STRING),
                                     glue_alpha.Column(name="email",type=glue_alpha.Schema.STRING),
                                     glue_alpha.Column(name="web",type=glue_alpha.Schema.STRING)],
                            partition_keys=[glue_alpha.Column(name="year", type=glue_alpha.Schema.SMALL_INT), 
                                            glue_alpha.Column(name="month",type=glue_alpha.Schema.SMALL_INT)],
                            data_format=glue_alpha.DataFormat.JSON
                        )



        #Creating Lambda function to grant tag-based lakeformation permissions
        fn = lambda_.Function(self, "lakeformation-grant-tag-based",
                              runtime = lambda_.Runtime.PYTHON_3_8,
                              handler = "tag-based-grant.lambda_handler",
                              code = lambda_.Code.from_asset("lake_formation_grant_lambda"))
        

        #Adding LakeFormation Permissions to the Lambda IAM Role
        fn.role.add_managed_policy(policy = iam.ManagedPolicy.from_managed_policy_arn(self, "LF Lambda Access", 
                                                                                      managed_policy_arn = "arn:aws:iam::aws:policy/AWSLakeFormationDataAdmin"))
        

        
