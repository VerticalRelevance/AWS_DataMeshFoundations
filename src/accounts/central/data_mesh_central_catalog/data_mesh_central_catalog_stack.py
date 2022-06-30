from constructs import Construct
from aws_cdk import (
    Duration,
    Stack,
    aws_glue as glue,
    aws_iam as iam,
    aws_lambda as lambda_
)


class CentralCatalogStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)


        #Create databse
        database = glue.Database(self, id='my_database_id',
                                database_name='producer-a-db'
        )

        #Creating Schema
        schema = glue.Table(self, "MyTable",
                            database= database,
                            table_name="us_customers-a",
                            columns=[glue.Column(name="first_name",type=glue.Schema.STRING),
                                     glue.Column(name="last_name",type=glue.Schema.STRING),
                                     glue.Column(name="company_name",type=glue.Schema.STRING),
                                     glue.Column(name="address",type=glue.Schema.STRING),
                                     glue.Column(name="city",type=glue.Schema.STRING),
                                     glue.Column(name="county",type=glue.Schema.STRING),
                                     glue.Column(name="state",type=glue.Schema.STRING),
                                     glue.Column(name="zip",type=glue.Schema.STRING),
                                     glue.Column(name="phone1",type=glue.Schema.STRING),
                                     glue.Column(name="phone2",type=glue.Schema.STRING),
                                     glue.Column(name="email",type=glue.Schema.STRING),
                                     glue.Column(name="web",type=glue.Schema.STRING)],
                            partition_keys=[glue.Column(name="year", type=glue.Schema.SMALL_INT), 
                                            glue.Column(name="month",type=glue.Schema.SMALL_INT)],
                            data_format=glue.DataFormat.JSON
                        )



        #Creating Lambda function to grant tag-based lakeformation permissions
        fn = lambda_.Function(self, "lakeformation-grant-tag-based",
                              runtime = lambda_.Runtime.PYTHON_3_8,
                              handler = "tag-based-grant.lambda_handler",
                              code = lambda_.Code.from_asset("lake_formation_grant_lambda"))
        

        #Adding LakeFormation Permissions to the Lambda IAM Role
        fn.role.add_managed_policy(policy = iam.ManagedPolicy.from_managed_policy_arn(self, "LF Lambda Access", 
                                                                                      managed_policy_arn = "arn:aws:iam::aws:policy/AWSLakeFormationDataAdmin"))
        

        
