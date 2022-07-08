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
        cfn_database = glue.CfnDatabase(self, "producer-database-a",
            catalog_id="480025846069",
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="producer-database-a"
                )
            )
    

        #Creating table
        cfn_table = glue.CfnTable(self, "us_customers-a",
            catalog_id="480025846069",
            database_name="producer-database-a",
            table_input=glue.CfnTable.TableInputProperty(
                description="description",
                name="us_customers-a",
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=[glue.CfnTable.ColumnProperty(name="first_name",type="string"),
                            glue.CfnTable.ColumnProperty(name="last_name",type="string"),
                            glue.CfnTable.ColumnProperty(name="company_name",type="string"),
                            glue.CfnTable.ColumnProperty(name="address",type="string"),
                            glue.CfnTable.ColumnProperty(name="city",type="string"),
                            glue.CfnTable.ColumnProperty(name="county",type="string"),
                            glue.CfnTable.ColumnProperty(name="state",type="string"),
                            glue.CfnTable.ColumnProperty(name="zip",type="string"),
                            glue.CfnTable.ColumnProperty(name="phone1",type="string"),
                            glue.CfnTable.ColumnProperty(name="phone2",type="string"),
                            glue.CfnTable.ColumnProperty(name="email",type="string"),
                            glue.CfnTable.ColumnProperty(name="web",type="string")] 
            )
        ))


        #Creating Lambda function to grant tag-based lakeformation permissions
        fn = lambda_.Function(self, "lakeformation-grant-tag-based",
                              runtime = lambda_.Runtime.PYTHON_3_8,
                              handler = "tag-based-grant.lambda_handler",
                              code = lambda_.Code.from_asset("lake_formation_grant_lambda"))
        

        #Adding LakeFormation Permissions to the Lambda IAM Role
        fn.role.add_managed_policy(policy = iam.ManagedPolicy.from_managed_policy_arn(self, "LF Lambda Access", 
                                                                                      managed_policy_arn = "arn:aws:iam::aws:policy/AWSLakeFormationDataAdmin"))
        

        
