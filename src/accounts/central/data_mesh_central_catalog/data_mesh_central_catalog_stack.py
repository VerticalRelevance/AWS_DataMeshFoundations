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


        #Create IAM role for Glue Crawler, and adding permission for it to access the Producer refined bucket
        glue_crawler_role = self.create_iam_role_glue(role_name = "producer-bucket-crawler-role")

        glue_crawler_role.add_to_principal_policy(statement = iam.PolicyStatement(actions = ["s3:*"], 
                                                                                  resources = ["arn:aws:s3:::data-mesh-refined/*",
                                                                                               "arn:aws:s3:::data-mesh-refined"]))


        #Creating Glue Crawler to crawl the bucket in the Producer Account
        producer_bucket_crawler = self.create_glue_crawler(crawler_name = "producer-crawler", 
                                                           bucket_path = "s3://data-mesh-refined/", 
                                                           iam_role = glue_crawler_role,
                                                           db_name = "producer-data")

        #Dependancies
        producer_bucket_crawler.node.add_dependency(glue_crawler_role) #creates iam role before crawler


        #Creating Lambda function to grant tag-based lakeformation permissions
        fn = lambda_.Function(self, "lakeformation-grant-tag-based",
                              runtime = lambda_.Runtime.PYTHON_3_8,
                              handler = "tag-based-grant.lambda_handler",
                              code = lambda_.Code.from_asset("lake_formation_grant_lambda"))
        

        #Adding LakeFormation Permissions to the Lambda IAM Role
        fn.role.add_managed_policy(policy = iam.ManagedPolicy.from_managed_policy_arn(self, "LF Lambda Access", 
                                                                                      managed_policy_arn = "arn:aws:iam::aws:policy/AWSLakeFormationDataAdmin"))
        

        

    #GENERAL FUNCTIONS FOR RESOURCES

    #Function to create Crawler 
    def create_glue_crawler(self, crawler_name:str, bucket_path: str, iam_role, db_name: str):

        s3_target = glue.CfnCrawler.TargetsProperty(s3_targets = [glue.CfnCrawler.S3TargetProperty(path = bucket_path)])

        crawler = glue.CfnCrawler(self, crawler_name, 
                                  role = iam_role.role_arn,
                                  targets = s3_target,
                                  database_name = db_name,
                                  configuration = '{"Version": 1.0,"Grouping": {"TableGroupingPolicy": "CombineCompatibleSchemas"}}'
        )

        return crawler


    #Function to Create IAM role for glue crawler with standard permissions
    def create_iam_role_glue(self, role_name: str):

        glue_role = iam.Role(self, role_name, 
                             assumed_by = iam.ServicePrincipal(service = "glue.amazonaws.com"), 
                             description = "Role for glue to crawl cross account buckets",
                             role_name = role_name)


        glue_role.add_managed_policy(policy = iam.ManagedPolicy.from_managed_policy_arn(self, "Glue Service Role", 
                                                                                        managed_policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"))

        glue_role.add_managed_policy(policy = iam.ManagedPolicy.from_managed_policy_arn(self, "S3 Access", 
                                                                                        managed_policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"))

        glue_role.add_managed_policy(policy = iam.ManagedPolicy.from_managed_policy_arn(self, "Lake Formation Access", 
                                                                                        managed_policy_arn = "arn:aws:iam::aws:policy/AWSLakeFormationDataAdmin"))

        glue_role.add_managed_policy(policy = iam.ManagedPolicy.from_managed_policy_arn(self, "Lake Formation Cross Account", 
                                                                                        managed_policy_arn = "arn:aws:iam::aws:policy/AWSLakeFormationCrossAccountManager"))

        glue_role.add_managed_policy(policy = iam.ManagedPolicy.from_managed_policy_arn(self, "Glue Console access", 
                                                                                        managed_policy_arn = "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess"))
        return glue_role
