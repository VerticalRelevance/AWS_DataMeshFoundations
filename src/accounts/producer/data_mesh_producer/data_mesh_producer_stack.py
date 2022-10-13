from constructs import Construct
import json
import boto3
from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_glue as glue,
    aws_glue_alpha as glue_alpha,
    aws_s3 as s3,
    aws_s3_deployment as deployment,
    aws_iam as iam,
    aws_lambda as lambda_
)


class ProducerAccountStack(Stack):
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
            variables = json.loads(f.read())

        
        acct_ids = self.build_account_id_map()
        central_account_id = acct_ids["central"]

        #CREATING PRODUCER ACCOUNT REFINED BUCKET
        refined_bucket  = self.create_s3_bucket(bucket_name = variables.get("producer_bucket_name"))

        #UPLOADING SAMPLE DATA TO REFINED BUCKET
        sample_data_deployment = self.upload_to_s3(directory = "./sample_data",
                                                   deployment_bucket = refined_bucket)


        resource1 = f"arn:aws:s3:::{variables.get('producer_bucket_name')}/*"
        resource2 = f"arn:aws:s3:::{variables.get('producer_bucket_name')}"
        #ADDING BUCKET POLICY TO REFINED BUCKET - ALLOW ACCESS TO CENTRAL CATALOG ACCOUNT
        refined_bucket.add_to_resource_policy(permission = iam.PolicyStatement(actions = ["s3:*"],
                                                                               principals = [iam.ArnPrincipal(arn = f"arn:aws:iam::{central_account_id}:root")],
                                                                               resources = [resource1, resource2]))

        #LAMBDA FUNCTION FOR RESOURCE LINK
        fn = lambda_.Function(self, "lf-resource-link",
                              runtime = lambda_.Runtime.PYTHON_3_8,
                              handler = "create_resource_link.lambda_handler",
                              code = lambda_.Code.from_asset("resource_link_lambda"))
        
        fn.role.add_managed_policy(policy = iam.ManagedPolicy.from_managed_policy_arn(self, "Lambda Glue Access", 
                                                                                      managed_policy_arn = "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess"))
        


    #FUNCTION TO CREATE S3 BUCKET
    def create_s3_bucket(self, bucket_name: str):

        bucket = s3.Bucket(self, bucket_name, bucket_name = bucket_name.lower(), 
                            removal_policy= RemovalPolicy.DESTROY,
                            auto_delete_objects= True,
                           block_public_access = s3.BlockPublicAccess(block_public_acls = True,  #blocking all public access
                                                                      block_public_policy = True, 
                                                                      ignore_public_acls  = True, 
                                                                      restrict_public_buckets = True)
        )

        return bucket


    #FUNCTION TO UPLOAD OBJECTS TO S3
    def upload_to_s3(self, directory: str, deployment_bucket):

        object_deployment = deployment.BucketDeployment(self, "Object_Deployment",
                                                        sources = [deployment.Source.asset(directory)],
                                                        destination_bucket = deployment_bucket
        )

        return object_deployment




    def add_glue_crawler(self):
        producer_database = glue_alpha.Database(self, "JoeTestDatabase", database_name ="joe_test_db" )
        glue_role = iam.Role(
            self,
            'producer-glue-test-role',
            assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole'),
            iam.ManagedPolicy.from_aws_managed_policy_name('AmazonS3FullAccess'),
            ]
        )
        crawler = glue.CfnCrawler(
            self, 
            'producer-crawler', 
            role=glue_role.role_arn,
            database_name=producer_database.database_name,
            targets={
                's3Targets': [{"path": f"s3://data-mesh-refined/customers/"}],
            },
            table_prefix = "joe_test"
        )



    
