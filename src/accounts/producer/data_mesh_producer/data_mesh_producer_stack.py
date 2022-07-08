from constructs import Construct
from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_glue as glue,
    aws_s3 as s3,
    aws_s3_deployment as deployment,
    aws_iam as iam,
    aws_lambda as lambda_
)


class ProducerAccountStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        
        #CREATING PRODUCER ACCOUNT REFINED BUCKET
        refined_bucket  = self.create_s3_bucket(bucket_name = "data-mesh-refined")

        #UPLOADING SAMPLE DATA TO REFINED BUCKET
        sample_data_deployment = self.upload_to_s3(directory = "./sample_data",
                                                   deployment_bucket = refined_bucket)

        #ADDING BUCKET POLICY TO REFINED BUCKET - ALLOW ACCESS TO CENTRAL CATALOG ACCOUNT
        refined_bucket.add_to_resource_policy(permission = iam.PolicyStatement(actions = ["s3:*"],
                                                                               principals = [iam.ArnPrincipal(arn = "arn:aws:iam::480025846069:root")],
                                                                               resources = ["arn:aws:s3:::data-mesh-refined/*",
                                                                                            "arn:aws:s3:::data-mesh-refined"]))

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




    
