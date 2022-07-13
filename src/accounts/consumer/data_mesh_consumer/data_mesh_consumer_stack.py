from aws_cdk import (
    # Duration,
    Stack,
    aws_athena as athena,
    aws_lambda as lambda_,
    aws_iam as iam
)
from constructs import Construct

class DataMeshConsumerStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        #CREATING WORKGROUPS
        consumer_workgroup = self.create_athena_workgroup(workgroup_name = "consumer")

        
        #LAMBDA FUNCTION FOR RESOURCE LINK
        fn = lambda_.Function(self, "lf-resource-link",
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

