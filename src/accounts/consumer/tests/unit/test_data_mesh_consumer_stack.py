import aws_cdk as core
import aws_cdk.assertions as assertions

from data_mesh_consumer.data_mesh_consumer_stack import DataMeshConsumerStack

# example tests. To run these tests, uncomment this file along with the example
# resource in data_mesh_consumer/data_mesh_consumer_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = DataMeshConsumerStack(app, "data-mesh-consumer")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
