import aws_cdk as core
import aws_cdk.assertions as assertions

from data_mesh_central_catalog.data_mesh_central_catalog_stack import DataMeshCentralCatalogStack

# example tests. To run these tests, uncomment this file along with the example
# resource in data_mesh_central_catalog/data_mesh_central_catalog_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = DataMeshCentralCatalogStack(app, "data-mesh-central-catalog")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
