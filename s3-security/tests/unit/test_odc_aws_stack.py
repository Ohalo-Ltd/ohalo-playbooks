import aws_cdk as core
import aws_cdk.assertions as assertions

from odc_aws.odc_aws_stack import OdcAwsStack

# example tests. To run these tests, uncomment this file along with the example
# resource in odc_aws/odc_aws_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = OdcAwsStack(app, "odc-aws")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
