import aws_cdk as core
import aws_cdk.assertions as assertions

from midterm.midterm_stack import MidtermStack

# example tests. To run these tests, uncomment this file along with the example
# resource in midterm/midterm_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = MidtermStack(app, "midterm")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
