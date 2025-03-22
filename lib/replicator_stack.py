# lib/replicator_stack.py
from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_s3_notifications as s3n,
    aws_iam as iam
)
from constructs import Construct

class ReplicatorStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # ✅ S3 Buckets
        self.bucket_src = s3.Bucket(self, "BucketSrc")
        self.bucket_dst = s3.Bucket(self, "BucketDst")

        # ✅ DynamoDB Table
        self.table = dynamodb.Table(
            self, "TableT",
            partition_key=dynamodb.Attribute(name="objectName", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="sortKey", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY
        )

        # ✅ GSI for disowned items
        self.table.add_global_secondary_index(
            index_name="DisownedIndex",
            partition_key=dynamodb.Attribute(name="disowned", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="disownTime", type=dynamodb.AttributeType.STRING)
        )

        # ✅ Lambda Function
        self.replicator_function = _lambda.Function(
            self, "ReplicatorFunction",
            runtime=_lambda.Runtime.PYTHON_3_11,
            architecture=_lambda.Architecture.ARM_64,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda_functions/replicator"),
            environment={
                "TABLE_NAME": self.table.table_name,
                "DST_BUCKET": self.bucket_dst.bucket_name
            }
        )

        # ✅ Permissions
        self.table.grant_read_write_data(self.replicator_function)
        self.bucket_src.grant_read(self.replicator_function)
        self.bucket_dst.grant_write(self.replicator_function)

        # ✅ Event notifications
        self.bucket_src.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.replicator_function)
        )
        self.bucket_src.add_event_notification(
            s3.EventType.OBJECT_REMOVED,
            s3n.LambdaDestination(self.replicator_function)
        )

        # ✅ Grant S3 permission to invoke Lambda
        self.replicator_function.add_permission(
            "AllowS3InvokeByBucketSrc",
            principal=iam.ServicePrincipal("s3.amazonaws.com"),
            source_arn=self.bucket_src.bucket_arn
        )
