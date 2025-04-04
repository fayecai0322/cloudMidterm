from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam
)
from constructs import Construct
import aws_cdk.aws_dynamodb as dynamodb
import aws_cdk.aws_s3 as s3

class CleanerStack(Stack):
    def __init__(self, scope: Construct, id: str, table_name: str, dst_bucket_name: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Import existing resources
        table = dynamodb.Table.from_table_name(self, "ImportedTable", table_name)
        bucket_dst = s3.Bucket.from_bucket_name(self, "ImportedDstBucket", dst_bucket_name)

        self.cleaner_function = _lambda.Function(
            self, "CleanerFunction",
            runtime=_lambda.Runtime.PYTHON_3_11,
            architecture=_lambda.Architecture.ARM_64,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda_functions/cleaner"),
            environment={
                "TABLE_NAME": table.table_name,
                "DST_BUCKET": bucket_dst.bucket_name
            },
            timeout=Duration.seconds(65)  # long enough for loop to run
        )

        # Permissions
        table.grant_read_write_data(self.cleaner_function)
        bucket_dst.grant_delete(self.cleaner_function)

        # ⛳️ Grant additional permission to query GSI
        self.cleaner_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["dynamodb:Query"],
                resources=[f"{table.table_arn}/index/DisownedIndex"]
            )
        )

        # Schedule the cleaner every 1 minute
        rule = events.Rule(
            self, "CleanerRule",
            schedule=events.Schedule.rate(Duration.minutes(1))
        )
        rule.add_target(targets.LambdaFunction(self.cleaner_function))
