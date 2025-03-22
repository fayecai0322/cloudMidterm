from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam
)
from constructs import Construct

class CleanerStack(Stack):
    def __init__(self, scope: Construct, id: str, table, dst_bucket, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self.cleaner_function = _lambda.Function(
            self, "CleanerFunction",
            runtime=_lambda.Runtime.PYTHON_3_11,
            architecture=_lambda.Architecture.ARM_64,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda_functions/cleaner"),
            environment={
                "TABLE_NAME": table.table_name,
                "DST_BUCKET": dst_bucket.bucket_name
            },
            timeout=Duration.seconds(90)
        )

        # ✅ 权限设置
        table.grant_read_write_data(self.cleaner_function)
        dst_bucket.grant_delete(self.cleaner_function)

        # ✅ 单独为 GSI 添加 Query 权限
        self.cleaner_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["dynamodb:Query"],
                resources=[f"{table.table_arn}/index/DisownedIndex"]
            )
        )

        # EventBridge 规则（每分钟触发）
        rule = events.Rule(
            self, "CleanerRule",
            schedule=events.Schedule.rate(Duration.minutes(1))
        )
        rule.add_target(targets.LambdaFunction(self.cleaner_function))