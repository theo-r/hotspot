import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_apigateway as apigateway,
)


class ApiStack(Stack):
    def __init__(self, scope, construct_id, hotspot_stack, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        hotspot_api_policy = iam.Policy(
            self,
            "FreshApiPolicy",
            statements=[
                iam.PolicyStatement(
                    actions=["s3:GetObject"],
                    resources=[f"{hotspot_stack.s3.bucket_arn}/fresh/*"],
                )
            ],
        )

        hotspot_api_lambda = _lambda.DockerImageFunction(
            self,
            "HotspotApiLambda",
            code=_lambda.DockerImageCode.from_image_asset("../lambda/hotspot_api"),
            architecture=_lambda.Architecture.X86_64,
            timeout=cdk.Duration.seconds(10),
        )

        hotspot_api_lambda.add_environment("BUCKET_NAME", hotspot_stack.s3.bucket_name)
        hotspot_api_lambda.add_environment("PAST_WEEK_PATH", "fresh/past_week.json")
        hotspot_api_lambda.add_environment("PAST_MONTH_PATH", "fresh/past_month.json")
        hotspot_api_lambda.add_environment("PAST_YEAR_PATH", "fresh/past_year.json")
        hotspot_api_lambda.add_environment("LOG_LEVEL", "INFO")
        hotspot_api_lambda.add_environment("POWERTOOLS_LOGGER_SAMPLE_RATE", "0.1")
        hotspot_api_lambda.add_environment("POWERTOOLS_LOGGER_LOG_EVENT", "true")
        hotspot_api_lambda.add_environment("POWERTOOLS_SERVICE_NAME", "HotspotApi")

        hotspot_api_lambda.role.attach_inline_policy(policy=hotspot_api_policy)

        hotspot_api_gateway = apigateway.LambdaRestApi(
            self, "HotspotApiGateway", handler=hotspot_api_lambda, proxy=True
        )
