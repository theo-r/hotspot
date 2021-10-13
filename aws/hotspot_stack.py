from aws_cdk import (
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as _lambda,
    aws_s3 as _s3,
    aws_s3_notifications,
    core as cdk
)
import os
import subprocess


class HotspotStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        ingest = _lambda.Function(
            self, 
            'ingest',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.asset(f'../ingest'),
            handler='ingest.lambda_handler',
            layers=[
                self.create_dependencies_layer(self.stack_name, 'ingest')
            ]
        )

        transform = _lambda.Function(
            self, 
            'transform',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.asset(f'../transform'),
            handler='transform.lambda_handler',
            layers=[
                self.create_dependencies_layer(self.stack_name, 'transform')
            ]
        )

        # create s3 bucket
        s3 = _s3.Bucket(self, "hotspotdata")

        # create s3 notification for lambda function
        notification = aws_s3_notifications.LambdaDestination(transform)

        # assign notification for the s3 event type (ex: OBJECT_CREATED)
        s3.add_event_notification(_s3.EventType.OBJECT_CREATED, notification)

        rule = events.Rule(
            self, "ingest-rule",
            schedule=events.Schedule.cron(
                minute='0',
                hour='10',
                day='*',
                month='*',
                year='*'),
        )
        rule.add_target(targets.LambdaFunction(ingest))


    def create_dependencies_layer(self, project_name, function_name: str) -> _lambda.LayerVersion:
        requirements_file = f'../requirements.{function_name}.txt'
        output_dir = f'../.build/{function_name}'

        if not os.environ.get('SKIP_PIP'):
            subprocess.check_call(
                f'pip install -r {requirements_file} -t {output_dir}/python'.split()
            )

        layer_id = f'{project_name}-{function_name}-dependencies'
        layer_code = _lambda.Code.from_asset(output_dir)

        return _lambda.LayerVersion(self, layer_id, code=layer_code)
