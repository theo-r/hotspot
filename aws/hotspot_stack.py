from aws_cdk import (
    aws_dynamodb,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as _lambda,
    aws_s3 as _s3,
    aws_s3_notifications,
    aws_ssm as ssm,
    aws_glue as glue,
    aws_iam as iam,
    aws_athena as athena,
    core as cdk
)
from aws_cdk.aws_lambda_python import PythonFunction
import os
import subprocess


class HotspotStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        spotipy_client_id = ssm.StringParameter.from_string_parameter_name(
            self, 'spotipy_client_id', string_parameter_name='SpotipyClientId'
        ).string_value

        spotipy_client_secret = ssm.StringParameter.from_string_parameter_name(
            self, 'spotipy_client_secret', string_parameter_name='SpotipyClientSecret'
        ).string_value

        spotipy_redirect_uri = ssm.StringParameter.from_string_parameter_name(
            self, 'spotipy_redirect_uri', string_parameter_name='SpotipyRedirectUri'
        ).string_value

        # create s3 bucket
        s3 = _s3.Bucket(self, "hotspotdata")

        # create dynamo table
        watermark_table = aws_dynamodb.Table(
            self, "watermark_table",
            partition_key=aws_dynamodb.Attribute(
                name="id",
                type=aws_dynamodb.AttributeType.STRING
            )
        )

        # create dynamo table
        cache_table = aws_dynamodb.Table(
            self, "cache_table",
            partition_key=aws_dynamodb.Attribute(
                name="id",
                type=aws_dynamodb.AttributeType.STRING
            )
        )

        glue_db = glue.Database(self, 'hotspotdb', database_name='hotspot')

        glue_table = glue.Table(
            self,
            'plays_table',
            database=glue_db,
            table_name='plays',
            columns=[
                {
                    'name': 'duration_ms',
                    'type': glue.Schema.BIG_INT
                },
                {
                    'name': 'explicit',
                    'type': glue.Schema.BOOLEAN
                },
                {
                    'name': 'id',
                    'type': glue.Schema.STRING
                },
                {
                    'name': 'name',
                    'type': glue.Schema.STRING
                },
                {
                    'name': 'popularity',
                    'type': glue.Schema.BIG_INT
                },
                {
                    'name': 'album_name',
                    'type': glue.Schema.STRING
                },
                {
                    'name': 'album_release_date',
                    'type': glue.Schema.STRING
                },
                {
                    'name': 'album_release_date_precision',
                    'type': glue.Schema.STRING
                },
                {
                    'name': 'album_image',
                    'type': glue.Schema.STRING
                },
                {
                    'name': 'artist_name',
                    'type': glue.Schema.STRING
                },
                {
                    'name': 'played_at',
                    'type': glue.Schema.TIMESTAMP
                },
                {
                    'name': 'year',
                    'type': glue.Schema.BIG_INT
                },
                {
                    'name': 'month',
                    'type': glue.Schema.BIG_INT
                },
                {
                    'name': 'day',
                    'type': glue.Schema.BIG_INT
                },
                {
                    'name': 'hour',
                    'type': glue.Schema.BIG_INT
                },
                {
                    'name': 'date',
                    'type': glue.Schema.DATE
                },
                {
                    'name': 'dayofweek',
                    'type': glue.Schema.BIG_INT
                }
            ],
            partition_keys=[
                {
                    'name': 'user_name',
                    'type': glue.Schema.STRING
                }
            ],
            data_format=glue.DataFormat.PARQUET,
            bucket=s3,
            s3_prefix='plays/'
        )

        ingest = PythonFunction(
            self,
            'ingest',
            entry='../ingest',
            index='ingest.py',
            handler='lambda_handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            timeout=cdk.Duration.seconds(30)
        )

        ingest.add_environment("WATERMARK_TABLE_NAME", watermark_table.table_name)
        ingest.add_environment("CACHE_TABLE_NAME", cache_table.table_name)
        ingest.add_environment("BUCKET_NAME", s3.bucket_name)
        ingest.add_environment("SPOTIPY_CLIENT_ID", spotipy_client_id)
        ingest.add_environment("SPOTIPY_CLIENT_SECRET", spotipy_client_secret)
        ingest.add_environment("SPOTIPY_REDIRECT_URI", spotipy_redirect_uri)

        s3.grant_read_write(ingest)
        watermark_table.grant_write_data(ingest)
        cache_table.grant_write_data(ingest)
        watermark_table.grant_read_data(ingest)
        cache_table.grant_read_data(ingest)

        transform = _lambda.DockerImageFunction(
            self,
            'transform2',
            code=_lambda.DockerImageCode.from_image_asset("../transform"),
            timeout=cdk.Duration.seconds(60)
        )

        transform.role.add_managed_policy(
            policy=iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole')
        )
        transform.add_environment("GLUE_TABLE_NAME", glue_table.table_name)
        transform.add_environment("GLUE_DB_NAME", glue_db.database_name)
        s3.grant_read_write(transform)

        notification = aws_s3_notifications.LambdaDestination(transform)
        s3.add_event_notification(
            _s3.EventType.OBJECT_CREATED, 
            notification,
            _s3.NotificationKeyFilter(prefix='landing')
        )

        rule = events.Rule(
            self, "ingest-rule",
            schedule=events.Schedule.cron(
                minute='0',
                hour='*',
                day='*',
                month='*',
                year='*'),
        )
        rule.add_target(targets.LambdaFunction(ingest))

        athena_workgroup = athena.CfnWorkGroup(
            self,
            'hotspotworkgroup',
            name='hotspotworkgroup'
        )
        hotspot_user_policy = iam.Policy(
            self,
            'hotspot_user_policy',
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "glue:GetDatabase",
                        "s3:GetObject",
                        "s3:getBucketLocation",
                        "athena:StartQueryExecution",
                        "athena:StopQueryExecution",
                        "athena:GetQueryExecution",
                        "athena:GetQueryResults",
                        "athena:GetWorkGroup",
                        "glue:GetPartition",
                        "glue:GetPartitions",
                        "s3:ListBucket",
                        "glue:GetTable"
                    ],
                    resources=[
                        f"{s3.bucket_arn}",
                        f"{s3.bucket_arn}/{glue_table.table_name}/*",
                        f"{glue_table.table_arn}",
                        f"{glue_db.database_arn}",
                        f"{glue_db.catalog_arn}",
                        f"arn:aws:athena:{self.region}:{self.account}:workgroup/{athena_workgroup.name}"
                    ]
                ),
                iam.PolicyStatement(
                    actions=[
                        "s3:PutObject",
                        "s3:GetObject"
                    ],
                    resources=[
                        f"{s3.bucket_arn}/athena-query-results/*"
                    ]
                )
            ]
        )

        hotspot_user = iam.User(self, 'hotspot')
        hotspot_user.attach_inline_policy(hotspot_user_policy)
        hotspot_user_access_key = iam.CfnAccessKey(
            self,
            'hotspot_user_access_key',
            user_name=hotspot_user.user_name
        )
