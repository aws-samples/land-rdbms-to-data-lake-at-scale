import boto3
from botocore.client import ClientError
from awsglue.blueprint.workflow import Workflow, Entities
from awsglue.blueprint.job import Job
from awsglue.blueprint.crawler import *
from logging import getLogger, StreamHandler, INFO

logger = getLogger(__name__)
handler = StreamHandler()
handler.setLevel(INFO)
logger.setLevel(INFO)
logger.addHandler(handler)
logger.propagate = False


def create_s3_bucket_if_needed(s3_client, bucket_name, region):
    """
    Create S3 bucket if not exists
    """
    location = {'LocationConstraint': region}
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"S3 Bucket already exists: {bucket_name}")
    except ClientError as ce1:
        if ce1.response['Error']['Code'] == "404":  # bucket not found
            logger.info(f"Creating S3 bucket: {bucket_name}")
            try:
                if region == "us-east-1":
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
                logger.info(f"Created S3 bucket: {bucket_name}")
            except ClientError as ce2:
                logger.error(f"Unexpected error occurred when creating S3 bucket: {bucket_name}, exception: {ce2}")
                raise
        else:
            logger.error(f"Unexpected error occurred when heading S3 bucket: {bucket_name}, exception: {ce1}")
            raise


def generate_layout(user_params, system_params):
    source_table_without_dot = user_params['SourceTable'].replace(".", "_")

    file_name = f"jdbc_to_s3_{source_table_without_dot}.py"

    session = boto3.Session(region_name=system_params['region'])
    glue = session.client('glue')
    s3_client = session.client('s3')

    workflow_name = user_params['WorkflowName']

    # Creating script bucket
    the_script_bucket = f"aws-glue-scripts-{system_params['accountId']}-{system_params['region']}"
    create_s3_bucket_if_needed(s3_client, the_script_bucket, system_params['region'])

    # Creating temp bucket
    the_temp_bucket = f"aws-glue-temporary-{system_params['accountId']}-{system_params['region']}"
    create_s3_bucket_if_needed(s3_client, the_temp_bucket, system_params['region'])
    the_temp_prefix = f"{workflow_name}/"
    the_temp_location = f"s3://{the_temp_bucket}/{the_temp_prefix}"

    # Upload job script to script bucket
    the_script_key = f"{workflow_name}/{file_name}"
    the_script_location = f"s3://{the_script_bucket}/{the_script_key}"
    with open("jdbc_to_s3.py", "rb") as f:
        s3_client.upload_fileobj(f, the_script_bucket, the_script_key)

    jobs = []
    crawlers = []
    # Glue command configuration
    command = {
        "Name": "glueetl",
        "ScriptLocation": the_script_location,
        "PythonVersion": "3"
    }
    # Glue job arguments
    arguments = {
        "--TempDir": the_temp_location,
        "--job-bookmark-option": "job-bookmark-disable",
        "--encryption-type": "sse-s3",
        "--job-language": "python",
        "--enable-glue-datacatalog ": "",
        "--enable-metrics": "",
        "--enable-continuous-cloudwatch-log": "true",
        "--user-jars-first": "true",
        "--secret_name": user_params['SecretName'],
        "--source_table_name": user_params['SourceTable'],
        "--destination_bucket": user_params['DestinationBucketName'],
        "--file_count": user_params['DestinationFileCount'],
        "--delta_col_name": user_params['DeltaColumnName'],
        "--output_table_partition_column": user_params['S3PartitionColumnName']
    }

    transform_job = Job(
        Name=f"{workflow_name}_jdbc_to_s3_{source_table_without_dot}",
        Command=command,
        Role=user_params['GlueExecutionRole'],
        DefaultArguments=arguments,
        Connections={
            "Connections": [
                user_params['NetworkConnectionName']
            ]
        },
        WorkerType="G.1X",
        NumberOfWorkers=user_params['NumberOfWorkers'],
        GlueVersion="3.0"
    )

    jobs.append(transform_job)
    
    # Check job schedule type. If `Cron` it will be schedule for running. Otherwise, it will be on-demand run.
    if user_params['JobScheduleType'] == "Cron":
        schedule_cron_workflow = Workflow(Name=workflow_name, Entities=Entities(Jobs=jobs, Crawlers=crawlers),
                                          Description=f"Blueprint Workflow for job {workflow_name}_jdbc_to_s3_{source_table_without_dot}",
                                          OnSchedule=f"cron({user_params['ScheduleCronPattern']})")
        return schedule_cron_workflow

    schedule_on_demand_workflow = Workflow(Name=workflow_name, Entities=Entities(Jobs=jobs, Crawlers=crawlers),
                                           Description=f"Blueprint Workflow for job {workflow_name}_jdbc_to_s3_{source_table_without_dot}")
    return schedule_on_demand_workflow
