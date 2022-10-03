import json

output_file = 'workflow_config.json'
workflow_list = []
total_workflows = 3
sample_workflow = {
    "WorkflowName": "rds_to_s3_public_regions_11",
    "GlueExecutionRole": "arn:aws:iam::123456789012:role/AWSGlueServiceRole-data-lake-landing",
    "NetworkConnectionName": "rds-vpc",
    "SecretName": "DemoDBSecret",
    "SourceTable": "public.regions",
    "DestinationBucketName": "data-lake-raw-layer-123456789012-eu-west-1",
    "DestinationFileCount": "1",
    "delta_col_name": "updated_at",
    "S3PartitionColumnName": "",
    "JobScheduleType": "Cron",
    "ScheduleCronPattern": "10 21 * * ? *",
    "NumberOfWorkers": "2"
}

for i in range(total_workflows):
    workflow = sample_workflow
    workflow['WorkflowName'] = sample_workflow['WorkflowName'].rsplit('_', 1)[0] + f'_{i}'
    workflow_list.append(workflow.copy())

output = {
    "WorkflowList": workflow_list
}

with open(output_file, "w") as f:
    json.dump(output, f)
