# Land relational databases to a data lake at scale using AWS Glue blueprints

Here you can find the companion source code for the AWS Big Data Blog: [Land data from databases to a data lake at scale using AWS Glue blueprints of the solution](https://aws.amazon.com/blogs/big-data/land-data-from-databases-to-a-data-lake-at-scale-using-aws-glue-blueprints/).

**Note**: *The sample code; software libraries; command line tools; proofs of concept; templates; or other related technology (including any of the foregoing that are provided by our personnel) is provided to you as AWS Content under the AWS Customer Agreement, or the relevant written agreement between you and AWS (whichever applies). You should not use this AWS Content in your production accounts, or on production or other critical data. You are responsible for testing, securing, and optimizing the AWS Content, such as sample code, as appropriate for production grade use based on your specific quality control practices and standards. Deploying AWS Content may incur AWS charges for creating or using AWS chargeable resources, such as running Amazon EC2 instances or using Amazon S3 storage.*

## Introduction

To build a data lake on AWS, a common [data ingestion pattern](https://docs.aws.amazon.com/whitepapers/latest/aws-cloud-data-ingestion-patterns-practices/heterogeneous-data-ingestion-patterns.html) is to use [AWS Glue](https://aws.amazon.com/glue/) to perform extract,
transform, and load (ETL) jobs from relational databases to [Amazon Simple Storage Service](https://aws.amazon.com/s3/) (Amazon S3).
A project often involves extracting hundreds of tables from source databases to the [data lake raw layer](https://docs.aws.amazon.com/prescriptive-guidance/latest/defining-bucket-names-data-lakes/data-layer-definitions.html).
And for each source table, it’s recommended to have a separate AWS Glue job to simplify operations,
state management, and error handling. This approach works perfectly with a small number of tables.
However, with hundreds of tables, this results in hundreds of ETL jobs, and managing AWS Glue jobs at
this scale may pose an operational challenge if you’re not yet ready to [deploy using a CI/CD pipeline](https://docs.aws.amazon.com/prescriptive-guidance/latest/patterns/deploy-an-aws-glue-job-with-an-aws-codepipeline-ci-cd-pipeline.html).
Instead, we tackle this issue by decoupling the following:

- **ETL job logic** – We use an [AWS Glue blueprint](https://docs.aws.amazon.com/glue/latest/dg/blueprints-overview.html), which allows you to reuse one blueprint for all
jobs with the same logic
- **Job definition** – We use a JSON file, so you can define jobs programmatically without learning a
new language
- **Job deployment** – With [AWS Step Functions](https://aws.amazon.com/step-functions/?step-functions.sort-by=item.additionalFields.postDateTime&step-functions.sort-order=desc), you can copy workflows to manage different data
processing use cases on AWS Glue

In this [
](https://docs.aws.amazon.com/blogs/), you will learn how to handle data lake landing jobs deployment in a standardized way—by
maintaining a JSON file with table names and a few parameters (for example, a workflow catalog). AWS
Glue workflows are created and updated after manually running the resources deployment flow in Step
Functions. You can further [customize the AWS Glue blueprints](https://docs.aws.amazon.com/glue/latest/dg/orchestrate-using-blueprints.html) to make your own multi-step data
pipelines to move data to downstream layers and purpose-built analytics services (example use cases
include [partitioning](https://github.com/awslabs/aws-glue-blueprint-libs/tree/master/samples/partitioning) or [importing to an Amazon DynamoDB table](https://github.com/awslabs/aws-glue-blueprint-libs/tree/master/samples/s3_to_dynamodb).

## Code Structure
- **cloudformation-templates** : This folder contains the AWS CloudFormation templates used in the blog.
  - database_stack.yml: This CloudFormation stack works only in AWS Regions where Amazon Aurora Serverless v1 is supported. It is an optional step to create a database with sample data.
  - main_stack.yml: This CloudFormation stack works in all AWS Regions, and it creates the AWS Glue blueprints and required resources.
- **glue-blueprint**: This folder contains the AWS Glue blueprint code and configuration.
- **step-functions-workflow**: This folder contains the AWS Step Functions definition.
- **artifacts**: This folder contains the artifacts used for the deployment.

## Architecture Diagram

The following diagram illustrates the solution architecture, which contains two major areas:

- Resource deployment (components 1–2) – This is run manually on demand to deploy the
required resources for the data lake landing jobs (for example, new or updated job definitions or
job logic)
- ETL job runs (components 3–6) – The ETL jobs (one per source table) run on the defined
schedule, and extract and land data to the data lake raw layer
![Alt text](artifacts/BDB1902.jpg?raw=true "Title")

The solution workflow contains the following steps:

1. An S3 bucket stores an AWS Glue blueprint (ZIP) and the workflow catalog (JSON file).
2. A Step Functions workflow orchestrates the AWS Glue resources creation.
3. We use [Amazon Aurora](https://aws.amazon.com/rds/aurora/) as the data source with our sample data, but any PostgreSQL database
works with the provided script, or other [JDBC sources](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html) with customization.
4. [AWS Secrets Manager](https://aws.amazon.com/secrets-manager/) stores the secrets of the source databases.
5. On the predefined schedule, AWS Glue triggers initiate relevant AWS Glue jobs to perform ETL.
6. An S3 bucket serves as the data lake raw layer.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
