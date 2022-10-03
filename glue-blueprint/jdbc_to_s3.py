import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import json
import boto3
import base64
from botocore.exceptions import ClientError
from pyspark.sql.functions import lit, current_timestamp, current_date, to_date

# Configure required parameters
params = [
    "JOB_NAME",
    "destination_bucket",
    "secret_name",
    "source_table_name",
    "delta_col_name"
]
# Configure optional parameters
is_output_file_count_provided = False
is_output_partition_column_provided = False
if '--file_count' in sys.argv:
    params.append('file_count')
    is_output_file_count_provided = True
if '--output_table_partition_column' in sys.argv:
    params.append('output_table_partition_column')

args = getResolvedOptions(sys.argv, params)


def folder_exists_and_not_empty(bucket: str, prefix: str) -> bool:
    """
    Check if a folder exists and is not empty
    """
    s3 = boto3.client("s3")
    # Append the trailing slash
    prefix if prefix.endswith("/") else prefix + "/"
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/", MaxKeys=1)
    return resp['KeyCount']


def get_secret():
    """
    Retrieve secret from Secrets Manager
    """
    secret_name = args["secret_name"]
    # Create a Secrets Manager client
    session = boto3.session.Session()
    # get region automatic using boto3
    client = session.client(
        service_name="secretsmanager", region_name=session.region_name
    )
    secret = ""
    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.

    except ClientError as e:
        if e.response["Error"]["Code"] == "DecryptionFailure":
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            logger.error("The requested secret can't be decrypted using the provided KMS key:", e)
            raise e
        elif e.response["Error"]["Code"] == "InternalServiceError":
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            logger.error("An error occurred on service side:", e)
            raise e
        elif e.response["Error"]["Code"] == "InvalidParameterException":
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            logger.error("The request had invalid params:", e)
            raise e
        elif e.response["Error"]["Code"] == "InvalidRequestException":
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            logger.error("The request was invalid due to:", e)
            raise e
        elif e.response["Error"]["Code"] == "ResourceNotFoundException":
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            logger.error("The requested secret " + secret_name + " was not found")
            raise e
    else:
        if "SecretString" in get_secret_value_response:
            secret = json.loads(get_secret_value_response["SecretString"])
        else:
            secret = json.loads(
                base64.b64decode(get_secret_value_response["SecretBinary"])
            )
    return secret


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
secret = get_secret()
logger = glueContext.get_logger()
FILE_FORMAT = "parquet"
delta_col_name = args['delta_col_name']
output_table_partition_column = ""
file_count = ""

if 'file_count' in args and args['file_count'] != "":
    file_count = args['file_count']
    is_output_partition_column_provided = True

if 'output_table_partition_column' in args:
    output_table_partition_column = args['output_table_partition_column'].lower()

# Database connection
jdbcUrl = "jdbc:postgresql://{0}:{1}/{2}".format(secret["host"], secret["port"], secret["dbname"])

sql_query = f"select * from {args['source_table_name']}"

# Check if the table has been ingested before
destination_prefix = f"{secret['dbname']}/{args['source_table_name']}/"
destination_location = f"s3://{args['destination_bucket']}/{destination_prefix}"
is_folder_exists_and_not_empty = folder_exists_and_not_empty(args["destination_bucket"], destination_prefix)

logger.info("check if the destination prefix is exists and not empty flag = " + str(is_folder_exists_and_not_empty))

# Rewrite the SQL query if this is not the first load
if is_folder_exists_and_not_empty:
    s3_table = spark.read.format(FILE_FORMAT).load(destination_location)
    if not s3_table.rdd.isEmpty():
        logger.info("Rewrite the SQL query if this is not the first load")
        max_datetime = s3_table.agg({delta_col_name: "max"}).collect()[0][0]
        sql_query += f" where {delta_col_name} > '{max_datetime}'"

# Read the source table into a dataframe
logger.info("Read the source table into a dataframe")
logger.info("sql_query: " + sql_query)

# if you want to parallelize the reads, specify partition related params (`partitionColumn, lowerBound, upperBound,
# and numPartitions`) explained in this link https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
jdbc_df = (spark.read.format("jdbc")
           .option("url", jdbcUrl)
           .option("user", secret["username"])
           .option("password", secret["password"])
           .option("query", sql_query)
           .load()
           )

# Add the date and timestamp columns
logger.info("Add the date and timestamp columns")
df_withdate = jdbc_df.withColumn("ingestion_timestamp", lit(current_timestamp()))

# check output partition column
is_partition_column_exists_in_df = output_table_partition_column in df_withdate.columns
is_partition_column_not_default = output_table_partition_column.strip() != ''

# Write the dataframe to S3
sink = df_withdate.coalesce(int(file_count)).write.mode("append").format(FILE_FORMAT) \
    if is_output_file_count_provided else df_withdate.write.mode("append").format(FILE_FORMAT)

if is_output_partition_column_provided and is_partition_column_not_default and is_partition_column_exists_in_df:
    logger.info("Partition the dataframe")
    sink = sink.partitionBy(output_table_partition_column)

logger.info("Write the dataframe to S3")
sink.save(destination_location)
